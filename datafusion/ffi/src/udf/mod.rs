// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use abi_stable::std_types::{RHashMap, ROption};
use abi_stable::{
    std_types::{RResult, RString, RVec},
    StableAbi,
};
use arrow::datatypes::DataType;
use arrow::{
    array::ArrayRef,
    error::ArrowError,
    ffi::{from_ffi, to_ffi, FFI_ArrowSchema},
};
use datafusion::{
    error::DataFusionError,
    logical_expr::{
        type_coercion::functions::data_types_with_scalar_udf, ReturnInfo, ReturnTypeArgs,
    },
};
use datafusion::{
    error::Result,
    logical_expr::{
        ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    },
};
use return_info::FFI_ReturnInfo;
use return_type_args::{
    FFI_ReturnTypeArgs, ForeignReturnTypeArgs, ForeignReturnTypeArgsOwned,
};
use std::collections::HashMap;
use std::{ffi::c_void, sync::Arc};

use crate::{
    arrow_wrappers::{WrappedArray, WrappedSchema},
    df_result, rresult, rresult_return,
    util::{rvec_wrapped_to_vec_datatype, vec_datatype_to_rvec_wrapped},
    volatility::FFI_Volatility,
};

pub mod return_info;
pub mod return_type_args;

/// A stable struct for sharing a [`ScalarUDF`] across FFI boundaries.
#[repr(C)]
#[derive(Debug, StableAbi)]
#[allow(non_camel_case_types)]
pub struct FFI_ScalarUDF {
    /// FFI equivalent to the `name` of a [`ScalarUDF`]
    pub name: RString,

    /// FFI equivalent to the `aliases` of a [`ScalarUDF`]
    pub aliases: RVec<RString>,

    /// FFI equivalent to the `volatility` of a [`ScalarUDF`]
    pub volatility: FFI_Volatility,

    /// Determines the return type of the underlying [`ScalarUDF`] based on the
    /// argument types.
    pub return_type: unsafe extern "C" fn(
        udf: &Self,
        arg_types: RVec<WrappedSchema>,
    ) -> RResult<WrappedSchema, RString>,

    /// Determines the return info of the underlying [`ScalarUDF`]. Either this
    /// or return_type may be implemented on a UDF.
    pub return_type_from_args: unsafe extern "C" fn(
        udf: &Self,
        args: FFI_ReturnTypeArgs,
    )
        -> RResult<FFI_ReturnInfo, RString>,

    /// Execute the underlying [`ScalarUDF`] and return the result as a `FFI_ArrowArray`
    /// within an AbiStable wrapper.
    #[allow(clippy::type_complexity)]
    pub invoke_with_args: unsafe extern "C" fn(
        udf: &Self,
        args: RVec<WrappedArray>,
        arg_metadata: RVec<ROption<RHashMap<RString, RString>>>,
        num_rows: usize,
        return_type: WrappedSchema,
    ) -> RResult<WrappedArray, RString>,

    /// See [`ScalarUDFImpl`] for details on short_circuits
    pub short_circuits: bool,

    /// Performs type coersion. To simply this interface, all UDFs are treated as having
    /// user defined signatures, which will in turn call coerce_types to be called. This
    /// call should be transparent to most users as the internal function performs the
    /// appropriate calls on the underlying [`ScalarUDF`]
    pub coerce_types: unsafe extern "C" fn(
        udf: &Self,
        arg_types: RVec<WrappedSchema>,
    ) -> RResult<RVec<WrappedSchema>, RString>,

    /// Used to create a clone on the provider of the udf. This should
    /// only need to be called by the receiver of the udf.
    pub clone: unsafe extern "C" fn(udf: &Self) -> Self,

    /// Release the memory of the private data when it is no longer being used.
    pub release: unsafe extern "C" fn(udf: &mut Self),

    /// Internal data. This is only to be accessed by the provider of the udf.
    /// A [`ForeignScalarUDF`] should never attempt to access this data.
    pub private_data: *mut c_void,
}

unsafe impl Send for FFI_ScalarUDF {}
unsafe impl Sync for FFI_ScalarUDF {}

pub struct ScalarUDFPrivateData {
    pub udf: Arc<ScalarUDF>,
}

unsafe extern "C" fn return_type_fn_wrapper(
    udf: &FFI_ScalarUDF,
    arg_types: RVec<WrappedSchema>,
) -> RResult<WrappedSchema, RString> {
    let private_data = udf.private_data as *const ScalarUDFPrivateData;
    let udf = &(*private_data).udf;

    let arg_types = rresult_return!(rvec_wrapped_to_vec_datatype(&arg_types));

    let return_type = udf
        .return_type(&arg_types)
        .and_then(|v| FFI_ArrowSchema::try_from(v).map_err(DataFusionError::from))
        .map(WrappedSchema);

    rresult!(return_type)
}

unsafe extern "C" fn return_type_from_args_fn_wrapper(
    udf: &FFI_ScalarUDF,
    args: FFI_ReturnTypeArgs,
) -> RResult<FFI_ReturnInfo, RString> {
    let private_data = udf.private_data as *const ScalarUDFPrivateData;
    let udf = &(*private_data).udf;

    let args: ForeignReturnTypeArgsOwned = rresult_return!((&args).try_into());
    let args_ref: ForeignReturnTypeArgs = (&args).into();

    let return_type = udf
        .return_type_from_args((&args_ref).into())
        .and_then(FFI_ReturnInfo::try_from);

    rresult!(return_type)
}

unsafe extern "C" fn coerce_types_fn_wrapper(
    udf: &FFI_ScalarUDF,
    arg_types: RVec<WrappedSchema>,
) -> RResult<RVec<WrappedSchema>, RString> {
    let private_data = udf.private_data as *const ScalarUDFPrivateData;
    let udf = &(*private_data).udf;

    let arg_types = rresult_return!(rvec_wrapped_to_vec_datatype(&arg_types));

    let return_types = rresult_return!(data_types_with_scalar_udf(&arg_types, udf));

    rresult!(vec_datatype_to_rvec_wrapped(&return_types))
}

unsafe extern "C" fn invoke_with_args_fn_wrapper(
    udf: &FFI_ScalarUDF,
    args: RVec<WrappedArray>,
    arg_metadata: RVec<ROption<RHashMap<RString, RString>>>,
    number_rows: usize,
    return_type: WrappedSchema,
) -> RResult<WrappedArray, RString> {
    let private_data = udf.private_data as *const ScalarUDFPrivateData;
    let udf = &(*private_data).udf;

    let args = args
        .into_iter()
        .map(|arr| {
            from_ffi(arr.array, &arr.schema.0)
                .map(|v| ColumnarValue::Array(arrow::array::make_array(v)))
        })
        .collect::<std::result::Result<_, _>>();

    let args = rresult_return!(args);
    let return_type = rresult_return!(DataType::try_from(&return_type.0));

    let arg_metadata_owned: Vec<Option<HashMap<String, String>>> = arg_metadata
        .into_iter()
        .map(|maybe_map| {
            maybe_map
                .map(|hashmap| {
                    hashmap
                        .into_iter()
                        .map(|kv| (String::from(kv.0), String::from(kv.1)))
                        .collect::<HashMap<_, _>>()
                })
                .into()
        })
        .collect();
    let arg_metadata = arg_metadata_owned
        .iter()
        .map(|maybe_map| maybe_map.as_ref())
        .collect::<Vec<_>>();

    let args = ScalarFunctionArgs {
        args,
        arg_metadata,
        number_rows,
        return_type: &return_type,
    };

    let result = rresult_return!(udf
        .invoke_with_args(args)
        .and_then(|r| r.to_array(number_rows)));

    let (result_array, result_schema) = rresult_return!(to_ffi(&result.to_data()));

    RResult::ROk(WrappedArray {
        array: result_array,
        schema: WrappedSchema(result_schema),
    })
}

unsafe extern "C" fn release_fn_wrapper(udf: &mut FFI_ScalarUDF) {
    let private_data = Box::from_raw(udf.private_data as *mut ScalarUDFPrivateData);
    drop(private_data);
}

unsafe extern "C" fn clone_fn_wrapper(udf: &FFI_ScalarUDF) -> FFI_ScalarUDF {
    let private_data = udf.private_data as *const ScalarUDFPrivateData;
    let udf_data = &(*private_data);

    Arc::clone(&udf_data.udf).into()
}

impl Clone for FFI_ScalarUDF {
    fn clone(&self) -> Self {
        unsafe { (self.clone)(self) }
    }
}

impl From<Arc<ScalarUDF>> for FFI_ScalarUDF {
    fn from(udf: Arc<ScalarUDF>) -> Self {
        let name = udf.name().into();
        let aliases = udf.aliases().iter().map(|a| a.to_owned().into()).collect();
        let volatility = udf.signature().volatility.into();
        let short_circuits = udf.short_circuits();

        let private_data = Box::new(ScalarUDFPrivateData { udf });

        Self {
            name,
            aliases,
            volatility,
            short_circuits,
            invoke_with_args: invoke_with_args_fn_wrapper,
            return_type: return_type_fn_wrapper,
            return_type_from_args: return_type_from_args_fn_wrapper,
            coerce_types: coerce_types_fn_wrapper,
            clone: clone_fn_wrapper,
            release: release_fn_wrapper,
            private_data: Box::into_raw(private_data) as *mut c_void,
        }
    }
}

impl Drop for FFI_ScalarUDF {
    fn drop(&mut self) {
        unsafe { (self.release)(self) }
    }
}

/// This struct is used to access an UDF provided by a foreign
/// library across a FFI boundary.
///
/// The ForeignScalarUDF is to be used by the caller of the UDF, so it has
/// no knowledge or access to the private data. All interaction with the UDF
/// must occur through the functions defined in FFI_ScalarUDF.
#[derive(Debug)]
pub struct ForeignScalarUDF {
    name: String,
    aliases: Vec<String>,
    udf: FFI_ScalarUDF,
    signature: Signature,
}

unsafe impl Send for ForeignScalarUDF {}
unsafe impl Sync for ForeignScalarUDF {}

impl TryFrom<&FFI_ScalarUDF> for ForeignScalarUDF {
    type Error = DataFusionError;

    fn try_from(udf: &FFI_ScalarUDF) -> Result<Self, Self::Error> {
        let name = udf.name.to_owned().into();
        let signature = Signature::user_defined((&udf.volatility).into());

        let aliases = udf.aliases.iter().map(|s| s.to_string()).collect();

        Ok(Self {
            name,
            udf: udf.clone(),
            aliases,
            signature,
        })
    }
}

impl ScalarUDFImpl for ForeignScalarUDF {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let arg_types = vec_datatype_to_rvec_wrapped(arg_types)?;

        let result = unsafe { (self.udf.return_type)(&self.udf, arg_types) };

        let result = df_result!(result);

        result.and_then(|r| (&r.0).try_into().map_err(DataFusionError::from))
    }

    fn return_type_from_args(&self, args: ReturnTypeArgs) -> Result<ReturnInfo> {
        let args: FFI_ReturnTypeArgs = args.try_into()?;

        let result = unsafe { (self.udf.return_type_from_args)(&self.udf, args) };

        let result = df_result!(result);

        result.and_then(|r| r.try_into())
    }

    fn invoke_with_args(&self, invoke_args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs {
            args,
            arg_metadata,
            number_rows,
            return_type,
        } = invoke_args;

        let args = args
            .into_iter()
            .map(|v| v.to_array(number_rows))
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .map(|v| {
                to_ffi(&v.to_data()).map(|(ffi_array, ffi_schema)| WrappedArray {
                    array: ffi_array,
                    schema: WrappedSchema(ffi_schema),
                })
            })
            .collect::<std::result::Result<Vec<_>, ArrowError>>()?
            .into();

        let arg_metadata = arg_metadata
            .into_iter()
            .map(|maybe_map| {
                maybe_map
                    .map(|hashmap| {
                        hashmap
                            .iter()
                            .map(|(k, v)| {
                                (RString::from(k.clone()), RString::from(v.clone()))
                            })
                            .collect::<RHashMap<_, _>>()
                    })
                    .into()
            })
            .collect::<RVec<_>>();

        let return_type = WrappedSchema(FFI_ArrowSchema::try_from(return_type)?);

        let result = unsafe {
            (self.udf.invoke_with_args)(
                &self.udf,
                args,
                arg_metadata,
                number_rows,
                return_type,
            )
        };

        let result = df_result!(result)?;
        let result_array: ArrayRef = result.try_into()?;

        Ok(ColumnarValue::Array(result_array))
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn short_circuits(&self) -> bool {
        self.udf.short_circuits
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        unsafe {
            let arg_types = vec_datatype_to_rvec_wrapped(arg_types)?;
            let result_types = df_result!((self.udf.coerce_types)(&self.udf, arg_types))?;
            Ok(rvec_wrapped_to_vec_datatype(&result_types)?)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_round_trip_scalar_udf() -> Result<()> {
        let original_udf = datafusion::functions::math::abs::AbsFunc::new();
        let original_udf = Arc::new(ScalarUDF::from(original_udf));

        let local_udf: FFI_ScalarUDF = Arc::clone(&original_udf).into();

        let foreign_udf: ForeignScalarUDF = (&local_udf).try_into()?;

        assert_eq!(original_udf.name(), foreign_udf.name());

        Ok(())
    }
}

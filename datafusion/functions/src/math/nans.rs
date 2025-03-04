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

//! Math function: `isnan()`.

use arrow::datatypes::{DataType, Float32Type, Float64Type};
use datafusion_common::{exec_err, Result};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, TypeSignature};

use arrow::array::{ArrayRef, AsArray, BooleanArray};
use datafusion_expr::{Documentation, ScalarUDFImpl, Signature, Volatility};
use datafusion_macros::user_doc;
use std::any::Any;
use std::sync::Arc;

#[user_doc(
    doc_section(label = "Math Functions"),
    description = "Returns true if a given number is +NaN or -NaN otherwise returns false.",
    syntax_example = "isnan(numeric_expression)",
    standard_argument(name = "numeric_expression", prefix = "Numeric")
)]
#[derive(Debug)]
pub struct IsNanFunc {
    signature: Signature,
}

impl Default for IsNanFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl IsNanFunc {
    pub fn new() -> Self {
        use DataType::*;
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![Float32]),
                    TypeSignature::Exact(vec![Float64]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for IsNanFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "isnan"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let args = ColumnarValue::values_to_arrays(&args.args)?;

        let arr: ArrayRef = match args[0].data_type() {
            DataType::Float64 => Arc::new(BooleanArray::from_unary(
                args[0].as_primitive::<Float64Type>(),
                f64::is_nan,
            )) as ArrayRef,

            DataType::Float32 => Arc::new(BooleanArray::from_unary(
                args[0].as_primitive::<Float32Type>(),
                f32::is_nan,
            )) as ArrayRef,
            other => {
                return exec_err!(
                    "Unsupported data type {other:?} for function {}",
                    self.name()
                )
            }
        };
        Ok(ColumnarValue::Array(arr))
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

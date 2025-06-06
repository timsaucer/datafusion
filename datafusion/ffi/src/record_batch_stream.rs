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

use std::{ffi::c_void, task::Poll};

use abi_stable::{
    std_types::{ROption, RResult, RString},
    StableAbi,
};
use arrow::array::{Array, RecordBatch};
use arrow::{
    array::{make_array, StructArray},
    ffi::{from_ffi, to_ffi},
};
use async_ffi::{ContextExt, FfiContext, FfiPoll};
use datafusion::error::Result;
use datafusion::{
    error::DataFusionError,
    execution::{RecordBatchStream, SendableRecordBatchStream},
};
use futures::{Stream, TryStreamExt};
use tokio::runtime::Handle;

use crate::{
    arrow_wrappers::{WrappedArray, WrappedSchema},
    rresult,
};

/// A stable struct for sharing [`RecordBatchStream`] across FFI boundaries.
/// We use the async-ffi crate for handling async calls across libraries.
#[repr(C)]
#[derive(Debug, StableAbi)]
#[allow(non_camel_case_types)]
pub struct FFI_RecordBatchStream {
    /// This mirrors the `poll_next` of [`RecordBatchStream`] but does so
    /// in a FFI safe manner.
    pub poll_next:
        unsafe extern "C" fn(
            stream: &Self,
            cx: &mut FfiContext,
        ) -> FfiPoll<ROption<RResult<WrappedArray, RString>>>,

    /// Return the schema of the record batch
    pub schema: unsafe extern "C" fn(stream: &Self) -> WrappedSchema,

    /// Internal data. This is only to be accessed by the provider of the plan.
    /// The foreign library should never attempt to access this data.
    pub private_data: *mut c_void,
}

pub struct RecordBatchStreamPrivateData {
    pub rbs: SendableRecordBatchStream,
    pub runtime: Option<Handle>,
}

impl From<SendableRecordBatchStream> for FFI_RecordBatchStream {
    fn from(stream: SendableRecordBatchStream) -> Self {
        Self::new(stream, None)
    }
}

impl FFI_RecordBatchStream {
    pub fn new(stream: SendableRecordBatchStream, runtime: Option<Handle>) -> Self {
        let private_data = Box::into_raw(Box::new(RecordBatchStreamPrivateData {
            rbs: stream,
            runtime,
        })) as *mut c_void;
        FFI_RecordBatchStream {
            poll_next: poll_next_fn_wrapper,
            schema: schema_fn_wrapper,
            private_data,
        }
    }
}

unsafe impl Send for FFI_RecordBatchStream {}

unsafe extern "C" fn schema_fn_wrapper(stream: &FFI_RecordBatchStream) -> WrappedSchema {
    let private_data = stream.private_data as *const RecordBatchStreamPrivateData;
    let stream = &(*private_data).rbs;

    (*stream).schema().into()
}

fn record_batch_to_wrapped_array(
    record_batch: RecordBatch,
) -> RResult<WrappedArray, RString> {
    let struct_array = StructArray::from(record_batch);
    rresult!(
        to_ffi(&struct_array.to_data()).map(|(array, schema)| WrappedArray {
            array,
            schema: WrappedSchema(schema)
        })
    )
}

// probably want to use pub unsafe fn from_ffi(array: FFI_ArrowArray, schema: &FFI_ArrowSchema) -> Result<ArrayData> {
fn maybe_record_batch_to_wrapped_stream(
    record_batch: Option<Result<RecordBatch>>,
) -> ROption<RResult<WrappedArray, RString>> {
    match record_batch {
        Some(Ok(record_batch)) => {
            ROption::RSome(record_batch_to_wrapped_array(record_batch))
        }
        Some(Err(e)) => ROption::RSome(RResult::RErr(e.to_string().into())),
        None => ROption::RNone,
    }
}

unsafe extern "C" fn poll_next_fn_wrapper(
    stream: &FFI_RecordBatchStream,
    cx: &mut FfiContext,
) -> FfiPoll<ROption<RResult<WrappedArray, RString>>> {
    let private_data = stream.private_data as *mut RecordBatchStreamPrivateData;
    let stream = &mut (*private_data).rbs;

    let _guard = (*private_data).runtime.as_ref().map(|rt| rt.enter());

    let poll_result = cx.with_context(|std_cx| {
        (*stream)
            .try_poll_next_unpin(std_cx)
            .map(maybe_record_batch_to_wrapped_stream)
    });

    poll_result.into()
}

impl RecordBatchStream for FFI_RecordBatchStream {
    fn schema(&self) -> arrow::datatypes::SchemaRef {
        let wrapped_schema = unsafe { (self.schema)(self) };
        wrapped_schema.into()
    }
}

fn wrapped_array_to_record_batch(array: WrappedArray) -> Result<RecordBatch> {
    let array_data =
        unsafe { from_ffi(array.array, &array.schema.0).map_err(DataFusionError::from)? };
    let array = make_array(array_data);
    let struct_array = array
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or(DataFusionError::Execution(
        "Unexpected array type during record batch collection in FFI_RecordBatchStream"
            .to_string(),
    ))?;

    Ok(struct_array.into())
}

fn maybe_wrapped_array_to_record_batch(
    array: ROption<RResult<WrappedArray, RString>>,
) -> Option<Result<RecordBatch>> {
    match array {
        ROption::RSome(RResult::ROk(wrapped_array)) => {
            Some(wrapped_array_to_record_batch(wrapped_array))
        }
        ROption::RSome(RResult::RErr(e)) => {
            Some(Err(DataFusionError::Execution(e.to_string())))
        }
        ROption::RNone => None,
    }
}

impl Stream for FFI_RecordBatchStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let poll_result =
            unsafe { cx.with_ffi_context(|ffi_cx| (self.poll_next)(&self, ffi_cx)) };

        match poll_result {
            FfiPoll::Ready(array) => {
                Poll::Ready(maybe_wrapped_array_to_record_batch(array))
            }
            FfiPoll::Pending => Poll::Pending,
            FfiPoll::Panicked => Poll::Ready(Some(Err(DataFusionError::Execution(
                "Error occurred during poll_next on FFI_RecordBatchStream".to_string(),
            )))),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::{
        common::record_batch, error::Result, execution::SendableRecordBatchStream,
        test_util::bounded_stream,
    };

    use super::FFI_RecordBatchStream;
    use futures::StreamExt;

    #[tokio::test]
    async fn test_round_trip_record_batch_stream() -> Result<()> {
        let record_batch = record_batch!(
            ("a", Int32, vec![1, 2, 3]),
            ("b", Float64, vec![Some(4.0), None, Some(5.0)])
        )?;
        let original_rbs = bounded_stream(record_batch.clone(), 1);

        let ffi_rbs: FFI_RecordBatchStream = original_rbs.into();
        let mut ffi_rbs: SendableRecordBatchStream = Box::pin(ffi_rbs);

        let schema = ffi_rbs.schema();
        assert_eq!(
            schema,
            Arc::new(Schema::new(vec![
                Field::new("a", DataType::Int32, true),
                Field::new("b", DataType::Float64, true)
            ]))
        );

        let batch = ffi_rbs.next().await;
        assert!(batch.is_some());
        assert!(batch.as_ref().unwrap().is_ok());
        assert_eq!(batch.unwrap().unwrap(), record_batch);

        // There should only be one batch
        let no_batch = ffi_rbs.next().await;
        assert!(no_batch.is_none());

        Ok(())
    }
}

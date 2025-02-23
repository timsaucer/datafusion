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

use crate::{udaf::FFI_AggregateUDF, udf::FFI_ScalarUDF};
use datafusion::{
    functions::math::abs::AbsFunc,
    functions_aggregate::{stddev::Stddev, sum::Sum},
    logical_expr::{AggregateUDF, ScalarUDF},
};

use std::sync::Arc;

pub(crate) extern "C" fn create_ffi_abs_func() -> FFI_ScalarUDF {
    let udf: Arc<ScalarUDF> = Arc::new(AbsFunc::new().into());

    udf.into()
}

pub(crate) extern "C" fn create_ffi_avg_func() -> FFI_AggregateUDF {
    let udaf: Arc<AggregateUDF> = Arc::new(Sum::new().into());

    udaf.into()
}

pub(crate) extern "C" fn create_ffi_stddev_func() -> FFI_AggregateUDF {
    let udaf: Arc<AggregateUDF> = Arc::new(Stddev::new().into());

    udaf.into()
}

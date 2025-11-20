use std::sync::Arc;

use datafusion::prelude::SessionContext;
use datafusion_execution::TaskContextProvider;
use datafusion_ffi::execution::FFI_TaskContextProvider;

pub(crate) fn test_session_and_ctx() -> (Arc<SessionContext>, FFI_TaskContextProvider) {
    let ctx = Arc::new(SessionContext::new());
    let task_ctx_provider = Arc::clone(&ctx) as Arc<dyn TaskContextProvider>;
    let task_ctx_provider = FFI_TaskContextProvider::from(&task_ctx_provider);

    (ctx, task_ctx_provider)
}

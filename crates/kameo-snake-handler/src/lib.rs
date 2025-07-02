pub mod serde_py;
pub use serde_py::{from_pyobject, to_pyobject, FromPyAny};

mod error;
pub use error::ErrorReply;
pub use kameo_child_process::error::PythonExecutionError;

mod builder;
pub use builder::PythonChildProcessBuilder;

pub use kameo_child_process::NoopCallbackHandler;

mod actor;
pub use actor::{child_process_main_with_python_actor, PythonActor, PythonConfig};

mod macros;

pub mod telemetry;

pub use crate::actor::PythonMessageHandler;

#[tracing::instrument(skip(builder), name = "setup_python_runtime")]
pub fn setup_python_runtime(builder: tokio::runtime::Builder) {
    pyo3::prepare_freethreaded_python();
    pyo3_async_runtimes::tokio::init(builder);
}

pub mod prelude {
    pub use super::{
        setup_python_runtime, PythonActor, PythonChildProcessBuilder, PythonConfig,
        PythonExecutionError,
    };
}

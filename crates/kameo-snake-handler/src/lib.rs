

pub mod serde_py;
pub use serde_py::{from_pyobject, to_pyobject, FromPyAny};

mod error;
pub use error::{PythonExecutionError, ErrorReply};

mod builder;
pub use builder::PythonChildProcessBuilder;

mod callback;
pub use callback::NoopCallbackHandler;

mod actor;
pub use actor::{PythonActor, PythonConfig, child_process_main_with_python_actor};

mod macros;

#[tracing::instrument(skip(builder), name = "setup_python_runtime")]
pub fn setup_python_runtime(builder: tokio::runtime::Builder) {
    pyo3::prepare_freethreaded_python();
    pyo3_async_runtimes::tokio::init(builder);
}

pub mod prelude {
    pub use super::{
        PythonActor, PythonChildProcessBuilder, PythonConfig, PythonExecutionError, setup_python_runtime
    };
}


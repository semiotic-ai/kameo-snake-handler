// PythonExecutionError and error handling traits for kameo-snake-handler
use kameo_child_process::error::PythonExecutionError;

/// Trait for creating a reply from a Python execution error
pub trait ErrorReply: Sized {
    fn from_error(err: PythonExecutionError) -> Self;
}

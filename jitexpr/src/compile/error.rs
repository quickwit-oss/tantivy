use crate::ast::{Function, InvalidFunctionCall, TypeError};
use crate::types::VarType;

#[derive(Debug, thiserror::Error)]
pub enum CompileError {
    #[error("type inference failed: {0}")]
    TypeInference(#[from] TypeError),
    #[error("JIT compilation failed: {0}")]
    Module(#[source] Box<cranelift_module::ModuleError>),
    #[error("cannot coerce an expression from {from_type:?} to {target:?}")]
    UnsupportedCoercion { from_type: VarType, target: VarType },
    #[error("cannot compile {function:?} with result type {return_type:?}")]
    UnsupportedFunctionType {
        function: Function,
        return_type: VarType,
    },
    #[error("invalid regular expression `{pattern}`: {source}")]
    InvalidRegex {
        pattern: String,
        #[source]
        source: regex::Error,
    },
    #[error("arguments do not match the function {0}")]
    InvalidArguments(#[from] InvalidFunctionCall),
}

impl From<cranelift_module::ModuleError> for CompileError {
    fn from(error: cranelift_module::ModuleError) -> Self {
        CompileError::Module(Box::new(error))
    }
}

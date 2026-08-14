//! Source types and nullable runtime value representations.

/// A value type supported by compiled expressions.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Ord, PartialOrd)]
pub enum VarType {
    Bool,
    F64,
    U64,
    I64,
    Str,
    None, // TODO: add other types.
}

/// A borrowed UTF-8 string descriptor passed opaquely through generated code.
///
/// The pointer can either refer to:
/// - an input str, if it is representing an arg
/// or if it is a return value that is a slice of an input str (e.g. a regex group).
/// - a literal from the original expression
/// - the arena passed to the function if the function "constructs"  a new string
/// (e.g. when calling uppercase).
///
/// Either way, its lifetime / ownership is controlled by the called of the function.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct StringRef {
    data: *const u8,
    len: usize,
}

impl StringRef {
    pub fn new(value: &str) -> Self {
        Self {
            data: value.as_ptr(),
            len: value.len(),
        }
    }

    pub const fn len(&self) -> usize {
        self.len
    }

    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Read the referenced UTF-8 string.
    ///
    /// # Safety
    ///
    /// The bytes used to construct this descriptor must still be alive and
    /// unchanged.
    pub unsafe fn as_str(&self) -> &str {
        // SAFETY: Guaranteed by the caller. StringRef can only be constructed
        // safely from a valid str, so the bytes are UTF-8 when still alive.
        unsafe { std::str::from_utf8_unchecked(std::slice::from_raw_parts(self.data, self.len)) }
    }
}

/// The source-level signature of a compiled expression.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Signature {
    pub args: Vec<VarType>,
    pub result: VarType,
}

/// The payload of a nullable argument or result slot.
///
/// This union is deliberately untagged. [`Variable`] carries nullness, while
/// the corresponding [`Signature`] or [`crate::NamedInput`] identifies the
/// active payload field.
#[repr(C)]
#[derive(Clone, Copy)]
pub union VariableValue {
    pub boolean: bool,
    pub float: f64,
    pub int_u64: u64,
    pub int_i64: i64,
    pub string: *mut StringRef, //< this has to be mut for results.
}

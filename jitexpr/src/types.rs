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
/// This is a transparent wrapper around Rust's `&str` fat pointer, which
/// carries both the address and byte length.
///
/// The pointer can either refer to:
/// - an input str, if it is representing an arg or if it is a return value that is a slice of an
///   input str (e.g. a regex group).
/// - a literal from the original expression
/// - the arena passed to the function if the function "constructs"  a new string (e.g. when calling
///   uppercase).
///
/// Either way, its lifetime / ownership is controlled by the caller of the function.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StringRef<'a> {
    value: &'a str,
}

impl<'a> StringRef<'a> {
    pub fn new(value: &'a str) -> Self {
        Self { value }
    }

    pub fn len(&self) -> usize {
        self.value.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn as_str(&self) -> &'a str {
        self.value
    }
}

/// The payload of a nullable argument or result slot.
///
/// This union is deliberately untagged. The corresponding
/// [`crate::compile::TypedVariable`] identifies the active payload field.
#[repr(C)]
#[derive(Clone, Copy)]
pub union VariableValue<'a> {
    pub boolean: bool,
    pub float: f64,
    pub int_u64: u64,
    pub int_i64: i64,
    pub string: *mut StringRef<'a>, //< this has to be mut for results.
}

impl<'a> From<bool> for VariableValue<'a> {
    fn from(value: bool) -> Self {
        VariableValue { boolean: value }
    }
}

impl<'a> From<f64> for VariableValue<'a> {
    fn from(value: f64) -> Self {
        VariableValue { float: value }
    }
}

impl<'a> From<u64> for VariableValue<'a> {
    fn from(value: u64) -> Self {
        VariableValue { int_u64: value }
    }
}

impl<'a> From<i64> for VariableValue<'a> {
    fn from(value: i64) -> Self {
        VariableValue { int_i64: value }
    }
}

impl<'value, 'string> From<&'value mut StringRef<'string>> for VariableValue<'value>
where 'string: 'value
{
    fn from(value: &'value mut StringRef<'string>) -> Self {
        // `StringRef` is covariant in its backing string lifetime, so it is
        // valid to shorten `'string` to `'value`. The raw mutable pointer is
        // invariant, however, and therefore needs an explicit cast.
        VariableValue {
            string: (value as *mut StringRef<'string>).cast::<StringRef<'value>>(),
        }
    }
}

impl<'a> Default for VariableValue<'a> {
    fn default() -> VariableValue<'a> {
        VariableValue { int_u64: 0u64 }
    }
}

/// A nullable value passed to or returned from a compiled expression.
///
/// `value` contains the payload described by the corresponding [`VarType`].
/// The payload is only meaningful when `is_present` is true.
#[repr(C)]
#[derive(Clone, Copy, Default)]
pub struct VariableOpt<'a> {
    pub value: VariableValue<'a>,
    pub is_present: bool,
}

impl<'a> VariableOpt<'a> {
    /// Wraps a present runtime value.
    pub fn some(value: impl Into<VariableValue<'a>>) -> Self {
        Self {
            value: value.into(),
            is_present: true,
        }
    }

    /// Creates an absent runtime value.
    pub fn none() -> Self {
        Self::default()
    }

    /// Returns the boolean payload, or `None` when this value is absent.
    ///
    /// # Safety
    ///
    /// When present, the active [`VariableValue`] member must be `boolean`.
    pub unsafe fn as_bool(self) -> Option<bool> {
        if self.is_present {
            // SAFETY: Guaranteed by the caller.
            Some(unsafe { self.value.boolean })
        } else {
            None
        }
    }

    /// Returns the `f64` payload, or `None` when this value is absent.
    ///
    /// # Safety
    ///
    /// When present, the active [`VariableValue`] member must be `float`.
    pub unsafe fn as_f64(self) -> Option<f64> {
        if self.is_present {
            // SAFETY: Guaranteed by the caller.
            Some(unsafe { self.value.float })
        } else {
            None
        }
    }

    /// Returns the `u64` payload, or `None` when this value is absent.
    ///
    /// # Safety
    ///
    /// When present, the active [`VariableValue`] member must be `int_u64`.
    pub unsafe fn as_u64(self) -> Option<u64> {
        if self.is_present {
            // SAFETY: Guaranteed by the caller.
            Some(unsafe { self.value.int_u64 })
        } else {
            None
        }
    }

    /// Returns the `i64` payload, or `None` when this value is absent.
    ///
    /// # Safety
    ///
    /// When present, the active [`VariableValue`] member must be `int_i64`.
    pub unsafe fn as_i64(self) -> Option<i64> {
        if self.is_present {
            // SAFETY: Guaranteed by the caller.
            Some(unsafe { self.value.int_i64 })
        } else {
            None
        }
    }

    /// Returns the string payload, or `None` when this value is absent.
    ///
    /// # Safety
    ///
    /// When present, the active [`VariableValue`] member must be `string` and
    /// point to a live [`StringRef`]. Its backing string must remain valid for
    /// the returned reference's lifetime.
    pub unsafe fn as_str(self) -> Option<&'a str> {
        if self.is_present {
            // SAFETY: Guaranteed by the caller.
            let string_ref: &'a StringRef = unsafe { &*self.value.string };
            Some(string_ref.as_str())
        } else {
            None
        }
    }
}

impl<'a, T: Into<VariableValue<'a>>> From<T> for VariableOpt<'a> {
    fn from(value: T) -> Self {
        Self::some(value.into())
    }
}

#[cfg(test)]
mod tests {
    use crate::types::{StringRef, VariableOpt, VariableValue};

    #[test]
    fn test_string_ref_wraps_raw_str_pointer() {
        let string_ref = StringRef::new("hello");
        assert_eq!(std::mem::size_of::<StringRef>(), 16);
        assert_eq!(string_ref.len(), 5);
        assert!(!string_ref.is_empty());
        assert_eq!(string_ref.as_str(), "hello");
    }

    #[test]
    fn test_variable_value_size() {
        assert_eq!(std::mem::size_of::<VariableValue>(), 8);
    }

    #[test]
    fn test_variable_opt_layout() {
        assert_eq!(std::mem::offset_of!(VariableOpt, value), 0);
        assert_eq!(std::mem::offset_of!(VariableOpt, is_present), 8);
        assert_eq!(std::mem::size_of::<VariableOpt>(), 16);
    }

    #[test]
    fn test_variable_opt_constructors() {
        let present = VariableOpt::some(3u64);
        assert_eq!(unsafe { present.as_u64() }, Some(3));
        assert_eq!(unsafe { VariableOpt::none().as_u64() }, None);
    }

    #[test]
    fn test_variable_opt_accessors() {
        assert_eq!(unsafe { VariableOpt::some(true).as_bool() }, Some(true));
        assert_eq!(unsafe { VariableOpt::some(1.5f64).as_f64() }, Some(1.5));
        assert_eq!(unsafe { VariableOpt::some(7u64).as_u64() }, Some(7));
        assert_eq!(unsafe { VariableOpt::some(-3i64).as_i64() }, Some(-3));

        let mut string_ref = StringRef::new("hello");
        assert_eq!(
            unsafe { VariableOpt::some(&mut string_ref).as_str() },
            Some("hello")
        );

        let none = VariableOpt::none();
        assert_eq!(unsafe { none.as_bool() }, None);
        assert_eq!(unsafe { none.as_f64() }, None);
        assert_eq!(unsafe { none.as_u64() }, None);
        assert_eq!(unsafe { none.as_i64() }, None);
        assert_eq!(unsafe { none.as_str() }, None);
    }
}

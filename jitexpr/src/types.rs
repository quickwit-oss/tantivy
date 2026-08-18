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

/// The payload of a primitive runtime value.
///
/// This union is deliberately untagged. The corresponding
/// [`crate::compile::TypedVariable`] identifies the active payload field.
#[repr(C)]
#[derive(Clone, Copy)]
pub union VariablePrimitive {
    pub boolean: bool,
    pub float: f64,
    pub int_u64: u64,
    pub int_i64: i64,
}

impl From<bool> for VariablePrimitive {
    fn from(value: bool) -> Self {
        VariablePrimitive { boolean: value }
    }
}

impl From<f64> for VariablePrimitive {
    fn from(value: f64) -> Self {
        VariablePrimitive { float: value }
    }
}

impl From<u64> for VariablePrimitive {
    fn from(value: u64) -> Self {
        VariablePrimitive { int_u64: value }
    }
}

impl From<i64> for VariablePrimitive {
    fn from(value: i64) -> Self {
        VariablePrimitive { int_i64: value }
    }
}

impl Default for VariablePrimitive {
    fn default() -> Self {
        VariablePrimitive { int_u64: 0 }
    }
}

/// A nullable primitive value.
///
/// `value` is meaningful only when `is_present` is true.
#[repr(C)]
#[derive(Clone, Copy, Default)]
pub struct VariablePrimitiveOpt {
    pub value: VariablePrimitive,
    pub is_present: bool,
}

impl VariablePrimitiveOpt {
    /// Wraps a present primitive value.
    pub fn some(value: impl Into<VariablePrimitive>) -> Self {
        Self {
            value: value.into(),
            is_present: true,
        }
    }

    /// Creates an absent primitive value.
    pub fn none() -> Self {
        Self::default()
    }

    /// Returns the boolean payload, or `None` when this value is absent.
    ///
    /// # Safety
    ///
    /// When present, the active [`VariablePrimitive`] member must be `boolean`.
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
    /// When present, the active [`VariablePrimitive`] member must be `float`.
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
    /// When present, the active [`VariablePrimitive`] member must be `int_u64`.
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
    /// When present, the active [`VariablePrimitive`] member must be `int_i64`.
    pub unsafe fn as_i64(self) -> Option<i64> {
        if self.is_present {
            // SAFETY: Guaranteed by the caller.
            Some(unsafe { self.value.int_i64 })
        } else {
            None
        }
    }
}

impl<T: Into<VariablePrimitive>> From<T> for VariablePrimitiveOpt {
    fn from(value: T) -> Self {
        Self::some(value)
    }
}

/// A nullable runtime argument or result slot.
///
/// Primitive values use the [`VariablePrimitiveOpt`] arm. Strings use the
/// nullable `string` arm: a null data pointer represents `None`, while a
/// non-null data pointer and its byte length represent a borrowed `str`.
/// Both arms occupy two machine words on the supported 64-bit targets.
#[repr(C)]
#[derive(Clone, Copy)]
pub union VariableValue<'a> {
    pub primitive: VariablePrimitiveOpt,
    pub string: Option<&'a str>,
}

const _: () = {
    assert!(std::mem::size_of::<VariablePrimitive>() == 8);
    assert!(std::mem::offset_of!(VariablePrimitiveOpt, value) == 0);
    assert!(std::mem::offset_of!(VariablePrimitiveOpt, is_present) == 8);
    assert!(std::mem::size_of::<VariablePrimitiveOpt>() == 16);
    assert!(std::mem::size_of::<Option<&str>>() == 16);
    assert!(std::mem::size_of::<VariableValue>() == 16);
    assert!(std::mem::align_of::<VariableValue>() == 8);
};

impl<'a> VariableValue<'a> {
    /// Wraps a present runtime value.
    pub fn some(value: impl Into<Self>) -> Self {
        value.into()
    }

    /// Creates an absent runtime value for either arm.
    pub fn none() -> Self {
        // SAFETY: All-zeroes is both an absent VariablePrimitiveOpt and the
        // null niche used by Option<&str>.
        unsafe { std::mem::zeroed() }
    }

    /// Returns the boolean payload, or `None` when this value is absent.
    ///
    /// # Safety
    ///
    /// This value must contain a primitive boolean or be absent.
    pub unsafe fn as_bool(self) -> Option<bool> {
        // SAFETY: Guaranteed by the caller.
        unsafe { self.primitive.as_bool() }
    }

    /// Returns the `f64` payload, or `None` when this value is absent.
    ///
    /// # Safety
    ///
    /// This value must contain a primitive `f64` or be absent.
    pub unsafe fn as_f64(self) -> Option<f64> {
        // SAFETY: Guaranteed by the caller.
        unsafe { self.primitive.as_f64() }
    }

    /// Returns the `u64` payload, or `None` when this value is absent.
    ///
    /// # Safety
    ///
    /// This value must contain a primitive `u64` or be absent.
    pub unsafe fn as_u64(self) -> Option<u64> {
        // SAFETY: Guaranteed by the caller.
        unsafe { self.primitive.as_u64() }
    }

    /// Returns the `i64` payload, or `None` when this value is absent.
    ///
    /// # Safety
    ///
    /// This value must contain a primitive `i64` or be absent.
    pub unsafe fn as_i64(self) -> Option<i64> {
        // SAFETY: Guaranteed by the caller.
        unsafe { self.primitive.as_i64() }
    }

    /// Returns the borrowed string payload, or `None` when it is absent.
    ///
    /// # Safety
    ///
    /// This value must contain the `string` arm or be the all-zero absent
    /// representation returned by [`VariableValue::none`].
    pub unsafe fn as_str(self) -> Option<&'a str> {
        // SAFETY: Guaranteed by the caller.
        unsafe { self.string }
    }
}

impl Default for VariableValue<'_> {
    fn default() -> Self {
        Self::none()
    }
}

impl From<bool> for VariableValue<'_> {
    fn from(value: bool) -> Self {
        Self {
            primitive: VariablePrimitiveOpt::some(value),
        }
    }
}

impl From<f64> for VariableValue<'_> {
    fn from(value: f64) -> Self {
        Self {
            primitive: VariablePrimitiveOpt::some(value),
        }
    }
}

impl From<u64> for VariableValue<'_> {
    fn from(value: u64) -> Self {
        Self {
            primitive: VariablePrimitiveOpt::some(value),
        }
    }
}

impl From<i64> for VariableValue<'_> {
    fn from(value: i64) -> Self {
        Self {
            primitive: VariablePrimitiveOpt::some(value),
        }
    }
}

impl<'a> From<&'a str> for VariableValue<'a> {
    fn from(value: &'a str) -> Self {
        Self {
            string: Some(value),
        }
    }
}

impl<'a> From<Option<&'a str>> for VariableValue<'a> {
    fn from(value: Option<&'a str>) -> Self {
        match value {
            Some(value) => Self::from(value),
            None => Self::none(),
        }
    }
}

impl<'a> From<VariablePrimitive> for VariableValue<'a> {
    fn from(value: VariablePrimitive) -> Self {
        Self {
            primitive: VariablePrimitiveOpt::some(value),
        }
    }
}

impl<'a> From<VariablePrimitiveOpt> for VariableValue<'a> {
    fn from(value: VariablePrimitiveOpt) -> Self {
        Self { primitive: value }
    }
}

#[cfg(test)]
mod tests {
    use crate::types::{VariablePrimitive, VariablePrimitiveOpt, VariableValue};

    #[test]
    fn test_runtime_value_layouts() {
        assert_eq!(std::mem::size_of::<VariablePrimitive>(), 8);
        assert_eq!(std::mem::offset_of!(VariablePrimitiveOpt, value), 0);
        assert_eq!(std::mem::offset_of!(VariablePrimitiveOpt, is_present), 8);
        assert_eq!(std::mem::size_of::<VariablePrimitiveOpt>(), 16);
        assert_eq!(std::mem::size_of::<Option<&str>>(), 16);
        assert_eq!(std::mem::size_of::<VariableValue>(), 16);
        assert_eq!(std::mem::align_of::<VariableValue>(), 8);

        let text = "hello";
        let words: [usize; 2] = unsafe { std::mem::transmute(VariableValue::some(text)) };
        assert_eq!(words, [text.as_ptr() as usize, text.len()]);
        let none_words: [usize; 2] = unsafe { std::mem::transmute(VariableValue::none()) };
        assert_eq!(none_words, [0, 0]);
    }

    #[test]
    fn test_variable_primitive_opt_accessors() {
        assert_eq!(
            unsafe { VariablePrimitiveOpt::some(true).as_bool() },
            Some(true)
        );
        assert_eq!(
            unsafe { VariablePrimitiveOpt::some(1.5f64).as_f64() },
            Some(1.5)
        );
        assert_eq!(
            unsafe { VariablePrimitiveOpt::some(7u64).as_u64() },
            Some(7)
        );
        assert_eq!(
            unsafe { VariablePrimitiveOpt::some(-3i64).as_i64() },
            Some(-3)
        );
        assert_eq!(unsafe { VariablePrimitiveOpt::none().as_u64() }, None);
    }

    #[test]
    fn test_variable_value_accessors() {
        assert_eq!(unsafe { VariableValue::some(true).as_bool() }, Some(true));
        assert_eq!(unsafe { VariableValue::some(1.5f64).as_f64() }, Some(1.5));
        assert_eq!(unsafe { VariableValue::some(7u64).as_u64() }, Some(7));
        assert_eq!(unsafe { VariableValue::some(-3i64).as_i64() }, Some(-3));
        assert_eq!(
            unsafe { VariableValue::some(VariablePrimitive { int_u64: 11 }).as_u64() },
            Some(11)
        );
        assert_eq!(
            unsafe { VariableValue::some("hello").as_str() },
            Some("hello")
        );

        let none = VariableValue::none();
        assert_eq!(unsafe { none.as_bool() }, None);
        assert_eq!(unsafe { none.as_f64() }, None);
        assert_eq!(unsafe { none.as_u64() }, None);
        assert_eq!(unsafe { none.as_i64() }, None);
        assert_eq!(unsafe { none.as_str() }, None);
        assert_eq!(unsafe { VariableValue::from(None::<&str>).as_str() }, None);
    }

    #[test]
    fn test_empty_string_is_distinct_from_none() {
        let empty = VariableValue::some("");
        let none = VariableValue::none();

        assert_eq!(unsafe { empty.as_str() }, Some(""));
        assert_eq!(unsafe { none.as_str() }, None);
    }
}

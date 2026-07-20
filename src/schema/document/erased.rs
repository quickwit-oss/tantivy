//! Object-safe, type-erased views of documents and their values.
//!
//! [`Document`] and [`Value`] both use GATs, so neither can be turned into a trait object.
//! These mirror types erase the generic parameter — the nested array/object iterators are
//! boxed, while leaf values stay borrowed — so a plugin behind a `dyn` boundary can read an
//! arbitrary document without it being materialized into a concrete type. A plugin that
//! knows the concrete document type can recover it for free via [`ErasedDocument::as_any`]
//! and skip the erased walk entirely.

use std::any::Any;

use super::{Document, ReferenceValue, ReferenceValueLeaf, Value};
use crate::schema::Field;

/// A type-erased mirror of [`ReferenceValue`].
///
/// Leaves are the same concrete, mostly-borrowed enum as `ReferenceValue`, so they cost
/// nothing. Only the nested cases carry a boxed iterator, so a flat document allocates once
/// (for the field iterator) and nested documents allocate once per container.
pub enum ErasedValue<'a> {
    /// A leaf value, borrowed from the document.
    Leaf(ReferenceValueLeaf<'a>),
    /// An array, walked lazily through a boxed iterator.
    Array(Box<dyn Iterator<Item = ErasedValue<'a>> + 'a>),
    /// A nested object, walked lazily through a boxed iterator.
    Object(Box<dyn Iterator<Item = (&'a str, ErasedValue<'a>)> + 'a>),
}

/// Object-safe counterpart to [`Document`].
///
/// A blanket impl covers every `Document`, so `&document as &dyn ErasedDocument` always
/// works. Consumers either downcast via [`as_any`](ErasedDocument::as_any) (zero cost, when
/// they know the concrete type) or walk [`erased_fields`](ErasedDocument::erased_fields).
pub trait ErasedDocument {
    /// Iterates the document's `(field, value)` pairs in erased form.
    fn erased_fields<'a>(&'a self) -> Box<dyn Iterator<Item = (Field, ErasedValue<'a>)> + 'a>;

    /// Recovers the concrete document. `Document: 'static`, so this is free — a consumer that
    /// knows the type can `as_any().downcast_ref::<ConcreteDoc>()` and avoid the erased walk.
    fn as_any(&self) -> &dyn Any;
}

fn erase_value<'a, V: Value<'a> + 'a>(value: ReferenceValue<'a, V>) -> ErasedValue<'a> {
    match value {
        ReferenceValue::Leaf(leaf) => ErasedValue::Leaf(leaf),
        ReferenceValue::Array(iter) => {
            ErasedValue::Array(Box::new(iter.map(|value| erase_value(value.as_value()))))
        }
        ReferenceValue::Object(iter) => ErasedValue::Object(Box::new(
            iter.map(|(key, value)| (key, erase_value(value.as_value()))),
        )),
    }
}

impl<D: Document> ErasedDocument for D {
    fn erased_fields<'a>(&'a self) -> Box<dyn Iterator<Item = (Field, ErasedValue<'a>)> + 'a> {
        Box::new(
            self.iter_fields_and_values()
                .map(|(field, value)| (field, erase_value(value.as_value()))),
        )
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::schema::{Field, OwnedValue};

    #[test]
    fn test_erased_document_walk_and_downcast() {
        let field = Field::from_field_id(0);
        let mut document: BTreeMap<Field, OwnedValue> = BTreeMap::new();
        document.insert(field, "hello".into());

        // Any `Document` is an `ErasedDocument` via the blanket impl.
        let erased: &dyn ErasedDocument = &document;

        // The erased walk yields the leaf borrowed straight from the document — no copy.
        let fields: Vec<(Field, ErasedValue)> = erased.erased_fields().collect();
        assert_eq!(fields.len(), 1);
        match &fields[0] {
            (got_field, ErasedValue::Leaf(ReferenceValueLeaf::Str(text))) => {
                assert_eq!(*got_field, field);
                assert_eq!(*text, "hello");
            }
            _ => panic!("expected a borrowed string leaf"),
        }

        // A consumer that knows the concrete type recovers it for free, skipping the walk.
        assert!(erased
            .as_any()
            .downcast_ref::<BTreeMap<Field, OwnedValue>>()
            .is_some());
    }
}

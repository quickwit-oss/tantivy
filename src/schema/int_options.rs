use crate::schema::flags::{FastFlag, IndexedFlag, SchemaFlagList, StoredFlag};
use serde::{Deserialize, Serialize};
use std::ops::BitOr;

/// Express whether a field is single-value or multi-valued.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub enum Cardinality {
    /// The document must have exactly one value associated to the document.
    #[serde(rename = "single")]
    SingleValue,
    /// The document can have any number of values associated to the document.
    /// This is more memory and CPU expensive than the SingleValue solution.
    #[serde(rename = "multi")]
    MultiValues,
}

/// Define how an u64, i64, of f64 field should be handled by tantivy.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(from = "IntOptionsDeser")]
pub struct IntOptions {
    indexed: bool,
    // This boolean has no effect if the field is not marked as indexed too.
    fieldnorms: bool, // This attribute only has an effect if indexed is true.
    #[serde(skip_serializing_if = "Option::is_none")]
    fast: Option<Cardinality>,
    stored: bool,
}

/// For backward compability we add an intermediary to interpret the
/// lack of fieldnorms attribute as "true" iff indexed.
///
/// (Downstream, for the moment, this attribute is not used anyway if not indexed...)
/// Note that: newly serialized IntOptions will include the new attribute.
#[derive(Deserialize)]
struct IntOptionsDeser {
    indexed: bool,
    #[serde(default)]
    fieldnorms: Option<bool>, // This attribute only has an effect if indexed is true.
    #[serde(default)]
    fast: Option<Cardinality>,
    stored: bool,
}

impl From<IntOptionsDeser> for IntOptions {
    fn from(deser: IntOptionsDeser) -> Self {
        IntOptions {
            indexed: deser.indexed,
            fieldnorms: deser.fieldnorms.unwrap_or(deser.indexed),
            fast: deser.fast,
            stored: deser.stored,
        }
    }
}

impl IntOptions {
    /// Returns true iff the value is stored.
    pub fn is_stored(&self) -> bool {
        self.stored
    }

    /// Returns true iff the value is indexed and therefore searchable.
    pub fn is_indexed(&self) -> bool {
        self.indexed
    }

    /// Returns true iff the field has fieldnorm.
    pub fn fieldnorms(&self) -> bool {
        self.fieldnorms && self.indexed
    }

    /// Returns true iff the value is a fast field.
    pub fn is_fast(&self) -> bool {
        self.fast.is_some()
    }

    /// Set the field as stored.
    ///
    /// Only the fields that are set as *stored* are
    /// persisted into the Tantivy's store.
    pub fn set_stored(mut self) -> IntOptions {
        self.stored = true;
        self
    }

    /// Set the field as indexed.
    ///
    /// Setting an integer as indexed will generate
    /// a posting list for each value taken by the integer.
    ///
    /// This is required for the field to be searchable.
    pub fn set_indexed(mut self) -> IntOptions {
        self.indexed = true;
        self
    }

    /// Set the field with fieldnorm.
    ///
    /// Setting an integer as fieldnorm will generate
    /// the fieldnorm data for it.
    pub fn set_fieldnorm(mut self) -> IntOptions {
        self.fieldnorms = true;
        self
    }

    /// Set the field as a single-valued fast field.
    ///
    /// Fast fields are designed for random access.
    /// Access time are similar to a random lookup in an array.
    /// If more than one value is associated to a fast field, only the last one is
    /// kept.
    pub fn set_fast(mut self, cardinality: Cardinality) -> IntOptions {
        self.fast = Some(cardinality);
        self
    }

    /// Returns the cardinality of the fastfield.
    ///
    /// If the field has not been declared as a fastfield, then
    /// the method returns None.
    pub fn get_fastfield_cardinality(&self) -> Option<Cardinality> {
        self.fast
    }
}

impl Default for IntOptions {
    fn default() -> IntOptions {
        IntOptions {
            indexed: false,
            fieldnorms: false,
            stored: false,
            fast: None,
        }
    }
}

impl From<()> for IntOptions {
    fn from(_: ()) -> IntOptions {
        IntOptions::default()
    }
}

impl From<FastFlag> for IntOptions {
    fn from(_: FastFlag) -> Self {
        IntOptions {
            indexed: false,
            fieldnorms: false,
            stored: false,
            fast: Some(Cardinality::SingleValue),
        }
    }
}

impl From<StoredFlag> for IntOptions {
    fn from(_: StoredFlag) -> Self {
        IntOptions {
            indexed: false,
            fieldnorms: false,
            stored: true,
            fast: None,
        }
    }
}

impl From<IndexedFlag> for IntOptions {
    fn from(_: IndexedFlag) -> Self {
        IntOptions {
            indexed: true,
            fieldnorms: true,
            stored: false,
            fast: None,
        }
    }
}

impl<T: Into<IntOptions>> BitOr<T> for IntOptions {
    type Output = IntOptions;

    fn bitor(self, other: T) -> IntOptions {
        let other = other.into();
        IntOptions {
            indexed: self.indexed | other.indexed,
            fieldnorms: self.fieldnorms | other.fieldnorms,
            stored: self.stored | other.stored,
            fast: self.fast.or(other.fast),
        }
    }
}

impl<Head, Tail> From<SchemaFlagList<Head, Tail>> for IntOptions
where
    Head: Clone,
    Tail: Clone,
    Self: BitOr<Output = Self> + From<Head> + From<Tail>,
{
    fn from(head_tail: SchemaFlagList<Head, Tail>) -> Self {
        Self::from(head_tail.head) | Self::from(head_tail.tail)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_int_options_deser_if_fieldnorm_missing_indexed_true() {
        let json = r#"{
            "indexed": true,
            "stored": false
        }"#;
        let int_options: IntOptions = serde_json::from_str(json).unwrap();
        assert_eq!(
            &int_options,
            &IntOptions {
                indexed: true,
                fieldnorms: true,
                fast: None,
                stored: false
            }
        );
    }

    #[test]
    fn test_int_options_deser_if_fieldnorm_missing_indexed_false() {
        let json = r#"{
            "indexed": false,
            "stored": false
        }"#;
        let int_options: IntOptions = serde_json::from_str(json).unwrap();
        assert_eq!(
            &int_options,
            &IntOptions {
                indexed: false,
                fieldnorms: false,
                fast: None,
                stored: false
            }
        );
    }

    #[test]
    fn test_int_options_deser_if_fieldnorm_false_indexed_true() {
        let json = r#"{
            "indexed": true,
            "fieldnorms": false,
            "stored": false
        }"#;
        let int_options: IntOptions = serde_json::from_str(json).unwrap();
        assert_eq!(
            &int_options,
            &IntOptions {
                indexed: true,
                fieldnorms: false,
                fast: None,
                stored: false
            }
        );
    }

    #[test]
    fn test_int_options_deser_if_fieldnorm_true_indexed_false() {
        // this one is kind of useless, at least at the moment
        let json = r#"{
            "indexed": false,
            "fieldnorms": true,
            "stored": false
        }"#;
        let int_options: IntOptions = serde_json::from_str(json).unwrap();
        assert_eq!(
            &int_options,
            &IntOptions {
                indexed: false,
                fieldnorms: true,
                fast: None,
                stored: false
            }
        );
    }
}

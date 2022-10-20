use std::ops::BitOr;

use serde::{Deserialize, Serialize};

use super::Cardinality;
use crate::schema::flags::{FastFlag, IndexedFlag, SchemaFlagList, StoredFlag};

/// DateTime Precision
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DatePrecision {
    /// Seconds precision
    Seconds,
    /// Milli-seconds precision.
    Milliseconds,
    /// Micro-seconds precision.
    Microseconds,
}

impl Default for DatePrecision {
    fn default() -> Self {
        DatePrecision::Seconds
    }
}

/// Defines how DateTime field should be handled by tantivy.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct DateOptions {
    indexed: bool,
    // This boolean has no effect if the field is not marked as indexed true.
    fieldnorms: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    fast: Option<Cardinality>,
    stored: bool,
    // Internal storage precision, used to optimize storage
    // compression on fast fields.
    #[serde(default)]
    precision: DatePrecision,
}

impl DateOptions {
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

    /// Returns true iff the value is a fast field and multivalue.
    pub fn is_multivalue_fast(&self) -> bool {
        if let Some(cardinality) = self.fast {
            cardinality == Cardinality::MultiValues
        } else {
            false
        }
    }

    /// Returns true iff the value is a fast field.
    pub fn is_fast(&self) -> bool {
        self.fast.is_some()
    }

    /// Set the field as stored.
    ///
    /// Only the fields that are set as *stored* are
    /// persisted into the Tantivy's store.
    #[must_use]
    pub fn set_stored(mut self) -> DateOptions {
        self.stored = true;
        self
    }

    /// Set the field as indexed.
    ///
    /// Setting an integer as indexed will generate
    /// a posting list for each value taken by the integer.
    ///
    /// This is required for the field to be searchable.
    #[must_use]
    pub fn set_indexed(mut self) -> DateOptions {
        self.indexed = true;
        self
    }

    /// Set the field with fieldnorm.
    ///
    /// Setting an integer as fieldnorm will generate
    /// the fieldnorm data for it.
    #[must_use]
    pub fn set_fieldnorm(mut self) -> DateOptions {
        self.fieldnorms = true;
        self
    }

    /// Set the field as a single-valued fast field.
    ///
    /// Fast fields are designed for random access.
    /// Access time are similar to a random lookup in an array.
    /// If more than one value is associated with a fast field, only the last one is
    /// kept.
    #[must_use]
    pub fn set_fast(mut self, cardinality: Cardinality) -> DateOptions {
        self.fast = Some(cardinality);
        self
    }

    /// Returns the cardinality of the fastfield.
    ///
    /// If the field has not been declared as a fastfield, then
    /// the method returns `None`.
    pub fn get_fastfield_cardinality(&self) -> Option<Cardinality> {
        self.fast
    }

    /// Sets the precision for this DateTime field.
    ///
    /// Internal storage precision, used to optimize storage
    /// compression on fast fields.
    pub fn set_precision(mut self, precision: DatePrecision) -> DateOptions {
        self.precision = precision;
        self
    }

    /// Returns the storage precision for this DateTime field.
    ///
    /// Internal storage precision, used to optimize storage
    /// compression on fast fields.
    pub fn get_precision(&self) -> DatePrecision {
        self.precision
    }
}

impl From<()> for DateOptions {
    fn from(_: ()) -> DateOptions {
        DateOptions::default()
    }
}

impl From<FastFlag> for DateOptions {
    fn from(_: FastFlag) -> Self {
        DateOptions {
            indexed: false,
            fieldnorms: false,
            stored: false,
            fast: Some(Cardinality::SingleValue),
            ..Default::default()
        }
    }
}

impl From<StoredFlag> for DateOptions {
    fn from(_: StoredFlag) -> Self {
        DateOptions {
            indexed: false,
            fieldnorms: false,
            stored: true,
            fast: None,
            ..Default::default()
        }
    }
}

impl From<IndexedFlag> for DateOptions {
    fn from(_: IndexedFlag) -> Self {
        DateOptions {
            indexed: true,
            fieldnorms: true,
            stored: false,
            fast: None,
            ..Default::default()
        }
    }
}

impl<T: Into<DateOptions>> BitOr<T> for DateOptions {
    type Output = DateOptions;

    fn bitor(self, other: T) -> DateOptions {
        let other = other.into();
        DateOptions {
            indexed: self.indexed | other.indexed,
            fieldnorms: self.fieldnorms | other.fieldnorms,
            stored: self.stored | other.stored,
            fast: self.fast.or(other.fast),
            precision: self.precision,
        }
    }
}

impl<Head, Tail> From<SchemaFlagList<Head, Tail>> for DateOptions
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
    fn test_date_options_consistent_with_default() {
        let date_time_options: DateOptions = serde_json::from_str(
            r#"{
            "indexed": false,
            "fieldnorms": false,
            "stored": false
        }"#,
        )
        .unwrap();
        assert_eq!(date_time_options, DateOptions::default());
    }

    #[test]
    fn test_serialize_date_option() {
        let date_options = serde_json::from_str::<DateOptions>(
            r#"
            {
                "indexed": true,
                "fieldnorms": false,
                "stored": false,
                "precision": "milliseconds"
            }"#,
        )
        .unwrap();

        let date_options_json = serde_json::to_value(&date_options).unwrap();
        assert_eq!(
            date_options_json,
            serde_json::json!({
                "precision": "milliseconds",
                "indexed": true,
                "fieldnorms": false,
                "stored": false
            })
        );
    }

    #[test]
    fn test_deserialize_date_options_with_wrong_options() {
        assert!(serde_json::from_str::<DateOptions>(
            r#"{
            "indexed": true,
            "fieldnorms": false,
            "stored": "wrong_value"
        }"#
        )
        .unwrap_err()
        .to_string()
        .contains("expected a boolean"));

        assert!(serde_json::from_str::<DateOptions>(
            r#"{
            "indexed": true,
            "fieldnorms": false,
            "stored": false,
            "precision": "hours"
        }"#
        )
        .unwrap_err()
        .to_string()
        .contains("unknown variant `hours`"));
    }
}

use std::ops::BitOr;

use columnar::StrColumnCodec;
use serde::{Deserialize, Serialize};

use super::flags::{FastFlag, IndexedFlag, SchemaFlagList, StoredFlag};
use super::text_options::merge_str_column_codecs;
/// Define how a bytes field should be handled by tantivy.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(from = "BytesOptionsDeser")]
pub struct BytesOptions {
    indexed: bool,
    fieldnorms: bool,
    /// Codec of the fast field, or `None` if the field is not a fast field.
    #[serde(with = "fast_field_bytes_options_serde")]
    fast: Option<StrColumnCodec>,
    stored: bool,
}

/// Wire format of the fast field options of bytes fields, as an `Option<StrColumnCodec>`:
/// - `false`: not a fast field.
/// - `true`: a fast field with the dictionary codec.
/// - `{"codec": "plain"}`: a fast field with the given codec. The key is optional.
mod fast_field_bytes_options_serde {
    use columnar::StrColumnCodec;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    #[derive(Serialize, Deserialize)]
    #[serde(untagged)]
    enum WireFormat {
        IsEnabled(bool),
        EnabledWithOptions {
            #[serde(default)]
            codec: StrColumnCodec,
        },
    }

    pub fn serialize<S>(
        fast_codec: &Option<StrColumnCodec>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let wire_format = match fast_codec {
            None => WireFormat::IsEnabled(false),
            Some(StrColumnCodec::Dictionary) => WireFormat::IsEnabled(true),
            Some(codec) => WireFormat::EnabledWithOptions { codec: *codec },
        };
        wire_format.serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<StrColumnCodec>, D::Error>
    where D: Deserializer<'de> {
        let fast_codec = match WireFormat::deserialize(deserializer)? {
            WireFormat::IsEnabled(false) => None,
            WireFormat::IsEnabled(true) => Some(StrColumnCodec::Dictionary),
            WireFormat::EnabledWithOptions { codec } => Some(codec),
        };
        Ok(fast_codec)
    }
}

/// For backward compatibility we add an intermediary to interpret the
/// lack of fieldnorms attribute as "true" if and only if indexed.
///
/// (Downstream, for the moment, this attribute is not used if not indexed...)
/// Note that: newly serialized NumericOptions will include the new attribute.
#[derive(Deserialize)]
struct BytesOptionsDeser {
    indexed: bool,
    #[serde(default)]
    fieldnorms: Option<bool>,
    #[serde(with = "fast_field_bytes_options_serde")]
    fast: Option<StrColumnCodec>,
    stored: bool,
}

impl From<BytesOptionsDeser> for BytesOptions {
    fn from(deser: BytesOptionsDeser) -> Self {
        BytesOptions {
            indexed: deser.indexed,
            fieldnorms: deser.fieldnorms.unwrap_or(deser.indexed),
            fast: deser.fast,
            stored: deser.stored,
        }
    }
}

impl BytesOptions {
    /// Returns true if the value is indexed.
    #[inline]
    pub fn is_indexed(&self) -> bool {
        self.indexed
    }

    /// Returns true if and only if the value is normed.
    #[inline]
    pub fn fieldnorms(&self) -> bool {
        self.fieldnorms
    }

    /// Returns true if the value is a fast field.
    #[inline]
    pub fn is_fast(&self) -> bool {
        self.fast.is_some()
    }

    /// Returns the codec of the fast field, or `None` if the field is not a fast field.
    #[inline]
    pub fn get_fast_field_codec(&self) -> Option<StrColumnCodec> {
        self.fast
    }

    /// Returns true if the value is stored.
    #[inline]
    pub fn is_stored(&self) -> bool {
        self.stored
    }

    /// Set the field as indexed.
    ///
    /// Setting an integer as indexed will generate
    /// a posting list for each value taken by the integer.
    #[must_use]
    pub fn set_indexed(mut self) -> BytesOptions {
        self.indexed = true;
        self
    }

    /// Set the field as normed.
    ///
    /// Setting an integer as normed will generate
    /// the fieldnorm data for it.
    #[must_use]
    pub fn set_fieldnorms(mut self) -> BytesOptions {
        self.fieldnorms = true;
        self
    }

    /// Set the field as a fast field.
    ///
    /// Fast fields are designed for random access.
    ///
    /// Keeps the codec if one was already set, with the dictionary codec as default.
    #[must_use]
    pub fn set_fast(mut self) -> BytesOptions {
        self.fast.get_or_insert(StrColumnCodec::Dictionary);
        self
    }

    /// Sets the codec of the fast field. The default is [`StrColumnCodec::Dictionary`].
    ///
    /// The field also becomes a fast field.
    ///
    /// The plain codec stores the values without term ordinals. It is better suited to fields
    /// with a high cardinality. Merging segments with plain columns is not supported yet: it
    /// returns an error.
    #[must_use]
    pub fn set_fast_codec(mut self, codec: StrColumnCodec) -> BytesOptions {
        self.fast = Some(codec);
        self
    }

    /// Set the field as stored.
    ///
    /// Only the fields that are set as *stored* are
    /// persisted into the Tantivy's store.
    #[must_use]
    pub fn set_stored(mut self) -> BytesOptions {
        self.stored = true;
        self
    }
}

impl<T: Into<BytesOptions>> BitOr<T> for BytesOptions {
    type Output = BytesOptions;

    fn bitor(self, other: T) -> BytesOptions {
        let other = other.into();
        BytesOptions {
            indexed: self.indexed | other.indexed,
            fieldnorms: self.fieldnorms | other.fieldnorms,
            stored: self.stored | other.stored,
            fast: match (self.fast, other.fast) {
                (Some(left_codec), Some(right_codec)) => {
                    Some(merge_str_column_codecs(left_codec, right_codec))
                }
                (left_codec_opt, right_codec_opt) => left_codec_opt.or(right_codec_opt),
            },
        }
    }
}

impl From<()> for BytesOptions {
    fn from(_: ()) -> Self {
        Self::default()
    }
}

impl From<FastFlag> for BytesOptions {
    fn from(_: FastFlag) -> Self {
        BytesOptions {
            indexed: false,
            fieldnorms: false,
            stored: false,
            fast: Some(StrColumnCodec::Dictionary),
        }
    }
}

impl From<StoredFlag> for BytesOptions {
    fn from(_: StoredFlag) -> Self {
        BytesOptions {
            indexed: false,
            fieldnorms: false,
            stored: true,
            fast: None,
        }
    }
}

impl From<IndexedFlag> for BytesOptions {
    fn from(_: IndexedFlag) -> Self {
        BytesOptions {
            indexed: true,
            fieldnorms: true,
            stored: false,
            fast: None,
        }
    }
}

impl<Head, Tail> From<SchemaFlagList<Head, Tail>> for BytesOptions
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
    use columnar::StrColumnCodec;

    use crate::schema::{BytesOptions, FAST, INDEXED, STORED};

    #[test]
    fn test_bytes_option_fast_flag() {
        assert_eq!(BytesOptions::default().set_fast(), FAST.into());
        assert_eq!(
            BytesOptions::default().set_indexed().set_fieldnorms(),
            INDEXED.into()
        );
        assert_eq!(BytesOptions::default().set_stored(), STORED.into());
    }
    #[test]
    fn test_bytes_option_fast_flag_composition() {
        assert_eq!(
            BytesOptions::default().set_fast().set_stored(),
            (FAST | STORED).into()
        );
        assert_eq!(
            BytesOptions::default()
                .set_indexed()
                .set_fieldnorms()
                .set_fast(),
            (INDEXED | FAST).into()
        );
        assert_eq!(
            BytesOptions::default()
                .set_stored()
                .set_fieldnorms()
                .set_indexed(),
            (STORED | INDEXED).into()
        );
    }

    #[test]
    fn test_bytes_option_fast_() {
        assert!(!BytesOptions::default().is_stored());
        assert!(!BytesOptions::default().is_fast());
        assert!(!BytesOptions::default().is_indexed());
        assert!(!BytesOptions::default().fieldnorms());
        assert!(BytesOptions::default().set_stored().is_stored());
        assert!(BytesOptions::default().set_fast().is_fast());
        assert!(BytesOptions::default().set_indexed().is_indexed());
        assert!(BytesOptions::default().set_fieldnorms().fieldnorms());
    }

    #[test]
    fn test_bytes_options_deser_if_fieldnorm_missing_indexed_true() {
        let json = r#"{
            "indexed": true,
            "fast": false,
            "stored": false
        }"#;
        let bytes_options: BytesOptions = serde_json::from_str(json).unwrap();
        assert_eq!(
            &bytes_options,
            &BytesOptions {
                indexed: true,
                fieldnorms: true,
                fast: None,
                stored: false
            }
        );
    }

    #[test]
    fn test_bytes_options_deser_if_fieldnorm_missing_indexed_false() {
        let json = r#"{
            "indexed": false,
            "stored": false,
            "fast": false
        }"#;
        let bytes_options: BytesOptions = serde_json::from_str(json).unwrap();
        assert_eq!(
            &bytes_options,
            &BytesOptions {
                indexed: false,
                fieldnorms: false,
                fast: None,
                stored: false
            }
        );
    }

    #[test]
    fn test_bytes_options_deser_if_fieldnorm_false_indexed_true() {
        let json = r#"{
            "indexed": true,
            "fieldnorms": false,
            "fast": false,
            "stored": false
        }"#;
        let bytes_options: BytesOptions = serde_json::from_str(json).unwrap();
        assert_eq!(
            &bytes_options,
            &BytesOptions {
                indexed: true,
                fieldnorms: false,
                fast: None,
                stored: false
            }
        );
    }

    #[test]
    fn test_bytes_options_deser_if_fieldnorm_true_indexed_false() {
        // this one is kind of useless, at least at the moment
        let json = r#"{
            "indexed": false,
            "fieldnorms": true,
            "fast": false,
            "stored": false
        }"#;
        let bytes_options: BytesOptions = serde_json::from_str(json).unwrap();
        assert_eq!(
            &bytes_options,
            &BytesOptions {
                indexed: false,
                fieldnorms: true,
                fast: None,
                stored: false
            }
        );
    }

    #[test]
    fn test_bytes_options_fast_codec_serde() {
        let bytes_options = BytesOptions::default().set_fast_codec(StrColumnCodec::Plain);
        assert!(bytes_options.is_fast());
        assert_eq!(
            bytes_options.get_fast_field_codec(),
            Some(StrColumnCodec::Plain)
        );
        let serialized = serde_json::to_value(&bytes_options).unwrap();
        assert_eq!(
            serialized,
            serde_json::json!({
                "indexed": false,
                "fieldnorms": false,
                "fast": { "codec": "plain" },
                "stored": false
            })
        );
        let deserialized: BytesOptions = serde_json::from_value(serialized).unwrap();
        assert_eq!(deserialized, bytes_options);

        // The dictionary codec is serialized as `true`.
        let serialized = serde_json::to_value(BytesOptions::default().set_fast()).unwrap();
        assert_eq!(serialized["fast"], serde_json::json!(true));
        let deserialized: BytesOptions = serde_json::from_value(serialized).unwrap();
        assert_eq!(
            deserialized.get_fast_field_codec(),
            Some(StrColumnCodec::Dictionary)
        );
        assert_eq!(BytesOptions::default().get_fast_field_codec(), None);
    }

    #[test]
    fn test_bytes_options_fast_codec_composition() {
        let plain_options = BytesOptions::default().set_fast_codec(StrColumnCodec::Plain);
        assert_eq!(
            (plain_options.clone() | STORED).get_fast_field_codec(),
            Some(StrColumnCodec::Plain)
        );
        assert_eq!(
            (BytesOptions::from(STORED) | plain_options).get_fast_field_codec(),
            Some(StrColumnCodec::Plain)
        );
    }
}

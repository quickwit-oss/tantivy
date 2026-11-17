use std::borrow::Cow;
use std::ops::BitOr;

use serde::{Deserialize, Serialize};

use super::flags::{CoerceFlag, FastFlag};
use crate::schema::flags::{SchemaFlagList, StoredFlag};
use crate::schema::IndexRecordOption;
use crate::tokenizer::{DEFAULT_TOKENIZER_NAME, RAW_TOKENIZER_NAME};

/// Define how a text field should be handled by tantivy.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Default)]
pub struct TextOptions {
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    indexing: Option<TextFieldIndexing>,
    #[serde(default)]
    stored: bool,
    #[serde(default)]
    #[serde(with = "fast_field_text_options_serde")]
    pub(crate) fast: Option<FastFieldTextOptions>,
    #[serde(default)]
    #[serde(skip_serializing_if = "is_false")]
    /// coerce values into string if they are not of type string
    coerce: bool,
}

/// Options controlling how the values of a text fast field are recorded.
#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct FastFieldTextOptions {
    /// Name of the analyzer applied to the values, looked up in the fast field tokenizer
    /// manager.
    ///
    /// `None` means values are recorded as is, without any lookup. This is not the same as
    /// `Some("raw")`: the analyzer registered under the raw name may have extra filters, such as
    /// a token length limit.
    pub tokenizer: Option<String>,
}

pub(super) fn merge_fast_field_options(
    left: Option<FastFieldTextOptions>,
    right: Option<FastFieldTextOptions>,
) -> Option<FastFieldTextOptions> {
    // A configured tokenizer takes precedence over recording values as is.
    match (left, right) {
        (Some(left), Some(right)) if left.tokenizer.is_none() => Some(right),
        (Some(left), _) => Some(left),
        (None, right) => right,
    }
}

pub(super) mod fast_field_text_options_serde {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    use super::FastFieldTextOptions;

    #[derive(Serialize, Deserialize)]
    #[serde(untagged)]
    enum WireFormat {
        IsEnabled(bool),
        EnabledWithTokenizer { with_tokenizer: String },
    }

    pub fn serialize<S>(
        fast_field_options: &Option<FastFieldTextOptions>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let wire_format = match fast_field_options {
            None => WireFormat::IsEnabled(false),
            Some(FastFieldTextOptions { tokenizer: None }) => WireFormat::IsEnabled(true),
            Some(FastFieldTextOptions {
                tokenizer: Some(tokenizer_name),
            }) => WireFormat::EnabledWithTokenizer {
                with_tokenizer: tokenizer_name.clone(),
            },
        };
        wire_format.serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<FastFieldTextOptions>, D::Error>
    where D: Deserializer<'de> {
        let wire_format = WireFormat::deserialize(deserializer)?;
        match wire_format {
            WireFormat::IsEnabled(false) => Ok(None),
            WireFormat::IsEnabled(true) => Ok(Some(FastFieldTextOptions::default())),
            WireFormat::EnabledWithTokenizer { with_tokenizer } => Ok(Some(FastFieldTextOptions {
                tokenizer: Some(with_tokenizer),
            })),
        }
    }
}

fn is_false(val: &bool) -> bool {
    !val
}

impl TextOptions {
    /// Returns the indexing options.
    #[inline]
    pub fn get_indexing_options(&self) -> Option<&TextFieldIndexing> {
        self.indexing.as_ref()
    }

    /// Returns true if the text is to be stored.
    #[inline]
    pub fn is_stored(&self) -> bool {
        self.stored
    }

    /// Returns true if and only if the value is a fast field.
    #[inline]
    pub fn is_fast(&self) -> bool {
        self.fast.is_some()
    }

    /// Returns the tokenizer used for the fast field, if the text field
    /// is a fast field and a tokenizer was configured for it.
    ///
    /// Returns `None` for a fast field set with the `FAST` flag: its values are recorded as is.
    #[inline]
    pub fn get_fast_field_tokenizer_name(&self) -> Option<&str> {
        self.fast.as_ref()?.tokenizer.as_deref()
    }

    /// Returns true if values should be coerced to strings (numbers, null).
    #[inline]
    pub fn should_coerce(&self) -> bool {
        self.coerce
    }

    /// Set the field as a fast field.
    ///
    /// Fast fields are designed for random access.
    /// Access time are similar to a random lookup in an array.
    /// Text fast fields will have the term ids stored in the fast field.
    ///
    /// The tokenizer is always looked up, including the raw tokenizer: the analyzer registered
    /// under the raw name may have extra filters. To record values as is, without any lookup,
    /// use [`TextOptions::set_fast_no_tokenizer`] or the `FAST` flag instead.
    ///
    /// The effective cardinality depends on the tokenizer. The tokenizer can be used to apply
    /// normalization like lower case.
    /// The passed tokenizer_name must be available on the fast field tokenizer manager.
    /// `Index::fast_field_tokenizer`.
    ///
    /// The original text can be retrieved via
    /// [`TermDictionary::ord_to_term()`](crate::termdict::TermDictionary::ord_to_term)
    /// from the dictionary.
    #[must_use]
    pub fn set_fast(mut self, tokenizer_name: impl ToString) -> TextOptions {
        let tokenizer = Some(tokenizer_name.to_string());
        self.fast = Some(FastFieldTextOptions { tokenizer });
        self
    }

    /// Set the field as a fast field, without any tokenizer.
    ///
    /// Values are recorded as is. This is the same as the `FAST` flag.
    #[must_use]
    pub fn set_fast_no_tokenizer(mut self) -> TextOptions {
        self.fast = Some(FastFieldTextOptions { tokenizer: None });
        self
    }

    /// Coerce values if they are not of type string. Defaults to false.
    #[must_use]
    pub fn set_coerce(mut self) -> TextOptions {
        self.coerce = true;
        self
    }

    /// Sets the field as stored.
    #[must_use]
    pub fn set_stored(mut self) -> TextOptions {
        self.stored = true;
        self
    }

    /// Sets the field as indexed, with the specific indexing options.
    #[must_use]
    pub fn set_indexing_options(mut self, indexing: TextFieldIndexing) -> TextOptions {
        self.indexing = Some(indexing);
        self
    }
}

/// Configuration defining indexing for a text field.
///
/// It defines
/// - The amount of information that should be stored about the presence of a term in a document.
///   Essentially, should we store the term frequency and/or the positions (See
///   [`IndexRecordOption`]).
/// - The name of the `Tokenizer` that should be used to process the field.
/// - Flag indicating, if fieldnorms should be stored (See [fieldnorm](crate::fieldnorm)). Defaults
///   to `true`.
#[derive(Clone, PartialEq, Debug, Eq, Serialize, Deserialize)]
pub struct TextFieldIndexing {
    #[serde(default)]
    record: IndexRecordOption,
    #[serde(default = "default_fieldnorms")]
    fieldnorms: bool,
    #[serde(default = "default_tokenizer")]
    tokenizer: Cow<'static, str>,
}

fn default_tokenizer() -> Cow<'static, str> {
    Cow::Borrowed(DEFAULT_TOKENIZER_NAME)
}

pub(crate) fn default_fieldnorms() -> bool {
    true
}

impl Default for TextFieldIndexing {
    fn default() -> TextFieldIndexing {
        TextFieldIndexing {
            tokenizer: default_tokenizer(),
            record: IndexRecordOption::default(),
            fieldnorms: default_fieldnorms(),
        }
    }
}

impl TextFieldIndexing {
    /// Sets the tokenizer to be used for a given field.
    #[must_use]
    pub fn set_tokenizer(mut self, tokenizer_name: &str) -> TextFieldIndexing {
        self.tokenizer = Cow::Owned(tokenizer_name.to_string());
        self
    }

    /// Returns the name of the tokenizer that will be used for this field.
    pub fn tokenizer(&self) -> &str {
        self.tokenizer.as_ref()
    }

    /// Sets fieldnorms
    #[must_use]
    pub fn set_fieldnorms(mut self, fieldnorms: bool) -> TextFieldIndexing {
        self.fieldnorms = fieldnorms;
        self
    }

    /// Returns true if and only if [fieldnorms](crate::fieldnorm) are stored.
    pub fn fieldnorms(&self) -> bool {
        self.fieldnorms
    }

    /// Sets which information should be indexed with the tokens.
    ///
    /// See [`IndexRecordOption`] for more detail.
    #[must_use]
    pub fn set_index_option(mut self, index_option: IndexRecordOption) -> TextFieldIndexing {
        self.record = index_option;
        self
    }

    /// Returns the indexing options associated with this field.
    ///
    /// See [`IndexRecordOption`] for more detail.
    pub fn index_option(&self) -> IndexRecordOption {
        self.record
    }
}

/// The field will be untokenized and indexed.
pub const STRING: TextOptions = TextOptions {
    indexing: Some(TextFieldIndexing {
        tokenizer: Cow::Borrowed(RAW_TOKENIZER_NAME),
        fieldnorms: true,
        record: IndexRecordOption::Basic,
    }),
    stored: false,
    fast: None,
    coerce: false,
};

/// The field will be tokenized and indexed.
pub const TEXT: TextOptions = TextOptions {
    indexing: Some(TextFieldIndexing {
        tokenizer: Cow::Borrowed(DEFAULT_TOKENIZER_NAME),
        fieldnorms: true,
        record: IndexRecordOption::WithFreqsAndPositions,
    }),
    stored: false,
    coerce: false,
    fast: None,
};

impl<T: Into<TextOptions>> BitOr<T> for TextOptions {
    type Output = TextOptions;

    fn bitor(self, other: T) -> TextOptions {
        let other = other.into();
        TextOptions {
            indexing: self.indexing.or(other.indexing),
            stored: self.stored | other.stored,
            fast: merge_fast_field_options(self.fast, other.fast),
            coerce: self.coerce | other.coerce,
        }
    }
}

impl From<()> for TextOptions {
    fn from(_: ()) -> TextOptions {
        TextOptions::default()
    }
}

impl From<StoredFlag> for TextOptions {
    fn from(_: StoredFlag) -> TextOptions {
        TextOptions {
            indexing: None,
            stored: true,
            fast: None,
            coerce: false,
        }
    }
}

impl From<CoerceFlag> for TextOptions {
    fn from(_: CoerceFlag) -> TextOptions {
        TextOptions {
            indexing: None,
            stored: false,
            fast: None,
            coerce: true,
        }
    }
}

impl From<FastFlag> for TextOptions {
    fn from(_: FastFlag) -> TextOptions {
        TextOptions {
            indexing: None,
            stored: false,
            fast: Some(FastFieldTextOptions::default()),
            coerce: false,
        }
    }
}

impl<Head, Tail> From<SchemaFlagList<Head, Tail>> for TextOptions
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
    use crate::schema::text_options::FastFieldTextOptions;
    use crate::schema::*;

    #[test]
    fn test_field_options() {
        let field_options = STORED | TEXT;
        assert!(field_options.is_stored());
        assert!(field_options.get_indexing_options().is_some());
        let mut schema_builder = Schema::builder();
        schema_builder.add_text_field("body", TEXT);
        let schema = schema_builder.build();
        let field = schema.get_field("body").unwrap();
        let field_entry = schema.get_field_entry(field);
        assert!(matches!(field_entry.field_type(),
                FieldType::Str(text_options)
                if text_options.get_indexing_options().unwrap().tokenizer() == "default"));
    }

    #[test]
    fn test_cmp_index_record_option() {
        assert!(IndexRecordOption::WithFreqsAndPositions > IndexRecordOption::WithFreqs);
        assert!(IndexRecordOption::WithFreqs > IndexRecordOption::Basic);
    }

    #[test]
    fn test_fast_field_options_composition_raw_tokenizer_gets_overridden() {
        let raw_options: TextOptions = FAST.into();
        let tokenized_options = TextOptions::default().set_fast("default");
        assert_eq!(
            (raw_options.clone() | tokenized_options.clone())
                .fast
                .unwrap()
                .tokenizer
                .as_deref(),
            Some("default")
        );
        assert_eq!(
            (tokenized_options | raw_options)
                .fast
                .unwrap()
                .tokenizer
                .as_deref(),
            Some("default")
        );
    }

    #[test]
    fn serde_default_test() {
        let json = r#"
        {
            "indexing": {
                "record": "basic",
                "fieldnorms": true,
                "tokenizer": "default"
            },
            "stored": false
        }
        "#;
        let options: TextOptions = serde_json::from_str(json).unwrap();
        let options2: TextOptions = serde_json::from_str("{\"indexing\": {}}").unwrap();
        assert_eq!(options, options2);
        assert_eq!(options.indexing.unwrap().record, IndexRecordOption::Basic);
        let options3: TextOptions = serde_json::from_str("{}").unwrap();
        assert_eq!(options3.indexing, None);
        assert_eq!(options3.fast, None);
    }

    #[test]
    fn serde_fast_field_tokenizer() {
        let json = r#" {
            "fast": { "with_tokenizer": "default" }
        } "#;
        let options: TextOptions = serde_json::from_str(json).unwrap();
        assert_eq!(
            options.fast,
            Some(FastFieldTextOptions {
                tokenizer: Some("default".to_string())
            })
        );
        let serialized = serde_json::to_value(&options).unwrap();
        assert_eq!(
            serialized["fast"],
            serde_json::json!({ "with_tokenizer": "default" })
        );
        let options: TextOptions = serde_json::from_value(serialized).unwrap();
        assert_eq!(
            options.fast,
            Some(FastFieldTextOptions {
                tokenizer: Some("default".to_string())
            })
        );

        let json = r#" {
            "fast": true
        } "#;
        let options: TextOptions = serde_json::from_str(json).unwrap();
        assert_eq!(options.fast, Some(FastFieldTextOptions { tokenizer: None }));
        let serialized = serde_json::to_value(&options).unwrap();
        assert_eq!(serialized["fast"], serde_json::json!(true));
        let options: TextOptions = serde_json::from_value(serialized).unwrap();
        assert_eq!(options.fast, Some(FastFieldTextOptions { tokenizer: None }));

        let json = r#" {
            "fast": false
        } "#;
        let options: TextOptions = serde_json::from_str(json).unwrap();
        assert_eq!(options.fast, None);
        let serialized = serde_json::to_value(&options).unwrap();
        assert_eq!(serialized["fast"], serde_json::json!(false));
        let options: TextOptions = serde_json::from_value(serialized).unwrap();
        assert_eq!(options.fast, None);
    }

    #[test]
    fn serde_fast_field_explicit_raw_tokenizer_is_kept() {
        let options = TextOptions::default().set_fast("raw");
        assert_eq!(options.get_fast_field_tokenizer_name(), Some("raw"));
        let serialized = serde_json::to_value(&options).unwrap();
        assert_eq!(
            serialized["fast"],
            serde_json::json!({ "with_tokenizer": "raw" })
        );
        let options: TextOptions = serde_json::from_value(serialized).unwrap();
        assert_eq!(options.get_fast_field_tokenizer_name(), Some("raw"));

        let fast_flag_options: TextOptions = FAST.into();
        assert!(fast_flag_options.is_fast());
        assert_eq!(fast_flag_options.get_fast_field_tokenizer_name(), None);
    }

    #[test]
    fn test_set_fast_no_tokenizer() {
        let options = TextOptions::default().set_fast_no_tokenizer();
        assert!(options.is_fast());
        assert_eq!(options.get_fast_field_tokenizer_name(), None);
        let fast_flag_options: TextOptions = FAST.into();
        assert_eq!(options.fast, fast_flag_options.fast);
        let serialized = serde_json::to_value(&options).unwrap();
        assert_eq!(serialized["fast"], serde_json::json!(true));
    }
}

use std::borrow::Cow;
use std::io;
use std::io::Write;

use columnar::MonotonicallyMappableToU128;
use common::{f64_to_u64, BinarySerializable, VInt};

use crate::schema::document::{type_codes, DocValue, DocumentAccess, ReferenceValue};
use crate::schema::Schema;

/// A serializer writing documents which implement [`DocumentAccess`] to a provided writer.
pub struct DocumentSerializer<'se, W> {
    writer: &'se mut W,
    schema: &'se Schema,
}

impl<'se, W> DocumentSerializer<'se, W>
where W: Write
{
    /// Creates a new serializer with a provided writer.
    pub(crate) fn new(writer: &'se mut W, schema: &'se Schema) -> Self {
        Self { writer, schema }
    }

    /// Attempts to serialize a given document and write the output
    /// to the writer.
    pub(crate) fn serialize_doc<D>(&mut self, doc: &D) -> io::Result<()>
    where D: DocumentAccess {
        let stored_field_values = || {
            doc.iter_fields_and_values()
                .filter(|(field, _)| self.schema.get_field_entry(*field).is_stored())
        };
        let num_field_values = stored_field_values().count();
        let mut actual_length = 0;

        VInt(num_field_values as u64).serialize(self.writer)?;
        for (field, value_access) in stored_field_values() {
            field.serialize(self.writer)?;

            let mut serializer = ValueSerializer::new(self.writer);
            serializer.serialize_value(value_access.as_value())?;

            actual_length += 1;
        }

        if num_field_values != actual_length {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!(
                    "Unexpected number of entries written to serializer, expected {} entries, got \
                     {} entries",
                    num_field_values, actual_length,
                ),
            ));
        }

        Ok(())
    }
}

/// A serializer for a single value.
pub struct ValueSerializer<'se, W> {
    writer: &'se mut W,
}

impl<'se, W> ValueSerializer<'se, W>
where W: Write
{
    /// Creates a new serializer with a provided writer.
    pub(crate) fn new(writer: &'se mut W) -> Self {
        Self { writer }
    }

    /// Attempts to serialize a given value and write the output
    /// to the writer.
    pub(crate) fn serialize_value<'a, V>(
        &mut self,
        value: ReferenceValue<'a, V>,
    ) -> io::Result<()>
    where
        V: DocValue<'a>,
    {
        match value {
            ReferenceValue::Null => self.write_type_code(type_codes::NULL_CODE),
            ReferenceValue::Str(val) => {
                self.write_type_code(type_codes::TEXT_CODE)?;

                let temp_val = Cow::Borrowed(val);
                temp_val.serialize(self.writer)
            }
            ReferenceValue::U64(val) => {
                self.write_type_code(type_codes::U64_CODE)?;

                val.serialize(self.writer)
            }
            ReferenceValue::I64(val) => {
                self.write_type_code(type_codes::I64_CODE)?;

                val.serialize(self.writer)
            }
            ReferenceValue::F64(val) => {
                self.write_type_code(type_codes::F64_CODE)?;

                f64_to_u64(val).serialize(self.writer)
            }
            ReferenceValue::Date(val) => {
                self.write_type_code(type_codes::DATE_CODE)?;
                val.serialize(self.writer)
            }
            ReferenceValue::Facet(val) => {
                self.write_type_code(type_codes::HIERARCHICAL_FACET_CODE)?;

                val.serialize(self.writer)
            }
            ReferenceValue::Bytes(val) => {
                self.write_type_code(type_codes::BYTES_CODE)?;

                let temp_val = Cow::Borrowed(val);
                temp_val.serialize(self.writer)
            }
            ReferenceValue::IpAddr(val) => {
                self.write_type_code(type_codes::IP_CODE)?;

                val.to_u128().serialize(self.writer)
            }
            ReferenceValue::Bool(val) => {
                self.write_type_code(type_codes::BOOL_CODE)?;

                val.serialize(self.writer)
            }
            ReferenceValue::PreTokStr(val) => {
                self.write_type_code(type_codes::EXT_CODE)?;
                self.write_type_code(type_codes::TOK_STR_EXT_CODE)?;

                val.serialize(self.writer)
            }
            ReferenceValue::Array(elements) => {
                self.write_type_code(type_codes::ARRAY_CODE)?;

                // Somewhat unfortunate that we do this here however, writing the
                // length at the end of the complicates things quite considerably.
                let elements: Vec<ReferenceValue<'_, V::ChildValue>> = elements.collect();

                let mut serializer = ArraySerializer::begin(elements.len(), self.writer)?;

                for value in elements {
                    serializer.serialize_value(value)?;
                }

                serializer.end()
            }
            ReferenceValue::Object(object) => {
                self.write_type_code(type_codes::OBJECT_CODE)?;

                // Somewhat unfortunate that we do this here however, writing the
                // length at the end of the complicates things quite considerably.
                let entries: Vec<(&str, ReferenceValue<'_, V::ChildValue>)> = object.collect();

                let mut serializer = ObjectSerializer::begin(entries.len(), self.writer)?;

                for (key, value) in entries {
                    serializer.serialize_entry(key, value)?;
                }

                serializer.end()
            }
        }
    }

    fn write_type_code(&mut self, code: u8) -> io::Result<()> {
        code.serialize(self.writer)
    }
}

/// A serializer for writing a sequence of values to a writer.
pub struct ArraySerializer<'se, W> {
    writer: &'se mut W,
    expected_length: usize,
    actual_length: usize,
}

impl<'se, W> ArraySerializer<'se, W>
where W: Write
{
    /// Creates a new array serializer and writes the length of the array to the writer.
    pub(crate) fn begin(length: usize, writer: &'se mut W) -> io::Result<Self> {
        VInt(length as u64).serialize(writer)?;

        Ok(Self {
            writer,
            expected_length: length,
            actual_length: 0,
        })
    }

    /// Attempts to serialize a given value and write the output
    /// to the writer.
    pub(crate) fn serialize_value<'a, V>(
        &mut self,
        value: ReferenceValue<'a, V>,
    ) -> io::Result<()>
    where
        V: DocValue<'a>,
    {
        let mut serializer = ValueSerializer::new(self.writer);
        serializer.serialize_value(value)?;

        self.actual_length += 1;
        Ok(())
    }

    /// Finishes writing the array to the writer and validates it.
    pub(crate) fn end(self) -> io::Result<()> {
        if self.expected_length != self.actual_length {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!(
                    "Unexpected number of entries written to serializer, expected {} entries, got \
                     {} entries",
                    self.expected_length, self.actual_length,
                ),
            ));
        }
        Ok(())
    }
}

/// A serializer for writing a set of key-value pairs to a writer.
pub struct ObjectSerializer<'se, W> {
    inner: ArraySerializer<'se, W>,
    expected_length: usize,
    actual_length: usize,
}

impl<'se, W> ObjectSerializer<'se, W>
where W: Write
{
    /// Creates a new object serializer and writes the length of the object to the writer.
    pub(crate) fn begin(length: usize, writer: &'se mut W) -> io::Result<Self> {
        // We mul by 2 here to count the keys and values separately:
        // [("a", 1), ("b", 2)] is actually stored as ["a", 1, "b", 2]
        let inner = ArraySerializer::begin(length * 2, writer)?;

        Ok(Self {
            inner,
            expected_length: length,
            actual_length: 0,
        })
    }

    /// Attempts to serialize a given value and write the output
    /// to the writer.
    pub(crate) fn serialize_entry<'a, V>(
        &mut self,
        key: &'a str,
        value: ReferenceValue<'a, V>,
    ) -> io::Result<()>
    where
        V: DocValue<'a>,
    {
        // Keys and values are stored inline with one another.
        // Technically this isn't the *most* optimal way of storing the objects
        // as we could avoid writing the extra byte per key. But the gain is
        // largely not worth it for the extra complexity it brings.
        self.inner
            .serialize_value(ReferenceValue::<'a, V>::Str(key))?;
        self.inner.serialize_value(value)?;

        self.actual_length += 1;
        Ok(())
    }

    /// Finishes writing the array to the writer and validates it.
    pub(crate) fn end(self) -> io::Result<()> {
        if self.expected_length != self.actual_length {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!(
                    "Unexpected number of entries written to serializer, expected {} entries, got \
                     {} entries",
                    self.expected_length, self.actual_length,
                ),
            ));
        }

        // This should never fail if the above statement is valid.
        self.inner.end()?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use common::DateTime;
    use serde_json::Number;
    use tokenizer_api::Token;

    use super::*;
    use crate::schema::document::helpers::{JsonArrayIter, JsonObjectIter};
    use crate::schema::{Facet, Field, FAST, STORED, TEXT};
    use crate::tokenizer::PreTokenizedString;

    fn serialize_value<'a>(value: ReferenceValue<'a, &'a serde_json::Value>) -> Vec<u8> {
        let mut writer = Vec::new();

        let mut serializer = ValueSerializer::new(&mut writer);
        serializer.serialize_value(value).expect("Serialize value");

        writer
    }

    /// A macro for defining the expected binary representation
    /// of the serialized values in a somewhat human readable way.
    macro_rules! binary_repr {
        ($( $type_code:expr $(, $ext_code:expr)? => $value:expr $(,)?)*) => {{
            let mut writer = Vec::new();

            $(
                $type_code.serialize(&mut writer).unwrap();

                $(
                    $ext_code.serialize(&mut writer).unwrap();
                )?

                $value.serialize(&mut writer).unwrap();
            )*

            writer
        }};
        (collection $code:expr, length $len:expr, $( $type_code:expr $(, $ext_code:expr)? => $value:expr $(,)?)*) => {{
            let mut writer = Vec::new();

            $code.serialize(&mut writer).unwrap();
            VInt($len as u64).serialize(&mut writer).unwrap();

            $(
                $type_code.serialize(&mut writer).unwrap();

                $(
                    $ext_code.serialize(&mut writer).unwrap();
                )?

                $value.serialize(&mut writer).unwrap();
            )*

            writer
        }};
    }

    #[test]
    fn test_simple_value_serialize() {
        let result = serialize_value(ReferenceValue::Null);
        let expected = binary_repr!(
            type_codes::NULL_CODE => (),
        );
        assert_eq!(
            result, expected,
            "Expected serialized value to match the binary representation"
        );

        let result = serialize_value(ReferenceValue::Str("hello, world"));
        let expected = binary_repr!(
            type_codes::TEXT_CODE => String::from("hello, world"),
        );
        assert_eq!(
            result, expected,
            "Expected serialized value to match the binary representation"
        );

        let result = serialize_value(ReferenceValue::U64(123));
        let expected = binary_repr!(
            type_codes::U64_CODE => 123u64,
        );
        assert_eq!(
            result, expected,
            "Expected serialized value to match the binary representation"
        );

        let result = serialize_value(ReferenceValue::I64(-123));
        let expected = binary_repr!(
            type_codes::I64_CODE => -123i64,
        );
        assert_eq!(
            result, expected,
            "Expected serialized value to match the binary representation"
        );

        let result = serialize_value(ReferenceValue::F64(123.3845));
        let expected = binary_repr!(
            type_codes::F64_CODE => f64_to_u64(123.3845f64),
        );
        assert_eq!(
            result, expected,
            "Expected serialized value to match the binary representation"
        );

        let result = serialize_value(ReferenceValue::Bool(false));
        let expected = binary_repr!(
            type_codes::BOOL_CODE => false,
        );
        assert_eq!(
            result, expected,
            "Expected serialized value to match the binary representation"
        );

        let result = serialize_value(ReferenceValue::Date(DateTime::MAX));
        let expected = binary_repr!(
            type_codes::DATE_CODE => DateTime::MAX,
        );
        assert_eq!(
            result, expected,
            "Expected serialized value to match the binary representation"
        );

        let facet = Facet::from_text("/hello/world").unwrap();
        let result = serialize_value(ReferenceValue::Facet(&facet));
        let expected = binary_repr!(
            type_codes::HIERARCHICAL_FACET_CODE => Facet::from_text("/hello/world").unwrap(),
        );
        assert_eq!(
            result, expected,
            "Expected serialized value to match the binary representation"
        );

        let pre_tok_str = PreTokenizedString {
            text: "hello, world".to_string(),
            tokens: vec![Token::default(), Token::default()],
        };
        let result = serialize_value(ReferenceValue::PreTokStr(&pre_tok_str));
        let expected = binary_repr!(
            type_codes::EXT_CODE, type_codes::TOK_STR_EXT_CODE => pre_tok_str,
        );
        assert_eq!(
            result, expected,
            "Expected serialized value to match the binary representation"
        );
    }

    #[test]
    fn test_array_serialize() {
        let elements = vec![serde_json::Value::Null, serde_json::Value::Null];
        let result = serialize_value(ReferenceValue::Array(JsonArrayIter(elements.iter())));
        let expected = binary_repr!(
            collection type_codes::ARRAY_CODE,
            length elements.len(),
            type_codes::NULL_CODE => (),
            type_codes::NULL_CODE => (),
        );
        assert_eq!(
            result, expected,
            "Expected serialized value to match the binary representation"
        );

        let elements = vec![
            serde_json::Value::String("Hello, world".into()),
            serde_json::Value::String("Some demo".into()),
        ];
        let result = serialize_value(ReferenceValue::Array(JsonArrayIter(elements.iter())));
        let expected = binary_repr!(
            collection type_codes::ARRAY_CODE,
            length elements.len(),
            type_codes::TEXT_CODE => String::from("Hello, world"),
            type_codes::TEXT_CODE => String::from("Some demo"),
        );
        assert_eq!(
            result, expected,
            "Expected serialized value to match the binary representation"
        );

        let elements = vec![];
        let result = serialize_value(ReferenceValue::Array(JsonArrayIter(elements.iter())));
        let expected = binary_repr!(
            collection type_codes::ARRAY_CODE,
            length elements.len(),
        );
        assert_eq!(
            result, expected,
            "Expected serialized value to match the binary representation"
        );

        let elements = vec![
            serde_json::Value::Null,
            serde_json::Value::String("Hello, world".into()),
            serde_json::Value::Number(12345.into()),
        ];
        let result = serialize_value(ReferenceValue::Array(JsonArrayIter(elements.iter())));
        let expected = binary_repr!(
            collection type_codes::ARRAY_CODE,
            length elements.len(),
            type_codes::NULL_CODE => (),
            type_codes::TEXT_CODE => String::from("Hello, world"),
            type_codes::U64_CODE => 12345u64,
        );
        assert_eq!(
            result, expected,
            "Expected serialized value to match the binary representation"
        );
    }

    #[test]
    fn test_object_serialize() {
        let mut object = serde_json::Map::new();
        object.insert(
            "my-first-key".into(),
            serde_json::Value::String("Hello".into()),
        );
        object.insert("my-second-key".into(), serde_json::Value::Null);
        object.insert(
            "my-third-key".into(),
            serde_json::Value::Number(Number::from_f64(123.0).unwrap()),
        );
        let result = serialize_value(ReferenceValue::Object(JsonObjectIter(object.iter())));
        let expected = binary_repr!(
            collection type_codes::OBJECT_CODE,
            length object.len() * 2,  // To account for keys counting towards the length
            type_codes::TEXT_CODE => String::from("my-first-key"),
            type_codes::TEXT_CODE => String::from("Hello"),
            type_codes::TEXT_CODE => String::from("my-second-key"),
            type_codes::NULL_CODE => (),
            type_codes::TEXT_CODE => String::from("my-third-key"),
            type_codes::F64_CODE => f64_to_u64(123.0),
        );
        assert_eq!(
            result, expected,
            "Expected serialized value to match the binary representation"
        );

        let object = serde_json::Map::new();
        let result = serialize_value(ReferenceValue::Object(JsonObjectIter(object.iter())));
        let expected = binary_repr!(
            collection type_codes::OBJECT_CODE,
            length object.len(),
        );
        assert_eq!(
            result, expected,
            "Expected serialized value to match the binary representation"
        );

        let mut object = serde_json::Map::new();
        object.insert("my-first-key".into(), serde_json::Value::Null);
        object.insert("my-second-key".into(), serde_json::Value::Null);
        object.insert("my-third-key".into(), serde_json::Value::Null);
        let result = serialize_value(ReferenceValue::Object(JsonObjectIter(object.iter())));
        let expected = binary_repr!(
            collection type_codes::OBJECT_CODE,
            length object.len() * 2, // To account for keys counting towards the length
            type_codes::TEXT_CODE => String::from("my-first-key"),
            type_codes::NULL_CODE => (),
            type_codes::TEXT_CODE => String::from("my-second-key"),
            type_codes::NULL_CODE => (),
            type_codes::TEXT_CODE => String::from("my-third-key"),
            type_codes::NULL_CODE => (),
        );
        assert_eq!(
            result, expected,
            "Expected serialized value to match the binary representation"
        );
    }

    #[test]
    fn test_nested_serialize() {
        let mut object = serde_json::Map::new();
        object.insert(
            "my-array".into(),
            serde_json::Value::Array(vec![
                serde_json::Value::Null,
                serde_json::Value::String(String::from("bobby of the sea")),
            ]),
        );
        object.insert(
            "my-object".into(),
            serde_json::Value::Object(
                vec![
                    (
                        "inner-1".to_string(),
                        serde_json::Value::Number((-123i64).into()),
                    ),
                    (
                        "inner-2".to_string(),
                        serde_json::Value::String(String::from("bobby of the sea 2")),
                    ),
                ]
                .into_iter()
                .collect(),
            ),
        );
        let result = serialize_value(ReferenceValue::Object(JsonObjectIter(object.iter())));

        let mut expected = Vec::new();
        let header = binary_repr!(
            collection type_codes::OBJECT_CODE,
            length object.len() * 2,
        );
        expected.extend_from_slice(&header);
        expected
            .extend_from_slice(&binary_repr!(type_codes::TEXT_CODE => String::from("my-array")));
        let nested_array = binary_repr!(
            collection type_codes::ARRAY_CODE,
            length 2,
            type_codes::NULL_CODE => (),
            type_codes::TEXT_CODE => String::from("bobby of the sea"),
        );
        expected.extend_from_slice(&nested_array);
        expected
            .extend_from_slice(&binary_repr!(type_codes::TEXT_CODE => String::from("my-object")));
        let nested_object = binary_repr!(
            collection type_codes::OBJECT_CODE,
            length 4,   // 2 keys, 2 values
            type_codes::TEXT_CODE => String::from("inner-1"),
            type_codes::I64_CODE => -123i64,
            type_codes::TEXT_CODE => String::from("inner-2"),
            type_codes::TEXT_CODE => String::from("bobby of the sea 2"),
        );
        expected.extend_from_slice(&nested_object);
        assert_eq!(
            result, expected,
            "Expected serialized value to match the binary representation"
        );

        // Some more extreme nesting that might behave weirdly
        let mut object = serde_json::Map::new();
        object.insert(
            "my-array".into(),
            serde_json::Value::Array(vec![serde_json::Value::Array(vec![
                serde_json::Value::Array(vec![]),
                serde_json::Value::Array(vec![serde_json::Value::Null]),
            ])]),
        );
        let result = serialize_value(ReferenceValue::Object(JsonObjectIter(object.iter())));

        let mut expected = Vec::new();
        let header = binary_repr!(
            collection type_codes::OBJECT_CODE,
            length object.len() * 2,
        );
        expected.extend_from_slice(&header);
        expected
            .extend_from_slice(&binary_repr!(type_codes::TEXT_CODE => String::from("my-array")));
        let nested_array = binary_repr!(
            collection type_codes::ARRAY_CODE,
            length 1,
        );
        expected.extend_from_slice(&nested_array);
        let nested_array = binary_repr!(
            collection type_codes::ARRAY_CODE,
            length 2,
        );
        expected.extend_from_slice(&nested_array);
        let nested_array = binary_repr!(
            collection type_codes::ARRAY_CODE,
            length 0,
        );
        expected.extend_from_slice(&nested_array);
        let nested_array = binary_repr!(
            collection type_codes::ARRAY_CODE,
            length 1,
            type_codes::NULL_CODE => (),
        );
        expected.extend_from_slice(&nested_array);
        assert_eq!(
            result, expected,
            "Expected serialized value to match the binary representation"
        );
    }

    fn serialize_doc<D: DocumentAccess>(doc: &D, schema: &Schema) -> Vec<u8> {
        let mut writer = Vec::new();

        let mut serializer = DocumentSerializer::new(&mut writer, schema);
        serializer.serialize_doc(doc).expect("Serialize value");

        writer
    }

    /// A helper macro for generating the expected binary representation of the document.
    macro_rules! expected_doc_data {
        (length $len:expr) => {{
            let mut writer = Vec::new();
            VInt($len as u64).serialize(&mut writer).unwrap();
            writer
        }};
        (length $len:expr, $( $field_id:expr => $value:expr $(,)?)*) => {{
            let mut writer = Vec::new();

            VInt($len as u64).serialize(&mut writer).unwrap();
            $(
                $field_id.serialize(&mut writer).unwrap();
                $value.serialize(&mut writer).unwrap();
            )*

            writer
        }};
    }

    #[test]
    fn test_document_serialize() {
        let mut builder = Schema::builder();
        let name = builder.add_text_field("name", TEXT | STORED);
        let age = builder.add_u64_field("age", FAST | STORED);
        let schema = builder.build();

        let mut document = BTreeMap::new();
        document.insert(name, crate::schema::Value::Str("ChillFish8".into()));
        document.insert(age, crate::schema::Value::U64(20));

        let result = serialize_doc(&document, &schema);
        let mut expected = expected_doc_data!(length document.len());
        name.serialize(&mut expected).unwrap();
        expected
            .extend_from_slice(&binary_repr!(type_codes::TEXT_CODE => String::from("ChillFish8")));
        age.serialize(&mut expected).unwrap();
        expected.extend_from_slice(&binary_repr!(type_codes::U64_CODE => 20u64));
        assert_eq!(
            result, expected,
            "Expected serialized document to match the binary representation"
        );

        let mut builder = Schema::builder();
        let name = builder.add_text_field("name", TEXT | STORED);
        // This should be skipped when serializing.
        let age = builder.add_u64_field("age", FAST);
        let schema = builder.build();

        let mut document = BTreeMap::new();
        document.insert(name, crate::schema::Value::Str("ChillFish8".into()));
        document.insert(age, crate::schema::Value::U64(20));

        let result = serialize_doc(&document, &schema);
        let mut expected = expected_doc_data!(length 1);
        name.serialize(&mut expected).unwrap();
        expected
            .extend_from_slice(&binary_repr!(type_codes::TEXT_CODE => String::from("ChillFish8")));
        assert_eq!(
            result, expected,
            "Expected serialized document to match the binary representation"
        );

        let builder = Schema::builder();
        let schema = builder.build();
        let document = BTreeMap::<Field, crate::schema::Value>::new();
        let result = serialize_doc(&document, &schema);
        let expected = expected_doc_data!(length document.len());
        assert_eq!(
            result, expected,
            "Expected serialized document to match the binary representation"
        );
    }
}

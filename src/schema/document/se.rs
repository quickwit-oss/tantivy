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

                let timestamp_micros = val.into_timestamp_micros();
                timestamp_micros.serialize(self.writer)
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

                if let Ok(text) = serde_json::to_string(val) {
                    text.serialize(self.writer)
                } else {
                    Err(io::Error::new(
                        io::ErrorKind::Other,
                        "Failed to dump PreTokenizedString to json.",
                    ))
                }
            }
            ReferenceValue::Array(elements) => {
                self.write_type_code(type_codes::ARRAY_CODE)?;

                // Somewhat unfortunate that we do this here however, writing the
                // length at the end of the complicates things quite considerably.
                let elements: Vec<ReferenceValue<'_, V>> = elements.collect();

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
                let entries: Vec<(&str, ReferenceValue<'_, V>)> = object.collect();

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
    pub(crate) fn end(mut self) -> io::Result<()> {
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

    fn write_type_code(&mut self, code: u8) -> io::Result<()> {
        code.serialize(self.writer)
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
    pub(crate) fn end(mut self) -> io::Result<()> {
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

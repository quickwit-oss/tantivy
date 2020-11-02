use crate::DocId;

use super::fieldnorm_to_id;
use super::FieldNormsSerializer;
use crate::schema::Field;
use crate::schema::Schema;
use std::io;

/// The `FieldNormsWriter` is in charge of tracking the fieldnorm byte
/// of each document for each field with field norms.
///
/// `FieldNormsWriter` stores a Vec<u8> for each tracked field, using a
/// byte per document per field.
pub struct FieldNormsWriter {
    fields: Vec<Field>,
    fieldnorms_buffer: Vec<Option<Vec<u8>>>,
}

impl FieldNormsWriter {
    /// Returns the fields that should have field norms computed
    /// according to the given schema.
    pub(crate) fn fields_with_fieldnorm(schema: &Schema) -> Vec<Field> {
        schema
            .fields()
            .filter_map(|(field, field_entry)| {
                if field_entry.has_fieldnorms() {
                    Some(field)
                } else {
                    None
                }
            })
            .collect::<Vec<_>>()
    }

    /// Initialize with state for tracking the field norm fields
    /// specified in the schema.
    pub fn for_schema(schema: &Schema) -> FieldNormsWriter {
        let fields = FieldNormsWriter::fields_with_fieldnorm(schema);
        let num_fields = schema.num_fields();
        let mut fieldnorms_buffer: Vec<Option<Vec<u8>>> = vec![None; num_fields];
        for field in &fields {
            fieldnorms_buffer[field.field_id() as usize] = Some(Vec::new());
        }
        FieldNormsWriter {
            fields,
            fieldnorms_buffer,
        }
    }

    /// Ensure that all documents in 0..max_doc have a byte associated with them
    /// in each of the fieldnorm vectors.
    ///
    /// Will extend with 0-bytes for documents that have not been seen.
    pub fn fill_up_to_max_doc(&mut self, max_doc: DocId) {
        for buffer_opt in self.fieldnorms_buffer.iter_mut() {
            if let Some(buffer) = buffer_opt {
                buffer.resize(max_doc as usize, 0u8);
            }
        }
    }

    /// Set the fieldnorm byte for the given document for the given field.
    ///
    /// Will internally convert the u32 `fieldnorm` value to the appropriate byte
    /// to approximate the field norm in less space.
    ///
    /// * doc       - the document id
    /// * field     - the field being set
    /// * fieldnorm - the number of terms present in document `doc` in field `field`
    pub fn record(&mut self, doc: DocId, field: Field, fieldnorm: u32) {
        if let Some(fieldnorm_buffer) = self.fieldnorms_buffer[field.field_id() as usize].as_mut() {
            assert!(
                fieldnorm_buffer.len() <= doc as usize,
                "Cannot register a given fieldnorm twice" // we fill intermediary `DocId` as  having a fieldnorm of 0.
            );
            fieldnorm_buffer.resize(doc as usize + 1, 0u8);
            fieldnorm_buffer[doc as usize] = fieldnorm_to_id(fieldnorm);
        }
    }

    /// Serialize the seen fieldnorm values to the serializer for all fields.
    pub fn serialize(&self, mut fieldnorms_serializer: FieldNormsSerializer) -> io::Result<()> {
        for &field in self.fields.iter() {
            if let Some(buffer) = self.fieldnorms_buffer[field.field_id() as usize].as_ref() {
                fieldnorms_serializer.serialize_field(field, &buffer[..])?;
            }
        }
        fieldnorms_serializer.close()?;
        Ok(())
    }
}

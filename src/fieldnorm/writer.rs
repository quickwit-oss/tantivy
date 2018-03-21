use DocId;

use schema::Field;
use super::FieldNormsSerializer;
use std::io;
use schema::Schema;
use super::fieldnorm_to_id;

pub struct FieldNormsWriter {
    fields: Vec<Field>,
    fieldnorms_buffer: Vec<Vec<u8>>
}

impl FieldNormsWriter {

    pub fn fields_with_fieldnorm(schema: &Schema) -> Vec<Field> {
        schema
            .fields()
            .iter()
            .enumerate()
            .filter(|&(_, field_entry)| {
                field_entry.is_indexed()
            })
            .map(|(field, _)| Field(field as u32))
            .collect::<Vec<Field>>()
    }

    pub fn for_schema(schema: &Schema) -> FieldNormsWriter {
        let fields = FieldNormsWriter::fields_with_fieldnorm(schema);
        let max_field = fields
            .iter()
            .map(|field| field.0)
            .max()
            .map(|max_field_id| max_field_id as usize + 1)
            .unwrap_or(0);
        FieldNormsWriter {
            fields,
            fieldnorms_buffer: (0..max_field)
                .map(|_| Vec::new())
                .collect::<Vec<_>>()
        }
    }

    pub fn fill_up_to_max_doc(&mut self, max_doc: DocId) {
        for &field in self.fields.iter() {
            self.fieldnorms_buffer[field.0 as usize].resize(max_doc as usize, 0u8);
        }
    }

    pub fn record(&mut self, doc: DocId, field: Field, fieldnorm: u32) {
        let fieldnorm_buffer: &mut Vec<u8> = &mut self.fieldnorms_buffer[field.0 as usize];
        assert!(fieldnorm_buffer.len() <= doc as usize, "Cannot register a given fieldnorm twice");
        // we fill intermediary `DocId` as  having a fieldnorm of 0.
        fieldnorm_buffer.resize(doc as usize + 1, 0u8);
        fieldnorm_buffer[doc as usize] = fieldnorm_to_id(fieldnorm);
    }

    pub fn serialize(&self, fieldnorms_serializer: &mut FieldNormsSerializer) -> io::Result<()> {
        for &field in self.fields.iter() {
            let fieldnorm_values: &[u8] = &self.fieldnorms_buffer[field.0 as usize][..];
            fieldnorms_serializer.serialize_field(field, fieldnorm_values)?;
        }
        Ok(())
    }
}
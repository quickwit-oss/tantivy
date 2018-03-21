use DocId;
use super::fieldnorm_to_id;
use schema::Field;

pub struct FieldNormsWriter {
    fieldnorms_buffer: Vec<Vec<u8>>
}

impl FieldNormsWriter {
    pub fn new(num_fields: usize) -> FieldNormsWriter {
        FieldNormsWriter {
            fieldnorms_buffer: (0..num_fields)
                .map(|_| Vec::new())
                .collect::<Vec<_>>()
        }
    }

    pub fn record(&mut self, doc: DocId, field: Field, fieldnorm: u32) {
        let fieldnorm_buffer: &mut Vec<u8> = &mut self.fieldnorms_buffer[field.0 as usize];
        assert!(fieldnorm_buffer.len() < doc as usize, "Cannot register a given fieldnorm twice");
        // we fill intermediary `DocId` as  having a fieldnorm of 0.
        fieldnorm_buffer.resize(doc as usize, 0u8);
        fieldnorm_buffer[doc as usize] = fieldnorm_to_id(fieldnorm);
    }
//
//    pub fn serialize(self) {
//
//    }
}
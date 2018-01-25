use fastfield::FastFieldSerializer;
use std::collections::HashMap;
use postings::UnorderedTermId;
use schema::Field;
use std::io;

pub struct MultiValueIntFastFieldWriter {
    field: Field,
    vals: Vec<UnorderedTermId>,
    doc_index: Vec<u64>,
}

impl MultiValueIntFastFieldWriter {
    /// Creates a new `IntFastFieldWriter`
    pub fn new(field: Field) -> Self {
        MultiValueIntFastFieldWriter {
            field: field,
            vals: Vec::new(),
            doc_index: Vec::new(),
        }
    }

    pub fn field(&self) -> Field {
        self.field
    }

    pub fn next_doc(&mut self) {
        self.doc_index.push(self.vals.len() as u64);
    }

    /// Records a new value.
    ///
    /// The n-th value being recorded is implicitely
    /// associated to the document with the `DocId` n.
    /// (Well, `n-1` actually because of 0-indexing)
    pub fn add_val(&mut self, val: UnorderedTermId) {
        self.vals.push(val);
    }

    /// Push the fast fields value to the `FastFieldWriter`.
    pub fn serialize(&self, serializer: &mut FastFieldSerializer, mapping: &HashMap<UnorderedTermId, usize>) -> io::Result<()> {
        {
            // writing the offset index
            let mut doc_index_serializer = serializer.new_u64_fast_field_with_idx(self.field, 0, self.vals.len() as u64, 0)?;
            for &offset in &self.doc_index {
                doc_index_serializer.add_val(offset)?;
            }
            doc_index_serializer.add_val(self.vals.len() as u64)?;
            doc_index_serializer.close_field()?;
        }
        {
            // writing the values themselves.
            let mut value_serializer = serializer.new_u64_fast_field_with_idx(self.field, 0u64, mapping.len() as u64, 1)?;
            for val in &self.vals {
                value_serializer.add_val(*mapping.get(val).expect("Missing term ordinal") as u64)?;
            }
            value_serializer.close_field()?;
        }
        Ok(())

    }
}

use fastfield::FastFieldSerializer;
use fastfield::serializer::FastSingleFieldSerializer;
use fastfield::value_to_u64;
use std::collections::HashMap;
use postings::UnorderedTermId;
use schema::{Document, Field};
use std::io;
use itertools::Itertools;

pub struct MultiValueIntFastFieldWriter {
    field: Field,
    vals: Vec<u64>,
    doc_index: Vec<u64>,
    is_facet: bool,
}

impl MultiValueIntFastFieldWriter {
    /// Creates a new `IntFastFieldWriter`
    pub fn new(field: Field, is_facet: bool) -> Self {
        MultiValueIntFastFieldWriter {
            field,
            vals: Vec::new(),
            doc_index: Vec::new(),
            is_facet,
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

    pub fn add_document(&mut self, doc: &Document) {
        if !self.is_facet {
            for field_value in doc.field_values() {
                if field_value.field() == self.field {
                    self.add_val(value_to_u64(field_value.value()));
                }
            }
        }
    }

    /// Serializes fast field values by pushing them to the `FastFieldSerializer`.
    ///
    /// HashMap makes it possible to remap them before serializing.
    /// Specifically, string terms are first stored in the writer as their
    /// position in the `IndexWriter`'s `HashMap`. This value is called
    /// an `UnorderedTermId`.
    ///
    /// During the serialization of the segment, terms gets sorted and
    /// `tantivy` builds a mapping to convert this `UnorderedTermId` into
    /// term ordinals.
    ///
    pub fn serialize(
        &self,
        serializer: &mut FastFieldSerializer,
        mapping_opt: Option<&HashMap<UnorderedTermId, usize>>,
    ) -> io::Result<()> {
        {
            // writing the offset index
            let mut doc_index_serializer =
                serializer.new_u64_fast_field_with_idx(self.field, 0, self.vals.len() as u64, 0)?;
            for &offset in &self.doc_index {
                doc_index_serializer.add_val(offset)?;
            }
            doc_index_serializer.add_val(self.vals.len() as u64)?;
            doc_index_serializer.close_field()?;
        }
        {
            // writing the values themselves.
            let mut value_serializer: FastSingleFieldSerializer<_>;
            match mapping_opt {
                Some(mapping) => {
                    value_serializer = serializer.new_u64_fast_field_with_idx(
                        self.field,
                        0u64,
                        mapping.len() as u64,
                        1,
                    )?;
                    for val in &self.vals {
                        let remapped_val = *mapping.get(val).expect("Missing term ordinal") as u64;
                        value_serializer.add_val(remapped_val)?;
                    }
                }
                None => {
                    let val_min_max = self.vals.iter().cloned().minmax();
                    let (val_min, val_max) = val_min_max.into_option().unwrap_or((0u64, 0u64));
                    value_serializer =
                        serializer.new_u64_fast_field_with_idx(self.field, val_min, val_max, 1)?;
                    for &val in &self.vals {
                        value_serializer.add_val(val)?;
                    }
                }
            }
            value_serializer.close_field()?;
        }
        Ok(())
    }
}

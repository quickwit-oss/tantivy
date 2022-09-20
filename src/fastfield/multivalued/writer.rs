use std::io;

use fastfield_codecs::{MonotonicallyMappableToU64, VecColumn};
use fnv::FnvHashMap;

use crate::fastfield::{
    value_to_u64, CompositeFastFieldSerializer, FastFieldType, MultivalueStartIndex,
};
use crate::indexer::doc_id_mapping::DocIdMapping;
use crate::postings::UnorderedTermId;
use crate::schema::{Document, Field, Value};
use crate::termdict::TermOrdinal;
use crate::{DatePrecision, DocId};

/// Writer for multi-valued (as in, more than one value per document)
/// int fast field.
///
/// This `Writer` is only useful for advanced users.
/// The normal way to get your multivalued int in your index
/// is to
/// - declare your field with fast set to
///   [`Cardinality::MultiValues`](crate::schema::Cardinality::MultiValues) in your schema
/// - add your document simply by calling `.add_document(...)`.
///
/// The `MultiValuedFastFieldWriter` can be acquired from the fastfield writer, by calling
/// [`FastFieldWriter::get_multivalue_writer_mut()`](crate::fastfield::FastFieldsWriter::get_multivalue_writer_mut).
///
/// Once acquired, writing is done by calling
/// [`.add_document(&Document)`](MultiValuedFastFieldWriter::add_document) once per value.
///
/// The serializer makes it possible to remap all of the values
/// that were pushed to the writer using a mapping.
/// This makes it possible to push unordered term ids,
/// during indexing and remap them to their respective
/// term ids when the segment is getting serialized.
pub struct MultiValuedFastFieldWriter {
    field: Field,
    precision_opt: Option<DatePrecision>,
    vals: Vec<UnorderedTermId>,
    doc_index: Vec<u64>,
    fast_field_type: FastFieldType,
}

impl MultiValuedFastFieldWriter {
    /// Creates a new `MultiValuedFastFieldWriter`
    pub(crate) fn new(
        field: Field,
        fast_field_type: FastFieldType,
        precision_opt: Option<DatePrecision>,
    ) -> Self {
        MultiValuedFastFieldWriter {
            field,
            precision_opt,
            vals: Vec::new(),
            doc_index: Vec::new(),
            fast_field_type,
        }
    }

    /// The memory used (inclusive childs)
    pub fn mem_usage(&self) -> usize {
        self.vals.capacity() * std::mem::size_of::<UnorderedTermId>()
            + self.doc_index.capacity() * std::mem::size_of::<u64>()
    }

    /// Access the field associated to the `MultiValuedFastFieldWriter`
    pub fn field(&self) -> Field {
        self.field
    }

    /// Finalize the current document.
    pub(crate) fn next_doc(&mut self) {
        self.doc_index.push(self.vals.len() as u64);
    }

    /// Pushes a new value to the current document.
    pub(crate) fn add_val(&mut self, val: UnorderedTermId) {
        self.vals.push(val);
    }

    /// Shift to the next document and adds
    /// all of the matching field values present in the document.
    pub fn add_document(&mut self, doc: &Document) {
        self.next_doc();
        // facets/texts are indexed in the `SegmentWriter` as we encode their unordered id.
        if self.fast_field_type.is_storing_term_ids() {
            return;
        }
        for field_value in doc.field_values() {
            if field_value.field == self.field {
                let value = field_value.value();
                let value_u64 = match (self.precision_opt, value) {
                    (Some(precision), Value::Date(date_val)) => {
                        date_val.truncate(precision).to_u64()
                    }
                    _ => value_to_u64(value),
                };
                self.add_val(value_u64);
            }
        }
    }

    /// Returns an iterator over values per doc_id in ascending doc_id order.
    ///
    /// Normally the order is simply iterating self.doc_id_index.
    /// With doc_id_map it accounts for the new mapping, returning values in the order of the
    /// new doc_ids.
    fn get_ordered_values<'a: 'b, 'b>(
        &'a self,
        doc_id_map: Option<&'b DocIdMapping>,
    ) -> impl Iterator<Item = &'b [u64]> {
        let doc_id_iter: Box<dyn Iterator<Item = u32>> = if let Some(doc_id_map) = doc_id_map {
            Box::new(doc_id_map.iter_old_doc_ids())
        } else {
            let max_doc = self.doc_index.len() as DocId;
            Box::new(0..max_doc)
        };
        doc_id_iter.map(move |doc_id| self.get_values_for_doc_id(doc_id))
    }

    /// returns all values for a doc_ids
    fn get_values_for_doc_id(&self, doc_id: u32) -> &[u64] {
        let start_pos = self.doc_index[doc_id as usize] as usize;
        let end_pos = self
            .doc_index
            .get(doc_id as usize + 1)
            .cloned()
            .unwrap_or(self.vals.len() as u64) as usize; // special case, last doc_id has no offset information
        &self.vals[start_pos..end_pos]
    }
    /// Serializes fast field values by pushing them to the `FastFieldSerializer`.
    ///
    /// If a mapping is given, the values are remapped *and sorted* before serialization.
    /// This is used when serializing `facets`. Specifically their terms are
    /// first stored in the writer as their position in the `IndexWriter`'s `HashMap`.
    /// This value is called an `UnorderedTermId`.
    ///
    /// During the serialization of the segment, terms gets sorted and
    /// `tantivy` builds a mapping to convert this `UnorderedTermId` into
    /// term ordinals.
    pub fn serialize(
        mut self,
        serializer: &mut CompositeFastFieldSerializer,
        term_mapping_opt: Option<&FnvHashMap<UnorderedTermId, TermOrdinal>>,
        doc_id_map: Option<&DocIdMapping>,
    ) -> io::Result<()> {
        {
            self.doc_index.push(self.vals.len() as u64);
            let col = VecColumn::from(&self.doc_index[..]);
            if let Some(doc_id_map) = doc_id_map {
                let multi_value_start_index = MultivalueStartIndex::new(&col, doc_id_map);
                serializer.create_auto_detect_u64_fast_field_with_idx(
                    self.field,
                    multi_value_start_index,
                    0,
                )?;
            } else {
                serializer.create_auto_detect_u64_fast_field_with_idx(self.field, col, 0)?;
            }
        }
        {
            // Writing the values themselves.
            // TODO FIXME: Use less memory.
            let mut values: Vec<u64> = Vec::new();
            if let Some(term_mapping) = term_mapping_opt {
                if self.fast_field_type.is_facet() {
                    let mut doc_vals: Vec<u64> = Vec::with_capacity(100);
                    for vals in self.get_ordered_values(doc_id_map) {
                        // In the case of facets, we want a vec of facet ord that is sorted.
                        doc_vals.clear();
                        let remapped_vals = vals
                            .iter()
                            .map(|val| *term_mapping.get(val).expect("Missing term ordinal"));
                        doc_vals.extend(remapped_vals);
                        doc_vals.sort_unstable();
                        for &val in &doc_vals {
                            values.push(val);
                        }
                    }
                } else {
                    for vals in self.get_ordered_values(doc_id_map) {
                        let remapped_vals = vals
                            .iter()
                            .map(|val| *term_mapping.get(val).expect("Missing term ordinal"));
                        for val in remapped_vals {
                            values.push(val);
                        }
                    }
                }
            } else {
                for vals in self.get_ordered_values(doc_id_map) {
                    // sort values in case of remapped doc_ids?
                    for &val in vals {
                        values.push(val);
                    }
                }
            }
            let col = VecColumn::from(&values[..]);
            serializer.create_auto_detect_u64_fast_field_with_idx(self.field, col, 1)?;
        }
        Ok(())
    }
}

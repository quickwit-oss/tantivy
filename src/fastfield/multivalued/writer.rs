use std::io;

use fnv::FnvHashMap;
use tantivy_bitpacker::minmax;

use crate::fastfield::serializer::BitpackedFastFieldSerializerLegacy;
use crate::fastfield::{value_to_u64, CompositeFastFieldSerializer};
use crate::indexer::doc_id_mapping::DocIdMapping;
use crate::postings::UnorderedTermId;
use crate::schema::{Document, Field};
use crate::termdict::TermOrdinal;
use crate::DocId;

/// Writer for multi-valued (as in, more than one value per document)
/// int fast field.
///
/// This `Writer` is only useful for advanced users.
/// The normal way to get your multivalued int in your index
/// is to
/// - declare your field with fast set to `Cardinality::MultiValues`
/// in your schema
/// - add your document simply by calling `.add_document(...)`.
///
/// The `MultiValuedFastFieldWriter` can be acquired from the
/// fastfield writer, by calling
/// [`.get_multivalue_writer_mut(...)`](./struct.FastFieldsWriter.html#method.
/// get_multivalue_writer_mut).
///
/// Once acquired, writing is done by calling
/// [`.add_document_vals(&[u64])`](MultiValuedFastFieldWriter::add_document_vals) once per document.
///
/// The serializer makes it possible to remap all of the values
/// that were pushed to the writer using a mapping.
/// This makes it possible to push unordered term ids,
/// during indexing and remap them to their respective
/// term ids when the segment is getting serialized.
pub struct MultiValuedFastFieldWriter {
    field: Field,
    vals: Vec<UnorderedTermId>,
    doc_index: Vec<u64>,
    is_facet: bool,
}

impl MultiValuedFastFieldWriter {
    /// Creates a new `IntFastFieldWriter`
    pub(crate) fn new(field: Field, is_facet: bool) -> Self {
        MultiValuedFastFieldWriter {
            field,
            vals: Vec::new(),
            doc_index: Vec::new(),
            is_facet,
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
        // facets are indexed in the `SegmentWriter` as we encode their unordered id.
        if !self.is_facet {
            for field_value in doc.field_values() {
                if field_value.field == self.field {
                    self.add_val(value_to_u64(field_value.value()));
                }
            }
        }
    }

    /// Register all of the values associated to a document.
    ///
    /// The method returns the `DocId` of the document that was
    /// just written.
    pub fn add_document_vals(&mut self, vals: &[UnorderedTermId]) -> DocId {
        let doc = self.doc_index.len() as DocId;
        self.next_doc();
        self.vals.extend_from_slice(vals);
        doc
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
        &self,
        serializer: &mut CompositeFastFieldSerializer,
        mapping_opt: Option<&FnvHashMap<UnorderedTermId, TermOrdinal>>,
        doc_id_map: Option<&DocIdMapping>,
    ) -> io::Result<()> {
        {
            // writing the offset index
            let mut doc_index_serializer =
                serializer.new_u64_fast_field_with_idx(self.field, 0, self.vals.len() as u64, 0)?;

            let mut offset = 0;
            for vals in self.get_ordered_values(doc_id_map) {
                doc_index_serializer.add_val(offset)?;
                offset += vals.len() as u64;
            }
            doc_index_serializer.add_val(self.vals.len() as u64)?;

            doc_index_serializer.close_field()?;
        }
        {
            // writing the values themselves.
            let mut value_serializer: BitpackedFastFieldSerializerLegacy<'_, _>;
            match mapping_opt {
                Some(mapping) => {
                    value_serializer = serializer.new_u64_fast_field_with_idx(
                        self.field,
                        0u64,
                        mapping.len() as u64,
                        1,
                    )?;

                    let mut doc_vals: Vec<u64> = Vec::with_capacity(100);
                    for vals in self.get_ordered_values(doc_id_map) {
                        doc_vals.clear();
                        let remapped_vals = vals
                            .iter()
                            .map(|val| *mapping.get(val).expect("Missing term ordinal"));
                        doc_vals.extend(remapped_vals);
                        doc_vals.sort_unstable();
                        for &val in &doc_vals {
                            value_serializer.add_val(val)?;
                        }
                    }
                }
                None => {
                    let val_min_max = minmax(self.vals.iter().cloned());
                    let (val_min, val_max) = val_min_max.unwrap_or((0u64, 0u64));
                    value_serializer =
                        serializer.new_u64_fast_field_with_idx(self.field, val_min, val_max, 1)?;
                    for vals in self.get_ordered_values(doc_id_map) {
                        // sort values in case of remapped doc_ids?
                        for &val in vals {
                            value_serializer.add_val(val)?;
                        }
                    }
                }
            }
            value_serializer.close_field()?;
        }
        Ok(())
    }
}

use crate::fastfield::serializer::FastSingleFieldSerializer;
use crate::fastfield::FastFieldSerializer;
use crate::postings::UnorderedTermId;
use crate::schema::{Document, Field};
use crate::termdict::TermOrdinal;
use crate::DocId;
use crate::{fastfield::value_to_u64, indexer::index_sorter::DocidMapping};
use fnv::FnvHashMap;
use std::io;
use std::iter::once;

/// Writer for multi-valued (as in, more than one value per document)
/// int fast field.
///
/// This `Writer` is only useful for advanced user.
/// The normal way to get your multivalued int in your index
/// is to
/// - declare your field with fast set to `Cardinality::MultiValues`
/// in your schema
/// - add your document simply by calling `.add_document(...)`.
///
/// The `MultiValuedFastFieldWriter` can be acquired from the
/// fastfield writer, by calling [`.get_multivalue_writer(...)`](./struct.FastFieldsWriter.html#method.get_multivalue_writer).
///
/// Once acquired, writing is done by calling calls to
/// `.add_document_vals(&[u64])` once per document.
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
                if field_value.field() == self.field {
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

    /// Returns an iterator over docids in ascending order.
    ///
    /// Normally this is simply iterating self.docid_index.
    /// With docid_map it accounts for the new mapping, returning old docids, but in the order of the
    /// new docids.
    fn get_ordered_docid_iter<'a: 'b, 'b>(
        &'a self,
        docid_map: Option<&'b DocidMapping>,
    ) -> Box<dyn Iterator<Item = u32> + 'b> {
        let docid_iter = if let Some(docid_map) = docid_map {
            Box::new(docid_map.iter().cloned()) as Box<dyn Iterator<Item = u32>>
        } else {
            Box::new(self.doc_index.iter().enumerate().map(|el| el.0 as u32))
                as Box<dyn Iterator<Item = u32>>
        };
        docid_iter
    }

    /// returns all values for a docids
    fn get_values_for_docid(&self, docid: u32) -> &[u64] {
        let start_pos = self.doc_index[docid as usize] as usize;
        let end_pos = if docid as usize + 1 == self.doc_index.len() {
            self.vals.len()
        } else {
            self.doc_index[docid as usize + 1] as usize
        };
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
    ///
    pub fn serialize(
        &self,
        serializer: &mut FastFieldSerializer,
        mapping_opt: Option<&FnvHashMap<UnorderedTermId, TermOrdinal>>,
        docid_map: Option<&DocidMapping>,
    ) -> io::Result<()> {
        {
            // writing the offset index
            let mut doc_index_serializer =
                serializer.new_u64_fast_field_with_idx(self.field, 0, self.vals.len() as u64, 0)?;

            let mut offset = 0;
            let iter = self.get_ordered_docid_iter(docid_map);
            for docid in iter {
                let vals = self.get_values_for_docid(docid);
                doc_index_serializer.add_val(offset)?;
                offset += vals.len() as u64;
            }
            doc_index_serializer.add_val(self.vals.len() as u64)?;

            doc_index_serializer.close_field()?;
        }
        {
            // writing the values themselves.
            let mut value_serializer: FastSingleFieldSerializer<'_, _>;
            match mapping_opt {
                Some(mapping) => {
                    value_serializer = serializer.new_u64_fast_field_with_idx(
                        self.field,
                        0u64,
                        mapping.len() as u64,
                        1,
                    )?;

                    let mut doc_vals: Vec<u64> = Vec::with_capacity(100);
                    let docid_iter = self.get_ordered_docid_iter(docid_map);
                    for vals in docid_iter.map(|docid| self.get_values_for_docid(docid)) {
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
                    let val_min_max = crate::common::minmax(self.vals.iter().cloned());
                    let (val_min, val_max) = val_min_max.unwrap_or((0u64, 0u64));
                    value_serializer =
                        serializer.new_u64_fast_field_with_idx(self.field, val_min, val_max, 1)?;

                    for vals in self
                        .get_ordered_docid_iter(docid_map)
                        .map(|docid| self.get_values_for_docid(docid))
                    {
                        // sort values in case of remapped docids?
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

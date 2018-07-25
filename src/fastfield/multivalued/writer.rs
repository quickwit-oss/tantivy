use fastfield::serializer::FastSingleFieldSerializer;
use fastfield::value_to_u64;
use fastfield::FastFieldSerializer;
use itertools::Itertools;
use postings::UnorderedTermId;
use schema::{Document, Field};
use std::collections::HashMap;
use std::io;
use termdict::TermOrdinal;
use DocId;

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
/// The `MultiValueIntFastFieldWriter` can be acquired from the
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
pub struct MultiValueIntFastFieldWriter {
    field: Field,
    vals: Vec<u64>,
    doc_index: Vec<u64>,
    is_facet: bool,
}

impl MultiValueIntFastFieldWriter {
    /// Creates a new `IntFastFieldWriter`
    pub(crate) fn new(field: Field, is_facet: bool) -> Self {
        MultiValueIntFastFieldWriter {
            field,
            vals: Vec::new(),
            doc_index: Vec::new(),
            is_facet,
        }
    }

    /// Access the field associated to the `MultiValueIntFastFieldWriter`
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
        mapping_opt: Option<&HashMap<UnorderedTermId, TermOrdinal>>,
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

                    let last_interval = (
                        self.doc_index.last().cloned().unwrap(),
                        self.vals.len() as u64,
                    );

                    let mut doc_vals: Vec<u64> = Vec::with_capacity(100);
                    for (start, stop) in self.doc_index
                        .windows(2)
                        .map(|interval| (interval[0], interval[1]))
                        .chain(Some(last_interval).into_iter())
                        .map(|(start, stop)| (start as usize, stop as usize))
                    {
                        doc_vals.clear();
                        let remapped_vals = self.vals[start..stop]
                            .iter()
                            .map(|val| *mapping.get(val).expect("Missing term ordinal"));
                        doc_vals.extend(remapped_vals);
                        doc_vals.sort();
                        for &val in &doc_vals {
                            value_serializer.add_val(val)?;
                        }
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

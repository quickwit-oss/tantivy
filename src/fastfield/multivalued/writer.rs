use std::io;

use fastfield_codecs::ip_codec::{ip_to_u128, IntervalCompressor};
use fnv::FnvHashMap;
use tantivy_bitpacker::minmax;

use crate::fastfield::serializer::BitpackedFastFieldSerializerLegacy;
use crate::fastfield::{value_to_u64, CompositeFastFieldSerializer, FastFieldType, FastValue};
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
        get_ordered_values(&self.vals, &self.doc_index, doc_id_map)
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
            if let Some(mapping) = mapping_opt {
                value_serializer = serializer.new_u64_fast_field_with_idx(
                    self.field,
                    0u64,
                    mapping.len() as u64,
                    1,
                )?;

                if self.fast_field_type.is_facet() {
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
                } else {
                    for vals in self.get_ordered_values(doc_id_map) {
                        let remapped_vals = vals
                            .iter()
                            .map(|val| *mapping.get(val).expect("Missing term ordinal"));
                        for val in remapped_vals {
                            value_serializer.add_val(val)?;
                        }
                    }
                }
            } else {
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
            value_serializer.close_field()?;
        }
        Ok(())
    }
}

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

pub struct U128MultiValueFastFieldWriter {
    field: Field,
    vals: Vec<u128>,
    doc_index: Vec<u64>,
}

impl U128MultiValueFastFieldWriter {
    /// Creates a new `U128MultiValueFastFieldWriter`
    pub(crate) fn new(field: Field) -> Self {
        U128MultiValueFastFieldWriter {
            field,
            vals: Vec::new(),
            doc_index: Vec::new(),
        }
    }

    /// The memory used (inclusive childs)
    pub fn mem_usage(&self) -> usize {
        self.vals.capacity() * std::mem::size_of::<UnorderedTermId>()
            + self.doc_index.capacity() * std::mem::size_of::<u64>()
    }

    /// Finalize the current document.
    pub(crate) fn next_doc(&mut self) {
        self.doc_index.push(self.vals.len() as u64);
    }

    /// Pushes a new value to the current document.
    pub(crate) fn add_val(&mut self, val: u128) {
        self.vals.push(val);
    }

    /// Shift to the next document and adds
    /// all of the matching field values present in the document.
    pub fn add_document(&mut self, doc: &Document) {
        self.next_doc();
        for field_value in doc.field_values() {
            if field_value.field == self.field {
                let value = field_value.value();
                let ip_addr = value.as_ip().unwrap();
                let value = ip_to_u128(ip_addr);
                self.add_val(value);
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
    ) -> impl Iterator<Item = &'b [u128]> {
        get_ordered_values(&self.vals, &self.doc_index, doc_id_map)
    }

    /// Serializes fast field values.
    pub fn serialize(
        &self,
        serializer: &mut CompositeFastFieldSerializer,
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
            let field_write = serializer.get_field_writer(self.field, 1);
            let compressor = IntervalCompressor::from_vals(self.vals.to_vec());
            let iter = self.get_ordered_values(doc_id_map).flatten().cloned();
            compressor.compress_into(iter, field_write)?;
        }
        Ok(())
    }
}

/// Returns an iterator over values per doc_id in ascending doc_id order.
///
/// Normally the order is simply iterating self.doc_id_index.
/// With doc_id_map it accounts for the new mapping, returning values in the order of the
/// new doc_ids.
fn get_ordered_values<'a: 'b, 'b, T>(
    vals: &'a [T],
    doc_index: &'a [u64],
    doc_id_map: Option<&'b DocIdMapping>,
) -> impl Iterator<Item = &'b [T]> {
    let doc_id_iter: Box<dyn Iterator<Item = u32>> = if let Some(doc_id_map) = doc_id_map {
        Box::new(doc_id_map.iter_old_doc_ids())
    } else {
        let max_doc = doc_index.len() as DocId;
        Box::new(0..max_doc)
    };
    doc_id_iter.map(move |doc_id| get_values_for_doc_id(doc_id, vals, doc_index))
}

/// returns all values for a doc_id
fn get_values_for_doc_id<'a, T>(doc_id: u32, vals: &'a [T], doc_index: &'a [u64]) -> &'a [T] {
    let start_pos = doc_index[doc_id as usize] as usize;
    let end_pos = doc_index
        .get(doc_id as usize + 1)
        .cloned()
        .unwrap_or(vals.len() as u64) as usize; // special case, last doc_id has no offset information
    &vals[start_pos..end_pos]
}

use std::io;

use fastfield_codecs::{
    Column, MonotonicallyMappableToU128, MonotonicallyMappableToU64, VecColumn,
};
use rustc_hash::FxHashMap;

use super::get_fastfield_codecs_for_multivalue;
use crate::fastfield::writer::unexpected_value;
use crate::fastfield::{value_to_u64, CompositeFastFieldSerializer, FastFieldType};
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

    /// Access the field associated with the `MultiValuedFastFieldWriter`
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
    pub fn add_document(&mut self, doc: &Document) -> crate::Result<()> {
        self.next_doc();
        // facets/texts are indexed in the `SegmentWriter` as we encode their unordered id.
        if self.fast_field_type.is_storing_term_ids() {
            return Ok(());
        }
        for field_value in doc.field_values() {
            if field_value.field == self.field {
                let value = field_value.value();
                let value_u64 = match (self.precision_opt, value) {
                    (Some(precision), Value::Date(date_val)) => {
                        date_val.truncate(precision).to_u64()
                    }
                    _ => value_to_u64(value)?,
                };
                self.add_val(value_u64);
            }
        }
        Ok(())
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
        term_mapping_opt: Option<&FxHashMap<UnorderedTermId, TermOrdinal>>,
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
            serializer.create_auto_detect_u64_fast_field_with_idx_and_codecs(
                self.field,
                col,
                1,
                &get_fastfield_codecs_for_multivalue(),
            )?;
        }
        Ok(())
    }
}

pub(crate) struct MultivalueStartIndex<'a, C: Column> {
    column: &'a C,
    doc_id_map: &'a DocIdMapping,
    min: u64,
    max: u64,
}

impl<'a, C: Column> MultivalueStartIndex<'a, C> {
    pub fn new(column: &'a C, doc_id_map: &'a DocIdMapping) -> Self {
        assert_eq!(column.num_vals(), doc_id_map.num_old_doc_ids() as u32 + 1);
        let (min, max) =
            tantivy_bitpacker::minmax(iter_remapped_multivalue_index(doc_id_map, column))
                .unwrap_or((0u64, 0u64));
        MultivalueStartIndex {
            column,
            doc_id_map,
            min,
            max,
        }
    }
}
impl<'a, C: Column> Column for MultivalueStartIndex<'a, C> {
    fn get_val(&self, _idx: u32) -> u64 {
        unimplemented!()
    }

    fn min_value(&self) -> u64 {
        self.min
    }

    fn max_value(&self) -> u64 {
        self.max
    }

    fn num_vals(&self) -> u32 {
        (self.doc_id_map.num_new_doc_ids() + 1) as u32
    }

    fn iter(&self) -> Box<dyn Iterator<Item = u64> + '_> {
        Box::new(iter_remapped_multivalue_index(
            self.doc_id_map,
            &self.column,
        ))
    }
}

fn iter_remapped_multivalue_index<'a, C: Column>(
    doc_id_map: &'a DocIdMapping,
    column: &'a C,
) -> impl Iterator<Item = u64> + 'a {
    let mut offset = 0;
    std::iter::once(0).chain(doc_id_map.iter_old_doc_ids().map(move |old_doc| {
        let num_vals_for_doc = column.get_val(old_doc + 1) - column.get_val(old_doc);
        offset += num_vals_for_doc;
        offset
    }))
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

pub struct MultiValueU128FastFieldWriter {
    field: Field,
    vals: Vec<u128>,
    doc_index: Vec<u64>,
}

impl MultiValueU128FastFieldWriter {
    /// Creates a new `U128MultiValueFastFieldWriter`
    pub(crate) fn new(field: Field) -> Self {
        MultiValueU128FastFieldWriter {
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
    pub fn add_document(&mut self, doc: &Document) -> crate::Result<()> {
        self.next_doc();
        for field_value in doc.field_values() {
            if field_value.field == self.field {
                let value = field_value.value();
                let ip_addr = value
                    .as_ip_addr()
                    .ok_or_else(|| unexpected_value("ip", value))?;
                let ip_addr_u128 = ip_addr.to_u128();
                self.add_val(ip_addr_u128);
            }
        }
        Ok(())
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
        mut self,
        serializer: &mut CompositeFastFieldSerializer,
        doc_id_map: Option<&DocIdMapping>,
    ) -> io::Result<()> {
        {
            // writing the offset index
            //
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
            let iter_gen = || self.get_ordered_values(doc_id_map).flatten().cloned();

            serializer.create_u128_fast_field_with_idx(
                self.field,
                iter_gen,
                self.vals.len() as u32,
                1,
            )?;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_multivalue_start_index() {
        let doc_id_mapping = DocIdMapping::from_new_id_to_old_id(vec![4, 1, 2]);
        assert_eq!(doc_id_mapping.num_old_doc_ids(), 5);
        let col = VecColumn::from(&[0u64, 3, 5, 10, 12, 16][..]);
        let multivalue_start_index = MultivalueStartIndex::new(
            &col, // 3, 2, 5, 2, 4
            &doc_id_mapping,
        );
        assert_eq!(multivalue_start_index.num_vals(), 4);
        assert_eq!(
            multivalue_start_index.iter().collect::<Vec<u64>>(),
            vec![0, 4, 6, 11]
        ); // 4, 2, 5
    }

    #[test]
    fn test_multivalue_get_vals() {
        let doc_id_mapping =
            DocIdMapping::from_new_id_to_old_id(vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
        assert_eq!(doc_id_mapping.num_old_doc_ids(), 10);
        let col = VecColumn::from(&[0, 1, 1, 2, 3, 5, 8, 13, 21, 34, 55][..]);
        let multivalue_start_index = MultivalueStartIndex::new(&col, &doc_id_mapping);
        assert_eq!(
            multivalue_start_index.iter().collect::<Vec<u64>>(),
            vec![0, 1, 1, 2, 3, 5, 8, 13, 21, 34, 55]
        );
        assert_eq!(multivalue_start_index.num_vals(), 11);
    }
}

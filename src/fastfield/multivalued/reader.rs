use std::ops::{Range, RangeInclusive};
use std::sync::Arc;

use fastfield_codecs::{Column, MonotonicallyMappableToU128};

use super::MultiValueIndex;
use crate::fastfield::FastValue;
use crate::DocId;

/// Reader for a multivalued `u64` fast field.
///
/// The reader is implemented as two `u64` fast field.
///
/// The `vals_reader` will access the concatenated list of all
/// values for all reader.
/// The `idx_reader` associated, for each document, the index of its first value.
/// Stores the start position for each document.
#[derive(Clone)]
pub struct MultiValuedFastFieldReader<Item: FastValue> {
    idx_reader: MultiValueIndex,
    vals_reader: Arc<dyn Column<Item>>,
}

impl<Item: FastValue> MultiValuedFastFieldReader<Item> {
    pub(crate) fn open(
        idx_reader: Arc<dyn Column<u64>>,
        vals_reader: Arc<dyn Column<Item>>,
    ) -> MultiValuedFastFieldReader<Item> {
        MultiValuedFastFieldReader {
            idx_reader: MultiValueIndex::new(idx_reader),
            vals_reader,
        }
    }

    /// Returns the array of values associated with the given `doc`.
    #[inline]
    fn get_vals_for_range(&self, range: Range<u32>, vals: &mut Vec<Item>) {
        let len = (range.end - range.start) as usize;
        vals.resize(len, Item::make_zero());
        self.vals_reader
            .get_range(range.start as u64, &mut vals[..]);
    }

    /// Returns the array of values associated with the given `doc`.
    #[inline]
    pub fn get_vals(&self, doc: DocId, vals: &mut Vec<Item>) {
        let range = self.idx_reader.range(doc);
        self.get_vals_for_range(range, vals);
    }

    /// returns the multivalue index
    pub fn get_index_reader(&self) -> &MultiValueIndex {
        &self.idx_reader
    }

    /// Returns the minimum value for this fast field.
    ///
    /// The min value does not take in account of possible
    /// deleted document, and should be considered as a lower bound
    /// of the actual minimum value.
    pub fn min_value(&self) -> Item {
        self.vals_reader.min_value()
    }

    /// Returns the maximum value for this fast field.
    ///
    /// The max value does not take in account of possible
    /// deleted document, and should be considered as an upper bound
    /// of the actual maximum value.
    pub fn max_value(&self) -> Item {
        self.vals_reader.max_value()
    }

    /// Returns the number of values associated with the document `DocId`.
    #[inline]
    pub fn num_vals(&self, doc: DocId) -> u32 {
        self.idx_reader.num_vals_for_doc(doc)
    }

    /// Returns the overall number of values in this field.
    #[inline]
    pub fn total_num_vals(&self) -> u64 {
        self.idx_reader.total_num_vals()
    }
}

/// Reader for a multivalued `u128` fast field.
///
/// The reader is implemented as a `u64` fast field for the index and a `u128` fast field.
///
/// The `vals_reader` will access the concatenated list of all
/// values for all reader.
/// The `idx_reader` associated, for each document, the index of its first value.
#[derive(Clone)]
pub struct MultiValuedU128FastFieldReader<T: MonotonicallyMappableToU128> {
    idx_reader: MultiValueIndex,
    vals_reader: Arc<dyn Column<T>>,
}

impl<T: MonotonicallyMappableToU128> MultiValuedU128FastFieldReader<T> {
    pub(crate) fn open(
        idx_reader: Arc<dyn Column<u64>>,
        vals_reader: Arc<dyn Column<T>>,
    ) -> MultiValuedU128FastFieldReader<T> {
        Self {
            idx_reader: MultiValueIndex::new(idx_reader),
            vals_reader,
        }
    }

    /// Returns the array of values associated to the given `doc`.
    #[inline]
    pub fn get_first_val(&self, doc: DocId) -> Option<T> {
        let range = self.idx_reader.range(doc);
        if range.is_empty() {
            return None;
        }
        Some(self.vals_reader.get_val(range.start))
    }

    /// Returns the array of values associated to the given `doc`.
    #[inline]
    fn get_vals_for_range(&self, range: Range<u32>, vals: &mut Vec<T>) {
        let len = (range.end - range.start) as usize;
        vals.resize(len, T::from_u128(0));
        self.vals_reader
            .get_range(range.start as u64, &mut vals[..]);
    }

    /// Returns the index reader
    pub fn get_index_reader(&self) -> &MultiValueIndex {
        &self.idx_reader
    }

    /// Returns the array of values associated to the given `doc`.
    #[inline]
    pub fn get_vals(&self, doc: DocId, vals: &mut Vec<T>) {
        let range = self.idx_reader.range(doc);
        self.get_vals_for_range(range, vals);
    }

    /// Iterates over all elements in the fast field
    pub fn iter(&self) -> impl Iterator<Item = T> + '_ {
        self.vals_reader.iter()
    }

    /// Returns the minimum value for this fast field.
    ///
    /// The min value does not take in account of possible
    /// deleted document, and should be considered as a lower bound
    /// of the actual mimimum value.
    pub fn min_value(&self) -> T {
        self.vals_reader.min_value()
    }

    /// Returns the maximum value for this fast field.
    ///
    /// The max value does not take in account of possible
    /// deleted document, and should be considered as an upper bound
    /// of the actual maximum value.
    pub fn max_value(&self) -> T {
        self.vals_reader.max_value()
    }

    /// Returns the number of values associated with the document `DocId`.
    #[inline]
    pub fn num_vals(&self, doc: DocId) -> u32 {
        self.idx_reader.num_vals_for_doc(doc)
    }

    /// Returns the overall number of values in this field. It does not include deletes.
    #[inline]
    pub fn total_num_vals(&self) -> u64 {
        assert_eq!(
            self.vals_reader.num_vals() as u64,
            self.get_index_reader().total_num_vals()
        );
        self.idx_reader.total_num_vals()
    }
}

impl<T: MonotonicallyMappableToU128> Column<T> for MultiValuedU128FastFieldReader<T> {
    fn get_val(&self, _idx: u32) -> T {
        panic!("calling get_val on a multivalue field indicates a bug")
    }

    fn min_value(&self) -> T {
        (self as &MultiValuedU128FastFieldReader<T>).min_value()
    }

    fn max_value(&self) -> T {
        (self as &MultiValuedU128FastFieldReader<T>).max_value()
    }

    fn num_vals(&self) -> u32 {
        self.total_num_vals() as u32
    }

    fn num_docs(&self) -> u32 {
        self.get_index_reader().num_docs()
    }

    #[inline]
    fn get_docids_for_value_range(
        &self,
        value_range: RangeInclusive<T>,
        doc_id_range: Range<u32>,
        positions: &mut Vec<u32>,
    ) {
        let position_range = self
            .get_index_reader()
            .docid_range_to_position_range(doc_id_range.clone());
        self.vals_reader
            .get_docids_for_value_range(value_range, position_range, positions);

        self.idx_reader.positions_to_docids(doc_id_range, positions);
    }
}

#[cfg(test)]
mod tests {

    use crate::core::Index;
    use crate::schema::{Cardinality, Facet, FacetOptions, NumericOptions, Schema};

    #[test]
    fn test_multifastfield_reader() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let facet_field = schema_builder.add_facet_field("facets", FacetOptions::default());
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut index_writer = index.writer_for_tests()?;
        index_writer.add_document(doc!(
            facet_field => Facet::from("/category/cat2"),
            facet_field => Facet::from("/category/cat1"),
        ))?;
        index_writer.add_document(doc!(facet_field => Facet::from("/category/cat2")))?;
        index_writer.add_document(doc!(facet_field => Facet::from("/category/cat3")))?;
        index_writer.commit()?;
        let searcher = index.reader()?.searcher();
        let segment_reader = searcher.segment_reader(0);
        let mut facet_reader = segment_reader.facet_reader(facet_field)?;

        let mut facet = Facet::root();
        {
            facet_reader.facet_from_ord(1, &mut facet).unwrap();
            assert_eq!(facet, Facet::from("/category"));
        }
        {
            facet_reader.facet_from_ord(2, &mut facet).unwrap();
            assert_eq!(facet, Facet::from("/category/cat1"));
        }
        {
            facet_reader.facet_from_ord(3, &mut facet).unwrap();
            assert_eq!(format!("{}", facet), "/category/cat2");
            assert_eq!(facet, Facet::from("/category/cat2"));
        }
        {
            facet_reader.facet_from_ord(4, &mut facet).unwrap();
            assert_eq!(facet, Facet::from("/category/cat3"));
        }

        let mut vals = Vec::new();
        {
            facet_reader.facet_ords(0, &mut vals);
            assert_eq!(&vals[..], &[2, 3]);
        }
        {
            facet_reader.facet_ords(1, &mut vals);
            assert_eq!(&vals[..], &[3]);
        }
        {
            facet_reader.facet_ords(2, &mut vals);
            assert_eq!(&vals[..], &[4]);
        }
        Ok(())
    }

    #[test]
    fn test_multifastfield_reader_min_max() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let field_options = NumericOptions::default()
            .set_indexed()
            .set_fast(Cardinality::MultiValues);
        let item_field = schema_builder.add_i64_field("items", field_options);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut index_writer = index
            .writer_for_tests()
            .expect("Failed to create index writer.");
        index_writer.add_document(doc!(
            item_field => 2i64,
            item_field => 3i64,
            item_field => -2i64,
        ))?;
        index_writer.add_document(doc!(item_field => 6i64, item_field => 3i64))?;
        index_writer.add_document(doc!(item_field => 4i64))?;
        index_writer.commit()?;
        let searcher = index.reader()?.searcher();
        let segment_reader = searcher.segment_reader(0);
        let field_reader = segment_reader.fast_fields().i64s(item_field)?;

        assert_eq!(field_reader.min_value(), -2);
        assert_eq!(field_reader.max_value(), 6);
        Ok(())
    }
}

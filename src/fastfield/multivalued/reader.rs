use core::fmt;
use std::ops::{Range, RangeInclusive};
use std::sync::Arc;

use fastfield_codecs::Column;

use super::MultiValueIndex;
use crate::fastfield::MakeZero;
use crate::DocId;

/// Reader for a multivalued fast field.
///
/// The reader is implemented as two fast fields, one u64 fast field for the index and one for the
/// values.
///
/// The `vals_reader` will access the concatenated list of all values.
/// The `idx_reader` associates, for each document, the index of its first value.
#[derive(Clone)]
pub struct MultiValuedFastFieldReader<T> {
    idx_reader: MultiValueIndex,
    vals_reader: Arc<dyn Column<T>>,
}

impl<T: PartialOrd + MakeZero + Copy + fmt::Debug> MultiValuedFastFieldReader<T> {
    pub(crate) fn open(
        idx_reader: Arc<dyn Column<u64>>,
        vals_reader: Arc<dyn Column<T>>,
    ) -> MultiValuedFastFieldReader<T> {
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
        vals.resize(len, T::make_zero());
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
    pub fn total_num_vals(&self) -> u32 {
        assert_eq!(
            self.vals_reader.num_vals(),
            self.get_index_reader().total_num_vals()
        );
        self.idx_reader.total_num_vals()
    }

    /// Returns the docids matching given doc_id_range and value_range.
    #[inline]
    pub fn get_docids_for_value_range(
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

    use time::{Duration, OffsetDateTime};

    use crate::collector::Count;
    use crate::core::Index;
    use crate::query::RangeQuery;
    use crate::schema::{Cardinality, Facet, FacetOptions, NumericOptions, Schema};
    use crate::{DateOptions, DatePrecision, DateTime};

    #[test]
    fn test_multivalued_date_docids_for_value_range_1() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let date_field = schema_builder.add_date_field(
            "multi_date_field",
            DateOptions::default()
                .set_fast(Cardinality::MultiValues)
                .set_indexed()
                .set_fieldnorm()
                .set_precision(DatePrecision::Microseconds)
                .set_stored(),
        );
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut index_writer = index.writer_for_tests()?;
        let first_time_stamp = OffsetDateTime::now_utc();
        index_writer.add_document(doc!(
            date_field => DateTime::from_utc(first_time_stamp),
            date_field => DateTime::from_utc(first_time_stamp),
        ))?;
        // add another second
        let two_secs_ahead = first_time_stamp + Duration::seconds(2);
        index_writer.commit()?;

        let reader = index.reader()?;
        let searcher = reader.searcher();
        let reader = searcher.segment_reader(0);

        let date_ff_reader = reader.fast_fields().dates(date_field).unwrap();
        let mut docids = vec![];
        date_ff_reader.get_docids_for_value_range(
            DateTime::from_utc(first_time_stamp)..=DateTime::from_utc(two_secs_ahead),
            0..5,
            &mut docids,
        );
        assert_eq!(docids, vec![0]);

        let count_multiples =
            |range_query: RangeQuery| searcher.search(&range_query, &Count).unwrap();

        assert_eq!(
            count_multiples(RangeQuery::new_date(
                date_field,
                DateTime::from_utc(first_time_stamp)..DateTime::from_utc(two_secs_ahead)
            )),
            1
        );

        Ok(())
    }

    #[test]
    fn test_multivalued_date_docids_for_value_range_2() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let date_field = schema_builder.add_date_field(
            "multi_date_field",
            DateOptions::default()
                .set_fast(Cardinality::MultiValues)
                // TODO: Test different precision after fixing https://github.com/quickwit-oss/tantivy/issues/1783
                .set_precision(DatePrecision::Microseconds)
                .set_indexed()
                .set_fieldnorm()
                .set_stored(),
        );
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut index_writer = index.writer_for_tests()?;
        let first_time_stamp = OffsetDateTime::now_utc();
        index_writer.add_document(doc!(
            date_field => DateTime::from_utc(first_time_stamp),
            date_field => DateTime::from_utc(first_time_stamp),
        ))?;
        index_writer.add_document(doc!())?;
        // add one second
        index_writer.add_document(doc!(
            date_field => DateTime::from_utc(first_time_stamp + Duration::seconds(1)),
        ))?;
        // add another second
        let two_secs_ahead = first_time_stamp + Duration::seconds(2);
        index_writer.add_document(doc!(
            date_field => DateTime::from_utc(two_secs_ahead),
            date_field => DateTime::from_utc(two_secs_ahead),
            date_field => DateTime::from_utc(two_secs_ahead),
        ))?;
        // add three seconds
        index_writer.add_document(doc!(
            date_field => DateTime::from_utc(first_time_stamp + Duration::seconds(3)),
        ))?;
        index_writer.commit()?;

        let reader = index.reader()?;
        let searcher = reader.searcher();
        let reader = searcher.segment_reader(0);
        assert_eq!(reader.num_docs(), 5);

        let date_ff_reader = reader.fast_fields().dates("multi_date_field").unwrap();
        let mut docids = vec![];
        date_ff_reader.get_docids_for_value_range(
            DateTime::from_utc(first_time_stamp)..=DateTime::from_utc(two_secs_ahead),
            0..5,
            &mut docids,
        );
        assert_eq!(docids, vec![0, 2, 3]);

        let count_multiples =
            |range_query: RangeQuery| searcher.search(&range_query, &Count).unwrap();

        assert_eq!(
            count_multiples(RangeQuery::new_date(
                "multi_date_field".to_string(),
                DateTime::from_utc(first_time_stamp)..DateTime::from_utc(two_secs_ahead)
            )),
            2
        );

        Ok(())
    }

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
        let field_reader = segment_reader.fast_fields().i64s("items")?;

        assert_eq!(field_reader.min_value(), -2);
        assert_eq!(field_reader.max_value(), 6);
        Ok(())
    }
}

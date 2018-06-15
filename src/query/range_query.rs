use common::BitSet;
use core::Searcher;
use core::SegmentReader;
use error::ErrorKind;
use query::BitSetDocSet;
use query::ConstScorer;
use query::{Query, Scorer, Weight};
use schema::Type;
use schema::{Field, IndexRecordOption, Term};
use std::collections::Bound;
use std::ops::Range;
use termdict::{TermDictionary, TermStreamer};
use Result;

fn map_bound<TFrom, TTo, Transform: Fn(&TFrom) -> TTo>(
    bound: &Bound<TFrom>,
    transform: &Transform,
) -> Bound<TTo> {
    use self::Bound::*;
    match bound {
        Excluded(ref from_val) => Excluded(transform(from_val)),
        Included(ref from_val) => Included(transform(from_val)),
        Unbounded => Unbounded,
    }
}

/// `RangeQuery` match all documents that have at least one term within a defined range.
///
/// Matched document will all get a constant `Score` of one.
///
/// # Implementation
///
/// The current implement will iterate over the terms within the range
/// and append all of the document cross into a `BitSet`.
///
/// # Example
///
/// ```rust
///
/// # #[macro_use]
/// # extern crate tantivy;
/// # use tantivy::Index;
/// # use tantivy::schema::{SchemaBuilder, INT_INDEXED};
/// # use tantivy::collector::CountCollector;
/// # use tantivy::query::Query;
/// # use tantivy::Result;
/// # use tantivy::query::RangeQuery;
/// #
/// # fn run() -> Result<()> {
/// #     let mut schema_builder = SchemaBuilder::new();
/// #     let year_field = schema_builder.add_u64_field("year", INT_INDEXED);
/// #     let schema = schema_builder.build();
/// #
/// #     let index = Index::create_in_ram(schema);
/// #     {
/// #         let mut index_writer = index.writer_with_num_threads(1, 6_000_000).unwrap();
/// #         for year in 1950u64..2017u64 {
/// #             let num_docs_within_year = 10 + (year - 1950) * (year - 1950);
/// #             for _ in 0..num_docs_within_year {
/// #                 index_writer.add_document(doc!(year_field => year));
/// #             }
/// #         }
/// #         index_writer.commit().unwrap();
/// #     }
/// #   index.load_searchers()?;
/// let searcher = index.searcher();
///
/// let docs_in_the_sixties = RangeQuery::new_u64(year_field, 1960..1970);
///
/// let mut count_collector = CountCollector::default();
/// docs_in_the_sixties.search(&*searcher, &mut count_collector)?;
///
/// let num_60s_books = count_collector.count();
///
/// #     assert_eq!(num_60s_books, 2285);
/// #     Ok(())
/// # }
/// #
/// # fn main() {
/// #   run().unwrap()
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct RangeQuery {
    field: Field,
    value_type: Type,
    left_bound: Bound<Vec<u8>>,
    right_bound: Bound<Vec<u8>>,
}

impl RangeQuery {
    /// Creates a new `RangeQuery` over a `i64` field.
    ///
    /// If the field is not of the type `i64`, tantivy
    /// will panic when the `Weight` object is created.
    pub fn new_i64(field: Field, range: Range<i64>) -> RangeQuery {
        RangeQuery::new_i64_bounds(
            field,
            Bound::Included(range.start),
            Bound::Excluded(range.end),
        )
    }

    /// Create a new `RangeQuery` over a `i64` field.
    ///
    /// The two `Bound` arguments make it possible to create more complex
    /// ranges than semi-inclusive range.
    ///
    /// If the field is not of the type `i64`, tantivy
    /// will panic when the `Weight` object is created.
    pub fn new_i64_bounds(
        field: Field,
        left_bound: Bound<i64>,
        right_bound: Bound<i64>,
    ) -> RangeQuery {
        let make_term_val = |val: &i64| Term::from_field_i64(field, *val).value_bytes().to_owned();
        RangeQuery {
            field,
            value_type: Type::I64,
            left_bound: map_bound(&left_bound, &make_term_val),
            right_bound: map_bound(&right_bound, &make_term_val),
        }
    }

    /// Create a new `RangeQuery` over a `u64` field.
    ///
    /// The two `Bound` arguments make it possible to create more complex
    /// ranges than semi-inclusive range.
    ///
    /// If the field is not of the type `u64`, tantivy
    /// will panic when the `Weight` object is created.
    pub fn new_u64_bounds(
        field: Field,
        left_bound: Bound<u64>,
        right_bound: Bound<u64>,
    ) -> RangeQuery {
        let make_term_val = |val: &u64| Term::from_field_u64(field, *val).value_bytes().to_owned();
        RangeQuery {
            field,
            value_type: Type::U64,
            left_bound: map_bound(&left_bound, &make_term_val),
            right_bound: map_bound(&right_bound, &make_term_val),
        }
    }

    /// Create a new `RangeQuery` over a `u64` field.
    ///
    /// If the field is not of the type `u64`, tantivy
    /// will panic when the `Weight` object is created.
    pub fn new_u64(field: Field, range: Range<u64>) -> RangeQuery {
        RangeQuery::new_u64_bounds(
            field,
            Bound::Included(range.start),
            Bound::Excluded(range.end),
        )
    }

    /// Create a new `RangeQuery` over a `Str` field.
    ///
    /// The two `Bound` arguments make it possible to create more complex
    /// ranges than semi-inclusive range.
    ///
    /// If the field is not of the type `Str`, tantivy
    /// will panic when the `Weight` object is created.
    pub fn new_str_bounds<'b>(
        field: Field,
        left: Bound<&'b str>,
        right: Bound<&'b str>,
    ) -> RangeQuery {
        let make_term_val = |val: &&str| val.as_bytes().to_vec();
        RangeQuery {
            field,
            value_type: Type::Str,
            left_bound: map_bound(&left, &make_term_val),
            right_bound: map_bound(&right, &make_term_val),
        }
    }

    /// Create a new `RangeQuery` over a `Str` field.
    ///
    /// If the field is not of the type `Str`, tantivy
    /// will panic when the `Weight` object is created.
    pub fn new_str<'b>(field: Field, range: Range<&'b str>) -> RangeQuery {
        RangeQuery::new_str_bounds(
            field,
            Bound::Included(range.start),
            Bound::Excluded(range.end),
        )
    }

    /// Field to search over
    pub fn field(&self) -> Field {
        self.field
    }

    /// Lower bound of range
    pub fn left_bound(&self) -> Bound<Term> {
        map_bound(&self.left_bound, &|bytes| Term::from_field_bytes(self.field, bytes))
    }

    /// Upper bound of range
    pub fn right_bound(&self) -> Bound<Term> {
        map_bound(&self.right_bound, &|bytes| Term::from_field_bytes(self.field, bytes))
    }
}

impl Query for RangeQuery {
    fn weight(&self, searcher: &Searcher, _scoring_enabled: bool) -> Result<Box<Weight>> {
        let schema = searcher.schema();
        let value_type = schema.get_field_entry(self.field).field_type().value_type();
        if value_type != self.value_type {
            let err_msg = format!(
                "Create a range query of the type {:?}, when the field given was of type {:?}",
                self.value_type, value_type
            );
            bail!(ErrorKind::SchemaError(err_msg))
        }
        Ok(Box::new(RangeWeight {
            field: self.field,
            left_bound: self.left_bound.clone(),
            right_bound: self.right_bound.clone(),
        }))
    }
}

pub struct RangeWeight {
    field: Field,
    left_bound: Bound<Vec<u8>>,
    right_bound: Bound<Vec<u8>>,
}

impl RangeWeight {
    fn term_range<'a>(&self, term_dict: &'a TermDictionary) -> TermStreamer<'a> {
        use std::collections::Bound::*;
        let mut term_stream_builder = term_dict.range();
        term_stream_builder = match self.left_bound {
            Included(ref term_val) => term_stream_builder.ge(term_val),
            Excluded(ref term_val) => term_stream_builder.gt(term_val),
            Unbounded => term_stream_builder,
        };
        term_stream_builder = match self.right_bound {
            Included(ref term_val) => term_stream_builder.le(term_val),
            Excluded(ref term_val) => term_stream_builder.lt(term_val),
            Unbounded => term_stream_builder,
        };
        term_stream_builder.into_stream()
    }
}

impl Weight for RangeWeight {
    fn scorer(&self, reader: &SegmentReader) -> Result<Box<Scorer>> {
        let max_doc = reader.max_doc();
        let mut doc_bitset = BitSet::with_max_value(max_doc);

        let inverted_index = reader.inverted_index(self.field);
        let term_dict = inverted_index.terms();
        let mut term_range = self.term_range(term_dict);
        while term_range.advance() {
            let term_info = term_range.value();
            let mut block_segment_postings = inverted_index
                .read_block_postings_from_terminfo(term_info, IndexRecordOption::Basic);
            while block_segment_postings.advance() {
                for &doc in block_segment_postings.docs() {
                    doc_bitset.insert(doc);
                }
            }
        }
        let doc_bitset = BitSetDocSet::from(doc_bitset);
        Ok(Box::new(ConstScorer::new(doc_bitset)))
    }
}

#[cfg(test)]
mod tests {

    use super::RangeQuery;
    use collector::CountCollector;
    use query::Query;
    use schema::{Document, Field, SchemaBuilder, INT_INDEXED};
    use std::collections::Bound;
    use Index;
    use Result;

    #[test]
    fn test_range_query_simple() {
        fn run() -> Result<()> {
            let mut schema_builder = SchemaBuilder::new();
            let year_field = schema_builder.add_u64_field("year", INT_INDEXED);
            let schema = schema_builder.build();

            let index = Index::create_in_ram(schema);
            {
                let mut index_writer = index.writer_with_num_threads(1, 6_000_000).unwrap();
                for year in 1950u64..2017u64 {
                    let num_docs_within_year = 10 + (year - 1950) * (year - 1950);
                    for _ in 0..num_docs_within_year {
                        index_writer.add_document(doc!(year_field => year));
                    }
                }
                index_writer.commit().unwrap();
            }
            index.load_searchers().unwrap();
            let searcher = index.searcher();

            let docs_in_the_sixties = RangeQuery::new_u64(year_field, 1960u64..1970u64);

            // ... or `1960..=1969` if inclusive range is enabled.
            let mut count_collector = CountCollector::default();
            docs_in_the_sixties.search(&*searcher, &mut count_collector)?;
            assert_eq!(count_collector.count(), 2285);
            Ok(())
        }

        run().unwrap();
    }

    #[test]
    fn test_range_query() {
        let int_field: Field;
        let schema = {
            let mut schema_builder = SchemaBuilder::new();
            int_field = schema_builder.add_i64_field("intfield", INT_INDEXED);
            schema_builder.build()
        };

        let index = Index::create_in_ram(schema);
        {
            let mut index_writer = index.writer_with_num_threads(2, 6_000_000).unwrap();

            for i in 1..100 {
                let mut doc = Document::new();
                for j in 1..100 {
                    if i % j == 0 {
                        doc.add_i64(int_field, j as i64);
                    }
                }
                index_writer.add_document(doc);
            }

            index_writer.commit().unwrap();
        }
        index.load_searchers().unwrap();
        let searcher = index.searcher();
        let count_multiples = |range_query: RangeQuery| {
            let mut count_collector = CountCollector::default();
            range_query
                .search(&*searcher, &mut count_collector)
                .unwrap();
            count_collector.count()
        };

        assert_eq!(count_multiples(RangeQuery::new_i64(int_field, 10..11)), 9);
        assert_eq!(
            count_multiples(RangeQuery::new_i64_bounds(
                int_field,
                Bound::Included(10),
                Bound::Included(11)
            )),
            18
        );
        assert_eq!(
            count_multiples(RangeQuery::new_i64_bounds(
                int_field,
                Bound::Excluded(9),
                Bound::Included(10)
            )),
            9
        );
        assert_eq!(
            count_multiples(RangeQuery::new_i64_bounds(
                int_field,
                Bound::Included(9),
                Bound::Unbounded
            )),
            91
        );
    }

}

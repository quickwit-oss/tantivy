use crate::common::BitSet;
use crate::core::Searcher;
use crate::core::SegmentReader;
use crate::error::TantivyError;
use crate::query::explanation::does_not_match;
use crate::query::ConstScorer;
use crate::query::{BitSetDocSet, Explanation};
use crate::query::{Query, Scorer, Weight};
use crate::schema::Type;
use crate::schema::{Field, IndexRecordOption, Term};
use crate::termdict::{TermDictionary, TermStreamer};
use crate::{DocId, Score};
use std::io;
use std::ops::{Bound, Range};

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
/// use tantivy::collector::Count;
/// use tantivy::query::RangeQuery;
/// use tantivy::schema::{Schema, INDEXED};
/// use tantivy::{doc, Index};
/// # fn test() -> tantivy::Result<()> {
/// let mut schema_builder = Schema::builder();
/// let year_field = schema_builder.add_u64_field("year", INDEXED);
/// let schema = schema_builder.build();
///
/// let index = Index::create_in_ram(schema);
/// let mut index_writer = index.writer_with_num_threads(1, 10_000_000)?;
/// for year in 1950u64..2017u64 {
///     let num_docs_within_year = 10 + (year - 1950) * (year - 1950);
///     for _ in 0..num_docs_within_year {
///       index_writer.add_document(doc!(year_field => year));
///     }
/// }
/// index_writer.commit()?;
///
/// let reader = index.reader()?;
/// let searcher = reader.searcher();
/// let docs_in_the_sixties = RangeQuery::new_u64(year_field, 1960..1970);
/// let num_60s_books = searcher.search(&docs_in_the_sixties, &Count)?;
/// assert_eq!(num_60s_books, 2285);
/// Ok(())
/// # }
/// # assert!(test().is_ok());
/// ```
#[derive(Clone, Debug)]
pub struct RangeQuery {
    field: Field,
    value_type: Type,
    left_bound: Bound<Vec<u8>>,
    right_bound: Bound<Vec<u8>>,
}

impl RangeQuery {
    /// Creates a new `RangeQuery` from bounded start and end terms.
    ///
    /// If the value type is not correct, something may go terribly wrong when
    /// the `Weight` object is created.
    pub fn new_term_bounds(
        field: Field,
        value_type: Type,
        left_bound: &Bound<Term>,
        right_bound: &Bound<Term>,
    ) -> RangeQuery {
        let verify_and_unwrap_term = |val: &Term| {
            assert_eq!(field, val.field());
            val.value_bytes().to_owned()
        };
        RangeQuery {
            field,
            value_type,
            left_bound: map_bound(&left_bound, &verify_and_unwrap_term),
            right_bound: map_bound(&right_bound, &verify_and_unwrap_term),
        }
    }

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

    /// Creates a new `RangeQuery` over a `f64` field.
    ///
    /// If the field is not of the type `f64`, tantivy
    /// will panic when the `Weight` object is created.
    pub fn new_f64(field: Field, range: Range<f64>) -> RangeQuery {
        RangeQuery::new_f64_bounds(
            field,
            Bound::Included(range.start),
            Bound::Excluded(range.end),
        )
    }

    /// Create a new `RangeQuery` over a `f64` field.
    ///
    /// The two `Bound` arguments make it possible to create more complex
    /// ranges than semi-inclusive range.
    ///
    /// If the field is not of the type `f64`, tantivy
    /// will panic when the `Weight` object is created.
    pub fn new_f64_bounds(
        field: Field,
        left_bound: Bound<f64>,
        right_bound: Bound<f64>,
    ) -> RangeQuery {
        let make_term_val = |val: &f64| Term::from_field_f64(field, *val).value_bytes().to_owned();
        RangeQuery {
            field,
            value_type: Type::F64,
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
    pub fn new_str_bounds(field: Field, left: Bound<&str>, right: Bound<&str>) -> RangeQuery {
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
    pub fn new_str(field: Field, range: Range<&str>) -> RangeQuery {
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
        map_bound(&self.left_bound, &|bytes| {
            Term::from_field_bytes(self.field, bytes)
        })
    }

    /// Upper bound of range
    pub fn right_bound(&self) -> Bound<Term> {
        map_bound(&self.right_bound, &|bytes| {
            Term::from_field_bytes(self.field, bytes)
        })
    }
}

impl Query for RangeQuery {
    fn weight(
        &self,
        searcher: &Searcher,
        _scoring_enabled: bool,
    ) -> crate::Result<Box<dyn Weight>> {
        let schema = searcher.schema();
        let value_type = schema.get_field_entry(self.field).field_type().value_type();
        if value_type != self.value_type {
            let err_msg = format!(
                "Create a range query of the type {:?}, when the field given was of type {:?}",
                self.value_type, value_type
            );
            return Err(TantivyError::SchemaError(err_msg));
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
    fn term_range<'a>(&self, term_dict: &'a TermDictionary) -> io::Result<TermStreamer<'a>> {
        use std::ops::Bound::*;
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
    fn scorer(&self, reader: &SegmentReader, boost: Score) -> crate::Result<Box<dyn Scorer>> {
        let max_doc = reader.max_doc();
        let mut doc_bitset = BitSet::with_max_value(max_doc);

        let inverted_index = reader.inverted_index(self.field)?;
        let term_dict = inverted_index.terms();
        let mut term_range = self.term_range(term_dict)?;
        while term_range.advance() {
            let term_info = term_range.value();
            let mut block_segment_postings = inverted_index
                .read_block_postings_from_terminfo(term_info, IndexRecordOption::Basic)?;
            loop {
                let docs = block_segment_postings.docs();
                if docs.is_empty() {
                    break;
                }
                for &doc in block_segment_postings.docs() {
                    doc_bitset.insert(doc);
                }
                block_segment_postings.advance();
            }
        }
        let doc_bitset = BitSetDocSet::from(doc_bitset);
        Ok(Box::new(ConstScorer::new(doc_bitset, boost)))
    }

    fn explain(&self, reader: &SegmentReader, doc: DocId) -> crate::Result<Explanation> {
        let mut scorer = self.scorer(reader, 1.0)?;
        if scorer.seek(doc) != doc {
            return Err(does_not_match(doc));
        }
        Ok(Explanation::new("RangeQuery", 1.0))
    }
}

#[cfg(test)]
mod tests {

    use super::RangeQuery;
    use crate::collector::{Count, TopDocs};
    use crate::query::QueryParser;
    use crate::schema::{Document, Field, Schema, INDEXED, TEXT};
    use crate::Index;
    use std::ops::Bound;

    #[test]
    fn test_range_query_simple() {
        let mut schema_builder = Schema::builder();
        let year_field = schema_builder.add_u64_field("year", INDEXED);
        let schema = schema_builder.build();

        let index = Index::create_in_ram(schema);
        {
            let mut index_writer = index.writer_for_tests().unwrap();
            for year in 1950u64..2017u64 {
                let num_docs_within_year = 10 + (year - 1950) * (year - 1950);
                for _ in 0..num_docs_within_year {
                    index_writer.add_document(doc!(year_field => year));
                }
            }
            index_writer.commit().unwrap();
        }
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();

        let docs_in_the_sixties = RangeQuery::new_u64(year_field, 1960u64..1970u64);

        // ... or `1960..=1969` if inclusive range is enabled.
        let count = searcher.search(&docs_in_the_sixties, &Count).unwrap();
        assert_eq!(count, 2285);
    }

    #[test]
    fn test_range_query() {
        let int_field: Field;
        let schema = {
            let mut schema_builder = Schema::builder();
            int_field = schema_builder.add_i64_field("intfield", INDEXED);
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
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        let count_multiples =
            |range_query: RangeQuery| searcher.search(&range_query, &Count).unwrap();

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

    #[test]
    fn test_range_float() {
        let float_field: Field;
        let schema = {
            let mut schema_builder = Schema::builder();
            float_field = schema_builder.add_f64_field("floatfield", INDEXED);
            schema_builder.build()
        };

        let index = Index::create_in_ram(schema);
        {
            let mut index_writer = index.writer_with_num_threads(2, 6_000_000).unwrap();

            for i in 1..100 {
                let mut doc = Document::new();
                for j in 1..100 {
                    if i % j == 0 {
                        doc.add_f64(float_field, j as f64);
                    }
                }
                index_writer.add_document(doc);
            }

            index_writer.commit().unwrap();
        }
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        let count_multiples =
            |range_query: RangeQuery| searcher.search(&range_query, &Count).unwrap();

        assert_eq!(
            count_multiples(RangeQuery::new_f64(float_field, 10.0..11.0)),
            9
        );
        assert_eq!(
            count_multiples(RangeQuery::new_f64_bounds(
                float_field,
                Bound::Included(10.0),
                Bound::Included(11.0)
            )),
            18
        );
        assert_eq!(
            count_multiples(RangeQuery::new_f64_bounds(
                float_field,
                Bound::Excluded(9.0),
                Bound::Included(10.0)
            )),
            9
        );
        assert_eq!(
            count_multiples(RangeQuery::new_f64_bounds(
                float_field,
                Bound::Included(9.0),
                Bound::Unbounded
            )),
            91
        );
    }

    #[test]
    fn test_bug_reproduce_range_query() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        schema_builder.add_text_field("title", TEXT);
        schema_builder.add_i64_field("year", INDEXED);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema.clone());
        let mut index_writer = index.writer_for_tests()?;
        let title = schema.get_field("title").unwrap();
        let year = schema.get_field("year").unwrap();
        index_writer.add_document(doc!(
          title => "hemoglobin blood",
          year => 1990 as i64
        ));
        index_writer.commit()?;
        let reader = index.reader()?;
        let searcher = reader.searcher();
        let query_parser = QueryParser::for_index(&index, vec![title]);
        let query = query_parser.parse_query("hemoglobin AND year:[1970 TO 1990]")?;
        let top_docs = searcher.search(&query, &TopDocs::with_limit(10))?;
        assert_eq!(top_docs.len(), 1);
        Ok(())
    }
}

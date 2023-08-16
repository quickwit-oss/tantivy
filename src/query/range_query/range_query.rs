use std::io;
use std::net::Ipv6Addr;
use std::ops::{Bound, Range};

use columnar::MonotonicallyMappableToU128;
use common::{BinarySerializable, BitSet};

use super::map_bound;
use super::range_query_u64_fastfield::FastFieldRangeWeight;
use crate::core::SegmentReader;
use crate::error::TantivyError;
use crate::query::explanation::does_not_match;
use crate::query::range_query::range_query_ip_fastfield::IPFastFieldRangeWeight;
use crate::query::range_query::{is_type_valid_for_fastfield_range_query, map_bound_res};
use crate::query::{BitSetDocSet, ConstScorer, EnableScoring, Explanation, Query, Scorer, Weight};
use crate::schema::{Field, IndexRecordOption, Term, Type};
use crate::termdict::{TermDictionary, TermStreamer};
use crate::{DateTime, DocId, Score};

/// `RangeQuery` matches all documents that have at least one term within a defined range.
///
/// Matched document will all get a constant `Score` of one.
///
/// # Implementation
///
/// ## Default
/// The default implementation collects all documents _upfront_ into a `BitSet`.
/// This is done by iterating over the terms within the range and loading all docs for each
/// `TermInfo` from the inverted index (posting list) and put them into a `BitSet`.
/// Depending on the number of terms matched, this is a potentially expensive operation.
///
/// ## IP fast field
/// For IP fast fields a custom variant is used, by scanning the fast field. Unlike the default
/// variant we can walk in a lazy fashion over it, since the fastfield is implicit orderered by
/// DocId.
///
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
/// let mut index_writer = index.writer_with_num_threads(1, 20_000_000)?;
/// for year in 1950u64..2017u64 {
///     let num_docs_within_year = 10 + (year - 1950) * (year - 1950);
///     for _ in 0..num_docs_within_year {
///       index_writer.add_document(doc!(year_field => year))?;
///     }
/// }
/// index_writer.commit()?;
///
/// let reader = index.reader()?;
/// let searcher = reader.searcher();
/// let docs_in_the_sixties = RangeQuery::new_u64("year".to_string(), 1960..1970);
/// let num_60s_books = searcher.search(&docs_in_the_sixties, &Count)?;
/// assert_eq!(num_60s_books, 2285);
/// Ok(())
/// # }
/// # assert!(test().is_ok());
/// ```
#[derive(Clone, Debug)]
pub struct RangeQuery {
    field: String,
    value_type: Type,
    lower_bound: Bound<Vec<u8>>,
    upper_bound: Bound<Vec<u8>>,
    limit: Option<u64>,
}

impl RangeQuery {
    /// Creates a new `RangeQuery` from bounded start and end terms.
    ///
    /// If the value type is not correct, something may go terribly wrong when
    /// the `Weight` object is created.
    pub fn new_term_bounds(
        field: String,
        value_type: Type,
        lower_bound: &Bound<Term>,
        upper_bound: &Bound<Term>,
    ) -> RangeQuery {
        let verify_and_unwrap_term = |val: &Term| val.serialized_value_bytes().to_owned();
        RangeQuery {
            field,
            value_type,
            lower_bound: map_bound(lower_bound, verify_and_unwrap_term),
            upper_bound: map_bound(upper_bound, verify_and_unwrap_term),
            limit: None,
        }
    }

    /// Creates a new `RangeQuery` over a `i64` field.
    ///
    /// If the field is not of the type `i64`, tantivy
    /// will panic when the `Weight` object is created.
    pub fn new_i64(field: String, range: Range<i64>) -> RangeQuery {
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
        field: String,
        lower_bound: Bound<i64>,
        upper_bound: Bound<i64>,
    ) -> RangeQuery {
        let make_term_val = |val: &i64| {
            Term::from_field_i64(Field::from_field_id(0), *val)
                .serialized_value_bytes()
                .to_owned()
        };
        RangeQuery {
            field,
            value_type: Type::I64,
            lower_bound: map_bound(&lower_bound, make_term_val),
            upper_bound: map_bound(&upper_bound, make_term_val),
            limit: None,
        }
    }

    /// Creates a new `RangeQuery` over a `f64` field.
    ///
    /// If the field is not of the type `f64`, tantivy
    /// will panic when the `Weight` object is created.
    pub fn new_f64(field: String, range: Range<f64>) -> RangeQuery {
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
        field: String,
        lower_bound: Bound<f64>,
        upper_bound: Bound<f64>,
    ) -> RangeQuery {
        let make_term_val = |val: &f64| {
            Term::from_field_f64(Field::from_field_id(0), *val)
                .serialized_value_bytes()
                .to_owned()
        };
        RangeQuery {
            field,
            value_type: Type::F64,
            lower_bound: map_bound(&lower_bound, make_term_val),
            upper_bound: map_bound(&upper_bound, make_term_val),
            limit: None,
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
        field: String,
        lower_bound: Bound<u64>,
        upper_bound: Bound<u64>,
    ) -> RangeQuery {
        let make_term_val = |val: &u64| {
            Term::from_field_u64(Field::from_field_id(0), *val)
                .serialized_value_bytes()
                .to_owned()
        };
        RangeQuery {
            field,
            value_type: Type::U64,
            lower_bound: map_bound(&lower_bound, make_term_val),
            upper_bound: map_bound(&upper_bound, make_term_val),
            limit: None,
        }
    }

    /// Create a new `RangeQuery` over a `ip` field.
    ///
    /// If the field is not of the type `ip`, tantivy
    /// will panic when the `Weight` object is created.
    pub fn new_ip_bounds(
        field: String,
        lower_bound: Bound<Ipv6Addr>,
        upper_bound: Bound<Ipv6Addr>,
    ) -> RangeQuery {
        let make_term_val = |val: &Ipv6Addr| {
            Term::from_field_ip_addr(Field::from_field_id(0), *val)
                .serialized_value_bytes()
                .to_owned()
        };
        RangeQuery {
            field,
            value_type: Type::IpAddr,
            lower_bound: map_bound(&lower_bound, make_term_val),
            upper_bound: map_bound(&upper_bound, make_term_val),
            limit: None,
        }
    }

    /// Create a new `RangeQuery` over a `u64` field.
    ///
    /// If the field is not of the type `u64`, tantivy
    /// will panic when the `Weight` object is created.
    pub fn new_u64(field: String, range: Range<u64>) -> RangeQuery {
        RangeQuery::new_u64_bounds(
            field,
            Bound::Included(range.start),
            Bound::Excluded(range.end),
        )
    }

    /// Create a new `RangeQuery` over a `date` field.
    ///
    /// The two `Bound` arguments make it possible to create more complex
    /// ranges than semi-inclusive range.
    ///
    /// If the field is not of the type `date`, tantivy
    /// will panic when the `Weight` object is created.
    pub fn new_date_bounds(
        field: String,
        lower_bound: Bound<DateTime>,
        upper_bound: Bound<DateTime>,
    ) -> RangeQuery {
        let make_term_val = |val: &DateTime| {
            Term::from_field_date(Field::from_field_id(0), *val)
                .serialized_value_bytes()
                .to_owned()
        };
        RangeQuery {
            field,
            value_type: Type::Date,
            lower_bound: map_bound(&lower_bound, make_term_val),
            upper_bound: map_bound(&upper_bound, make_term_val),
            limit: None,
        }
    }

    /// Create a new `RangeQuery` over a `date` field.
    ///
    /// If the field is not of the type `date`, tantivy
    /// will panic when the `Weight` object is created.
    pub fn new_date(field: String, range: Range<DateTime>) -> RangeQuery {
        RangeQuery::new_date_bounds(
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
    pub fn new_str_bounds(
        field: String,
        lower_bound: Bound<&str>,
        upper_bound: Bound<&str>,
    ) -> RangeQuery {
        let make_term_val = |val: &&str| val.as_bytes().to_vec();
        RangeQuery {
            field,
            value_type: Type::Str,
            lower_bound: map_bound(&lower_bound, make_term_val),
            upper_bound: map_bound(&upper_bound, make_term_val),
            limit: None,
        }
    }

    /// Create a new `RangeQuery` over a `Str` field.
    ///
    /// If the field is not of the type `Str`, tantivy
    /// will panic when the `Weight` object is created.
    pub fn new_str(field: String, range: Range<&str>) -> RangeQuery {
        RangeQuery::new_str_bounds(
            field,
            Bound::Included(range.start),
            Bound::Excluded(range.end),
        )
    }

    /// Field to search over
    pub fn field(&self) -> &str {
        &self.field
    }

    /// Limit the number of term the `RangeQuery` will go through.
    ///
    /// This does not limit the number of matching document, only the number of
    /// different terms that get matched.
    pub(crate) fn limit(&mut self, limit: u64) {
        self.limit = Some(limit);
    }
}

/// Returns true if the type maps to a u64 fast field
pub(crate) fn maps_to_u64_fastfield(typ: Type) -> bool {
    match typ {
        Type::U64 | Type::I64 | Type::F64 | Type::Bool | Type::Date => true,
        Type::IpAddr => false,
        Type::Str | Type::Facet | Type::Bytes | Type::Json => false,
    }
}

impl Query for RangeQuery {
    fn weight(&self, enable_scoring: EnableScoring<'_>) -> crate::Result<Box<dyn Weight>> {
        let schema = enable_scoring.schema();
        let field_type = schema
            .get_field_entry(schema.get_field(&self.field)?)
            .field_type();
        let value_type = field_type.value_type();
        if value_type != self.value_type {
            let err_msg = format!(
                "Create a range query of the type {:?}, when the field given was of type \
                 {value_type:?}",
                self.value_type
            );
            return Err(TantivyError::SchemaError(err_msg));
        }

        if field_type.is_fast() && is_type_valid_for_fastfield_range_query(self.value_type) {
            if field_type.is_ip_addr() {
                let parse_ip_from_bytes = |data: &Vec<u8>| {
                    let ip_u128_bytes: [u8; 16] = data.as_slice().try_into().map_err(|_| {
                        crate::TantivyError::InvalidArgument(
                            "Expected 8 bytes for ip address".to_string(),
                        )
                    })?;
                    let ip_u128 = u128::from_be_bytes(ip_u128_bytes);
                    crate::Result::<Ipv6Addr>::Ok(Ipv6Addr::from_u128(ip_u128))
                };
                let lower_bound = map_bound_res(&self.lower_bound, parse_ip_from_bytes)?;
                let upper_bound = map_bound_res(&self.upper_bound, parse_ip_from_bytes)?;
                Ok(Box::new(IPFastFieldRangeWeight::new(
                    self.field.to_string(),
                    lower_bound,
                    upper_bound,
                )))
            } else {
                // We run the range query on u64 value space for performance reasons and simpicity
                // assert the type maps to u64
                assert!(maps_to_u64_fastfield(self.value_type));
                let parse_from_bytes = |data: &Vec<u8>| {
                    u64::from_be(BinarySerializable::deserialize(&mut &data[..]).unwrap())
                };

                let lower_bound = map_bound(&self.lower_bound, parse_from_bytes);
                let upper_bound = map_bound(&self.upper_bound, parse_from_bytes);
                Ok(Box::new(FastFieldRangeWeight::new_u64_lenient(
                    self.field.to_string(),
                    lower_bound,
                    upper_bound,
                )))
            }
        } else {
            Ok(Box::new(RangeWeight {
                field: self.field.to_string(),
                lower_bound: self.lower_bound.clone(),
                upper_bound: self.upper_bound.clone(),
                limit: self.limit,
            }))
        }
    }
}

pub struct RangeWeight {
    field: String,
    lower_bound: Bound<Vec<u8>>,
    upper_bound: Bound<Vec<u8>>,
    limit: Option<u64>,
}

impl RangeWeight {
    fn term_range<'a>(&self, term_dict: &'a TermDictionary) -> io::Result<TermStreamer<'a>> {
        use std::ops::Bound::*;
        let mut term_stream_builder = term_dict.range();
        term_stream_builder = match self.lower_bound {
            Included(ref term_val) => term_stream_builder.ge(term_val),
            Excluded(ref term_val) => term_stream_builder.gt(term_val),
            Unbounded => term_stream_builder,
        };
        term_stream_builder = match self.upper_bound {
            Included(ref term_val) => term_stream_builder.le(term_val),
            Excluded(ref term_val) => term_stream_builder.lt(term_val),
            Unbounded => term_stream_builder,
        };
        #[cfg(feature = "quickwit")]
        if let Some(limit) = self.limit {
            term_stream_builder = term_stream_builder.limit(limit);
        }
        term_stream_builder.into_stream()
    }
}

impl Weight for RangeWeight {
    fn scorer(&self, reader: &SegmentReader, boost: Score) -> crate::Result<Box<dyn Scorer>> {
        let max_doc = reader.max_doc();
        let mut doc_bitset = BitSet::with_max_value(max_doc);

        let inverted_index = reader.inverted_index(reader.schema().get_field(&self.field)?)?;
        let term_dict = inverted_index.terms();
        let mut term_range = self.term_range(term_dict)?;
        let mut processed_count = 0;
        while term_range.advance() {
            if let Some(limit) = self.limit {
                if limit <= processed_count {
                    break;
                }
            }
            processed_count += 1;
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

    use std::net::IpAddr;
    use std::ops::Bound;
    use std::str::FromStr;

    use rand::seq::SliceRandom;

    use super::RangeQuery;
    use crate::collector::{Count, TopDocs};
    use crate::indexer::NoMergePolicy;
    use crate::query::QueryParser;
    use crate::schema::{Document, Field, IntoIpv6Addr, Schema, FAST, INDEXED, STORED, TEXT};
    use crate::{doc, Index};

    #[test]
    fn test_range_query_simple() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let year_field = schema_builder.add_u64_field("year", INDEXED);
        let schema = schema_builder.build();

        let index = Index::create_in_ram(schema);
        {
            let mut index_writer = index.writer_for_tests()?;
            for year in 1950u64..2017u64 {
                let num_docs_within_year = 10 + (year - 1950) * (year - 1950);
                for _ in 0..num_docs_within_year {
                    index_writer.add_document(doc!(year_field => year))?;
                }
            }
            index_writer.commit()?;
        }
        let reader = index.reader()?;
        let searcher = reader.searcher();

        let docs_in_the_sixties = RangeQuery::new_u64("year".to_string(), 1960u64..1970u64);

        // ... or `1960..=1969` if inclusive range is enabled.
        let count = searcher.search(&docs_in_the_sixties, &Count)?;
        assert_eq!(count, 2285);
        Ok(())
    }

    #[test]
    fn test_range_query_with_limit() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let year_field = schema_builder.add_u64_field("year", INDEXED);
        let schema = schema_builder.build();

        let index = Index::create_in_ram(schema);
        {
            let mut index_writer = index.writer_for_tests()?;
            for year in 1950u64..2017u64 {
                if year == 1963 {
                    continue;
                }
                let num_docs_within_year = 10 + (year - 1950) * (year - 1950);
                for _ in 0..num_docs_within_year {
                    index_writer.add_document(doc!(year_field => year))?;
                }
            }
            index_writer.commit()?;
        }
        let reader = index.reader()?;
        let searcher = reader.searcher();

        let mut docs_in_the_sixties = RangeQuery::new_u64("year".to_string(), 1960u64..1970u64);
        docs_in_the_sixties.limit(5);

        // due to the limit and no docs in 1963, it's really only 1960..=1965
        let count = searcher.search(&docs_in_the_sixties, &Count)?;
        assert_eq!(count, 836);
        Ok(())
    }

    #[test]
    fn test_range_query() -> crate::Result<()> {
        let int_field: Field;
        let schema = {
            let mut schema_builder = Schema::builder();
            int_field = schema_builder.add_i64_field("intfield", INDEXED);
            schema_builder.build()
        };

        let index = Index::create_in_ram(schema);
        {
            let mut index_writer = index.writer_with_num_threads(1, 60_000_000)?;
            index_writer.set_merge_policy(Box::new(NoMergePolicy));

            for i in 1..100 {
                let mut doc = Document::new();
                for j in 1..100 {
                    if i % j == 0 {
                        doc.add_i64(int_field, j as i64);
                    }
                }
                index_writer.add_document(doc)?;
                if i == 10 {
                    index_writer.commit()?;
                }
            }

            index_writer.commit()?;
        }
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        assert_eq!(searcher.segment_readers().len(), 2);
        let count_multiples =
            |range_query: RangeQuery| searcher.search(&range_query, &Count).unwrap();

        assert_eq!(
            count_multiples(RangeQuery::new_i64("intfield".to_string(), 10..11)),
            9
        );
        assert_eq!(
            count_multiples(RangeQuery::new_i64_bounds(
                "intfield".to_string(),
                Bound::Included(10),
                Bound::Included(11)
            )),
            18
        );
        assert_eq!(
            count_multiples(RangeQuery::new_i64_bounds(
                "intfield".to_string(),
                Bound::Excluded(9),
                Bound::Included(10)
            )),
            9
        );
        assert_eq!(
            count_multiples(RangeQuery::new_i64_bounds(
                "intfield".to_string(),
                Bound::Included(9),
                Bound::Unbounded
            )),
            91
        );
        Ok(())
    }

    #[test]
    fn test_range_float() -> crate::Result<()> {
        let float_field: Field;
        let schema = {
            let mut schema_builder = Schema::builder();
            float_field = schema_builder.add_f64_field("floatfield", INDEXED);
            schema_builder.build()
        };

        let index = Index::create_in_ram(schema);
        {
            let mut index_writer = index.writer_with_num_threads(1, 60_000_000).unwrap();
            let mut docs = Vec::new();
            for i in 1..100 {
                let mut doc = Document::new();
                for j in 1..100 {
                    if i % j == 0 {
                        doc.add_f64(float_field, j as f64);
                    }
                }
                docs.push(doc);
            }

            docs.shuffle(&mut rand::thread_rng());
            let mut docs_it = docs.into_iter();
            for doc in (&mut docs_it).take(50) {
                index_writer.add_document(doc)?;
            }
            index_writer.commit()?;
            for doc in docs_it {
                index_writer.add_document(doc)?;
            }
            index_writer.commit()?;
        }
        let reader = index.reader()?;
        let searcher = reader.searcher();
        assert_eq!(searcher.segment_readers().len(), 2);
        let count_multiples =
            |range_query: RangeQuery| searcher.search(&range_query, &Count).unwrap();

        assert_eq!(
            count_multiples(RangeQuery::new_f64("floatfield".to_string(), 10.0..11.0)),
            9
        );
        assert_eq!(
            count_multiples(RangeQuery::new_f64_bounds(
                "floatfield".to_string(),
                Bound::Included(10.0),
                Bound::Included(11.0)
            )),
            18
        );
        assert_eq!(
            count_multiples(RangeQuery::new_f64_bounds(
                "floatfield".to_string(),
                Bound::Excluded(9.0),
                Bound::Included(10.0)
            )),
            9
        );
        assert_eq!(
            count_multiples(RangeQuery::new_f64_bounds(
                "floatfield".to_string(),
                Bound::Included(9.0),
                Bound::Unbounded
            )),
            91
        );
        Ok(())
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
          year => 1990_i64
        ))?;
        index_writer.commit()?;
        let reader = index.reader()?;
        let searcher = reader.searcher();
        let query_parser = QueryParser::for_index(&index, vec![title]);
        let query = query_parser.parse_query("hemoglobin AND year:[1970 TO 1990]")?;
        let top_docs = searcher.search(&query, &TopDocs::with_limit(10))?;
        assert_eq!(top_docs.len(), 1);
        Ok(())
    }

    #[test]
    fn search_ip_range_test_posting_list() {
        search_ip_range_test_opt(false);
    }

    #[test]
    fn search_ip_range_test() {
        search_ip_range_test_opt(true);
    }

    fn search_ip_range_test_opt(with_fast_field: bool) {
        let mut schema_builder = Schema::builder();
        let ip_field = if with_fast_field {
            schema_builder.add_ip_addr_field("ip", INDEXED | STORED | FAST)
        } else {
            schema_builder.add_ip_addr_field("ip", INDEXED | STORED)
        };
        let text_field = schema_builder.add_text_field("text", TEXT | STORED);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let ip_addr_1 = IpAddr::from_str("127.0.0.10").unwrap().into_ipv6_addr();
        let ip_addr_2 = IpAddr::from_str("127.0.0.20").unwrap().into_ipv6_addr();

        {
            let mut index_writer = index.writer_for_tests().unwrap();
            for _ in 0..1_000 {
                index_writer
                    .add_document(doc!(
                        ip_field => ip_addr_1,
                        text_field => "BLUBBER"
                    ))
                    .unwrap();
            }
            for _ in 0..1_000 {
                index_writer
                    .add_document(doc!(
                        ip_field => ip_addr_2,
                        text_field => "BLOBBER"
                    ))
                    .unwrap();
            }
            index_writer.commit().unwrap();
        }
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        assert_eq!(searcher.segment_readers().len(), 1);

        let get_num_hits = |query| {
            let (_top_docs, count) = searcher
                .search(&query, &(TopDocs::with_limit(10), Count))
                .unwrap();
            count
        };
        let query_from_text = |text: &str| {
            QueryParser::for_index(&index, vec![])
                .parse_query(text)
                .unwrap()
        };

        // Inclusive range
        assert_eq!(
            get_num_hits(query_from_text("ip:[127.0.0.1 TO 127.0.0.20]")),
            2000
        );

        assert_eq!(
            get_num_hits(query_from_text("ip:[127.0.0.10 TO 127.0.0.20]")),
            2000
        );

        assert_eq!(
            get_num_hits(query_from_text("ip:[127.0.0.11 TO 127.0.0.20]")),
            1000
        );

        assert_eq!(
            get_num_hits(query_from_text("ip:[127.0.0.11 TO 127.0.0.19]")),
            0
        );

        assert_eq!(get_num_hits(query_from_text("ip:[127.0.0.11 TO *]")), 1000);
        assert_eq!(get_num_hits(query_from_text("ip:[127.0.0.21 TO *]")), 0);
        assert_eq!(get_num_hits(query_from_text("ip:[* TO 127.0.0.9]")), 0);
        assert_eq!(get_num_hits(query_from_text("ip:[* TO 127.0.0.10]")), 1000);

        // Exclusive range
        assert_eq!(
            get_num_hits(query_from_text("ip:{127.0.0.1 TO 127.0.0.20}")),
            1000
        );

        assert_eq!(
            get_num_hits(query_from_text("ip:{127.0.0.1 TO 127.0.0.21}")),
            2000
        );

        assert_eq!(
            get_num_hits(query_from_text("ip:{127.0.0.10 TO 127.0.0.20}")),
            0
        );

        assert_eq!(
            get_num_hits(query_from_text("ip:{127.0.0.11 TO 127.0.0.20}")),
            0
        );

        assert_eq!(
            get_num_hits(query_from_text("ip:{127.0.0.11 TO 127.0.0.19}")),
            0
        );

        assert_eq!(get_num_hits(query_from_text("ip:{127.0.0.11 TO *}")), 1000);
        assert_eq!(get_num_hits(query_from_text("ip:{127.0.0.10 TO *}")), 1000);
        assert_eq!(get_num_hits(query_from_text("ip:{127.0.0.21 TO *}")), 0);
        assert_eq!(get_num_hits(query_from_text("ip:{127.0.0.20 TO *}")), 0);
        assert_eq!(get_num_hits(query_from_text("ip:{127.0.0.19 TO *}")), 1000);
        assert_eq!(get_num_hits(query_from_text("ip:{* TO 127.0.0.9}")), 0);
        assert_eq!(get_num_hits(query_from_text("ip:{* TO 127.0.0.10}")), 0);
        assert_eq!(get_num_hits(query_from_text("ip:{* TO 127.0.0.11}")), 1000);

        // Inclusive/Exclusive range
        assert_eq!(
            get_num_hits(query_from_text("ip:[127.0.0.1 TO 127.0.0.20}")),
            1000
        );

        assert_eq!(
            get_num_hits(query_from_text("ip:{127.0.0.1 TO 127.0.0.20]")),
            2000
        );

        // Intersection
        assert_eq!(
            get_num_hits(query_from_text(
                "text:BLUBBER AND ip:[127.0.0.10 TO 127.0.0.10]"
            )),
            1000
        );

        assert_eq!(
            get_num_hits(query_from_text(
                "text:BLOBBER AND ip:[127.0.0.10 TO 127.0.0.10]"
            )),
            0
        );

        assert_eq!(
            get_num_hits(query_from_text(
                "text:BLOBBER AND ip:[127.0.0.20 TO 127.0.0.20]"
            )),
            1000
        );

        assert_eq!(
            get_num_hits(query_from_text(
                "text:BLUBBER AND ip:[127.0.0.20 TO 127.0.0.20]"
            )),
            0
        );
    }
}

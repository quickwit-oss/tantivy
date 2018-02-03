use schema::{Field, IndexRecordOption, Term};
use query::{Query, Scorer, Weight};
use termdict::{TermDictionary, TermStreamer, TermStreamerBuilder};
use core::SegmentReader;
use common::DocBitSet;
use Result;
use std::any::Any;
use core::Searcher;
use query::BitSetDocSet;
use query::ConstScorer;

#[derive(Clone, Debug)]
enum Boundary {
    Included(Vec<u8>),
    Excluded(Vec<u8>),
    Unbounded,
}

#[derive(Clone, Debug)]
pub struct TermRange {
    field: Field,
    left_bound: Boundary,
    right_bound: Boundary,
}

impl TermRange {
    fn for_field(field: Field) -> TermRange {
        TermRange {
            field,
            left_bound: Boundary::Unbounded,
            right_bound: Boundary::Unbounded,
        }
    }

    fn left_included(mut self, left: Term) -> TermRange {
        assert_eq!(left.field(), self.field);
        self.left_bound = Boundary::Included(left.value_bytes().to_owned());
        self
    }

    fn left_excluded(mut self, left: Term) -> TermRange {
        assert_eq!(left.field(), self.field);
        self.left_bound = Boundary::Excluded(left.value_bytes().to_owned());
        self
    }

    fn right_included(mut self, right: Term) -> TermRange {
        assert_eq!(right.field(), self.field);
        self.right_bound = Boundary::Included(right.value_bytes().to_owned());
        self
    }

    fn right_excluded(mut self, right: Term) -> TermRange {
        assert_eq!(right.field(), self.field);
        self.right_bound = Boundary::Excluded(right.value_bytes().to_owned());
        self
    }

    fn term_range<'a, T>(&self, term_dict: &'a T) -> T::Streamer
    where
        T: TermDictionary<'a> + 'a,
    {
        use self::Boundary::*;
        let mut term_stream_builder = term_dict.range();
        term_stream_builder = match &self.left_bound {
            &Included(ref term_val) => term_stream_builder.ge(term_val),
            &Excluded(ref term_val) => term_stream_builder.gt(term_val),
            &Unbounded => term_stream_builder,
        };
        term_stream_builder = match &self.right_bound {
            &Included(ref term_val) => term_stream_builder.le(term_val),
            &Excluded(ref term_val) => term_stream_builder.lt(term_val),
            &Unbounded => term_stream_builder,
        };
        term_stream_builder.into_stream()
    }
}

#[derive(Debug)]
pub struct RangeQuery {
    range_definition: TermRange,
}

impl RangeQuery {
    fn new(range_definition: TermRange) -> RangeQuery {
        RangeQuery { range_definition }
    }
}

impl Query for RangeQuery {
    fn as_any(&self) -> &Any {
        self
    }

    fn weight(&self, _searcher: &Searcher) -> Result<Box<Weight>> {
        Ok(box RangeWeight {
            range_definition: self.range_definition.clone(),
        })
    }
}

pub struct RangeWeight {
    range_definition: TermRange,
}

impl Weight for RangeWeight {
    fn scorer<'a>(&'a self, reader: &'a SegmentReader) -> Result<Box<Scorer + 'a>> {
        let max_doc = reader.max_doc();
        let mut doc_bitset = DocBitSet::with_maxdoc(max_doc);

        let inverted_index = reader.inverted_index(self.range_definition.field);
        let term_dict = inverted_index.terms();
        let mut term_range = self.range_definition.term_range(term_dict);
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
        Ok(box ConstScorer::new(doc_bitset))
    }
}

#[cfg(test)]
mod tests {

    use Index;
    use schema::{Document, Field, SchemaBuilder, INT_INDEXED};

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
        use collector::CountCollector;
        use schema::Term;
        use query::Query;
        use super::{RangeQuery, TermRange};

        let count_multiples = |range: TermRange| {
            let mut count_collector = CountCollector::default();
            let range_query = RangeQuery::new(range);
            range_query
                .search(&*searcher, &mut count_collector)
                .unwrap();
            count_collector.count()
        };

        assert_eq!(
            count_multiples(
                TermRange::for_field(int_field)
                    .left_included(Term::from_field_i64(int_field, 10))
                    .right_excluded(Term::from_field_i64(int_field, 11))
            ),
            9
        );
        assert_eq!(
            count_multiples(
                TermRange::for_field(int_field)
                    .left_included(Term::from_field_i64(int_field, 10))
                    .right_included(Term::from_field_i64(int_field, 11))
            ),
            18
        );
        assert_eq!(
            count_multiples(
                TermRange::for_field(int_field)
                    .left_excluded(Term::from_field_i64(int_field, 9))
                    .right_included(Term::from_field_i64(int_field, 10))
            ),
            9
        );
        assert_eq!(
            count_multiples(
                TermRange::for_field(int_field).left_excluded(Term::from_field_i64(int_field, 9))
            ),
            90
        );
    }

}

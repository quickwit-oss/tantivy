use common::BitSet;
use crate::query::{BitSetDocSet, ConstScorer, EnableScoring, Explanation, Query, Scorer, Weight};
use crate::query::explanation::does_not_match;
use crate::{DocId, Score, SegmentReader, Result};
use crate::schema::{Field, IndexRecordOption};

/// An Exists Query matches all of the documents
/// containing a specific indexed field.
///
/// ```rust
/// use tantivy::collector::Count;
/// use tantivy::query::ExistsQuery;
/// use tantivy::schema::{Schema, TEXT};
/// use tantivy::{doc, Index};
///
/// # fn test() -> tantivy::Result<()> {
/// let mut schema_builder = Schema::builder();
/// let title = schema_builder.add_text_field("title", TEXT);
/// let author = schema_builder.add_text_field("author", TEXT);
/// let schema = schema_builder.build();
/// let index = Index::create_in_ram(schema);
/// {
///     let mut index_writer = index.writer(3_000_000)?;
///     index_writer.add_document(doc!(
///         title => "The Name of the Wind",
///         author => "Patrick Rothfuss"
///     ))?;
///     index_writer.add_document(doc!(
///         title => "The Diary of Muadib",
///     ))?;
///     index_writer.add_document(doc!(
///         title => "A Dairy Cow",
///         author => "John Webster"
///     ))?;
///     index_writer.commit()?;
/// }
///
/// let reader = index.reader()?;
/// let searcher = reader.searcher();
///
/// let query = ExistsQuery::new(author);
/// let count = searcher.search(&query, &Count)?;
/// assert_eq!(count, 2);
/// Ok(())
/// # }
/// # assert!(test().is_ok());
/// ```
#[derive(Clone, Debug)]
pub struct ExistsQuery {
    field: Field,
}

impl ExistsQuery {
    /// Creates a new ExistsQuery with a given field
    pub fn new(field: Field) -> Self {
        ExistsQuery { field }
    }
}

impl Query for ExistsQuery {
    fn weight(&self, _: EnableScoring<'_>) -> Result<Box<dyn Weight>> {
        Ok(Box::new(ExistsWeight {
            field: self.field
        }))
    }
}

/// Weight associated with the `ExistsQuery` query.
pub struct ExistsWeight {
    field: Field,
}

impl Weight for ExistsWeight {
    fn scorer(&self, reader: &SegmentReader, boost: Score) -> Result<Box<dyn Scorer>> {
        let max_doc = reader.max_doc();
        let mut doc_bitset = BitSet::with_max_value(max_doc);

        let inverted_index = reader.inverted_index(self.field)?;
        let mut term_stream = inverted_index.terms().stream()?;

        while term_stream.advance() {
            let term_info = term_stream.value();

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

    fn explain(&self, reader: &SegmentReader, doc: DocId) -> Result<Explanation> {
        let mut scorer = self.scorer(reader, 1.0)?;
        if scorer.seek(doc) != doc {
            return Err(does_not_match(doc));
        }
        Ok(Explanation::new("ExistsQuery", 1.0))
    }
}

#[cfg(test)]
mod tests {
    use crate::{DocSet, Index, TERMINATED};
    use crate::query::{EnableScoring, Query};
    use crate::query::exists_query::ExistsQuery;
    use crate::schema::{Field, Schema, TEXT};

    fn create_test_index() -> crate::Result<(Index, Field)> {
        let mut schema_builder = Schema::builder();
        let field1 = schema_builder.add_text_field("text1", TEXT);
        let field2 = schema_builder.add_text_field("text2", TEXT);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema.clone());
        let mut index_writer = index.writer_for_tests()?;
        index_writer.add_document(doc!(field1=>"bbb"))?;
        index_writer.add_document(doc!(field2=>"aaa"))?;
        index_writer.add_document(doc!(field1=>"ccc"))?;
        index_writer.commit()?;
        index_writer.add_document(doc!(field1=>"ddd"))?;
        index_writer.commit()?;
        Ok((index, field1))
    }

    #[test]
    fn test_exists_query() -> crate::Result<()> {
        let (index, field) = create_test_index()?;
        let reader = index.reader()?;
        let searcher = reader.searcher();
        let query = ExistsQuery::new(field);
        let weight = query.weight(EnableScoring::disabled_from_schema(&index.schema()))?;
        {
            let reader = searcher.segment_reader(0);
            let mut scorer = weight.scorer(reader, 1.0)?;
            assert_eq!(scorer.doc(), 0u32);
            assert_eq!(scorer.advance(), 2u32);
            assert_eq!(scorer.doc(), 2u32);
            assert_eq!(scorer.advance(), TERMINATED);
        }
        {
            let reader = searcher.segment_reader(1);
            let mut scorer = weight.scorer(reader, 1.0)?;
            assert_eq!(scorer.doc(), 0u32);
            assert_eq!(scorer.advance(), TERMINATED);
        }
        Ok(())
    }

    #[test]
    fn test_exists_query_with_boost() -> crate::Result<()> {
        let (index, field) = create_test_index()?;
        let reader = index.reader()?;
        let searcher = reader.searcher();
        let query = ExistsQuery::new(field);
        let weight = query.weight(EnableScoring::disabled_from_schema(searcher.schema()))?;
        let reader = searcher.segment_reader(0);
        {
            let mut scorer = weight.scorer(reader, 2.0)?;
            assert_eq!(scorer.doc(), 0u32);
            assert_eq!(scorer.score(), 2.0);
            assert_eq!(scorer.advance(), 2u32);
            assert_eq!(scorer.doc(), 2u32);
            assert_eq!(scorer.score(), 2.0);
        }
        {
            let mut scorer = weight.scorer(reader, 1.5)?;
            assert_eq!(scorer.doc(), 0u32);
            assert_eq!(scorer.score(), 1.5);
        }
        Ok(())
    }
}

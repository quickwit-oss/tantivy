use tantivy::collector::TopDocs;
use tantivy::query::{
    BooleanQuery, EnableScoring, Explanation, Occur, Query, Scorer, TermQuery, Weight,
};
use tantivy::schema::{IndexRecordOption, Schema, TextFieldIndexing, TextOptions};
use tantivy::{doc, DocId, Index, Score, SegmentReader, TantivyDocument, Term};

#[derive(Debug)]
struct GenericQuery(Box<dyn Query>);
struct GenericWeight(Box<dyn Weight>);

impl Clone for GenericQuery {
    fn clone(&self) -> Self {
        Self(self.0.box_clone())
    }
}

impl Query for GenericQuery {
    fn weight(&self, scoring: EnableScoring<'_>) -> tantivy::Result<Box<dyn Weight>> {
        Ok(Box::new(GenericWeight(self.0.weight(scoring)?)))
    }
}

impl Weight for GenericWeight {
    fn scorer(&self, reader: &SegmentReader, boost: Score) -> tantivy::Result<Box<dyn Scorer>> {
        self.0.scorer(reader, boost)
    }

    fn explain(&self, reader: &SegmentReader, doc: DocId) -> tantivy::Result<Explanation> {
        self.0.explain(reader, doc)
    }
}

#[test]
fn test_block_wand_without_fieldnorms() -> tantivy::Result<()> {
    for record_option in [
        IndexRecordOption::WithFreqs,
        IndexRecordOption::WithFreqsAndPositions,
    ] {
        for num_docs in [128, 256, 300] {
            let mut schema = Schema::builder();
            let field = schema.add_text_field(
                "text",
                TextOptions::default().set_indexing_options(
                    TextFieldIndexing::default()
                        .set_tokenizer("default")
                        .set_index_option(record_option)
                        .set_fieldnorms(false),
                ),
            );
            let index = Index::create_in_ram(schema.build());
            let mut writer = index.writer::<TantivyDocument>(20_000_000)?;
            for id in 0..num_docs {
                let frequency = if id == num_docs - 1 { 100 } else { 1 };
                writer.add_document(doc!(field => std::iter::repeat_n("amber stone", frequency)
                    .collect::<Vec<_>>().join(" ")))?;
            }
            writer.commit()?;
            let searcher = index.reader()?.searcher();
            let term_query =
                || TermQuery::new(Term::from_field_text(field, "amber"), record_option);
            let queries: Vec<Box<dyn Query>> = vec![
                Box::new(term_query()),
                Box::new(BooleanQuery::new(vec![
                    (Occur::Should, Box::new(term_query())),
                    (
                        Occur::Should,
                        Box::new(TermQuery::new(
                            Term::from_field_text(field, "stone"),
                            record_option,
                        )),
                    ),
                ])),
            ];
            for query in queries {
                for limit in [1, 10] {
                    let collector = TopDocs::with_limit(limit).order_by_score();
                    let expected = searcher.search(&GenericQuery(query.box_clone()), &collector)?;
                    assert_eq!(expected[0].1.doc_id, num_docs - 1);
                    let actual = searcher.search(&query, &collector)?;
                    let bits = |hits: Vec<(Score, tantivy::DocAddress)>| {
                        hits.into_iter()
                            .map(|(score, address)| (score.to_bits(), address))
                            .collect::<Vec<_>>()
                    };
                    assert_eq!(
                        bits(actual),
                        bits(expected),
                        "{record_option:?}, {num_docs}, {limit}, {query:?}"
                    );
                }
            }
        }
    }
    Ok(())
}

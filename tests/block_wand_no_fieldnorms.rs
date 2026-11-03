use tantivy::collector::TopDocs;
use tantivy::merge_policy::NoMergePolicy;
use tantivy::query::{
    BooleanQuery, BoostQuery, EnableScoring, Explanation, Occur, Query, Scorer, TermQuery, Weight,
};
use tantivy::schema::{IndexRecordOption, Schema, TextFieldIndexing, TextOptions, INDEXED};
use tantivy::{
    doc, DocAddress, DocId, Index, Score, Searcher, SegmentReader, TantivyDocument, Term,
};

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

fn assert_pruning_parity(
    searcher: &Searcher,
    query: &dyn Query,
    limit: usize,
) -> tantivy::Result<Vec<(Score, DocAddress)>> {
    let collector = TopDocs::with_limit(limit).order_by_score();
    let expected = searcher.search(&GenericQuery(query.box_clone()), &collector)?;
    let actual = searcher.search(query, &collector)?;
    let bits = |hits: &[(Score, DocAddress)]| {
        hits.iter()
            .map(|(score, address)| (score.to_bits(), *address))
            .collect::<Vec<_>>()
    };
    assert_eq!(bits(&actual), bits(&expected), "{query:?}, limit={limit}");
    Ok(actual)
}

#[test]
fn test_block_wand_without_fieldnorms() -> tantivy::Result<()> {
    for record_option in [
        IndexRecordOption::WithFreqs,
        IndexRecordOption::WithFreqsAndPositions,
    ] {
        for (tokens, early_frequency, winning_frequency) in [
            ("amber stone", 1, 100),
            ("amber", 3, 20),
            ("amber stone", 3, 20),
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
                let mut writer = index.writer_with_num_threads::<TantivyDocument>(1, 20_000_000)?;
                for id in 0..num_docs {
                    let frequency = if id == num_docs - 1 {
                        winning_frequency
                    } else if id < 10 {
                        early_frequency
                    } else {
                        1
                    };
                    writer.add_document(doc!(field => std::iter::repeat_n(tokens, frequency)
                    .collect::<Vec<_>>().join(" ")))?;
                }
                writer.commit()?;
                let searcher = index.reader()?.searcher();
                let term_query =
                    || TermQuery::new(Term::from_field_text(field, "amber"), record_option);
                let mut queries: Vec<Box<dyn Query>> = vec![
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
                if tokens.contains("stone") {
                    queries.push(Box::new(BooleanQuery::new(vec![
                        (Occur::Must, Box::new(term_query())),
                        (
                            Occur::Must,
                            Box::new(TermQuery::new(
                                Term::from_field_text(field, "stone"),
                                record_option,
                            )),
                        ),
                    ])));
                }
                for query in queries {
                    for limit in [1, 10] {
                        let hits = assert_pruning_parity(&searcher, query.as_ref(), limit)?;
                        assert_eq!(hits[0].1.doc_id, num_docs - 1);
                    }
                }
            }
        }
    }
    Ok(())
}

#[test]
fn test_block_wand_with_implicit_frequencies() -> tantivy::Result<()> {
    for fieldnorms in [false, true] {
        let mut schema = Schema::builder();
        let field = schema.add_text_field(
            "text",
            TextOptions::default().set_indexing_options(
                TextFieldIndexing::default()
                    .set_tokenizer("default")
                    .set_index_option(IndexRecordOption::Basic)
                    .set_fieldnorms(fieldnorms),
            ),
        );
        let index = Index::create_in_ram(schema.build());
        let mut writer = index.writer_with_num_threads::<TantivyDocument>(1, 20_000_000)?;
        for id in 0..256 {
            let padding = if id == 255 { 0 } else { 10 };
            writer.add_document(
                doc!(field => format!("amber stone {}", "padding ".repeat(padding))),
            )?;
        }
        writer.commit()?;
        let searcher = index.reader()?.searcher();
        let mut queries: Vec<Box<dyn Query>> = vec![Box::new(TermQuery::new(
            Term::from_field_text(field, "amber"),
            IndexRecordOption::Basic,
        ))];
        for occur in [Occur::Should, Occur::Must] {
            queries.push(Box::new(BooleanQuery::new(
                ["amber", "stone"]
                    .into_iter()
                    .map(|text| {
                        (
                            occur,
                            Box::new(TermQuery::new(
                                Term::from_field_text(field, text),
                                IndexRecordOption::Basic,
                            )) as Box<dyn Query>,
                        )
                    })
                    .collect(),
            )));
        }
        for query in queries {
            let hits = assert_pruning_parity(&searcher, query.as_ref(), 10)?;
            if fieldnorms {
                assert_eq!(hits[0].1.doc_id, 255);
            }
        }
    }
    Ok(())
}

#[test]
fn test_block_wand_without_fieldnorms_after_delete_and_merge() -> tantivy::Result<()> {
    let mut schema = Schema::builder();
    let id_field = schema.add_u64_field("id", INDEXED);
    let field = schema.add_text_field(
        "text",
        TextOptions::default().set_indexing_options(
            TextFieldIndexing::default()
                .set_tokenizer("default")
                .set_index_option(IndexRecordOption::WithFreqs)
                .set_fieldnorms(false),
        ),
    );
    let index = Index::create_in_ram(schema.build());
    let mut writer = index.writer_with_num_threads::<TantivyDocument>(1, 20_000_000)?;
    writer.set_merge_policy(Box::new(NoMergePolicy));
    for segment in 0..2 {
        for id in 0..300 {
            let frequency = if id == 299 { 100 } else { 1 };
            let text = std::iter::repeat_n("amber stone", frequency)
                .collect::<Vec<_>>()
                .join(" ");
            writer.add_document(doc!(id_field => (segment * 300 + id) as u64, field => text))?;
        }
        writer.commit()?;
    }
    writer.delete_term(Term::from_field_u64(id_field, 599));
    writer.commit()?;
    for merged in [false, true] {
        if merged {
            writer.merge(&index.searchable_segment_ids()?).wait()?;
        }
        let searcher = index.reader()?.searcher();
        assert_eq!(searcher.num_docs(), 599);
        assert_eq!(searcher.segment_readers().len(), if merged { 1 } else { 2 });
        let term_query = || {
            Box::new(TermQuery::new(
                Term::from_field_text(field, "amber"),
                IndexRecordOption::WithFreqs,
            )) as Box<dyn Query>
        };
        let mut queries = vec![term_query()];
        for occur in [Occur::Should, Occur::Must] {
            queries.push(Box::new(BooleanQuery::new(vec![
                (occur, term_query()),
                (
                    occur,
                    Box::new(TermQuery::new(
                        Term::from_field_text(field, "stone"),
                        IndexRecordOption::WithFreqs,
                    )),
                ),
            ])));
        }
        for query in queries {
            for limit in [1, 10] {
                assert_pruning_parity(&searcher, query.as_ref(), limit)?;
            }
        }
    }
    Ok(())
}

#[test]
fn test_block_wand_boosts() -> tantivy::Result<()> {
    for fieldnorms in [false, true] {
        for disjoint in [false, true] {
            let mut schema = Schema::builder();
            let field = schema.add_text_field(
                "text",
                TextOptions::default().set_indexing_options(
                    TextFieldIndexing::default()
                        .set_tokenizer("default")
                        .set_index_option(IndexRecordOption::WithFreqs)
                        .set_fieldnorms(fieldnorms),
                ),
            );
            let index = Index::create_in_ram(schema.build());
            let mut writer = index.writer_with_num_threads::<TantivyDocument>(1, 20_000_000)?;
            for id in 0..256 {
                let tokens = if !disjoint || id % 3 == 0 {
                    "amber stone"
                } else if id % 3 == 1 {
                    "amber"
                } else {
                    "stone"
                };
                let frequency = if id < 128 { 3 } else { 1 };
                let text = std::iter::repeat_n(tokens, frequency)
                    .collect::<Vec<_>>()
                    .join(" ");
                writer.add_document(doc!(field => text))?;
            }
            writer.commit()?;
            let searcher = index.reader()?.searcher();
            for (amber_boost, stone_boost) in [(-1.0, -1.0), (-1.0, 1.0), (0.0, 1.0), (0.5, 3.0)] {
                for occur in [Occur::Should, Occur::Must] {
                    let query = BooleanQuery::new(
                        [("amber", amber_boost), ("stone", stone_boost)]
                            .into_iter()
                            .map(|(text, boost)| {
                                (
                                    occur,
                                    Box::new(BoostQuery::new(
                                        Box::new(TermQuery::new(
                                            Term::from_field_text(field, text),
                                            IndexRecordOption::WithFreqs,
                                        )),
                                        boost,
                                    )) as Box<dyn Query>,
                                )
                            })
                            .collect(),
                    );
                    for limit in [1, 10] {
                        assert_pruning_parity(&searcher, &query, limit)?;
                    }
                }
            }
        }
    }
    Ok(())
}

#[test]
fn test_block_wand_json_implicit_frequencies() -> tantivy::Result<()> {
    for num_docs in [10, 127, 128, 129, 256, 384] {
        let mut schema = Schema::builder();
        let field = schema.add_json_field("json", tantivy::schema::TEXT);
        let schema = schema.build();
        let index = Index::create_in_ram(schema.clone());
        let mut writer = index.writer_with_num_threads::<TantivyDocument>(1, 20_000_000)?;
        for id in 0..num_docs {
            let number = if id % 3 == 0 { 5 } else { 7 };
            let json = format!(r#"{{"json":{{"num":{number},"text":"foo"}}}}"#);
            writer.add_document(TantivyDocument::parse_json(&schema, &json).unwrap())?;
        }
        writer.commit()?;
        let searcher = index.reader()?.searcher();
        let mut number = Term::from_field_json_path(field, "num", false);
        number.append_type_and_fast_value(5i64);
        let mut text = Term::from_field_json_path(field, "text", false);
        text.append_type_and_str("foo");
        for terms in [
            [number.clone(), text.clone()],
            [text.clone(), number.clone()],
        ] {
            for occur in [Occur::Should, Occur::Must] {
                let query = BooleanQuery::new(
                    terms
                        .iter()
                        .map(|term| {
                            (
                                occur,
                                Box::new(TermQuery::new(term.clone(), IndexRecordOption::WithFreqs))
                                    as Box<dyn Query>,
                            )
                        })
                        .collect(),
                );
                for limit in [1, 10, 50] {
                    let hits = assert_pruning_parity(&searcher, &query, limit)?;
                    let expected = if occur == Occur::Must {
                        (num_docs + 2) / 3
                    } else {
                        num_docs
                    };
                    assert_eq!(hits.len(), limit.min(expected));
                }
            }
        }
    }
    Ok(())
}

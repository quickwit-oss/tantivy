use tantivy::indexer::NoMergePolicy;
use tantivy::postings::Postings;
use tantivy::schema::{IndexRecordOption, Schema, TextFieldIndexing, TextOptions, FAST, INDEXED};
use tantivy::{
    doc, DocSet, Index, IndexSettings, IndexSortByField, IndexWriter, Order, Term, TERMINATED,
};

#[test]
fn sorted_merge_preserves_postings_with_deletes_and_missing_sort_keys() -> tantivy::Result<()> {
    for order in [Order::Asc, Order::Desc] {
        for record in [
            IndexRecordOption::Basic,
            IndexRecordOption::WithFreqs,
            IndexRecordOption::WithFreqsAndPositions,
        ] {
            let mut schema = Schema::builder();
            let id_field = schema.add_u64_field("id", FAST | INDEXED);
            let sort_field = schema.add_u64_field("sort", FAST);
            let text = schema.add_text_field(
                "text",
                TextOptions::default().set_indexing_options(
                    TextFieldIndexing::default()
                        .set_tokenizer("default")
                        .set_index_option(record),
                ),
            );
            let index = Index::builder()
                .schema(schema.build())
                .settings(IndexSettings {
                    sort_by_field: Some(IndexSortByField {
                        field: "sort".into(),
                        order,
                    }),
                    ..Default::default()
                })
                .create_in_ram()?;
            let mut writer: IndexWriter = index.writer_with_num_threads(1, 15_000_000)?;
            writer.set_merge_policy(Box::new(NoMergePolicy));
            for segment in 0..3u64 {
                for row in 0..50u64 {
                    let id = row * 3 + segment;
                    let mut document =
                        doc!(id_field=>id, text=>"common anchor ".repeat((id%7+1) as usize));
                    if id % 5 != 0 {
                        document.add_u64(sort_field, id % 11);
                    }
                    writer.add_document(document)?;
                }
                writer.commit()?;
            }
            // Includes deletion of the first posting in some cursors and gaps later.
            for id in (0..150u64).step_by(7) {
                writer.delete_term(Term::from_field_u64(id_field, id));
            }
            writer.commit()?;
            let ids = index.searchable_segment_ids()?;
            assert_eq!(ids.len(), 3);
            writer.merge(&ids).wait()?;
            let reader = index.reader()?;
            let searcher = reader.searcher();
            assert_eq!(searcher.segment_readers().len(), 1);
            let segment = &searcher.segment_readers()[0];
            let ids = segment.fast_fields().u64("id")?;
            let inverted = segment.inverted_index(text)?;
            for (token, offset) in [("common", 0), ("anchor", 1)] {
                let mut postings = inverted
                    .read_postings(&Term::from_field_text(text, token), record)?
                    .unwrap();
                let mut seen = std::collections::BTreeSet::new();
                let mut positions = Vec::new();
                let mut previous_doc = None;
                while postings.doc() != TERMINATED {
                    let doc = postings.doc();
                    if let Some(previous) = previous_doc {
                        assert!(doc > previous);
                    }
                    previous_doc = Some(doc);
                    let id = ids.first(doc).unwrap();
                    assert_ne!(id % 7, 0);
                    assert!(seen.insert(id));
                    let repetitions = (id % 7 + 1) as u32;
                    if record != IndexRecordOption::Basic {
                        assert_eq!(postings.term_freq(), repetitions);
                    }
                    if record == IndexRecordOption::WithFreqsAndPositions {
                        postings.positions(&mut positions);
                        assert_eq!(
                            positions,
                            (0..repetitions).map(|p| 2 * p + offset).collect::<Vec<_>>()
                        );
                    }
                    postings.advance();
                }
                assert_eq!(seen, (0..150u64).filter(|id| id % 7 != 0).collect());
            }
        }
    }
    Ok(())
}

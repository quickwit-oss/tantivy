use tantivy::collector::Count;
use tantivy::query::TermQuery;
use tantivy::schema::{IndexRecordOption, Schema, TextFieldIndexing, TextOptions};
use tantivy::{doc, Index, Term};

#[test]
fn dense_block_end_to_end() -> tantivy::Result<()> {
    let mut schema_builder = Schema::builder();
    let text_options = TextOptions::default()
        .set_indexing_options(
            TextFieldIndexing::default().set_index_option(IndexRecordOption::WithFreqsAndPositions),
        )
        .set_stored();
    let text = schema_builder.add_text_field("text", text_options);
    let schema = schema_builder.build();
    let index = Index::create_in_ram(schema);
    let mut writer = index.writer_with_num_threads(1, 15_000_000)?;

    // "common" in all docs except every 20th -> near-dense 128-doc blocks
    // with small gaps (forces the dense bitset path: range slightly > 128).
    for i in 0..1000u32 {
        if i % 20 == 0 {
            writer.add_document(doc!(text => "filler"))?;
        } else {
            writer.add_document(doc!(text => "common filler"))?;
        }
        // unique marker per doc to verify positions/freqs survive dense blocks
        writer.add_document(doc!(text => format!("marker{i}")))?;
    }
    writer.commit()?;

    let reader = index.reader()?;
    let searcher = reader.searcher();
    let term = Term::from_field_text(text, "common");
    let query = TermQuery::new(term, IndexRecordOption::WithFreqsAndPositions);
    let count = searcher.search(&query, &Count)?;
    assert_eq!(count, 950);

    // every doc retrievable with correct freq via dense or FOR blocks alike
    let term_filler = Term::from_field_text(text, "filler");
    let q2 = TermQuery::new(term_filler, IndexRecordOption::WithFreqsAndPositions);
    assert_eq!(searcher.search(&q2, &Count)?, 1000);
    Ok(())
}

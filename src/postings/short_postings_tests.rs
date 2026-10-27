use crate::collector::{Count, TopDocs};
use crate::indexer::NoMergePolicy;
use crate::postings::Postings;
use crate::query::{PhraseQuery, TermQuery};
use crate::schema::{IndexRecordOption, Schema, TextFieldIndexing, TextOptions, TEXT};
use crate::{DocSet, Index, IndexSettings, Term, TERMINATED};

#[test]
fn test_short_postings_json_top_score_and_numeric_frequency_resets() -> crate::Result<()> {
    use crate::query::QueryParser;

    let mut schema = Schema::builder();
    let json = schema.add_json_field("json", TEXT);
    let mut index = Index::create_in_ram(schema.build());
    index.settings_mut().max_inline_docs = 8;
    let mut writer = index.writer_for_tests()?;
    writer.add_document(doc!(json => serde_json::json!({"s": "text", "n": 2})))?;
    writer.add_document(doc!(json => serde_json::json!({"s": "text text text text text text text text text text", "n": 2})))?;
    writer.commit()?;
    let searcher = index.reader()?.searcher();
    let query =
        QueryParser::for_index(&index, vec![json]).parse_query("json.s:text OR json.n:2")?;
    let top_one = searcher.search(&query, &TopDocs::with_limit(1).order_by_score())?;
    let top_two = searcher.search(&query, &TopDocs::with_limit(2).order_by_score())?;
    assert_eq!(top_two[0].1.doc_id, 1);
    assert_eq!(top_one[0], top_two[0]);

    let mut schema = Schema::builder();
    let json = schema.add_json_field("json", TEXT);
    let mut index = Index::create_in_ram(schema.build());
    index.settings_mut().max_inline_docs = 8;
    let mut writer = index.writer_for_tests()?;
    for doc in 0..130 {
        let mut value = serde_json::json!({"n": 2});
        if doc == 0 {
            value["s"] = serde_json::json!("text text");
            value["short"] = serde_json::json!(3);
        }
        writer.add_document(doc!(json => value))?;
    }
    writer.commit()?;
    let searcher = index.reader()?.searcher();
    let inverted = searcher.segment_reader(0).inverted_index(json)?;
    let mut numeric = Term::from_field_json_path(json, "n", false);
    numeric.append_type_and_fast_value(2i64);
    let mut short_numeric = Term::from_field_json_path(json, "short", false);
    short_numeric.append_type_and_fast_value(3i64);
    let mut text = Term::from_field_json_path(json, "s", false);
    text.append_type_and_str("text");
    for initial in [&numeric, &short_numeric, &text] {
        let mut block = inverted
            .read_block_postings(initial, IndexRecordOption::WithFreqsAndPositions)?
            .unwrap();
        for term in [&text, &short_numeric, &numeric, &text, &numeric] {
            let info = inverted.get_term_info(term)?.unwrap();
            inverted.reset_block_postings_from_terminfo(&info, &mut block)?;
            assert_eq!(block.doc(0), 0);
            if term == &text {
                assert_eq!(block.freqs(), [2]);
            } else {
                assert!(block.freqs().is_empty());
                assert_eq!(block.freq(0), 1);
            }
            if term == &numeric {
                let i = block.seek(129);
                assert_eq!(block.doc(i), 129);
                assert_eq!(block.freq(i), 1);
            }
        }
    }
    Ok(())
}

#[test]
fn test_short_postings_record_options_at_every_inline_cutoff() -> crate::Result<()> {
    let options = [
        IndexRecordOption::Basic,
        IndexRecordOption::WithFreqs,
        IndexRecordOption::WithFreqsAndPositions,
    ];
    for max_inline_docs in 0..=32 {
        for indexed_option in options {
            let mut schema = Schema::builder();
            let field = schema.add_text_field(
                "text",
                TextOptions::default().set_indexing_options(
                    TextFieldIndexing::default().set_index_option(indexed_option),
                ),
            );
            let index = Index::builder()
                .schema(schema.build())
                .settings(IndexSettings {
                    max_inline_docs,
                    ..Default::default()
                })
                .create_in_ram()?;
            let mut writer = index.writer_for_tests()?;
            let largest_count = max_inline_docs + 1;
            // Gaps distinguish absolute document ids from posting ordinals.
            for doc in 0..largest_count * 2 + 1 {
                let mut tokens = Vec::new();
                if doc % 2 == 1 {
                    let ordinal = doc / 2;
                    for count in 1..=largest_count {
                        if ordinal < count {
                            for _ in 0..=ordinal % 3 {
                                tokens.push(format!("term{count}"));
                            }
                        }
                    }
                }
                writer.add_document(doc!(field => tokens.join(" ")))?;
            }
            writer.commit()?;
            let searcher = index.reader()?.searcher();
            assert_eq!(searcher.segment_readers().len(), 1);
            let inverted = searcher.segment_reader(0).inverted_index(field)?;
            for count in 1..=largest_count {
                let term = Term::from_field_text(field, &format!("term{count}"));
                let info = inverted.get_term_info(&term)?.unwrap();
                assert_eq!(info.doc_freq, count);
                assert_eq!(
                    info.short_postings.is_some(),
                    count <= max_inline_docs,
                    "max={max_inline_docs}, count={count}, option={indexed_option:?}"
                );
                let expected_docs: Vec<_> = (0..count).map(|ordinal| ordinal * 2 + 1).collect();
                if let Some(short) = &info.short_postings {
                    assert_eq!(short.doc_ids, expected_docs);
                    assert_eq!(
                        short.term_freqs.is_empty(),
                        indexed_option == IndexRecordOption::Basic
                    );
                }
                for requested_option in options {
                    let mut postings = inverted.read_postings(&term, requested_option)?.unwrap();
                    for ordinal in 0..count {
                        assert_eq!(postings.doc(), ordinal * 2 + 1);
                        let freq = if indexed_option == IndexRecordOption::Basic
                            || requested_option == IndexRecordOption::Basic
                        {
                            1
                        } else {
                            ordinal % 3 + 1
                        };
                        assert_eq!(postings.term_freq(), freq);
                        if indexed_option == IndexRecordOption::WithFreqsAndPositions
                            && requested_option == IndexRecordOption::WithFreqsAndPositions
                        {
                            let start = count.saturating_sub(ordinal + 1) * freq;
                            let mut positions = Vec::new();
                            postings.positions(&mut positions);
                            assert_eq!(positions, (start..start + freq).collect::<Vec<_>>());
                        }
                        postings.advance();
                    }
                    assert_eq!(postings.doc(), TERMINATED);
                    let mut postings = inverted.read_postings(&term, requested_option)?.unwrap();
                    assert_eq!(postings.seek((count * 2 - 2).max(1)), count * 2 - 1);
                    assert_eq!(postings.seek(count * 2), TERMINATED);
                }
            }
            // Reuse a reader across both representations and different list lengths.
            let mut block = inverted
                .read_block_postings(
                    &Term::from_field_text(field, "term1"),
                    IndexRecordOption::WithFreqs,
                )?
                .unwrap();
            for count in [largest_count, max_inline_docs.max(1), 1, largest_count, 1] {
                let info = inverted
                    .get_term_info(&Term::from_field_text(field, &format!("term{count}")))?
                    .unwrap();
                inverted.reset_block_postings_from_terminfo(&info, &mut block)?;
                let docs: Vec<_> = (0..count).map(|ordinal| ordinal * 2 + 1).collect();
                let freqs: Vec<_> = (0..count)
                    .map(|ordinal| {
                        if indexed_option == IndexRecordOption::Basic {
                            1
                        } else {
                            ordinal % 3 + 1
                        }
                    })
                    .collect();
                assert_eq!(block.docs(), docs);
                assert_eq!(
                    block.freqs().is_empty(),
                    indexed_option == IndexRecordOption::Basic
                );
                for (ordinal, freq) in freqs.into_iter().enumerate() {
                    assert_eq!(block.freq(ordinal), freq);
                }
            }
        }
    }
    Ok(())
}

#[test]
fn test_short_postings_phrase_matches_and_bm25_scores() -> crate::Result<()> {
    let mut schema = Schema::builder();
    let field = schema.add_text_field("text", TEXT);
    let mut index = Index::create_in_ram(schema.build());
    index.settings_mut().max_inline_docs = 8;
    let mut writer = index.writer_for_tests()?;
    for text in [
        "alpha beta filler filler",
        "alpha beta alpha beta",
        "beta alpha filler filler",
    ] {
        writer.add_document(doc!(field => text))?;
    }
    writer.commit()?;
    let searcher = index.reader()?.searcher();
    let inverted = searcher.segment_reader(0).inverted_index(field)?;
    let alpha = Term::from_field_text(field, "alpha");
    let beta = Term::from_field_text(field, "beta");
    for term in [&alpha, &beta] {
        assert!(inverted
            .get_term_info(term)?
            .unwrap()
            .short_postings
            .is_some());
    }
    for (terms, expected) in [
        (vec![alpha.clone(), beta.clone()], 2),
        (vec![beta.clone(), alpha.clone()], 2),
        (vec![alpha.clone(), alpha.clone()], 0),
    ] {
        assert_eq!(searcher.search(&PhraseQuery::new(terms), &Count)?, expected);
    }
    let query = TermQuery::new(alpha, IndexRecordOption::WithFreqs);
    let hits = searcher.search(&query, &TopDocs::with_limit(3).order_by_score())?;
    assert_eq!(hits.len(), 3);
    // All field lengths are four. BM25 reduces to idf * tf * 2.2 / (tf + 1.2).
    let idf = (1.0f32 + 0.5 / 3.5).ln();
    for ((score, address), (doc, freq)) in hits.iter().zip([(1, 2.0), (0, 1.0), (2, 1.0)]) {
        assert_eq!(address.doc_id, doc);
        let expected = idf * freq * 2.2 / (freq + 1.2);
        assert!((score - expected).abs() < 1e-6, "{score} != {expected}");
    }
    Ok(())
}

#[test]
fn test_short_postings_term_info_survives_dictionary_block_changes_and_stream_drop(
) -> crate::Result<()> {
    let mut schema = Schema::builder();
    let field = schema.add_text_field("text", TEXT);
    let mut index = Index::create_in_ram(schema.build());
    index.settings_mut().max_inline_docs = 8;
    let mut writer = index.writer_for_tests()?;
    // Distinct long suffixes put the key deltas across several 4 KB dictionary blocks.
    let terms: Vec<_> = (0..1024u64)
        .map(|i| format!("t{i:04x}{:016x}", i.wrapping_mul(0x9e3779b97f4a7c15)))
        .collect();
    let mut tokens = Vec::new();
    let mut term_positions = Vec::new();
    for (i, term) in terms.iter().enumerate() {
        let start = tokens.len() as u32;
        tokens.push(term.as_str());
        if i % 2 == 1 {
            tokens.push(term.as_str());
        }
        term_positions.push((start..tokens.len() as u32).collect::<Vec<_>>());
    }
    writer.add_document(doc!(field => tokens.join(" ")))?;
    writer.commit()?;
    let searcher = index.reader()?.searcher();
    let inverted = searcher.segment_reader(0).inverted_index(field)?;
    let mut stream = inverted.terms().stream()?;
    let mut saved = Vec::new();
    let mut count = 0;
    while stream.advance() {
        assert_eq!(stream.key(), terms[count].as_bytes());
        assert!(stream.value().short_postings.is_some());
        if [0, 511, 1023].contains(&count) {
            saved.push((count, stream.value().clone()));
        }
        count += 1;
    }
    assert_eq!(count, terms.len());
    drop(stream);
    for (i, info) in saved {
        let short = info.short_postings.as_ref().unwrap();
        assert_eq!(short.doc_ids, [0]);
        assert_eq!(short.term_freqs, [term_positions[i].len() as u32]);
        let mut deltas = term_positions[i].clone();
        if deltas.len() == 2 {
            deltas[1] -= deltas[0];
        }
        assert_eq!(short.position_deltas, deltas);
        let mut postings = inverted
            .read_postings_from_terminfo(&info, IndexRecordOption::WithFreqsAndPositions)?;
        assert_eq!(postings.doc(), 0);
        let mut positions = Vec::new();
        postings.positions(&mut positions);
        assert_eq!(positions, term_positions[i]);
        assert_eq!(postings.advance(), TERMINATED);
        let term = Term::from_field_text(field, &terms[i]);
        let mut postings = inverted
            .read_postings(&term, IndexRecordOption::WithFreqsAndPositions)?
            .unwrap();
        postings.positions(&mut positions);
        assert_eq!(positions, term_positions[i]);
    }
    Ok(())
}

#[test]
fn test_short_postings_merge_and_delete_remapping() -> crate::Result<()> {
    let mut schema = Schema::builder();
    let field = schema.add_text_field("text", TEXT);
    let basic = schema.add_text_field("basic", crate::schema::STRING);
    let mut index = Index::create_in_ram(schema.build());
    index.settings_mut().max_inline_docs = 8;
    let mut writer = index.writer_for_tests()?;
    writer.set_merge_policy(Box::new(NoMergePolicy));
    writer.add_document(doc!(field => "shrink remove"))?;
    for _ in 0..8 {
        writer.add_document(
            doc!(field => "shrink grow grow", basic => "present", basic => "short"),
        )?;
    }
    writer.commit()?;
    let first_segment = index.searchable_segment_ids()?[0];
    writer.wait_merging_threads()?;
    index.settings_mut().max_inline_docs = 32;
    let mut writer = index.writer_for_tests()?;
    writer.set_merge_policy(Box::new(NoMergePolicy));
    writer.add_document(doc!(field => "grow", basic => "present"))?;
    writer.commit()?;
    let second_segment = index
        .searchable_segment_ids()?
        .into_iter()
        .find(|id| *id != first_segment)
        .unwrap();
    let shrink = Term::from_field_text(field, "shrink");
    let grow = Term::from_field_text(field, "grow");
    let reader = index.reader()?;
    for segment in reader.searcher().segment_readers() {
        let inverted = segment.inverted_index(field)?;
        let grow_info = inverted.get_term_info(&grow)?.unwrap();
        assert_eq!(
            grow_info.doc_freq,
            if segment.max_doc() == 9 { 8 } else { 1 }
        );
        assert!(grow_info.short_postings.is_some());
        if segment.segment_id() == first_segment {
            let shrink_info = inverted.get_term_info(&shrink)?.unwrap();
            assert_eq!(shrink_info.doc_freq, 9);
            assert!(shrink_info.short_postings.is_none());
        }
    }
    writer.delete_term(Term::from_field_text(field, "remove"));
    writer.commit()?;
    writer.wait_merging_threads()?;
    index.settings_mut().max_inline_docs = 8;
    let mut writer = index.writer_for_tests::<crate::TantivyDocument>()?;
    writer.set_merge_policy(Box::new(NoMergePolicy));
    writer.merge(&[first_segment, second_segment]).wait()?;
    reader.reload()?;
    let searcher = reader.searcher();
    assert_eq!(searcher.segment_readers().len(), 1);
    assert_eq!(searcher.num_docs(), 9);
    let inverted = searcher.segment_reader(0).inverted_index(field)?;
    for (term, count) in [(&shrink, 8), (&grow, 9)] {
        let info = inverted.get_term_info(term)?.unwrap();
        assert_eq!(info.doc_freq, count);
        assert_eq!(info.short_postings.is_some(), count == 8);
        let mut postings = inverted
            .read_postings(term, IndexRecordOption::WithFreqsAndPositions)?
            .unwrap();
        for doc in 0..count {
            assert_eq!(postings.doc(), doc);
            let expected = if term == &shrink || doc == 8 {
                vec![0]
            } else {
                vec![1, 2]
            };
            assert_eq!(postings.term_freq(), expected.len() as u32);
            let mut positions = Vec::new();
            postings.positions(&mut positions);
            assert_eq!(positions, expected);
            postings.advance();
        }
        assert_eq!(postings.doc(), TERMINATED);
    }
    let basic_index = searcher.segment_reader(0).inverted_index(basic)?;
    for (token, count) in [("present", 9), ("short", 8)] {
        let term = Term::from_field_text(basic, token);
        let info = basic_index.get_term_info(&term)?.unwrap();
        assert_eq!(info.doc_freq, count);
        assert_eq!(info.short_postings.is_some(), count == 8);
        if let Some(short) = info.short_postings {
            assert!(short.term_freqs.is_empty());
            assert!(short.position_deltas.is_empty());
        }
        assert!(basic_index
            .read_block_postings(&term, IndexRecordOption::WithFreqs)?
            .unwrap()
            .freqs()
            .is_empty());
    }
    assert!(inverted
        .get_term_info(&Term::from_field_text(field, "remove"))?
        .is_none());
    assert_eq!(
        searcher.search(&PhraseQuery::new(vec![shrink, grow]), &Count)?,
        8
    );
    Ok(())
}

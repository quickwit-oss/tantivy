use std::collections::HashSet;

use rand::{thread_rng, Rng};

use crate::schema::*;
use crate::{doc, schema, Index, IndexSettings, IndexSortByField, Order, Searcher};

fn check_index_content(searcher: &Searcher, vals: &[u64]) -> crate::Result<()> {
    assert!(searcher.segment_readers().len() < 20);
    assert_eq!(searcher.num_docs() as usize, vals.len());
    for segment_reader in searcher.segment_readers() {
        let store_reader = segment_reader.get_store_reader()?;
        for doc_id in 0..segment_reader.max_doc() {
            let _doc = store_reader.get(doc_id)?;
        }
    }
    Ok(())
}

#[test]
#[ignore]
fn test_functional_store() -> crate::Result<()> {
    let mut schema_builder = Schema::builder();

    let id_field = schema_builder.add_u64_field("id", INDEXED | STORED);
    let schema = schema_builder.build();

    let index = Index::create_in_ram(schema);
    let reader = index.reader()?;

    let mut rng = thread_rng();

    let mut index_writer = index.writer_with_num_threads(3, 12_000_000)?;

    let mut doc_set: Vec<u64> = Vec::new();

    let mut doc_id = 0u64;
    for _iteration in 0..get_num_iterations() {
        let num_docs: usize = rng.gen_range(0..4);
        if !doc_set.is_empty() {
            let doc_to_remove_id = rng.gen_range(0..doc_set.len());
            let removed_doc_id = doc_set.swap_remove(doc_to_remove_id);
            index_writer.delete_term(Term::from_field_u64(id_field, removed_doc_id));
        }
        for _ in 0..num_docs {
            doc_set.push(doc_id);
            index_writer.add_document(doc!(id_field=>doc_id))?;
            doc_id += 1;
        }
        index_writer.commit()?;
        reader.reload()?;
        let searcher = reader.searcher();
        check_index_content(&searcher, &doc_set)?;
    }
    Ok(())
}

fn get_num_iterations() -> usize {
    std::env::var("NUM_FUNCTIONAL_TEST_ITERATIONS")
        .map(|str| str.parse().unwrap())
        .unwrap_or(2000)
}
#[test]
#[ignore]
fn test_functional_indexing_sorted() -> crate::Result<()> {
    let mut schema_builder = Schema::builder();

    let id_field = schema_builder.add_u64_field("id", INDEXED | FAST);
    let multiples_field = schema_builder.add_u64_field("multiples", INDEXED);
    let text_field_options = TextOptions::default()
        .set_indexing_options(
            TextFieldIndexing::default()
                .set_index_option(schema::IndexRecordOption::WithFreqsAndPositions),
        )
        .set_stored();
    let text_field = schema_builder.add_text_field("text_field", text_field_options);
    let schema = schema_builder.build();

    let mut index_builder = Index::builder().schema(schema);
    index_builder = index_builder.settings(IndexSettings {
        sort_by_field: Some(IndexSortByField {
            field: "id".to_string(),
            order: Order::Desc,
        }),
        ..Default::default()
    });
    let index = index_builder.create_from_tempdir().unwrap();

    let reader = index.reader()?;

    let mut rng = thread_rng();

    let mut index_writer = index.writer_with_num_threads(3, 120_000_000)?;

    let mut committed_docs: HashSet<u64> = HashSet::new();
    let mut uncommitted_docs: HashSet<u64> = HashSet::new();

    for _ in 0..get_num_iterations() {
        let random_val = rng.gen_range(0..20);
        if random_val == 0 {
            index_writer.commit()?;
            committed_docs.extend(&uncommitted_docs);
            uncommitted_docs.clear();
            reader.reload()?;
            let searcher = reader.searcher();
            // check that everything is correct.
            check_index_content(
                &searcher,
                &committed_docs.iter().cloned().collect::<Vec<u64>>(),
            )?;
        } else if committed_docs.remove(&random_val) || uncommitted_docs.remove(&random_val) {
            let doc_id_term = Term::from_field_u64(id_field, random_val);
            index_writer.delete_term(doc_id_term);
        } else {
            uncommitted_docs.insert(random_val);
            let mut doc = Document::new();
            doc.add_u64(id_field, random_val);
            for i in 1u64..10u64 {
                doc.add_u64(multiples_field, random_val * i);
            }
            doc.add_text(text_field, get_text());
            index_writer.add_document(doc)?;
        }
    }
    Ok(())
}

const LOREM: &str = "Doc Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod \
                     tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, \
                     quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo \
                     consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse \
                     cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat \
                     non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.";
fn get_text() -> String {
    use rand::seq::SliceRandom;
    let mut rng = thread_rng();
    let tokens: Vec<_> = LOREM.split(' ').collect();
    let random_val = rng.gen_range(0..20);

    (0..random_val)
        .map(|_| tokens.choose(&mut rng).unwrap())
        .cloned()
        .collect::<Vec<_>>()
        .join(" ")
}

#[test]
#[ignore]
fn test_functional_indexing_unsorted() -> crate::Result<()> {
    let mut schema_builder = Schema::builder();

    let id_field = schema_builder.add_u64_field("id", INDEXED);
    let multiples_field = schema_builder.add_u64_field("multiples", INDEXED);
    let text_field_options = TextOptions::default()
        .set_indexing_options(
            TextFieldIndexing::default()
                .set_index_option(schema::IndexRecordOption::WithFreqsAndPositions),
        )
        .set_stored();
    let text_field = schema_builder.add_text_field("text_field", text_field_options);
    let schema = schema_builder.build();

    let index = Index::create_from_tempdir(schema)?;
    let reader = index.reader()?;

    let mut rng = thread_rng();

    let mut index_writer = index.writer_with_num_threads(3, 120_000_000)?;

    let mut committed_docs: HashSet<u64> = HashSet::new();
    let mut uncommitted_docs: HashSet<u64> = HashSet::new();

    for _ in 0..get_num_iterations() {
        let random_val = rng.gen_range(0..20);
        if random_val == 0 {
            index_writer.commit()?;
            committed_docs.extend(&uncommitted_docs);
            uncommitted_docs.clear();
            reader.reload()?;
            let searcher = reader.searcher();
            // check that everything is correct.
            check_index_content(
                &searcher,
                &committed_docs.iter().cloned().collect::<Vec<u64>>(),
            )?;
        } else if committed_docs.remove(&random_val) || uncommitted_docs.remove(&random_val) {
            let doc_id_term = Term::from_field_u64(id_field, random_val);
            index_writer.delete_term(doc_id_term);
        } else {
            uncommitted_docs.insert(random_val);
            let mut doc = Document::new();
            doc.add_u64(id_field, random_val);
            for i in 1u64..10u64 {
                doc.add_u64(multiples_field, random_val * i);
            }
            doc.add_text(text_field, get_text());
            index_writer.add_document(doc)?;
        }
    }
    Ok(())
}

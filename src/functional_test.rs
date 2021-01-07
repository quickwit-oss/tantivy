use crate::Index;
use crate::Searcher;
use crate::{doc, schema::*};
use rand::thread_rng;
use rand::Rng;
use std::collections::HashSet;

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
    for iteration in 0..500 {
        dbg!(iteration);
        let num_docs: usize = rng.gen_range(0..4);
        if doc_set.len() >= 1 {
            let doc_to_remove_id = rng.gen_range(0..doc_set.len());
            let removed_doc_id = doc_set.swap_remove(doc_to_remove_id);
            index_writer.delete_term(Term::from_field_u64(id_field, removed_doc_id));
        }
        for _ in 0..num_docs {
            doc_set.push(doc_id);
            index_writer.add_document(doc!(id_field=>doc_id));
            doc_id += 1;
        }
        index_writer.commit()?;
        reader.reload()?;
        let searcher = reader.searcher();
        check_index_content(&searcher, &doc_set)?;
    }
    Ok(())
}

#[test]
#[ignore]
fn test_functional_indexing() -> crate::Result<()> {
    let mut schema_builder = Schema::builder();

    let id_field = schema_builder.add_u64_field("id", INDEXED);
    let multiples_field = schema_builder.add_u64_field("multiples", INDEXED);
    let schema = schema_builder.build();

    let index = Index::create_from_tempdir(schema)?;
    let reader = index.reader()?;

    let mut rng = thread_rng();

    let mut index_writer = index.writer_with_num_threads(3, 120_000_000)?;

    let mut committed_docs: HashSet<u64> = HashSet::new();
    let mut uncommitted_docs: HashSet<u64> = HashSet::new();

    for _ in 0..200 {
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
        } else {
            if committed_docs.remove(&random_val) || uncommitted_docs.remove(&random_val) {
                let doc_id_term = Term::from_field_u64(id_field, random_val);
                index_writer.delete_term(doc_id_term);
            } else {
                uncommitted_docs.insert(random_val);
                let mut doc = Document::new();
                doc.add_u64(id_field, random_val);
                for i in 1u64..10u64 {
                    doc.add_u64(multiples_field, random_val * i);
                }
                index_writer.add_document(doc);
            }
        }
    }
    Ok(())
}

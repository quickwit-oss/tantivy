use rand::thread_rng;
use std::collections::HashSet;

use rand::Rng;
use rand::distributions::Range;
use schema::*;
use Index;
use Searcher;

fn check_index_content(searcher: &Searcher, vals: &HashSet<u64>) {
    assert!(searcher.segment_readers().len() < 20);
    assert_eq!(searcher.num_docs() as usize, vals.len());
}

#[test]
#[ignore]
#[cfg(feature = "mmap")]
fn test_indexing() {
    let mut schema_builder = SchemaBuilder::default();

    let id_field = schema_builder.add_u64_field("id", INT_INDEXED);
    let multiples_field = schema_builder.add_u64_field("multiples", INT_INDEXED);
    let schema = schema_builder.build();

    let index = Index::create_from_tempdir(schema).unwrap();

    let universe = Range::new(0u64, 20u64);
    let mut rng = thread_rng();

    let mut index_writer = index.writer_with_num_threads(3, 120_000_000).unwrap();

    let mut committed_docs: HashSet<u64> = HashSet::new();
    let mut uncommitted_docs: HashSet<u64> = HashSet::new();

    for _ in 0..200 {
        let random_val = rng.sample(&universe);
        if random_val == 0 {
            index_writer.commit().expect("Commit failed");
            committed_docs.extend(&uncommitted_docs);
            uncommitted_docs.clear();
            index.load_searchers().unwrap();
            let searcher = index.searcher();
            // check that everything is correct.
            check_index_content(&searcher, &committed_docs);
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
}

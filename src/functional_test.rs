use std::collections::HashSet;
use rand::thread_rng;

use schema::*;
use Index;
use Searcher;
use rand::distributions::{IndependentSample, Range};

fn check_index_content(searcher: &Searcher, vals: &HashSet<u32>) {
    assert!(searcher.segment_readers().len() < 20);
    assert_eq!(searcher.num_docs() as usize, vals.len());
}

#[test]
#[ignore]
fn test_indexing() {

    let mut schema_builder = SchemaBuilder::default();

    let id_field = schema_builder.add_u32_field("id", U32_INDEXED);
    let multiples_field = schema_builder.add_u32_field("multiples", U32_INDEXED);    
    let schema = schema_builder.build();

    let index = Index::create_from_tempdir(schema).unwrap();

    let universe = Range::new(0u32, 20u32);
    let mut rng = thread_rng();

    let mut index_writer = index.writer_with_num_threads(3, 120_000_000).unwrap();

    let mut committed_docs: HashSet<u32> = HashSet::new();
    let mut uncommitted_docs: HashSet<u32> = HashSet::new();

    for _ in 0..200 {
        let random_val = universe.ind_sample(&mut rng);
        if random_val == 0 {
            index_writer.commit().expect("Commit failed");
            committed_docs.extend(&uncommitted_docs);
            uncommitted_docs.clear();
            index.load_searchers().unwrap();
            let searcher = index.searcher();
            // check that everything is correct.
            check_index_content(&searcher, &committed_docs);
        }
        else {
            if committed_docs.remove(&random_val) ||
               uncommitted_docs.remove(&random_val) {
                let doc_id_term = Term::from_field_u32(id_field, random_val);
                index_writer.delete_term(doc_id_term);
            }
            else {
                uncommitted_docs.insert(random_val);
                let mut doc = Document::new();
                doc.add_u32(id_field, random_val);
                for i in 1u32..10u32 {
                    doc.add_u32(multiples_field, random_val * i);
                }
                index_writer.add_document(doc);
            }
        }
    }
}

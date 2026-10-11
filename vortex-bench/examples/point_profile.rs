//! Repeats the filtered `load_block` scan of one Vortex column, for profiling.
//!
//! Usage: `cargo run --release --example point_profile -- <index_dir> <field> <num_iters>`

use std::sync::Arc;

use tantivy::aggregation::ValueSourceProvider;
use tantivy::collector::DocSetCollector;
use tantivy::query::TermQuery;
use tantivy::schema::IndexRecordOption;
use tantivy::{DocId, Index, Term};
use vortex_bench::{VortexPlugin, VortexSegmentCache, VortexValueSourceProvider};

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mut index = Index::open_in_dir(&args[1]).unwrap();
    index.register_plugin(Arc::new(VortexPlugin));
    let num_iters: usize = args[3].parse().unwrap();
    let searcher = index.reader().unwrap().searcher();
    let reader = searcher.segment_reader(0);
    let filter_field = index.schema().get_field("filter_field").unwrap();
    let query = TermQuery::new(Term::from_field_text(filter_field, "a"), IndexRecordOption::Basic);
    let mut docs: Vec<DocId> = searcher
        .search(&query, &DocSetCollector)
        .unwrap()
        .into_iter()
        .map(|doc_address| doc_address.doc_id)
        .collect();
    docs.sort_unstable();
    let provider = VortexValueSourceProvider::new(&args[2], Arc::new(VortexSegmentCache::default()));
    let (mut values, mut docids, mut row_ids) = (Vec::new(), Vec::new(), Vec::new());
    let mut checksum = 0u64;
    let start = std::time::Instant::now();
    for _ in 0..num_iters {
        let mut source = provider.for_segment(reader, None).unwrap().unwrap();
        for block in docs.chunks(64) {
            source.load_block(block, &mut values, &mut docids, &mut row_ids);
            checksum = checksum.wrapping_add(values.iter().sum::<u64>());
        }
    }
    let elapsed = start.elapsed();
    println!(
        "checksum {checksum} {:.2} ns/value",
        elapsed.as_nanos() as f64 / (num_iters * docs.len()) as f64
    );
}

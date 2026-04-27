use std::collections::HashMap;
use std::path::Path;

use binggan::plugins::PeakMemAllocPlugin;
use binggan::{black_box, InputGroup, PeakMemAlloc, INSTRUMENTED_SYSTEM};
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::SeedableRng;
use rand_distr::Distribution;
use serde_json::json;
use tantivy::aggregation::agg_req::Aggregations;
use tantivy::aggregation::AggregationCollector;
use tantivy::query::AllQuery;
use tantivy::schema::{Schema, FAST, STRING};
use tantivy::{doc, Index};

#[global_allocator]
pub static GLOBAL: &PeakMemAlloc<std::alloc::System> = &INSTRUMENTED_SYSTEM;

const NUM_UNIQUE_IPS: u64 = 2_000_000;
const NUM_ASNS: u64 = 22_600;
const NUM_PATHS: u64 = 500_000;

const INDEX_DIR: &str = "bench-index";

fn main() {
    let index = get_or_build_index().unwrap();
    let inputs = vec![("nested_terms_cardinality", index)];
    let mut group = InputGroup::new_with_inputs(inputs);
    group.add_plugin(PeakMemAllocPlugin::new(GLOBAL));

    group.register("terms_ip_50_terms_asn_10_cardinality_path", |index| {
        let agg_req = json!({
            "by_ip": {
                "terms": { "field": "ip", "size": 50 },
                "aggs": {
                    "by_asn": {
                        "terms": { "field": "asn", "size": 10 },
                        "aggs": {
                            "path_cardinality": {
                                "cardinality": { "field": "path" }
                            }
                        }
                    }
                }
            }
        });
        execute_agg(index, agg_req);
    });

    group.run();
}

fn execute_agg(index: &Index, agg_req: serde_json::Value) {
    let agg_req: Aggregations = serde_json::from_value(agg_req).unwrap();
    let collector = AggregationCollector::from_aggs(agg_req, Default::default());
    let reader = index.reader().unwrap();
    let searcher = reader.searcher();
    black_box(searcher.search(&AllQuery, &collector).unwrap());
}

fn get_or_build_index() -> tantivy::Result<Index> {
    let index_path = Path::new(INDEX_DIR);
    if index_path.exists() {
        return Index::open_in_dir(index_path);
    }
    std::fs::create_dir_all(index_path)?;

    let mut schema_builder = Schema::builder();
    let ip_field = schema_builder.add_text_field("ip", STRING | FAST);
    let asn_field = schema_builder.add_text_field("asn", STRING | FAST);
    let path_field = schema_builder.add_text_field("path", STRING | FAST);
    let schema = schema_builder.build();

    let index = Index::create_in_dir(index_path, schema)?;

    let mut rng = StdRng::from_seed([42u8; 32]);
    let zipf = rand_distr::Zipf::new(NUM_UNIQUE_IPS as f64, 1.1).unwrap();

    // Pre-assign a stable asn to each ip.
    let mut ip_to_asn: HashMap<u64, u64> = HashMap::with_capacity(NUM_UNIQUE_IPS as usize);

    // Generate all rows: long-tail ip distribution, each ip gets a deterministic asn,
    // and every ip in 0..NUM_UNIQUE_IPS appears at least once.
    let mut rows: Vec<(u64, u64)> = Vec::with_capacity(NUM_UNIQUE_IPS as usize * 2);

    // Guarantee every ip appears at least once.
    for ip_val in 0..NUM_UNIQUE_IPS {
        let asn_val = ip_val % NUM_ASNS;
        ip_to_asn.insert(ip_val, asn_val);
        rows.push((ip_val, asn_val));
    }

    // Add extra rows drawn from the Zipf distribution to create the long-tail effect.
    let extra_rows = NUM_UNIQUE_IPS;
    for _ in 0..extra_rows {
        let ip_val = (zipf.sample(&mut rng) as u64 - 1).min(NUM_UNIQUE_IPS - 1);
        let asn_val = *ip_to_asn
            .entry(ip_val)
            .or_insert_with(|| ip_val % NUM_ASNS);
        rows.push((ip_val, asn_val));
    }

    // Shuffle so that document order is not sorted by ip.
    rows.shuffle(&mut rng);

    let uniform_path = rand_distr::Uniform::new(0u64, NUM_PATHS).unwrap();

    let mut index_writer = index.writer_with_num_threads(1, 200_000_000)?;
    for (ip_val, asn_val) in &rows {
        let path_val: u64 = uniform_path.sample(&mut rng);
        index_writer.add_document(doc!(
            ip_field => format!("ip_{ip_val}"),
            asn_field => format!("asn_{asn_val}"),
            path_field => format!("path_{path_val}"),
        ))?;
    }
    index_writer.commit()?;

    Ok(index)
}

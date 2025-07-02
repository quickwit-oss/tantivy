use binggan::plugins::PeakMemAllocPlugin;
use binggan::{black_box, InputGroup, PeakMemAlloc, INSTRUMENTED_SYSTEM};
use rand::prelude::SliceRandom;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use rand_distr::Distribution;
use serde_json::json;
use tantivy::collector::TopDocs;
use tantivy::fastfield::FastValue;
use tantivy::query::AllQuery;
use tantivy::schema::{IndexRecordOption, Schema, TextFieldIndexing, FAST, STRING};
use tantivy::{doc, Index, Order};

#[global_allocator]
pub static GLOBAL: &PeakMemAlloc<std::alloc::System> = &INSTRUMENTED_SYSTEM;

/// Mini macro to register a function via its name
macro_rules! register {
    ($runner:expr, $func:ident) => {
        $runner.register(stringify!($func), move |index| {
            $func(index);
        })
    };
}

fn main() {
    let inputs = vec![
        ("full", get_test_index_bench(Cardinality::Full).unwrap()),
        (
            "dense",
            get_test_index_bench(Cardinality::OptionalDense).unwrap(),
        ),
        (
            "sparse",
            get_test_index_bench(Cardinality::OptionalSparse).unwrap(),
        ),
        (
            "multivalue",
            get_test_index_bench(Cardinality::Multivalued).unwrap(),
        ),
    ];

    bench_collector(InputGroup::new_with_inputs(inputs));
}

fn bench_collector(mut group: InputGroup<Index>) {
    group.add_plugin(PeakMemAllocPlugin::new(GLOBAL));

    register!(group, top_docs_small_shallow);
    register!(group, top_docs_small_deep);

    register!(group, top_docs_large_shallow);
    register!(group, top_docs_large_deep);

    group.run();
}

fn execute_top_docs<F: FastValue>(
    index: &Index,
    fast_field: &str,
    order: Order,
    offset: usize,
    limit: usize,
) {
    let collector = TopDocs::with_limit(limit)
        .and_offset(offset)
        .order_by_fast_field::<F>(fast_field, order);

    let reader = index.reader().unwrap();
    let searcher = reader.searcher();
    black_box(searcher.search(&AllQuery, &collector).unwrap());
}
fn top_docs_small_deep(index: &Index) {
    execute_top_docs::<u64>(index, "score", Order::Asc, 10000, 10);
}
fn top_docs_small_shallow(index: &Index) {
    execute_top_docs::<u64>(index, "score", Order::Asc, 0, 10);
}
fn top_docs_large_deep(index: &Index) {
    execute_top_docs::<u64>(index, "score", Order::Asc, 10000, 1000);
}
fn top_docs_large_shallow(index: &Index) {
    execute_top_docs::<u64>(index, "score", Order::Asc, 0, 1000);
}

#[derive(Clone, Copy, Hash, Default, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Cardinality {
    /// All documents contain exactly one value.
    /// `Full` is the default for auto-detecting the Cardinality, since it is the most strict.
    #[default]
    Full = 0,
    /// All documents contain at most one value.
    OptionalDense = 1,
    /// All documents may contain any number of values.
    Multivalued = 2,
    /// 1 / 20 documents has a value
    OptionalSparse = 3,
}

fn get_test_index_bench(cardinality: Cardinality) -> tantivy::Result<Index> {
    let mut schema_builder = Schema::builder();
    let text_fieldtype = tantivy::schema::TextOptions::default()
        .set_indexing_options(
            TextFieldIndexing::default().set_index_option(IndexRecordOption::WithFreqs),
        )
        .set_stored();
    let text_field = schema_builder.add_text_field("text", text_fieldtype);
    let json_field = schema_builder.add_json_field("json", FAST);
    let text_field_many_terms = schema_builder.add_text_field("text_many_terms", STRING | FAST);
    let text_field_few_terms = schema_builder.add_text_field("text_few_terms", STRING | FAST);
    let score_fieldtype = tantivy::schema::NumericOptions::default().set_fast();
    let score_field = schema_builder.add_u64_field("score", score_fieldtype.clone());
    let score_field_f64 = schema_builder.add_f64_field("score_f64", score_fieldtype.clone());
    let score_field_i64 = schema_builder.add_i64_field("score_i64", score_fieldtype);
    let index = Index::create_from_tempdir(schema_builder.build())?;
    let few_terms_data = ["INFO", "ERROR", "WARN", "DEBUG"];

    let lg_norm = rand_distr::LogNormal::new(2.996f64, 0.979f64).unwrap();

    let many_terms_data = (0..150_000)
        .map(|num| format!("author{num}"))
        .collect::<Vec<_>>();
    {
        let mut rng = StdRng::from_seed([1u8; 32]);
        let mut index_writer = index.writer_with_num_threads(8, 200_000_000)?;
        // To make the different test cases comparable we just change one doc to force the
        // cardinality
        if cardinality == Cardinality::OptionalDense {
            index_writer.add_document(doc!())?;
        }
        if cardinality == Cardinality::Multivalued {
            index_writer.add_document(doc!(
                json_field => json!({"mixed_type": 10.0}),
                json_field => json!({"mixed_type": 10.0}),
                text_field => "cool",
                text_field => "cool",
                text_field_many_terms => "cool",
                text_field_many_terms => "cool",
                text_field_few_terms => "cool",
                text_field_few_terms => "cool",
                score_field => 1u64,
                score_field => 1u64,
                score_field_f64 => lg_norm.sample(&mut rng),
                score_field_f64 => lg_norm.sample(&mut rng),
                score_field_i64 => 1i64,
                score_field_i64 => 1i64,
            ))?;
        }
        let mut doc_with_value = 1_000_000;
        if cardinality == Cardinality::OptionalSparse {
            doc_with_value /= 20;
        }
        let _val_max = 1_000_000.0;
        for _ in 0..doc_with_value {
            let val: f64 = rng.gen_range(0.0..1_000_000.0);
            let json = if rng.gen_bool(0.1) {
                // 10% are numeric values
                json!({ "mixed_type": val })
            } else {
                json!({"mixed_type": many_terms_data.choose(&mut rng).unwrap().to_string()})
            };
            index_writer.add_document(doc!(
                text_field => "cool",
                json_field => json,
                text_field_many_terms => many_terms_data.choose(&mut rng).unwrap().to_string(),
                text_field_few_terms => few_terms_data.choose(&mut rng).unwrap().to_string(),
                score_field => val as u64,
                score_field_f64 => lg_norm.sample(&mut rng),
                score_field_i64 => val as i64,
            ))?;
            if cardinality == Cardinality::OptionalSparse {
                for _ in 0..20 {
                    index_writer.add_document(doc!(text_field => "cool"))?;
                }
            }
        }
        // writing the segment
        index_writer.commit()?;
    }

    Ok(index)
}

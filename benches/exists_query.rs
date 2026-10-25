use binggan::{black_box, BenchRunner};
use rand::prelude::*;
use rand::rngs::StdRng;
use rand::SeedableRng;
use serde_json::json;
use tantivy::collector::Count;
use tantivy::query::{BooleanQuery, ExistsQuery, Query, TermQuery};
use tantivy::schema::{Field, IndexRecordOption, Schema, FAST, TEXT};
use tantivy::{Index, ReloadPolicy, Searcher, TantivyDocument, Term};

const NUM_DOCS: usize = 5_000_000;
const EXISTS_SELECTIVITIES: &[f64] = &[0.001, 0.1, 0.9];
const TERM_SELECTIVITIES: &[f64] = &[0.0001, 0.01, 0.5];

struct ColumnFields {
    optional: Field,
    multivalued: Field,
    multiple_columns: Field,
}

struct BenchIndex {
    #[allow(dead_code)]
    index: Index,
    searcher: Searcher,
    selector: Field,
}

fn field_suffix(selectivity: f64) -> String {
    format_pct(selectivity)
        .replace('.', "d")
        .replace('%', "pct")
}

fn format_pct(selectivity: f64) -> String {
    let pct = selectivity * 100.0;
    if pct >= 1.0 {
        format!("{pct:.0}%")
    } else if pct >= 0.1 {
        format!("{pct:.1}%")
    } else {
        format!("{pct:.2}%")
    }
}

fn selector_term(selectivity: f64) -> String {
    format!("selector{}", field_suffix(selectivity))
}

fn column_encoding(selectivity: f64) -> &'static str {
    if selectivity < 0.1 {
        "sparse blocks"
    } else {
        "dense blocks"
    }
}

fn build_index() -> BenchIndex {
    let mut schema_builder = Schema::builder();
    let selector = schema_builder.add_text_field("selector", TEXT);
    let columns: Vec<ColumnFields> = EXISTS_SELECTIVITIES
        .iter()
        .map(|&selectivity| {
            let suffix = field_suffix(selectivity);
            let optional_name = format!("optional_{suffix}");
            let multivalued_name = format!("multivalued_{suffix}");
            let multiple_columns_name = format!("multiple_columns_{suffix}");
            ColumnFields {
                optional: schema_builder.add_u64_field(&optional_name, FAST),
                multivalued: schema_builder.add_u64_field(&multivalued_name, FAST),
                multiple_columns: schema_builder
                    .add_json_field(&multiple_columns_name, TEXT | FAST),
            }
        })
        .collect();
    let index = Index::create_in_ram(schema_builder.build());
    let mut rng = StdRng::from_seed([7u8; 32]);

    {
        let mut writer = index.writer_with_num_threads(1, 4_000_000_000).unwrap();
        for doc_id in 0..NUM_DOCS {
            let mut doc = TantivyDocument::default();

            for (&selectivity, fields) in EXISTS_SELECTIVITIES.iter().zip(&columns) {
                if rng.random_bool(selectivity) {
                    doc.add_u64(fields.optional, doc_id as u64);
                    // Two values force this column to use MultiValueIndex while preserving the
                    // same set of matching documents as the optional column.
                    doc.add_u64(fields.multivalued, doc_id as u64);
                    doc.add_u64(fields.multivalued, doc_id as u64 + 1);
                    let multiple_columns_value = if doc_id % 2 == 0 {
                        json!({"optional": doc_id as u64})
                    } else {
                        json!({"multivalued": [doc_id as u64, doc_id as u64 + 1]})
                    };
                    doc.add_field_value(fields.multiple_columns, &multiple_columns_value);
                }
            }

            for &selectivity in TERM_SELECTIVITIES {
                if rng.random_bool(selectivity) {
                    doc.add_text(selector, selector_term(selectivity));
                }
            }

            writer.add_document(doc).unwrap();
        }
        writer.commit().unwrap();
    }

    let reader = index
        .reader_builder()
        .reload_policy(ReloadPolicy::Manual)
        .try_into()
        .unwrap();
    let searcher = reader.searcher();

    BenchIndex {
        index,
        searcher,
        selector,
    }
}

fn exists_query(column_kind: &str, selectivity: f64) -> Box<dyn Query> {
    Box::new(ExistsQuery::new(
        format!("{column_kind}_{}", field_suffix(selectivity)),
        column_kind == "multiple_columns",
    ))
}

fn intersection_query(
    bench_index: &BenchIndex,
    column_kind: &str,
    exists_selectivity: f64,
    term_selectivity: f64,
) -> Box<dyn Query> {
    let term = Term::from_field_text(bench_index.selector, &selector_term(term_selectivity));
    let term_query = TermQuery::new(term, IndexRecordOption::Basic);
    Box::new(BooleanQuery::intersection(vec![
        Box::new(term_query),
        exists_query(column_kind, exists_selectivity),
    ]))
}

fn register_query(
    group: &mut binggan::BenchGroup,
    bench_index: &BenchIndex,
    name: &str,
    query: Box<dyn Query>,
) {
    let task = SearchTask {
        searcher: bench_index.searcher.clone(),
        query,
    };
    group.register(name.to_string(), move |_| black_box(task.run()));
}

fn main() {
    let bench_index = build_index();

    let mut exists_runner = BenchRunner::with_name("exists");
    for &exists_selectivity in EXISTS_SELECTIVITIES {
        let mut group = exists_runner.new_group();
        group.set_name(format!(
            "column populated in {} of docs ({})",
            format_pct(exists_selectivity),
            column_encoding(exists_selectivity)
        ));
        register_query(
            &mut group,
            &bench_index,
            "optional",
            exists_query("optional", exists_selectivity),
        );
        register_query(
            &mut group,
            &bench_index,
            "multivalued",
            exists_query("multivalued", exists_selectivity),
        );
        register_query(
            &mut group,
            &bench_index,
            "multiple_columns",
            exists_query("multiple_columns", exists_selectivity),
        );
        group.run();
    }

    let mut intersection_runner = BenchRunner::with_name("term_AND_exists");
    for &exists_selectivity in EXISTS_SELECTIVITIES {
        for &term_selectivity in TERM_SELECTIVITIES {
            let mut group = intersection_runner.new_group();
            group.set_name(format!(
                "term matches {} of docs, column populated in {} ({})",
                format_pct(term_selectivity),
                format_pct(exists_selectivity),
                column_encoding(exists_selectivity)
            ));
            for column_kind in ["optional", "multivalued", "multiple_columns"] {
                register_query(
                    &mut group,
                    &bench_index,
                    column_kind,
                    intersection_query(
                        &bench_index,
                        column_kind,
                        exists_selectivity,
                        term_selectivity,
                    ),
                );
            }
            group.run();
        }
    }
}

struct SearchTask {
    searcher: Searcher,
    query: Box<dyn Query>,
}

impl SearchTask {
    #[inline(never)]
    fn run(&self) -> usize {
        self.searcher.search(&self.query, &Count).unwrap()
    }
}

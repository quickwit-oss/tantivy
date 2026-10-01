use binggan::{InputGroup, black_box};
use tantivy_columnar::{Column, ColumnarReader, ColumnarWriter};

const NUM_DOCS: u32 = 1_000_000;
const NUM_DISTINCT_VALUES: u64 = 1000;

/// A multivalued column where `fill_percent` of the docs hold 1 to 4 values,
/// spread over `NUM_DISTINCT_VALUES` distinct values.
fn generate_multivalued_column(fill_percent: u32) -> Column {
    let mut columnar_writer = ColumnarWriter::default();
    for doc in 0..NUM_DOCS {
        if doc % 100 >= fill_percent {
            continue;
        }
        let num_values = 1 + doc % 4;
        for value_idx in 0..num_values {
            let value = (doc as u64 * 7 + value_idx as u64) % NUM_DISTINCT_VALUES;
            columnar_writer.record_numerical(doc, "field", value);
        }
    }
    let mut buffer: Vec<u8> = Vec::new();
    columnar_writer
        .serialize(NUM_DOCS, None, &mut buffer)
        .unwrap();
    let reader = ColumnarReader::open(buffer).unwrap();
    reader.read_columns("field").unwrap()[0]
        .open_u64_lenient()
        .unwrap()
        .unwrap()
}

fn main() {
    let inputs: Vec<(String, Column)> = [100, 50, 10]
        .into_iter()
        .map(|fill_percent| {
            (
                format!("multi 1-4 values, {fill_percent}% docs"),
                generate_multivalued_column(fill_percent),
            )
        })
        .collect();
    let mut group: InputGroup<Column> = InputGroup::new_with_inputs(inputs);

    group.register("docids_all_values", |column: &Column| {
        let mut doc_ids = Vec::new();
        column.get_docids_for_value_range(0..=u64::MAX, 0..NUM_DOCS, &mut doc_ids);
        black_box(doc_ids);
    });
    group.register("docids_1pct_values", |column: &Column| {
        let mut doc_ids = Vec::new();
        column.get_docids_for_value_range(0..=9, 0..NUM_DOCS, &mut doc_ids);
        black_box(doc_ids);
    });
    // The block-wise fetch of a range query's doc set.
    group.register("docids_all_values_blocks_of_1024", |column: &Column| {
        let mut doc_ids = Vec::new();
        let mut num_docs = 0;
        for block_start in (0..NUM_DOCS).step_by(1024) {
            let block_end = (block_start + 1024).min(NUM_DOCS);
            doc_ids.clear();
            column.get_docids_for_value_range(0..=u64::MAX, block_start..block_end, &mut doc_ids);
            num_docs += doc_ids.len();
        }
        black_box(num_docs);
    });

    group.run();
}

use binggan::{InputGroup, black_box};
use tantivy_columnar::{ColumnarReader, ColumnarWriter, DynamicColumn};

fn bench_sparse_query() {
    let size = 100_000u32;

    let mut columnar_writer = ColumnarWriter::default();
    for doc in 0u32..size {
        columnar_writer.record_numerical(doc, "vals", (doc * 10) as u64);
        columnar_writer.record_numerical(doc, "vals", (doc * 10 + 1) as u64);
    }

    let mut buffer: Vec<u8> = Vec::new();
    columnar_writer.serialize(size, &mut buffer).unwrap();

    let reader = ColumnarReader::open(buffer).unwrap();
    let column = reader.read_columns("vals").unwrap()[0]
        .open()
        .unwrap()
        .coerce_numerical(tantivy_columnar::NumericalType::U64)
        .unwrap();

    let DynamicColumn::U64(column) = column else {
        panic!();
    };

    let mut group: InputGroup<()> =
        InputGroup::new_with_inputs(vec![(format!("sparse_query_{}docs", size), ())]);

    let mid = (size / 2) as u64 * 10;
    let sparse_range = mid..=(mid + 100);
    let num_docs = size;

    group.register("sparse_range_1pct", move |_: &()| {
        let mut docids = Vec::new();
        column.get_docids_for_value_range(sparse_range.clone(), 0..num_docs, &mut docids);
        black_box(docids.len());
    });

    group.run();
}

fn bench_full_scan() {
    let size = 100_000u32;

    let mut columnar_writer = ColumnarWriter::default();
    for doc in 0u32..size {
        columnar_writer.record_numerical(doc, "vals", (doc * 10) as u64);
        columnar_writer.record_numerical(doc, "vals", (doc * 10 + 1) as u64);
    }

    let mut buffer: Vec<u8> = Vec::new();
    columnar_writer.serialize(size, &mut buffer).unwrap();

    let reader = ColumnarReader::open(buffer).unwrap();
    let column = reader.read_columns("vals").unwrap()[0]
        .open()
        .unwrap()
        .coerce_numerical(tantivy_columnar::NumericalType::U64)
        .unwrap();

    let DynamicColumn::U64(column) = column else {
        panic!();
    };

    let mut group: InputGroup<()> =
        InputGroup::new_with_inputs(vec![(format!("full_scan_{}docs", size), ())]);

    let full_range = 0..=u64::MAX;
    let num_docs = size;

    group.register("full_range_all", move |_: &()| {
        let mut docids = Vec::new();
        column.get_docids_for_value_range(full_range.clone(), 0..num_docs, &mut docids);
        black_box(docids.len());
    });

    group.run();
}

fn main() {
    bench_sparse_query();
    bench_full_scan();
}

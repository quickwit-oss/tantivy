//! Compares the plain and the dictionary codecs on a high cardinality text column.

use binggan::{InputGroup, black_box};
use tantivy_columnar::{
    ColumnType, ColumnarReader, ColumnarWriter, DynamicColumn, RowId, StrColumnCodec,
};

const NUM_DOCS: u32 = 300_000;
const NUM_RANDOM_LOOKUPS: usize = 1_000;

/// Deterministic pseudo random numbers.
fn splitmix64(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// Log lines with a high cardinality: almost every line is unique.
fn generate_log_lines(num_docs: u32) -> Vec<String> {
    let services = [
        "checkout",
        "cart",
        "search",
        "auth",
        "payments",
        "inventory",
    ];
    let paths = ["items", "users", "orders", "sessions", "carts"];
    let mut rng_state = 42u64;
    (0..num_docs)
        .map(|doc| {
            let random = splitmix64(&mut rng_state);
            format!(
                "2026-10-12T10:{:02}:{:02}.{:03}Z INFO [{}] request_id={:016x} user={} GET \
                 /api/v{}/{}/{} status={} latency_ms={}",
                (doc / 60_000) % 60,
                (doc / 1_000) % 60,
                doc % 1_000,
                services[(random % 6) as usize],
                splitmix64(&mut rng_state),
                random % 100_000,
                1 + random % 3,
                paths[(random % 5) as usize],
                random % 1_000_000,
                [200, 200, 200, 404, 500][(random % 5) as usize],
                random % 2_000,
            )
        })
        .collect()
}

fn serialize_columnar(log_lines: &[String], codec: StrColumnCodec) -> Vec<u8> {
    let mut columnar_writer = ColumnarWriter::default();
    for (doc, log_line) in log_lines.iter().enumerate() {
        columnar_writer.record_str_with_codec(doc as RowId, "message", log_line, codec);
    }
    let mut buffer = Vec::new();
    columnar_writer
        .serialize(log_lines.len() as RowId, None, &mut buffer)
        .unwrap();
    buffer
}

fn open_message_column(columnar_bytes: &[u8]) -> DynamicColumn {
    let columnar = ColumnarReader::open(columnar_bytes.to_vec()).unwrap();
    let handles = columnar.read_columns("message").unwrap();
    assert_eq!(handles[0].column_type(), ColumnType::Str);
    handles[0].open().unwrap()
}

/// Reads the value of each doc of `docs`, and returns the total length of the values.
fn read_values(column: &DynamicColumn, docs: &[RowId]) -> usize {
    let mut total_len = 0;
    let mut value: Vec<u8> = Vec::new();
    match column {
        DynamicColumn::Str(str_column) => {
            for &doc in docs {
                for term_ord in str_column.term_ords(doc) {
                    str_column.ord_to_bytes(term_ord, &mut value).unwrap();
                    total_len += value.len();
                }
            }
        }
        DynamicColumn::PlainStr(plain_str_column) => {
            let mut reader = plain_str_column.reader();
            for &doc in docs {
                value.clear();
                reader.append_first_value(doc, &mut value).unwrap();
                total_len += value.len();
            }
        }
        _ => panic!("unexpected column type"),
    }
    total_len
}

fn main() {
    let log_lines = generate_log_lines(NUM_DOCS);
    let raw_num_bytes: usize = log_lines.iter().map(String::len).sum();
    let dictionary_columnar = serialize_columnar(&log_lines, StrColumnCodec::Dictionary);
    let plain_columnar = serialize_columnar(&log_lines, StrColumnCodec::Plain);
    println!("raw values:          {raw_num_bytes:>12} bytes");
    println!(
        "dictionary columnar: {:>12} bytes",
        dictionary_columnar.len()
    );
    println!("plain columnar:      {:>12} bytes", plain_columnar.len());

    let mut rng_state = 7u64;
    let random_docs: Vec<RowId> = {
        let mut random_docs: Vec<RowId> = (0..NUM_RANDOM_LOOKUPS)
            .map(|_| (splitmix64(&mut rng_state) % NUM_DOCS as u64) as RowId)
            .collect();
        random_docs.sort_unstable();
        random_docs
    };
    let all_docs: Vec<RowId> = (0..NUM_DOCS).collect();

    let mut serialize_group =
        InputGroup::new_with_inputs(vec![("log_lines".to_string(), log_lines)]);
    serialize_group.register("serialize_dictionary", |log_lines| {
        black_box(serialize_columnar(log_lines, StrColumnCodec::Dictionary));
    });
    serialize_group.register("serialize_plain", |log_lines| {
        black_box(serialize_columnar(log_lines, StrColumnCodec::Plain));
    });
    serialize_group.run();

    let mut read_group = InputGroup::new_with_inputs(vec![
        (
            "dictionary".to_string(),
            open_message_column(&dictionary_columnar),
        ),
        ("plain".to_string(), open_message_column(&plain_columnar)),
    ]);
    read_group.register("read_all_values", move |column| {
        black_box(read_values(column, &all_docs));
    });
    read_group.register("read_1000_random_values", move |column| {
        black_box(read_values(column, &random_docs));
    });
    read_group.run();
}

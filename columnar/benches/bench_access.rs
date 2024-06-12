use core::fmt;
use std::fmt::{Display, Formatter};

use binggan::{black_box, InputGroup};
use tantivy_columnar::*;

enum Card {
    MultiSparse,
    Multi,
    Sparse,
    Dense,
    Full,
}
impl Display for Card {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            Card::MultiSparse => write!(f, "multi sparse 1/13"),
            Card::Multi => write!(f, "multi 2x"),
            Card::Sparse => write!(f, "sparse 1/13"),
            Card::Dense => write!(f, "dense 1/12"),
            Card::Full => write!(f, "full"),
        }
    }
}

const NUM_DOCS: u32 = 2_000_000;

fn generate_columnar(card: Card, num_docs: u32) -> Column {
    use tantivy_columnar::ColumnarWriter;

    let mut columnar_writer = ColumnarWriter::default();

    match card {
        Card::MultiSparse => {
            columnar_writer.record_numerical(0, "price", 10u64);
            columnar_writer.record_numerical(0, "price", 10u64);
        }
        _ => {}
    }

    for i in 0..num_docs {
        match card {
            Card::MultiSparse | Card::Sparse => {
                if i % 13 == 0 {
                    columnar_writer.record_numerical(i, "price", i as u64);
                }
            }
            Card::Dense => {
                if i % 12 == 0 {
                    columnar_writer.record_numerical(i, "price", i as u64);
                }
            }
            Card::Full => {
                columnar_writer.record_numerical(i, "price", i as u64);
            }
            Card::Multi => {
                columnar_writer.record_numerical(i, "price", i as u64);
                columnar_writer.record_numerical(i, "price", i as u64);
            }
        }
    }

    let mut wrt: Vec<u8> = Vec::new();
    columnar_writer.serialize(num_docs, None, &mut wrt).unwrap();

    let reader = ColumnarReader::open(wrt).unwrap();
    reader.read_columns("price").unwrap()[0]
        .open_u64_lenient()
        .unwrap()
        .unwrap()
}
fn main() {
    let mut inputs = Vec::new();

    let mut add_card = |card1: Card| {
        inputs.push((format!("{card1}"), generate_columnar(card1, NUM_DOCS)));
    };

    add_card(Card::MultiSparse);
    add_card(Card::Multi);
    add_card(Card::Sparse);
    add_card(Card::Dense);
    add_card(Card::Full);

    bench_group(InputGroup::new_with_inputs(inputs));
}

fn bench_group(mut runner: InputGroup<Column>) {
    runner.register("access_values_for_doc", |column| {
        let mut sum = 0;
        for i in 0..NUM_DOCS {
            for value in column.values_for_doc(i) {
                sum += value;
            }
        }
        black_box(sum);
    });
    runner.register("access_first_vals", |column| {
        let mut sum = 0;
        const BLOCK_SIZE: usize = 32;
        let mut docs = vec![0; BLOCK_SIZE];
        let mut buffer = vec![None; BLOCK_SIZE];
        for i in (0..NUM_DOCS).step_by(BLOCK_SIZE) {
            // fill docs
            for idx in 0..BLOCK_SIZE {
                docs[idx] = idx as u32 + i;
            }

            column.first_vals(&docs, &mut buffer);
            for val in buffer.iter() {
                if let Some(val) = val {
                    sum += *val;
                }
            }
        }

        black_box(sum);
    });
    runner.run();
}

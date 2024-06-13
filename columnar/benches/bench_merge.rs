use core::fmt;
use std::fmt::{Display, Formatter};

use binggan::{black_box, BenchRunner};
use tantivy_columnar::*;

enum Card {
    Multi,
    Sparse,
    Dense,
}
impl Display for Card {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            Card::Multi => write!(f, "multi"),
            Card::Sparse => write!(f, "sparse"),
            Card::Dense => write!(f, "dense"),
        }
    }
}

const NUM_DOCS: u32 = 100_000;

fn generate_columnar(card: Card, num_docs: u32) -> ColumnarReader {
    use tantivy_columnar::ColumnarWriter;

    let mut columnar_writer = ColumnarWriter::default();

    match card {
        Card::Multi => {
            columnar_writer.record_numerical(0, "price", 10u64);
            columnar_writer.record_numerical(0, "price", 10u64);
        }
        _ => {}
    }

    for i in 0..num_docs {
        match card {
            Card::Multi | Card::Sparse => {
                if i % 13 == 0 {
                    columnar_writer.record_numerical(i, "price", i as u64);
                }
            }
            Card::Dense => {
                if i % 12 == 0 {
                    columnar_writer.record_numerical(i, "price", i as u64);
                }
            }
        }
    }

    let mut wrt: Vec<u8> = Vec::new();
    columnar_writer.serialize(num_docs, &mut wrt).unwrap();

    ColumnarReader::open(wrt).unwrap()
}
fn main() {
    let mut inputs = Vec::new();

    let mut add_combo = |card1: Card, card2: Card| {
        inputs.push((
            format!("merge_{card1}_and_{card2}"),
            vec![
                generate_columnar(card1, NUM_DOCS),
                generate_columnar(card2, NUM_DOCS),
            ],
        ));
    };

    add_combo(Card::Multi, Card::Multi);
    add_combo(Card::Dense, Card::Dense);
    add_combo(Card::Sparse, Card::Sparse);
    add_combo(Card::Sparse, Card::Dense);
    add_combo(Card::Multi, Card::Dense);
    add_combo(Card::Multi, Card::Sparse);

    let runner: BenchRunner = BenchRunner::new();
    let mut group = runner.new_group();
    for (input_name, columnar_readers) in inputs.iter() {
        group.register_with_input(
            input_name,
            columnar_readers,
            move |columnar_readers: &Vec<ColumnarReader>| {
                let mut out = Vec::new();
                let columnar_readers = columnar_readers.iter().collect::<Vec<_>>();
                let merge_row_order = StackMergeOrder::stack(&columnar_readers[..]);

                merge_columnar(&columnar_readers, &[], merge_row_order.into(), &mut out).unwrap();
                black_box(out);
            },
        );
    }
    group.run();
}

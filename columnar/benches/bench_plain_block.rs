//! Cost of opening an OnPair16 block of a plain column, and of reading values from it.
//!
//! The values are the messages of the loghub `Apache_2k.log` sample, see `data/README.md`.

use std::cell::RefCell;

use binggan::{InputGroup, black_box};
use common::OwnedBytes;
use tantivy_columnar::{DictOffsetsBuffer, PlainBlock, encode_plain_block};

/// Kind byte of an OnPair16 block.
const BLOCK_KIND_ONPAIR16: u8 = 1;

const MESSAGES: &str = include_str!("data/apache_2k_messages.txt");

struct BenchInput {
    block_bytes: OwnedBytes,
    num_values: u32,
    /// The block, already open: reads are measured without the cost of opening it.
    open_block: PlainBlock,
    /// Reused across the openings, as `PlainValueReader` does.
    dict_offsets_buffer: RefCell<Option<DictOffsetsBuffer>>,
}

fn build_input() -> BenchInput {
    let mut bytes: Vec<u8> = Vec::new();
    let mut offsets: Vec<u32> = vec![0];
    for message in MESSAGES.lines() {
        bytes.extend_from_slice(message.as_bytes());
        offsets.push(bytes.len() as u32);
    }
    let num_values = (offsets.len() - 1) as u32;
    let mut block_bytes = Vec::new();
    let mut scratch = Vec::new();
    encode_plain_block(&bytes, &offsets, &mut block_bytes, &mut scratch).unwrap();
    assert_eq!(
        block_bytes[0], BLOCK_KIND_ONPAIR16,
        "the sample is expected to be compressed with OnPair16"
    );
    println!(
        "{num_values} values, {} raw bytes, {} block bytes",
        bytes.len(),
        block_bytes.len()
    );
    let block_bytes = OwnedBytes::new(block_bytes);
    let open_block = PlainBlock::open(block_bytes.clone(), num_values, &mut None);
    BenchInput {
        block_bytes,
        num_values,
        open_block,
        dict_offsets_buffer: RefCell::new(None),
    }
}

fn main() {
    let input = build_input();
    let mut group = InputGroup::new_with_inputs(vec![("apache_2k".to_string(), input)]);

    group.register("open_onpair16_block", |input| {
        let mut dict_offsets_buffer = input.dict_offsets_buffer.borrow_mut();
        // `OwnedBytes::clone` only clones an `Arc`.
        let block = PlainBlock::open(
            input.block_bytes.clone(),
            input.num_values,
            &mut dict_offsets_buffer,
        );
        black_box(&block);
        block.recycle(&mut dict_offsets_buffer);
    });

    // What a new `PlainValueReader` pays on its first block: the buffer is created.
    group.register("open_onpair16_block_fresh_buffer", |input| {
        let mut dict_offsets_buffer: Option<DictOffsetsBuffer> = None;
        let block = PlainBlock::open(
            input.block_bytes.clone(),
            input.num_values,
            &mut dict_offsets_buffer,
        );
        black_box(block);
    });

    // Divide by the number of values (2,000) to get the cost of reading one value.
    group.register("read_all_values", |input| {
        let mut codes_buffer: Vec<u16> = Vec::new();
        let mut value: Vec<u8> = Vec::new();
        let mut total_len = 0;
        for slot in 0..input.num_values {
            value.clear();
            input
                .open_block
                .append_value(slot, &mut codes_buffer, &mut value);
            total_len += value.len();
        }
        black_box(total_len);
    });

    // A point lookup: open the block, then read one value.
    group.register("open_and_read_one_value", |input| {
        let mut dict_offsets_buffer = input.dict_offsets_buffer.borrow_mut();
        let block = PlainBlock::open(
            input.block_bytes.clone(),
            input.num_values,
            &mut dict_offsets_buffer,
        );
        let mut codes_buffer: Vec<u16> = Vec::new();
        let mut value: Vec<u8> = Vec::new();
        block.append_value(input.num_values / 2, &mut codes_buffer, &mut value);
        black_box(value);
        block.recycle(&mut dict_offsets_buffer);
    });

    group.run();
}

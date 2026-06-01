use std::cell::RefCell;

use binggan::{BenchRunner, black_box};
use rand::rng;
use rand::seq::IteratorRandom;
use tantivy_bitpacker::{BitPacker, BitUnpacker, BlockedBitpacker};

fn create_bitpacked_data(bit_width: u8, num_els: u32) -> Vec<u8> {
    let mut bitpacker = BitPacker::new();
    let mut buffer = Vec::new();
    for _ in 0..num_els {
        bitpacker.write(0u64, bit_width, &mut buffer).unwrap();
        bitpacker.flush(&mut buffer).unwrap();
    }
    buffer
}

const N: usize = 100_000;
const MAX_VAL: u64 = 1_000;
const BIT_WIDTH: u8 = 10; // 2^10 = 1024 > MAX_VAL

fn create_packed_data() -> (BitUnpacker, Vec<u8>) {
    let mut bitpacker = BitPacker::new();
    let mut data = Vec::new();
    for i in 0..N as u64 {
        let val = i * MAX_VAL / N as u64;
        bitpacker.write(val, BIT_WIDTH, &mut data).unwrap();
    }
    bitpacker.close(&mut data).unwrap();
    (BitUnpacker::new(BIT_WIDTH), data)
}

fn bench_bitpacking() {
    let mut runner = BenchRunner::new();
    let bit_width = 3;
    let num_els = 1_000_000u32;
    let bit_unpacker = BitUnpacker::new(bit_width);
    let data = create_bitpacked_data(bit_width, num_els);
    let idxs: Vec<u32> = (0..num_els).choose_multiple(&mut rng(), 100_000);
    runner.bench_function("bitpacking_read", move |_| {
        let mut out = 0u64;
        for &idx in &idxs {
            out = out.wrapping_add(bit_unpacker.get(idx, &data[..]));
        }
        black_box(out);
    });
}

fn bench_blocked_bitpacker() {
    let mut runner = BenchRunner::new();
    let mut blocked_bitpacker = BlockedBitpacker::new();
    for val in 0..=21500 {
        blocked_bitpacker.add(val * val);
    }
    runner.bench_function("blockedbitp_read", move |_| {
        let mut out = 0u64;
        for val in 0..=21500 {
            out = out.wrapping_add(blocked_bitpacker.get(val));
        }
        black_box(out);
    });
    runner.bench_function("blockedbitp_create", |_| {
        let mut blocked_bitpacker = BlockedBitpacker::new();
        for val in 0..=21500 {
            blocked_bitpacker.add(val * val);
        }
        black_box(blocked_bitpacker);
    });
}

fn bench_filter_vec() {
    let mut runner = BenchRunner::new();

    let (unpacker, data) = create_packed_data();
    let positions = RefCell::new(Vec::with_capacity(N));
    runner.bench_function("filter_vec_dense", move |_| {
        unpacker.get_ids_for_value_range(250..=750, 0..N as u32, &data, &mut positions.borrow_mut());
        black_box(positions.borrow().len());
    });

    let (unpacker, data) = create_packed_data();
    let positions = RefCell::new(Vec::with_capacity(N));
    runner.bench_function("filter_vec_sparse", move |_| {
        unpacker.get_ids_for_value_range(0..=50, 0..N as u32, &data, &mut positions.borrow_mut());
        black_box(positions.borrow().len());
    });

    let (unpacker, data) = create_packed_data();
    let positions = RefCell::new(Vec::with_capacity(N));
    runner.bench_function("filter_vec_full", move |_| {
        unpacker.get_ids_for_value_range(0..=MAX_VAL, 0..N as u32, &data, &mut positions.borrow_mut());
        black_box(positions.borrow().len());
    });
}

fn main() {
    bench_bitpacking();
    bench_blocked_bitpacker();
    bench_filter_vec();
}

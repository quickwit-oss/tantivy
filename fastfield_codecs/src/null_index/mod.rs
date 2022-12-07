pub use dense::{serialize_dense_codec, DenseCodec};

mod dense;

fn get_bit_at(input: u64, n: u32) -> bool {
    input & (1 << n) != 0
}

fn set_bit_at(input: &mut u64, n: u64) {
    *input |= 1 << n;
}

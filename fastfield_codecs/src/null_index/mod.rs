pub use dense::{serialize_dense_codec, DenseCodec};

mod dense;
mod sparse;

#[inline]
fn get_bit_at(input: u64, n: u32) -> bool {
    input & (1 << n) != 0
}

#[inline]
fn set_bit_at(input: &mut u64, n: u64) {
    *input |= 1 << n;
}

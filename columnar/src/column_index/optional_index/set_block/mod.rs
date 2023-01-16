mod set_block;
mod sparse;

pub use set_block::{DenseBlock, DenseBlockCodec, DENSE_BLOCK_NUM_BYTES};
pub use sparse::{SparseBlock, SparseBlockCodec};

#[cfg(test)]
mod tests;

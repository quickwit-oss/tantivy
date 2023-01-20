mod dense;
mod sparse;

pub use dense::{DenseBlock, DenseBlockCodec, DENSE_BLOCK_NUM_BYTES};
pub use sparse::{SparseBlock, SparseBlockCodec};

#[cfg(test)]
mod tests;

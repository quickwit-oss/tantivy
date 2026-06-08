mod plugin;
mod presence;
mod reader;
mod writer;

pub(crate) const FLATVEC_EXT: &str = "flatvec";

pub(crate) use plugin::merge_flat;
pub use reader::{FlatVecReader, FlatVectorColumn};
pub use writer::FlatVecWriter;

#[cfg(test)]
mod tests;

//! Vector storage plugin.
//!
//! [`VectorPlugin`] owns per-segment vector storage end-to-end:
//! - During indexing, accumulates raw vector bytes per doc and writes flat `.vecmeta` and
//!   `.flatvec` files at segment finalize.
//! - During merge, copies vectors forward into flat `.vecmeta` and `.flatvec`.
//! - During reads, [`VectorReader`](super::reader::VectorReader) uses the segment-level `.vecmeta`
//!   marker to open the storage format.

use super::flat::{merge_flat, FlatVecWriter, FLATVEC_EXT};
use super::meta::VECMETA_EXT;
use crate::plugin::{PluginMergeContext, PluginWriter, PluginWriterContext, SegmentPlugin};

pub struct VectorPlugin;

impl SegmentPlugin for VectorPlugin {
    fn extensions(&self) -> &[&str] {
        &[FLATVEC_EXT, VECMETA_EXT]
    }

    fn create_writer(&self, ctx: &PluginWriterContext) -> crate::Result<Box<dyn PluginWriter>> {
        Ok(Box::new(FlatVecWriter::for_schema(&ctx.segment.schema())))
    }

    fn merge(&self, ctx: PluginMergeContext) -> crate::Result<()> {
        merge_flat(&ctx)
    }
}

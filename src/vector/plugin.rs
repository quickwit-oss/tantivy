//! Unified vector storage plugin.
//!
//! [`VectorPlugin`] owns per-segment vector storage end-to-end:
//! - During indexing, accumulates raw vector bytes per doc and writes a single `.vec` file at
//!   segment finalize (always flat — clustering is a merge-time transform).
//! - During merge, picks one of two output formats by target doc count: below
//!   [`IndexSettings::vector_clustering_threshold`](crate::index::IndexSettings::vector_clustering_threshold)
//!   it copies vectors forward into a flat `.vec`; at or above the threshold it writes an IVF
//!   `.vec` (with `IdMap::Explicit`) plus a `.centroids` file.
//! - During reads, [`VectorIndexReader`](super::VectorIndexReader) opens the field's `.vec` slots
//!   (and the `.centroids` sidecar when present) via
//!   [`SegmentReader::vector_index`](crate::SegmentReader::vector_index).
//!
//! Owning both flat and IVF extensions on one plugin keeps the "exactly one
//! format per segment" invariant right by construction: the dispatch is one
//! `if` inside one `merge()` method, not a cross-plugin coordination problem.

use super::flat::{merge_flat, FlatVecWriter};
use super::ivf::{merge_ivf, CENTROIDS_EXT};
use super::VEC_EXT;
use crate::plugin::{PluginMergeContext, PluginWriter, PluginWriterContext, SegmentPlugin};

pub struct VectorPlugin;

impl SegmentPlugin for VectorPlugin {
    fn extensions(&self) -> &[&str] {
        &[VEC_EXT, CENTROIDS_EXT]
    }

    fn create_writer(&self, ctx: &PluginWriterContext) -> crate::Result<Box<dyn PluginWriter>> {
        // Per-doc indexing only ever produces flatvec — clustering
        // exists exclusively as a merge-time transformation.
        Ok(Box::new(FlatVecWriter::for_schema(&ctx.segment.schema())))
    }

    fn merge(&self, ctx: PluginMergeContext) -> crate::Result<()> {
        // simple merge strategy, may change later
        // do clustering only if the target segment has more than the threshold number of docs
        let target_docs: u32 = ctx.readers.iter().map(|r| r.num_docs()).sum();
        let threshold = ctx.settings.vector_clustering_threshold();
        if (target_docs as usize) < threshold {
            merge_flat(&ctx)
        } else {
            merge_ivf(&ctx, ctx.target_segment.index().ivf_clusterer())
        }
    }
}

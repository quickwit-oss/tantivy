//! Experiment: a Vortex-backed columnar for tantivy aggregations.
//!
//! - [`VortexPlugin`] is a segment plugin that writes the fast fields of a segment as Vortex
//!   files (the `.vortex` component).
//! - [`VortexValueSourceProvider`] exposes these columns to aggregations through the
//!   `ValueSource` abstraction.
//! - [`OpaqueColumnarProvider`] exposes the regular tantivy columns the same way. It is the
//!   control group: it pays the same "registered source" penalty as Vortex.

mod format;
mod opaque;
mod point;
mod source;
mod writer;

use std::sync::{Arc, LazyLock};

pub use format::{read_footer, ColumnEntry, StoredCardinality, StoredType, VORTEX_EXTENSION};
pub use opaque::OpaqueColumnarProvider;
pub use source::{
    open_vortex_segment, VortexColumnData, VortexSegment, VortexSegmentCache, VortexValueSource,
    VortexValueSourceProvider,
};
use tantivy::aggregation::ValueSourceRegistry;
use vortex::io::runtime::current::CurrentThreadRuntime;
use vortex::io::runtime::BlockingRuntime;
use vortex::io::session::RuntimeSessionExt;
use vortex::session::VortexSession;
use vortex::VortexSessionDefault;
pub use writer::{
    convert_column, convert_doc_values, read_doc_values, write_vortex_file, ConvertedColumn,
    DocValues, VortexPlugin,
};

pub(crate) struct VortexEnv {
    pub runtime: CurrentThreadRuntime,
    pub session: VortexSession,
}

static VORTEX_ENV: LazyLock<VortexEnv> = LazyLock::new(|| {
    let runtime = CurrentThreadRuntime::new();
    let session = VortexSession::default().with_handle(runtime.handle());
    VortexEnv { runtime, session }
});

pub(crate) fn vortex_env() -> &'static VortexEnv {
    &VORTEX_ENV
}

/// The Vortex session used to write and read the `.vortex` components.
pub fn vortex_session() -> &'static VortexSession {
    &vortex_env().session
}

/// Size of `array` once compressed in memory by BtrBlocks (no file format overhead).
pub fn compressed_nbytes(array: &vortex::array::ArrayRef) -> tantivy::Result<u64> {
    use vortex::array::VortexSessionExecute;
    let session = vortex_session();
    let mut ctx = session.create_execution_ctx();
    let compressed = vortex::compressor::BtrBlocksCompressor::from_session(session)
        .compress(array, &mut ctx)
        .map_err(vortex_err_to_tantivy)?;
    Ok(compressed.nbytes())
}

/// Short description of the encoding tree of an array, e.g.
/// `fastlanes.bitpacked` or `vortex.dict(fastlanes.bitpacked, vortex.primitive)`.
pub fn encoding_tree(array: &vortex::array::ArrayRef, max_depth: usize) -> String {
    let encoding = array.encoding_id().to_string();
    let children = array.children();
    if children.is_empty() || max_depth == 0 {
        return encoding;
    }
    let children_desc: Vec<String> = children
        .iter()
        .map(|child| encoding_tree(child, max_depth - 1))
        .collect();
    format!("{encoding}({})", children_desc.join(", "))
}

pub(crate) fn vortex_err_to_tantivy(err: vortex::error::VortexError) -> tantivy::TantivyError {
    tantivy::TantivyError::InternalError(format!("vortex error: {err}"))
}

/// Registry mapping each field name to its Vortex column.
pub fn vortex_registry(field_names: &[&str]) -> ValueSourceRegistry {
    let cache = Arc::new(VortexSegmentCache::default());
    let mut registry = ValueSourceRegistry::default();
    for &field_name in field_names {
        registry.register(
            field_name,
            Arc::new(VortexValueSourceProvider::new(field_name, cache.clone())),
        );
    }
    registry
}

/// Registry mapping each field name to its (hidden) tantivy column.
pub fn opaque_registry(field_names: &[&str]) -> ValueSourceRegistry {
    let mut registry = ValueSourceRegistry::default();
    for &field_name in field_names {
        registry.register(field_name, Arc::new(OpaqueColumnarProvider::new(field_name)));
    }
    registry
}

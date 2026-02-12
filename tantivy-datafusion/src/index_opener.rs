use std::fmt;

use async_trait::async_trait;
use datafusion::common::Result;
use tantivy::Index;

/// Defers tantivy `Index` opening from planning time to execution time.
///
/// During planning, only [`schema`](IndexOpener::schema) and
/// [`segment_sizes`](IndexOpener::segment_sizes) are called — these must
/// be cheap and synchronous. The actual [`open`](IndexOpener::open) is
/// called at stream poll time (inside `DataSource::open`), allowing
/// distributed executors to open splits from local storage/cache.
#[async_trait]
pub trait IndexOpener: Send + Sync + fmt::Debug {
    /// Open (or return cached) the tantivy Index.
    /// Called at execution time (stream poll), not planning time.
    async fn open(&self) -> Result<Index>;

    /// Tantivy schema — available without opening the index.
    /// Used during planning for Arrow schema derivation and query parsing.
    fn schema(&self) -> tantivy::schema::Schema;

    /// Number of segments and max_doc per segment.
    /// Used during planning to determine partition count and chunking.
    /// Returns empty vec if unknown (single-partition fallback).
    fn segment_sizes(&self) -> Vec<u32>;
}

/// An [`IndexOpener`] that wraps an already-opened `Index`.
///
/// This is the default for local (non-distributed) usage. `open()` returns
/// the existing index immediately (just an `Arc` clone internally).
#[derive(Debug, Clone)]
pub struct DirectIndexOpener {
    index: Index,
}

impl DirectIndexOpener {
    pub fn new(index: Index) -> Self {
        Self { index }
    }
}

#[async_trait]
impl IndexOpener for DirectIndexOpener {
    async fn open(&self) -> Result<Index> {
        Ok(self.index.clone())
    }

    fn schema(&self) -> tantivy::schema::Schema {
        self.index.schema()
    }

    fn segment_sizes(&self) -> Vec<u32> {
        match self.index.reader() {
            Ok(reader) => {
                let searcher = reader.searcher();
                searcher
                    .segment_readers()
                    .iter()
                    .map(|r| r.max_doc())
                    .collect()
            }
            Err(_) => vec![],
        }
    }
}

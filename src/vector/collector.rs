//! Top-N vector-similarity collector.
//!
//! Unlike the other `TopDocs::order_by_*` paths, vector similarity is
//! not a [`SortKeyComputer`](crate::collector::sort_key::SortKeyComputer).
//! It is its own [`Collector`] with an overridden
//! [`Collector::collect_segment`] that hands the filter `Weight` down to
//! the per-segment [`VectorBackend`](super::backend::VectorBackend),
//! which owns the loop.

use std::cmp::Ordering;
use std::sync::Arc;

use super::backend::VectorBackend;
use super::VectorElement;
use crate::collector::{Collector, SegmentCollector};
use crate::index::SegmentReader;
use crate::query::Weight;
use crate::schema::{Field, FieldType, Schema};
use crate::{DocAddress, DocId, Score, SegmentOrdinal, TantivyError};

/// Top-N by vector similarity. Returns documents in descending
/// similarity order. Only docs that actually have a vector are
/// returned — docs that match the filter but lack a vector for `field`
/// are dropped.
///
/// Generic over `T: VectorElement` — `T` must match the schema's
/// declared dtype, checked at [`Collector::check_schema`] time.
pub struct TopDocsByVectorSimilarity<T: VectorElement> {
    field: Field,
    query: Arc<Vec<T>>,
    limit: usize,
    offset: usize,
}

impl<T: VectorElement> TopDocsByVectorSimilarity<T> {
    pub fn new(field: Field, query: Vec<T>, limit: usize) -> Self {
        Self {
            field,
            query: Arc::new(query),
            limit,
            offset: 0,
        }
    }

    /// Drop the first `offset` results in the global ranking — used to
    /// paginate. Each segment still produces its top `limit + offset`
    /// to ensure the global window has enough candidates.
    pub fn and_offset(mut self, offset: usize) -> Self {
        self.offset = offset;
        self
    }

    fn segment_top_n(&self) -> usize {
        self.limit.saturating_add(self.offset)
    }
}

impl<T: VectorElement> Collector for TopDocsByVectorSimilarity<T> {
    type Fruit = Vec<(Score, DocAddress)>;
    type Child = NoOpSegmentCollector;

    fn check_schema(&self, schema: &Schema) -> crate::Result<()> {
        let entry = schema.get_field_entry(self.field);
        let opts = match entry.field_type() {
            FieldType::Vector(o) => o,
            _ => {
                return Err(TantivyError::SchemaError(format!(
                    "field {:?} is not a vector field",
                    entry.name(),
                )));
            }
        };
        if opts.dim() != self.query.len() {
            return Err(TantivyError::SchemaError(format!(
                "query vector length {} does not match field {:?} dim {}",
                self.query.len(),
                entry.name(),
                opts.dim(),
            )));
        }
        if opts.dtype() != T::DTYPE {
            return Err(TantivyError::SchemaError(format!(
                "query dtype {:?} does not match field {:?} dtype {:?}",
                T::DTYPE,
                entry.name(),
                opts.dtype(),
            )));
        }
        Ok(())
    }

    fn for_segment(
        &self,
        _segment_local_id: SegmentOrdinal,
        _reader: &SegmentReader,
    ) -> crate::Result<Self::Child> {
        // Never called at runtime — we override `collect_segment`. The
        // child type exists only to satisfy the trait bound.
        Ok(NoOpSegmentCollector)
    }

    fn requires_scoring(&self) -> bool {
        // Similarity is computed from the stored vectors, not from the
        // filter's BM25 score — let tantivy take the no-score fast path.
        false
    }

    fn collect_segment(
        &self,
        weight: &dyn Weight,
        segment_ord: SegmentOrdinal,
        reader: &SegmentReader,
    ) -> crate::Result<Vec<(Score, DocAddress)>> {
        let backend =
            VectorBackend::for_segment(reader, segment_ord, self.field, Arc::clone(&self.query))?;
        backend.top_n(weight, reader, self.segment_top_n())
    }

    fn merge_fruits(
        &self,
        segment_fruits: Vec<Vec<(Score, DocAddress)>>,
    ) -> crate::Result<Self::Fruit> {
        // Per-segment fruits are each already top-(limit+offset);
        // flatten, sort descending, drop offset, truncate to limit.
        let mut all: Vec<(Score, DocAddress)> = segment_fruits.into_iter().flatten().collect();
        all.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(Ordering::Equal));
        if self.offset >= all.len() {
            return Ok(Vec::new());
        }
        all.drain(..self.offset);
        all.truncate(self.limit);
        Ok(all)
    }
}

/// Trait-bound shim: the collector overrides [`Collector::collect_segment`]
/// so the per-doc path never fires, but the `Child: SegmentCollector`
/// bound on `Collector` still has to be satisfied.
pub struct NoOpSegmentCollector;

impl SegmentCollector for NoOpSegmentCollector {
    type Fruit = Vec<(Score, DocAddress)>;
    fn collect(&mut self, _doc: DocId, _score: Score) {}
    fn harvest(self) -> Self::Fruit {
        Vec::new()
    }
}

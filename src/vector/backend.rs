//! Per-segment dispatch over vector storage formats.
//!
//! Picked once per segment by
//! [`TopDocsByVectorSimilarity`](super::collector::TopDocsByVectorSimilarity). The backend owns
//! its top-N loop: [`FlatBackend`] iterates the filter `Scorer` doc-by-doc.
//!
//! Adding a new format (IVF, HNSW, etc.) is a new enum variant — the
//! collector layer doesn't change.

use std::sync::Arc;

use super::flat::FlatVectorColumn;
use super::reader::{VectorColumn, VectorColumnReader, VectorReader};
use super::VectorElement;
use crate::collector::TopNComputer;
use crate::query::Weight;
use crate::schema::{Field, FieldType, Metric, Schema};
use crate::{DocAddress, DocId, Score, SegmentOrdinal, SegmentReader, TantivyError};

/// Per-segment vector backend. Pick via [`VectorBackend::for_segment`].
pub enum VectorBackend<T: VectorElement> {
    Flat(FlatBackend<T>),
}

pub struct FlatBackend<T: VectorElement> {
    column: FlatVectorColumn,
    metric: Metric,
    query: Arc<Vec<T>>,
    segment_ord: SegmentOrdinal,
}

impl<T: VectorElement> VectorBackend<T> {
    /// Open the segment's vector column using the storage format recorded in
    /// vector metadata.
    /// Returns an error if the segment has no vector data at all.
    pub fn for_segment(
        segment_reader: &SegmentReader,
        segment_ord: SegmentOrdinal,
        field: Field,
        query: Arc<Vec<T>>,
    ) -> crate::Result<Self> {
        let schema = segment_reader.schema();
        let metric = lookup_metric(schema, field)?;

        let vec_reader = VectorReader::open(segment_reader)?;

        match vec_reader.open_column(field)? {
            VectorColumn::Flat(column) => Ok(Self::Flat(FlatBackend {
                column,
                metric,
                query,
                segment_ord,
            })),
        }
    }

    /// Top-N within this segment.
    /// Hits come back already tagged with `DocAddress` (the backend
    /// holds its own `SegmentOrdinal`), so the collector doesn't need
    /// a second pass to attach the segment.
    pub fn top_n(
        &self,
        weight: &dyn Weight,
        segment_reader: &SegmentReader,
        top_n: usize,
    ) -> crate::Result<Vec<(Score, DocAddress)>> {
        match self {
            Self::Flat(b) => b.top_n(weight, segment_reader, top_n),
        }
    }
}

impl<T: VectorElement> FlatBackend<T> {
    fn top_n(
        &self,
        weight: &dyn Weight,
        segment_reader: &SegmentReader,
        top_n: usize,
    ) -> crate::Result<Vec<(Score, DocAddress)>> {
        // `for_each_no_score` walks the filter DocSet in ascending doc
        // order, which lets us use the fast `TopNComputer::push` path
        // (strict-greater threshold short-circuit, valid only under
        // ascending-D pushes).
        //
        // The heap keys on segment-local `DocId` (cheaper compares than
        // `DocAddress`); we tag with `self.segment_ord` at drain time
        // so the collector returns ready-to-use `DocAddress`es without
        // a second pass.
        let mut topn = TopNComputer::<Score, DocId, _>::new(top_n);
        let alive = segment_reader.alive_bitset();
        weight.for_each_no_score(segment_reader, &mut |docs| {
            for &doc in docs {
                if let Some(bs) = alive {
                    if !bs.is_alive(doc) {
                        continue;
                    }
                }
                if let Some(bytes) = self.column.vector_bytes_at(doc) {
                    let score = self.metric.similarity_bytes(&self.query[..], bytes);
                    topn.push(score, doc);
                }
            }
        })?;
        let segment_ord = self.segment_ord;
        Ok(topn
            .into_sorted_vec()
            .into_iter()
            .map(|cd| (cd.sort_key, DocAddress::new(segment_ord, cd.doc)))
            .collect())
    }
}

fn lookup_metric(schema: &Schema, field: Field) -> crate::Result<Metric> {
    let entry = schema.get_field_entry(field);
    match entry.field_type() {
        FieldType::Vector(opts) => Ok(opts.metric()),
        other => Err(TantivyError::SchemaError(format!(
            "field {:?} is not a vector field (got {:?})",
            entry.name(),
            other.value_type(),
        ))),
    }
}

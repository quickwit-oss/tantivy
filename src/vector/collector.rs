//! Top-N vector-similarity collector.
//!
//! Unlike the other `TopDocs::order_by_*` paths, vector similarity is
//! not a [`SortKeyComputer`](crate::collector::sort_key::SortKeyComputer).
//! IVF needs to drain the filter `DocSet` into a bitmap upfront and
//! drive its own cluster iteration — that inverts the per-doc pull
//! model that sort-key computers assume. So this is its own
//! [`Collector`] with an overridden [`Collector::collect_segment`] that
//! hands the filter `Weight` down to the per-segment
//! [`VectorBackend`](super::backend::VectorBackend), which owns the
//! loop. Flat fits the model trivially; IVF gets to drive.

use std::cmp::Ordering;
use std::sync::Arc;

use super::backend::{ProbeStats, VectorBackend};
use super::ivf::AdaptiveProbeParams;
use super::VectorElement;
use crate::collector::{Collector, SegmentCollector};
use crate::index::SegmentReader;
use crate::query::Weight;
use crate::schema::{Field, FieldType, Schema};
use crate::{DocAddress, DocId, Score, SegmentOrdinal, TantivyError};

/// Top-N by vector similarity. Returns documents in descending
/// similarity order. Only docs that actually have a vector are
/// returned — docs that match the filter but lack a vector for `field`
/// are dropped (this is required for IVF compatibility, which can't
/// see vectorless docs at all).
///
/// Generic over `T: VectorElement` — `T` must match the schema's
/// declared dtype, checked at [`Collector::check_schema`] time.
pub struct TopDocsByVectorSimilarity<T: VectorElement> {
    field: Field,
    query: Arc<Vec<T>>,
    limit: usize,
    offset: usize,
    adaptive: AdaptiveProbeParams,
}

impl<T: VectorElement> TopDocsByVectorSimilarity<T> {
    pub fn new(field: Field, query: Vec<T>, limit: usize) -> Self {
        Self {
            field,
            query: Arc::new(query),
            limit,
            offset: 0,
            adaptive: AdaptiveProbeParams::default(),
        }
    }

    /// Drop the first `offset` results in the global ranking — used to
    /// paginate. Each segment still produces its top `limit + offset`
    /// to ensure the global window has enough candidates.
    pub fn and_offset(mut self, offset: usize) -> Self {
        self.offset = offset;
        self
    }

    /// Override the adaptive probing parameters (ignored by flat-only
    /// segments).
    pub fn with_adaptive_params(mut self, params: AdaptiveProbeParams) -> Self {
        self.adaptive = params;
        self
    }

    fn segment_top_n(&self) -> usize {
        self.limit.saturating_add(self.offset)
    }
}

/// What a [`TopDocsByVectorSimilarity`] search returns: the global top-N
/// plus each searched segment's [`ProbeStats`], so callers can inspect or
/// aggregate probe metrics without a side channel.
#[derive(Debug, Default)]
pub struct VectorSimilarityFruit {
    /// Global top-N `(score, address)` pairs in descending-similarity order.
    pub results: Vec<(Score, DocAddress)>,
    /// One [`ProbeStats`] per collected segment, in segment-ordinal order
    /// after [`Collector::merge_fruits`]. The counter fields are summable
    /// across segments; `min_candidates` and `termination` only carry
    /// per-segment meaning.
    pub stats: Vec<ProbeStats>,
}

impl<T: VectorElement> Collector for TopDocsByVectorSimilarity<T> {
    type Fruit = VectorSimilarityFruit;
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
    ) -> crate::Result<Self::Fruit> {
        let backend = VectorBackend::for_segment(
            reader,
            segment_ord,
            self.field,
            Arc::clone(&self.query),
            self.adaptive.clone(),
        )?;
        let (results, stats) = backend.top_n(weight, reader, self.segment_top_n())?;
        Ok(VectorSimilarityFruit {
            results,
            stats: vec![stats],
        })
    }

    fn merge_fruits(
        &self,
        segment_fruits: Vec<VectorSimilarityFruit>,
    ) -> crate::Result<Self::Fruit> {
        // Per-segment fruits are each already top-(limit+offset);
        // flatten, sort descending, drop offset, truncate to limit.
        // Stats concatenate untouched — one entry per segment, kept even
        // when the offset swallows every result.
        let mut stats = Vec::with_capacity(segment_fruits.len());
        let mut all: Vec<(Score, DocAddress)> = Vec::new();
        for fruit in segment_fruits {
            stats.extend(fruit.stats);
            all.extend(fruit.results);
        }
        all.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(Ordering::Equal));
        all.drain(..self.offset.min(all.len()));
        all.truncate(self.limit);
        Ok(VectorSimilarityFruit {
            results: all,
            stats,
        })
    }
}

/// Trait-bound shim: the collector overrides [`Collector::collect_segment`]
/// so the per-doc path never fires, but the `Child: SegmentCollector`
/// bound on `Collector` still has to be satisfied.
pub struct NoOpSegmentCollector;

impl SegmentCollector for NoOpSegmentCollector {
    type Fruit = VectorSimilarityFruit;
    fn collect(&mut self, _doc: DocId, _score: Score) {}
    fn harvest(self) -> Self::Fruit {
        VectorSimilarityFruit::default()
    }
}

#[cfg(test)]
mod ivf_e2e_tests {
    //! End-to-end coverage: drives the full
    //! `searcher.search → TopDocsByVectorSimilarity → collect_segment
    //! → IvfBackend::top_n → merge_fruits` path against the shared
    //! `TestVectorIndex` fixture and asserts the resulting global
    //! top-K matches `index.ground_truth(...)`. Built on the shared
    //! fixture so the manual flat/ivf scene construction the
    //! pre-consolidation tests carried is gone — `vector_storage_format`
    //! is the only knob.

    use std::sync::Arc;

    use crate::collector::TopDocs;
    use crate::index::IndexSettings;
    use crate::indexer::NoMergePolicy;
    use crate::query::AllQuery;
    use crate::schema::{Schema, STORED, STRING};
    use crate::vector::tests::{exhaustive_params, ground_truth, Grid2DClusterer, TestVectorIndex};
    use crate::vector::{Metric, VectorDType, VectorOptions, VectorStorageFormat};
    use crate::{Index, TantivyDocument};

    /// IVF + exhaustive probing matches the global oracle. The shared
    /// fixture produces multiple IVF segments (it merges raw segments
    /// pairwise), so this single test already exercises cross-segment
    /// merge_fruits.
    #[test]
    fn e2e_ivf_matches_global_oracle() -> crate::Result<()> {
        let index = TestVectorIndex::builder(VectorDType::F32)
            .metric(Metric::L2)
            .vector_storage_format(VectorStorageFormat::Ivf)
            .build()?;
        let searcher = index.index.reader()?.searcher();
        let params = exhaustive_params(9);
        for query in [[0.5_f32, 0.5], [9.7, 10.3]] {
            for k in [1usize, 4, 8] {
                let expected = index.ground_truth(query, k)?;
                let collector = TopDocs::with_limit(k)
                    .order_by_similarity(index.embedding_field(), query.to_vec())
                    .with_adaptive_params(params.clone());
                let actual = searcher.search(&AllQuery, &collector)?;
                assert_eq!(actual.results, expected, "IVF query={query:?} k={k}");
            }
        }
        Ok(())
    }

    /// The production path: the fruit of a normal `searcher.search` carries
    /// one `ProbeStats` per IVF segment, each satisfying the counter
    /// invariant, so callers can aggregate probe metrics straight off the
    /// search result.
    #[test]
    fn e2e_ivf_fruit_carries_per_segment_probe_stats() -> crate::Result<()> {
        let index = TestVectorIndex::builder(VectorDType::F32)
            .metric(Metric::L2)
            .vector_storage_format(VectorStorageFormat::Ivf)
            .build()?;
        let searcher = index.index.reader()?.searcher();
        let num_segments = searcher.segment_readers().len();

        let collector = TopDocs::with_limit(4)
            .order_by_similarity(index.embedding_field(), vec![0.5_f32, 0.5])
            .with_adaptive_params(exhaustive_params(9));
        let fruit = searcher.search(&AllQuery, &collector)?;

        // One ProbeStats per searched segment.
        assert_eq!(fruit.stats.len(), num_segments);
        let mut total_visited = 0usize;
        for s in &fruit.stats {
            assert_eq!(
                s.vectors_visited,
                s.pruned_filter + s.pruned_dead + s.pruned_seen + s.candidates_scored,
                "invariant per segment: {s:?}"
            );
            assert!(s.routing.visited_count > 0);
            total_visited += s.vectors_visited;
        }
        assert!(total_visited > 0, "exhaustive probe should visit docs");
        Ok(())
    }

    /// `and_offset(n)` returns the oracle's `[n, n+k)` slice.
    #[test]
    fn e2e_offset_window_matches_oracle_slice() -> crate::Result<()> {
        let index = TestVectorIndex::builder(VectorDType::F32)
            .metric(Metric::L2)
            .vector_storage_format(VectorStorageFormat::Ivf)
            .build()?;
        let searcher = index.index.reader()?.searcher();
        let query = [0.5_f32, 0.5];
        let k = 3;
        let offset = 4;
        let full = index.ground_truth(query, offset + k)?;
        let expected = full[offset..].to_vec();
        let collector = TopDocs::with_limit(k)
            .and_offset(offset)
            .order_by_similarity(index.embedding_field(), query.to_vec())
            .with_adaptive_params(exhaustive_params(9));
        let actual = searcher.search(&AllQuery, &collector)?;
        assert_eq!(actual.results, expected);
        Ok(())
    }

    /// Flat-format build also matches the oracle. Pairs with
    /// `e2e_ivf_matches_global_oracle` to exercise the per-segment
    /// dispatch on both backend variants — `vector_storage_format`
    /// is the only thing that changes between them.
    #[test]
    fn e2e_flat_matches_global_oracle() -> crate::Result<()> {
        let index = TestVectorIndex::builder(VectorDType::F32)
            .metric(Metric::L2)
            .vector_storage_format(VectorStorageFormat::Flat)
            .build()?;
        let searcher = index.index.reader()?.searcher();
        for query in [[0.5_f32, 0.5], [9.7, 10.3]] {
            for k in [1usize, 4, 8] {
                let expected = index.ground_truth(query, k)?;
                let collector = TopDocs::with_limit(k)
                    .order_by_similarity(index.embedding_field(), query.to_vec());
                let actual = searcher.search(&AllQuery, &collector)?;
                assert_eq!(actual.results, expected, "Flat query={query:?} k={k}");
            }
        }
        Ok(())
    }

    /// Single index containing both a Flat segment (un-merged commit) and
    /// an Ivf segment (merged commit under `vector_clustering_threshold=1`)
    /// so the collector has to dispatch `FlatBackend::top_n` on one and
    /// `IvfBackend::top_n` on the other in a single `searcher.search`.
    /// Hand-built — `TestVectorIndex` produces a single format index-wide
    /// — but uses the shared `Grid2DClusterer` and `ground_truth::top_k`
    /// so there's no parallel oracle / clusterer to drift.
    #[test]
    fn e2e_mixed_flat_and_ivf_matches_global_oracle() -> crate::Result<()> {
        let centroids: Vec<[f32; 2]> = vec![[0.0, 0.0], [10.0, 10.0]];
        let metric = Metric::L2;
        let vector_options = VectorOptions::new(2, metric).with_dtype(VectorDType::F32);
        let mut schema_builder = Schema::builder();
        let embedding_field = schema_builder.add_vector_field("embedding", vector_options);
        let label_field = schema_builder.add_text_field("label", STRING | STORED);
        let schema = schema_builder.build();
        let settings = IndexSettings {
            vector_clustering_threshold: 1,
            ..IndexSettings::default()
        };
        let index = Index::builder()
            .schema(schema)
            .settings(settings)
            .ivf_clusterer(Arc::new(Grid2DClusterer {
                centroids: centroids.clone(),
            }))
            .create_in_ram()?;
        let mut writer = index.writer_with_num_threads(1, 15_000_000)?;
        writer.set_merge_policy(Box::new(NoMergePolicy));

        // Two commits → two flat segments; pairwise merge → one Ivf segment
        // (threshold=1 trips the format flip).
        let ivf_batches: [&[(&str, [f32; 2])]; 2] = [
            &[
                ("ivf0", [0.1, 0.1]),
                ("ivf1", [0.3, -0.2]),
                ("ivf2", [10.1, 9.9]),
            ],
            &[
                ("ivf3", [9.9, 10.1]),
                ("ivf4", [-0.2, 0.3]),
                ("ivf5", [10.4, 9.8]),
            ],
        ];
        for batch in ivf_batches {
            for (lbl, v) in batch {
                let mut doc = TantivyDocument::new();
                doc.add_vector(embedding_field, v);
                doc.add_text(label_field, *lbl);
                writer.add_document(doc)?;
            }
            writer.commit()?;
        }
        let mut ivf_targets = index.searchable_segment_ids()?;
        ivf_targets.sort();
        assert_eq!(ivf_targets.len(), 2, "expected two segments to merge");
        writer.merge(&ivf_targets).wait()?;

        // One more un-merged commit → flat segment.
        let flat_batch: [(&str, [f32; 2]); 3] = [
            ("flat0", [0.4, 0.4]),
            ("flat1", [10.3, 10.3]),
            ("flat2", [-0.1, 0.2]),
        ];
        for (lbl, v) in flat_batch {
            let mut doc = TantivyDocument::new();
            doc.add_vector(embedding_field, &v);
            doc.add_text(label_field, lbl);
            writer.add_document(doc)?;
        }
        writer.commit()?;
        writer.wait_merging_threads()?;

        // Confirm both formats are actually represented — the whole point
        // of this test is mixed dispatch, so a vacuous all-Flat or all-Ivf
        // index should fail loudly here.
        let searcher = index.reader()?.searcher();
        let mut flat_count = 0usize;
        let mut ivf_count = 0usize;
        for reader in searcher.segment_readers() {
            match reader.vector_index(embedding_field)?.index() {
                None => flat_count += 1,
                Some(_) => ivf_count += 1,
            }
        }
        assert!(
            flat_count >= 1 && ivf_count >= 1,
            "expected mixed segments, got {flat_count} flat / {ivf_count} ivf"
        );

        // Exhaustive probing on the Ivf side so the only thing being
        // tested here is per-segment dispatch + merge_fruits — not the
        // adaptive loop, which is covered separately.
        let params = exhaustive_params(9);
        for query in [[0.0_f32, 0.0], [10.0, 10.0], [5.0, 5.0]] {
            for k in [1usize, 3, 6] {
                let expected = ground_truth::top_k(&index, embedding_field, metric, &query, k)?;
                let collector = TopDocs::with_limit(k)
                    .order_by_similarity(embedding_field, query.to_vec())
                    .with_adaptive_params(params.clone());
                let actual = searcher.search(&AllQuery, &collector)?;
                assert_eq!(actual.results, expected, "mixed query={query:?} k={k}");
            }
        }
        Ok(())
    }
}

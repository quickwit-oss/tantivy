//! KNN query for vector similarity search.
//!
//! `KnnQuery` performs approximate nearest neighbor search over a vector field
//! using the HNSW index and returns scored document results.

use crate::device::GpuContext;
use crate::error::GpuResult;
use crate::vector::distance::DistanceMetric;
use crate::vector::hnsw::HnswIndex;

/// A kNN (k-Nearest Neighbors) query over a vector field.
///
/// ## Usage
///
/// ```ignore
/// let query = KnnQuery::new("embedding", vec![0.1, 0.2, ...], 10)
///     .with_metric(DistanceMetric::Cosine)
///     .with_ef(100);
///
/// let results = query.execute(&index)?;
/// ```
pub struct KnnQuery {
    /// Field name containing vectors.
    pub field: String,
    /// Query vector.
    pub query_vector: Vec<f32>,
    /// Number of nearest neighbors to return.
    pub k: usize,
    /// Distance metric.
    pub metric: DistanceMetric,
    /// ef parameter for HNSW search (higher = more accurate, slower).
    pub ef: usize,
    /// Optional GPU context for accelerated search.
    pub gpu_ctx: Option<GpuContext>,
}

/// Result of a kNN query: (distance, internal_doc_id).
#[derive(Debug, Clone)]
pub struct KnnResult {
    /// Raw distance value from the metric computation.
    pub distance: f32,
    /// Internal document ID.
    pub doc_id: u32,
    /// Converted to a relevance score: 1 / (1 + distance) for L2,
    /// or cos_similarity for cosine.
    pub score: f32,
}

impl KnnQuery {
    /// Create a new kNN query.
    pub fn new(field: impl Into<String>, query_vector: Vec<f32>, k: usize) -> Self {
        Self {
            field: field.into(),
            query_vector,
            k,
            metric: DistanceMetric::L2,
            ef: 100,
            gpu_ctx: None,
        }
    }

    /// Set the distance metric.
    pub fn with_metric(mut self, metric: DistanceMetric) -> Self {
        self.metric = metric;
        self
    }

    /// Set the ef search parameter.
    pub fn with_ef(mut self, ef: usize) -> Self {
        self.ef = ef;
        self
    }

    /// Enable GPU-accelerated search.
    pub fn with_gpu(mut self, ctx: GpuContext) -> Self {
        self.gpu_ctx = Some(ctx);
        self
    }

    /// Execute the kNN query against an HNSW index.
    ///
    /// The score conversion uses the **index's** distance metric (not the query's),
    /// since search results are computed using the index metric.
    pub fn execute(&self, index: &HnswIndex) -> GpuResult<Vec<KnnResult>> {
        let raw_results = if self.gpu_ctx.is_some() {
            index.search_gpu(&self.query_vector, self.k, self.ef)?
        } else {
            index.search(&self.query_vector, self.k, self.ef)
        };

        // Use the index's metric for score conversion (search used the index's metric)
        let index_metric = index.metric();

        Ok(raw_results
            .into_iter()
            .map(|(distance, doc_id)| {
                let score = distance_to_score(distance, index_metric);
                KnnResult {
                    distance,
                    doc_id,
                    score,
                }
            })
            .collect())
    }
}

/// Convert a raw distance to a relevance score (higher = more relevant).
fn distance_to_score(distance: f32, metric: DistanceMetric) -> f32 {
    match metric {
        DistanceMetric::L2 => {
            // 1 / (1 + distance) — maps [0, ∞) to (0, 1]
            1.0 / (1.0 + distance)
        }
        DistanceMetric::Cosine => {
            // cosine distance is 1 - cos_sim, so score = 1 - distance = cos_sim
            1.0 - distance
        }
        DistanceMetric::DotProduct => {
            // distance = -dot, so score = -distance = dot
            -distance
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_knn_query_basic() {
        let mut index = HnswIndex::new(3, DistanceMetric::L2);
        for i in 0..50u32 {
            index
                .insert(vec![i as f32, (i * 2) as f32, (i * 3) as f32])
                .unwrap();
        }

        let query = KnnQuery::new("embedding", vec![25.0, 50.0, 75.0], 5);
        let results = query.execute(&index).unwrap();

        assert_eq!(results.len(), 5);
        assert_eq!(results[0].doc_id, 25);
        assert_eq!(results[0].distance, 0.0);
        assert_eq!(results[0].score, 1.0); // 1/(1+0)
    }

    #[test]
    fn test_knn_query_cosine() {
        let mut index = HnswIndex::new(2, DistanceMetric::Cosine);
        index.insert(vec![1.0, 0.0]).unwrap();
        index.insert(vec![0.0, 1.0]).unwrap();
        index.insert(vec![0.707, 0.707]).unwrap();

        let query = KnnQuery::new("vec", vec![1.0, 1.0], 3).with_metric(DistanceMetric::Cosine);
        let results = query.execute(&index).unwrap();

        assert_eq!(results.len(), 3);
        // [0.707, 0.707] is closest to [1, 1] by cosine
        assert_eq!(results[0].doc_id, 2);
        assert!(
            results[0].score > 0.99,
            "score should be ~1.0: {}",
            results[0].score
        );
    }

    #[test]
    fn test_distance_to_score() {
        assert_eq!(distance_to_score(0.0, DistanceMetric::L2), 1.0);
        assert!((distance_to_score(1.0, DistanceMetric::L2) - 0.5).abs() < 1e-6);
        assert_eq!(distance_to_score(0.0, DistanceMetric::Cosine), 1.0);
        assert_eq!(distance_to_score(0.5, DistanceMetric::Cosine), 0.5);
    }
}

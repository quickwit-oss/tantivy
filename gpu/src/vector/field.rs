//! Vector field type definition and options.
//!
//! Provides `VectorFieldOptions` and the schema-level types needed to
//! define and configure vector fields in a Tantivy index.
//!
//! Since Tantivy's core `Type` enum doesn't include vectors, we store
//! vector data as a sidecar file (`.vec`) alongside each segment, managed
//! by `VectorFieldWriter` and `VectorFieldReader`.

use serde::{Deserialize, Serialize};

use crate::vector::distance::DistanceMetric;

/// Options for a vector field.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct VectorFieldOptions {
    /// Vector dimension (number of floats per vector).
    pub dimension: usize,
    /// Distance metric for similarity search.
    pub metric: DistanceMetricConfig,
    /// Whether to build an HNSW index for kNN search.
    pub indexed: bool,
    /// Whether to store raw vectors for retrieval.
    pub stored: bool,
    /// HNSW parameters.
    pub hnsw: HnswConfig,
}

/// Distance metric configuration (serializable).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DistanceMetricConfig {
    /// L2 (Euclidean) distance.
    L2,
    /// Cosine similarity distance.
    Cosine,
    /// Dot product distance.
    DotProduct,
}

impl From<DistanceMetricConfig> for DistanceMetric {
    fn from(config: DistanceMetricConfig) -> Self {
        match config {
            DistanceMetricConfig::L2 => DistanceMetric::L2,
            DistanceMetricConfig::Cosine => DistanceMetric::Cosine,
            DistanceMetricConfig::DotProduct => DistanceMetric::DotProduct,
        }
    }
}

impl From<DistanceMetric> for DistanceMetricConfig {
    fn from(metric: DistanceMetric) -> Self {
        match metric {
            DistanceMetric::L2 => DistanceMetricConfig::L2,
            DistanceMetric::Cosine => DistanceMetricConfig::Cosine,
            DistanceMetric::DotProduct => DistanceMetricConfig::DotProduct,
        }
    }
}

/// HNSW index construction parameters.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct HnswConfig {
    /// Maximum connections per node at level 0.
    pub m: usize,
    /// ef parameter for construction (higher = better quality, slower build).
    pub ef_construction: usize,
}

impl Default for HnswConfig {
    fn default() -> Self {
        Self {
            m: 16,
            ef_construction: 200,
        }
    }
}

impl Default for VectorFieldOptions {
    fn default() -> Self {
        Self {
            dimension: 128,
            metric: DistanceMetricConfig::L2,
            indexed: true,
            stored: true,
            hnsw: HnswConfig::default(),
        }
    }
}

impl VectorFieldOptions {
    /// Create options for a vector field with given dimension.
    pub fn new(dimension: usize) -> Self {
        Self {
            dimension,
            ..Default::default()
        }
    }

    /// Set the distance metric.
    pub fn with_metric(mut self, metric: DistanceMetric) -> Self {
        self.metric = metric.into();
        self
    }

    /// Enable/disable HNSW indexing.
    pub fn with_indexed(mut self, indexed: bool) -> Self {
        self.indexed = indexed;
        self
    }

    /// Enable/disable raw vector storage.
    pub fn with_stored(mut self, stored: bool) -> Self {
        self.stored = stored;
        self
    }

    /// Set HNSW configuration.
    pub fn with_hnsw(mut self, m: usize, ef_construction: usize) -> Self {
        self.hnsw = HnswConfig { m, ef_construction };
        self
    }
}

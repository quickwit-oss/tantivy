//! GPU-accelerated vector similarity search.
//!
//! Provides HNSW (Hierarchical Navigable Small World) graph index with
//! GPU-accelerated distance computation for kNN queries.
//!
//! ## Architecture
//!
//! ```text
//! ┌──────────────────────────────────────────────┐
//! │  KnnQuery                                     │
//! │  ├─ query_vector: Vec<f32>                    │
//! │  ├─ k: usize                                  │
//! │  └─ field: String                             │
//! └──────────────┬───────────────────────────────┘
//!                ↓
//! ┌──────────────────────────────────────────────┐
//! │  HnswIndex (per-segment)                      │
//! │  ├─ vectors: Vec<Vec<f32>>  (flat storage)    │
//! │  ├─ graph: Vec<Vec<u32>>    (neighbor lists)  │
//! │  ├─ levels: Vec<u8>         (node levels)     │
//! │  └─ entry_point: u32                          │
//! └──────────────┬───────────────────────────────┘
//!                ↓
//! ┌──────────────────────────────────────────────┐
//! │  GPU Distance Kernel (WGSL)                   │
//! │  Computes L2/cosine/dot distances for batch   │
//! │  of candidate vectors vs query vector         │
//! └──────────────────────────────────────────────┘
//! ```

pub mod distance;
pub mod field;
pub mod gpu_cache;
pub mod hnsw;
pub mod knn_query;
pub mod persistence;

pub use distance::{DistanceMetric, GpuDistanceKernel};
pub use field::VectorFieldOptions;
pub use hnsw::HnswIndex;
pub use knn_query::KnnQuery;

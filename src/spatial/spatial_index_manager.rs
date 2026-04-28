//! Registry for spatial index implemenations.
//!
//! `SpatialIndexManager` follows the same pattern as `TokenizerManager`. Users register
//! strategies by name at index creation time. The registered strategies are retrieved
//! at the writer, merger, and query integration points.
//!
//! Implemenations are monomorphic implementations behind a `dyn SpatialIndex` trait. The
//! vtable lookup occurs at field boundaries, not per-geometry, making the cost negligible.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// HUSH
pub struct SpatialIndex {}

/// Manages a map of index implementations keyed on a user specified index type name.
#[derive(Clone)]
pub struct SpatialIndexManager {
    /// A map of spatial index implementations.
    pub indices: Arc<RwLock<HashMap<String, SpatialIndex>>>,
}

impl Default for SpatialIndexManager {
    fn default() -> Self {
        Self {
            indices: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

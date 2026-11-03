//! Tantivy integration for spatial indexes.

pub use tantivy_spatial::*;

pub mod executor;
pub mod geometry;
mod native_input;
pub mod plugin;
pub mod reader;
pub mod serializer;
pub mod spatial_index_manager;
pub mod stats;

pub use geometry::{to_geometry_set, Geometry};
pub use native_input::NativeSpatial;
pub use plugin::{Cons, Nil, S2Plugin, SpatialInput};
pub use spatial_index_manager::SurfaceIndex;

#[cfg(test)]
mod lifecycle_tests;

#[cfg(test)]
#[path = "tests/executor_tests.rs"]
mod executor_tests;

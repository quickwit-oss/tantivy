//! Tantivy integration for spatial indexes.

pub use tantivy_spatial::*;

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
#[path = "tests/covering_query_tests.rs"]
mod covering_query_tests;

#[cfg(test)]
#[path = "tests/footer_tests.rs"]
mod footer_tests;

#[cfg(test)]
#[path = "../../spatial/tests/partition.rs"]
mod partition;

#[cfg(test)]
#[path = "tests/query_tests.rs"]
mod query_tests;

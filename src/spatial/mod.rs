//! Spatial module (implements a block kd-tree)

pub mod bkd;
pub mod delta;
pub mod geometry;
pub mod plane;
pub mod point;
pub mod radix_select;
pub mod reader;
pub mod serializer;
pub mod spatial_index_manager;
pub mod sphere;
pub mod surface;
pub mod triangle;
pub mod writer;
pub mod xor;

pub use geometry::Geometry;

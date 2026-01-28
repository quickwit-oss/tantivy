//! Spatial module (implements a block kd-tree)
//!
//! References:
//!
//! https://s2geometry.io/
//! https://s2geometry.io/about/overview
//! https://s2geometry.io/devguide/s2cell_hierarchy.html
//! The above contains the visualization of face projection.
//! https://proj.org/en/stable/operations/projections/s2.html
//! https://s2geometry.io/resources/earthcube.html
//! https://www.geopipe.ai/posts/s2-sees-the-world-differently
//! https://blog.christianperone.com/2015/08/googles-s2-geometry-on-the-sphere-cells-and-hilbert-curve/
//! https://benfeifke.com/posts/geospatial-indexing-explained/
//! https://docs.s2cell.aliddell.com/en/stable/s2_concepts.html

pub mod bkd;
pub mod containment;
pub mod crossings;
pub mod delta;
pub mod exact;
pub mod geometry;
pub mod math;
pub mod plane;
pub mod point;
pub mod r1interval;
pub mod r2rect;
pub mod radix_select;
pub mod reader;
pub mod s2cell;
pub mod s2cell_id;
pub mod s2coords;
pub mod s2edge_clipping;
pub mod s2padded_cell;
pub mod serializer;
pub mod spatial_index_manager;
pub mod sphere;
pub mod surface;
pub mod triangle;
pub mod writer;
pub mod xor;

pub use geometry::Geometry;

//! Spatial module.
//!
//! Geospatial indexing using S2 cell partitioning. Polygon edges are stored in a streaming
//! binary format backed by an S2 cell index, enabling polygon containment queries at search
//! engine scale.
//!
//! References:
//!
//! https://s2geometry.io/
//! https://s2geometry.io/about/overview
//! https://s2geometry.io/devguide/s2cell_hierarchy.html
//! https://proj.org/en/stable/operations/projections/s2.html
//! https://s2geometry.io/resources/earthcube.html

pub mod cell_index_reader;
pub mod cell_union;
pub mod clip_options;
pub mod clipped_shape;
pub mod clipper;
pub mod closest_edge_query;
pub mod collapse;
pub mod containment;
pub mod contains_query;
pub mod crossings;
pub mod delta;
pub mod edge_cache;
pub mod edge_reader;
pub mod edge_writer;
pub mod exact;
pub mod executor;
pub mod geometry;
pub mod geometry_set;
pub mod interleaver;
pub mod intersects_query;
pub mod latlng_rect;
pub mod math;
pub mod merge;
pub mod plane;
pub mod r1interval;
pub mod r2rect;
pub mod reader;
pub mod region;
pub mod region_coverer;
pub mod s1chord_angle;
pub mod s1interval;
pub mod s2cap;
pub mod s2cell;
pub mod s2cell_id;
pub mod s2coords;
pub mod s2edge_clipping;
pub mod s2edge_distances;
pub mod s2loop_measures;
pub mod s2metrics;
pub mod s2padded_cell;
pub mod serializer;
pub mod shape_index;
pub mod shape_index_region;
pub mod spatial_index_manager;
pub mod sphere;
pub mod surface;
pub mod writer;
pub mod xor;

pub use geometry::Geometry;

//! Registry for spatial index implementations.
//!
//! `SpatialIndexManager` follows the same pattern as `TokenizerManager`. Users register
//! implementations by name at index creation time. The registered implementations are retrieved
//! at the writer, merger, and query integration points.
//!
//! Implementations are monomorphic behind a `dyn SpatialIndex` trait. The vtable lookup occurs
//! at field boundaries, not per-geometry.

use std::collections::HashMap;
use std::io;
use std::io::Write;
use std::sync::{Arc, RwLock};

use common::CountingWriter;

use crate::directory::WritePtr;
use crate::spatial::geometry::Geometry;
use crate::spatial::geometry_set::{to_geometry_set, GeometrySet};
use crate::spatial::plane::Plane;
use crate::spatial::surface::Surface;
use crate::spatial::clip_options::ClipOptions;
use crate::spatial::clipper::Clipper;
use crate::spatial::edge_writer::EdgeWriter;

/// Default skip interval for the edge index skip list directory.
const EDGE_SKIP_INTERVAL: u32 = 16;

/// Factory for per-field spatial writers. Registered in `SpatialIndexManager` by name.
pub trait SpatialIndex: Send + Sync + SpatialIndexClone {
    /// Create a writer that accumulates geometries for one field.
    fn create_field_writer(&self) -> Box<dyn SpatialFieldWriter>;
}

/// Accumulates geometries for one field and serializes them.
pub trait SpatialFieldWriter: Send {
    /// Project, smash, and accumulate a geometry.
    fn add_geometry(&mut self, doc_id: u32, geometry: &Geometry<Plane>);

    /// Memory usage estimate for accumulated geometry.
    fn mem_usage(&self) -> usize;

    /// Serialize accumulated geometry to the cell index and edge index.
    fn serialize(
        self: Box<Self>,
        cells_write: &mut CountingWriter<WritePtr>,
        edges_write: &mut CountingWriter<WritePtr>,
    ) -> io::Result<()>;
}

/// Clone support for trait objects.
pub trait SpatialIndexClone {
    /// Clone into a boxed trait object.
    fn clone_box(&self) -> Box<dyn SpatialIndex>;
}

impl<T: SpatialIndex + Clone + 'static> SpatialIndexClone for T {
    fn clone_box(&self) -> Box<dyn SpatialIndex> {
        Box::new(self.clone())
    }
}

impl Clone for Box<dyn SpatialIndex> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}

/// Concrete spatial index for a given surface.
#[derive(Clone)]
pub struct SurfaceIndex<S: Surface> {
    _surface: std::marker::PhantomData<S>,
}

impl<S: Surface> SurfaceIndex<S> {
    /// Create a new surface index.
    pub fn new() -> Self {
        SurfaceIndex {
            _surface: std::marker::PhantomData,
        }
    }
}

impl<S: Surface + Send + Sync + Clone + 'static> SpatialIndex for SurfaceIndex<S> {
    fn create_field_writer(&self) -> Box<dyn SpatialFieldWriter> {
        Box::new(SurfaceFieldWriter::<S>::new())
    }
}

/// Per-field writer for a given surface. Accumulates `GeometrySet<S>` and serializes them.
struct SurfaceFieldWriter<S: Surface> {
    sets: Vec<GeometrySet<S>>,
}

impl<S: Surface> SurfaceFieldWriter<S> {
    fn new() -> Self {
        SurfaceFieldWriter { sets: Vec::new() }
    }
}

impl<S: Surface + Send + 'static> SpatialFieldWriter for SurfaceFieldWriter<S> {
    fn add_geometry(&mut self, doc_id: u32, geometry: &Geometry<Plane>) {
        let projected = geometry.project::<S>();
        let set = to_geometry_set(&projected, doc_id);
        self.sets.push(set);
    }

    fn mem_usage(&self) -> usize {
        self.sets
            .iter()
            .map(|set| {
                set.members
                    .iter()
                    .map(|m| m.vertices.len() * S::DIMENSIONS * 8)
                    .sum::<usize>()
            })
            .sum()
    }

    fn serialize(
        self: Box<Self>,
        cells_write: &mut CountingWriter<WritePtr>,
        edges_write: &mut CountingWriter<WritePtr>,
    ) -> io::Result<()> {
        if self.sets.is_empty() {
            return Ok(());
        }

        let builder = Clipper::new(ClipOptions::default());
        let cell_index = builder.build(&self.sets);

        cell_index.write(cells_write);
        cells_write.flush()?;

        let mut edge_writer = EdgeWriter::<S>::new(edges_write, EDGE_SKIP_INTERVAL);
        for set in &self.sets {
            edge_writer.insert(set);
        }
        edge_writer.finish();
        edges_write.flush()?;

        Ok(())
    }
}

/// Manages spatial index implementations keyed by name.
#[derive(Clone)]
pub struct SpatialIndexManager {
    indices: Arc<RwLock<HashMap<String, Box<dyn SpatialIndex>>>>,
}

impl SpatialIndexManager {
    /// Register a spatial index implementation under a name.
    pub fn register(&self, name: &str, index: Box<dyn SpatialIndex>) {
        self.indices
            .write()
            .expect("acquiring the lock should never fail")
            .insert(name.to_string(), index);
    }

    /// Retrieve a spatial index implementation by name.
    pub fn get(&self, name: &str) -> Option<Box<dyn SpatialIndex>> {
        self.indices
            .read()
            .expect("acquiring the lock should never fail")
            .get(name)
            .map(|i| i.clone_box())
    }
}

impl Default for SpatialIndexManager {
    fn default() -> Self {
        let manager = SpatialIndexManager {
            indices: Arc::new(RwLock::new(HashMap::new())),
        };
        manager.register("sphere", Box::new(SurfaceIndex::<crate::spatial::sphere::Sphere>::new()));
        manager.register("plane", Box::new(SurfaceIndex::<Plane>::new()));
        manager
    }
}

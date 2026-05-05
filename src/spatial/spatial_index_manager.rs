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

use common::{CountingWriter, ReadOnlyBitSet};

use crate::directory::WritePtr;
use crate::spatial::cell_index_reader::CellIndexReader;
use crate::spatial::edge_cache::EdgeCache;
use crate::spatial::edge_reader::EdgeReader;
use crate::spatial::edge_writer::EdgeWriter;
use crate::spatial::geometry::Geometry;
use crate::spatial::geometry_set::{to_geometry_set, GeometrySet};
use crate::spatial::interleaver::Interleaver;
use crate::spatial::plane::Plane;
use crate::spatial::surface::Surface;
use crate::spatial::clip_options::ClipOptions;
use crate::spatial::clipper::Clipper;
use crate::spatial::contains_query::ContainsQuery;
use crate::spatial::intersects_query::IntersectsQuery;
use crate::spatial::region_coverer::CovererOptions;
use crate::spatial::sphere::Sphere;
use crate::DocId;

/// Default skip interval for the edge index skip list directory.
const EDGE_SKIP_INTERVAL: u32 = 16;

/// Source segment data for a spatial merge.
pub struct MergeSource<'a> {
    /// Raw cell index bytes from the mmap'd segment.
    pub cells_bytes: &'a [u8],
    /// Raw edge index bytes from the mmap'd segment.
    pub edges_bytes: &'a [u8],
    /// Segment name for diagnostics.
    pub segment_name: String,
}

/// Factory for per-field spatial writers, mergers, and queries. Registered in
/// `SpatialIndexManager` by name.
pub trait SpatialIndex: Send + Sync + SpatialIndexClone {
    /// Create a writer that accumulates geometries for one field.
    fn create_field_writer(&self) -> Box<dyn SpatialFieldWriter>;

    /// Merge source segments into a new segment.
    fn merge_field<'a>(
        &self,
        sources: &[MergeSource<'a>],
        alive_bitsets: &'a [Option<ReadOnlyBitSet>],
        cells_write: &mut CountingWriter<WritePtr>,
        edges_write: &mut CountingWriter<WritePtr>,
        doc_id_inverse: &[Vec<Option<DocId>>],
    ) -> io::Result<MergeResult>;

    /// Prepare an intersects query from a query polygon in lon/lat.
    fn prepare_intersects(
        &self,
        geometry: &Geometry<Plane>,
    ) -> Box<dyn PreparedSpatialQuery>;

    /// Prepare a contains query from a query polygon in lon/lat.
    fn prepare_contains(
        &self,
        geometry: &Geometry<Plane>,
    ) -> Box<dyn PreparedSpatialQuery>;
}

/// A prepared spatial query that searches one segment at a time.
pub trait PreparedSpatialQuery: Send + Sync {
    /// Search one segment from raw cell index and edge index bytes.
    fn search_segment_bytes(&self, cells_bytes: &[u8], edges_bytes: &[u8]) -> Vec<u32>;
}

/// Result of a spatial merge.
pub struct MergeResult {
    /// For each source segment, the mapping from old geometry position to new geometry ID.
    pub old_to_new: Vec<Vec<u32>>,
    /// Total number of geometries in the merged output.
    pub geometry_count: u32,
    /// Number of cells emitted.
    pub cell_count: u64,
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

    fn merge_field<'a>(
        &self,
        sources: &[MergeSource<'a>],
        alive_bitsets: &'a [Option<ReadOnlyBitSet>],
        cells_write: &mut CountingWriter<WritePtr>,
        edges_write: &mut CountingWriter<WritePtr>,
        doc_id_inverse: &[Vec<Option<DocId>>],
    ) -> io::Result<MergeResult> {
        let mut cell_readers: Vec<CellIndexReader> = Vec::new();
        let mut edge_readers: Vec<EdgeReader<'_, S>> = Vec::new();
        let mut geometry_counts: Vec<u32> = Vec::new();
        let mut segment_names: Vec<String> = Vec::new();

        for source in sources {
            let cr = CellIndexReader::open(source.cells_bytes);
            let er = EdgeReader::<S>::open(source.edges_bytes);
            geometry_counts.push(er.geometry_count());
            cell_readers.push(cr);
            edge_readers.push(er);
            segment_names.push(source.segment_name.clone());
        }

        let edge_cache = EdgeCache::new(edge_readers, 2_000_000_000);
        let iters = cell_readers.iter().map(|cr| cr.iter()).collect();
        let mut interleave = Interleaver::new(iters, &edge_cache, alive_bitsets, segment_names);

        const NOT_ASSIGNED: u32 = u32::MAX;
        let mut old_to_new: Vec<Vec<u32>> = geometry_counts
            .iter()
            .map(|&count| vec![NOT_ASSIGNED; count as usize])
            .collect();
        let mut next_new_id: u32 = 0;

        let mut edge_writer = EdgeWriter::<S>::new(edges_write, EDGE_SKIP_INTERVAL);
        let cells_base = cells_write.written_bytes();
        let mut offsets: Vec<(u64, u64)> = Vec::new();
        let mut cell_count: u64 = 0;

        while let Some(cell) = interleave.next() {
            cell_count += 1;
            for shape in &cell.shapes {
                let segment = shape.geometry_id.0 as usize;
                let position = shape.geometry_id.1 as usize;
                if old_to_new[segment][position] != NOT_ASSIGNED {
                    continue;
                }
                let entry = edge_cache.get(shape.geometry_id);
                let set_size = entry.member_count();
                for i in 0..set_size {
                    let pos = (entry.head() + i) as usize;
                    if old_to_new[segment][pos] == NOT_ASSIGNED {
                        old_to_new[segment][pos] = next_new_id;
                        next_new_id += 1;
                    }
                }
                let old_doc_id = entry.doc_id();
                let new_doc_id = doc_id_inverse[segment][old_doc_id as usize].unwrap();
                let mut set = entry.geometry_set().clone();
                set.doc_id = new_doc_id as u32;
                edge_writer.insert(&set);
            }

            let offset = cells_write.written_bytes() - cells_base;
            cells_write.write_all(&cell.cell_id.0.to_le_bytes()).unwrap();
            cells_write
                .write_all(&(cell.shapes.len() as u32).to_le_bytes())
                .unwrap();
            for shape in &cell.shapes {
                let segment = shape.geometry_id.0 as usize;
                let position = shape.geometry_id.1 as usize;
                let new_id = old_to_new[segment][position];
                assert_ne!(new_id, NOT_ASSIGNED);
                cells_write.write_all(&new_id.to_le_bytes()).unwrap();
                cells_write.write_all(&[shape.contains_center as u8]).unwrap();
                cells_write
                    .write_all(&(shape.edge_indices.len() as u32).to_le_bytes())
                    .unwrap();
                for &edge_id in &shape.edge_indices {
                    cells_write.write_all(&edge_id.to_le_bytes()).unwrap();
                }
            }
            offsets.push((cell.cell_id.0, offset));
        }

        assert_eq!(
            next_new_id,
            edge_writer.geometry_count(),
        );
        edge_writer.finish();

        interleave.report();

        // Cell index dictionary and footer.
        let dir_offset = cells_write.written_bytes() - cells_base;
        for &(cell_id, offset) in &offsets {
            cells_write.write_all(&cell_id.to_le_bytes()).unwrap();
            cells_write.write_all(&offset.to_le_bytes()).unwrap();
        }
        cells_write
            .write_all(&(cell_count as u32).to_le_bytes())
            .unwrap();
        cells_write
            .write_all(&dir_offset.to_le_bytes())
            .unwrap();
        cells_write.flush()?;
        edges_write.flush()?;

        Ok(MergeResult {
            old_to_new,
            geometry_count: next_new_id,
            cell_count,
        })
    }

    fn prepare_intersects(
        &self,
        geometry: &Geometry<Plane>,
    ) -> Box<dyn PreparedSpatialQuery> {
        let projected = geometry.project::<S>();
        let set = to_geometry_set(&projected, 0);
        Box::new(PreparedIntersects::<S> {
            query: IntersectsQuery::new(set, CovererOptions::default()),
        })
    }

    fn prepare_contains(
        &self,
        geometry: &Geometry<Plane>,
    ) -> Box<dyn PreparedSpatialQuery> {
        // ContainsQuery is Sphere-only for now.
        let projected = geometry.project::<Sphere>();
        let set = to_geometry_set(&projected, 0);
        Box::new(PreparedContains {
            query: ContainsQuery::new(set, CovererOptions::default()),
        })
    }
}

struct PreparedIntersects<S: Surface> {
    query: IntersectsQuery<S>,
}


impl<S: Surface + 'static> PreparedSpatialQuery for PreparedIntersects<S> {
    fn search_segment_bytes(&self, cells_bytes: &[u8], edges_bytes: &[u8]) -> Vec<u32> {
        let cell_reader = CellIndexReader::open(cells_bytes);
        let edge_reader = EdgeReader::<S>::open(edges_bytes);
        let mut edge_cache = EdgeCache::new(vec![edge_reader], 100_000);
        self.query.search_segment(&cell_reader, &mut edge_cache)
    }
}

struct PreparedContains {
    query: ContainsQuery,
}


impl PreparedSpatialQuery for PreparedContains {
    fn search_segment_bytes(&self, cells_bytes: &[u8], edges_bytes: &[u8]) -> Vec<u32> {
        let cell_reader = CellIndexReader::open(cells_bytes);
        let edge_reader = EdgeReader::<Sphere>::open(edges_bytes);
        let mut edge_cache = EdgeCache::new(vec![edge_reader], 100_000);
        self.query.search_segment(&cell_reader, &mut edge_cache)
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
        manager.register("sphere", Box::new(SurfaceIndex::<Sphere>::new()));
        manager.register("plane", Box::new(SurfaceIndex::<Plane>::new()));
        manager
    }
}

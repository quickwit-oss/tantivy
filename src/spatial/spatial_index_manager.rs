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

use common::{BitSet, CountingWriter, ReadOnlyBitSet};

use crate::directory::WritePtr;
use crate::spatial::cell_index_reader::CellIndexReader;
use crate::spatial::clip_options::ClipOptions;
use crate::spatial::clipper::Clipper;
use crate::spatial::contains::Contains;
use crate::spatial::distance::Distance;
use crate::spatial::edge_reader::EdgeReader;
use crate::spatial::edge_resolver::EdgeResolver;
use crate::spatial::edge_writer::EdgeWriter;
use crate::spatial::executor::SpatialRelation;
use crate::spatial::geometry::Geometry;
use crate::spatial::geometry_set::{to_geometry_set, GeometrySet};
use crate::spatial::interleaver::Interleaver;
use crate::spatial::intersects::Intersects;
use crate::spatial::plane::query::PlaneQuery;
use crate::spatial::plane::{Plane, Predicate};
use crate::spatial::region_coverer::CovererOptions;
use crate::spatial::shape_index::write_cell_footer;
use crate::spatial::sphere::Sphere;
use crate::spatial::surface::Surface;
use crate::spatial::within::Within;
use crate::DocId;

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
        doc_ids_write: &mut CountingWriter<WritePtr>,
        doc_id_inverse: &[Vec<Option<DocId>>],
    ) -> io::Result<MergeResult>;

    /// Prepare an intersects query from a geometry in lon/lat.
    fn prepare_intersects(&self, geometry: &Geometry<Plane>) -> Box<dyn PreparedSpatialQuery>;

    /// Prepare a contains query from a geometry in lon/lat.
    fn prepare_contains(&self, geometry: &Geometry<Plane>) -> Box<dyn PreparedSpatialQuery>;

    /// Prepare boundary-inclusive coverage. Unsupported surfaces return an error.
    fn prepare_covers(
        &self,
        _geometry: &Geometry<Plane>,
    ) -> crate::Result<Box<dyn PreparedSpatialQuery>> {
        Err(crate::TantivyError::InvalidArgument(
            "Covers is not supported on this surface".into(),
        ))
    }

    /// Enumerate stored value heads owned by a document.
    fn value_heads(&self, _edges: &[u8], _doc_id: u32, _heads: &mut Vec<u32>) -> crate::Result<()> {
        Err(crate::TantivyError::InvalidArgument(
            "Stored-value joins are not supported by this spatial index".into(),
        ))
    }

    /// Prepare a probe for a stored outer value without projecting it a second time.
    fn prepare_join_query(
        &self,
        _edges: &[u8],
        _head: u32,
        _relation: &SpatialRelation,
    ) -> crate::Result<Box<dyn PreparedSpatialQuery>> {
        Err(crate::TantivyError::InvalidArgument(
            "Stored-value joins are not supported by this spatial index".into(),
        ))
    }

    /// Check a join's surface contract before enumerating values.
    fn validate_join(&self, _relation: &SpatialRelation) -> crate::Result<()> {
        Err(crate::TantivyError::InvalidArgument(
            "Stored-value joins are not supported by this spatial index".into(),
        ))
    }

    /// Prepare a covered-by query from a geometry in lon/lat.
    fn prepare_within(&self, geometry: &Geometry<Plane>) -> Box<dyn PreparedSpatialQuery>;

    /// Prepare a distance query from a geometry in lon/lat. Distance in radians.
    fn prepare_distance(
        &self,
        geometry: &Geometry<Plane>,
        max_distance: f64,
    ) -> crate::Result<Box<dyn PreparedSpatialQuery>>;
}

/// A prepared spatial query that searches one segment at a time.
pub trait PreparedSpatialQuery: Send + Sync {
    /// Search one segment from raw cell index, edge index, and doc ID index bytes.
    fn search_segment_bytes(
        &self,
        cells_bytes: &[u8],
        edges_bytes: &[u8],
        doc_ids_bytes: &[u8],
        max_doc: u32,
    ) -> io::Result<BitSet>;

    /// Search only eligible documents. Built-in surfaces apply this before geometry work.
    fn search_segment_filtered(
        &self,
        cells_bytes: &[u8],
        edges_bytes: &[u8],
        doc_ids_bytes: &[u8],
        max_doc: u32,
        filter: &BitSet,
    ) -> io::Result<BitSet> {
        let mut hits =
            self.search_segment_bytes(cells_bytes, edges_bytes, doc_ids_bytes, max_doc)?;
        for doc in 0..max_doc {
            if !filter.contains(doc) {
                hits.remove(doc);
            }
        }
        Ok(hits)
    }
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
    /// If supplied, `old_to_new` maps each input document ID to its final ID.
    fn serialize(
        self: Box<Self>,
        cells_write: &mut CountingWriter<WritePtr>,
        edges_write: &mut CountingWriter<WritePtr>,
        old_to_new: Option<&[DocId]>,
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
        doc_ids_write: &mut CountingWriter<WritePtr>,
        doc_id_inverse: &[Vec<Option<DocId>>],
    ) -> io::Result<MergeResult> {
        let mut cell_readers: Vec<CellIndexReader> = Vec::new();
        let mut edge_readers: Vec<EdgeReader<'_, S>> = Vec::new();
        let mut geometry_counts: Vec<u32> = Vec::new();
        let mut segment_names: Vec<String> = Vec::new();

        for source in sources {
            let cr = CellIndexReader::open(source.cells_bytes)?;
            let er = EdgeReader::<S>::open(source.edges_bytes)?;
            geometry_counts.push(er.geometry_count());
            cell_readers.push(cr);
            edge_readers.push(er);
            segment_names.push(source.segment_name.clone());
        }

        let edge_resolver = EdgeResolver::new(edge_readers);
        let iters = cell_readers.iter().map(|cr| cr.iter()).collect();
        let mut interleave = Interleaver::new(iters, &edge_resolver, alive_bitsets, segment_names);

        const NOT_ASSIGNED: u32 = u32::MAX;
        let mut old_to_new: Vec<Vec<u32>> = geometry_counts
            .iter()
            .map(|&count| vec![NOT_ASSIGNED; count as usize])
            .collect();
        let mut next_new_id: u32 = 0;

        let mut edge_writer = EdgeWriter::<S>::new(edges_write);
        let cells_base = cells_write.written_bytes();
        let doc_ids_base = doc_ids_write.written_bytes();
        let mut offsets: Vec<(u64, u64, u64)> = Vec::new();
        let mut cell_count: u64 = 0;

        while let Some(cell) = interleave.next() {
            cell_count += 1;
            for shape in &cell.shapes {
                let segment = shape.geometry_id.0 as usize;
                let position = shape.geometry_id.1 as usize;
                if old_to_new[segment][position] != NOT_ASSIGNED {
                    continue;
                }
                let entry = edge_resolver.locate(shape.geometry_id);
                let set_size = entry.member_count;
                for i in 0..set_size {
                    let pos = (entry.head + i) as usize;
                    if old_to_new[segment][pos] == NOT_ASSIGNED {
                        old_to_new[segment][pos] = next_new_id;
                        next_new_id += 1;
                    }
                }
                let old_doc_id = entry.doc_id;
                let new_doc_id = doc_id_inverse[segment][old_doc_id as usize].unwrap();
                let (_, mut set) = edge_resolver.read_geometry_set(shape.geometry_id);
                set.doc_id = new_doc_id as u32;
                edge_writer.insert(&set);
            }

            let offset = cells_write.written_bytes() - cells_base;
            let doc_id_offset = doc_ids_write.written_bytes() - doc_ids_base;
            cells_write
                .write_all(&cell.cell_id.0.to_le_bytes())
                .unwrap();
            cells_write
                .write_all(&(cell.shapes.len() as u32).to_le_bytes())
                .unwrap();
            for shape in &cell.shapes {
                let segment = shape.geometry_id.0 as usize;
                let position = shape.geometry_id.1 as usize;
                let new_id = old_to_new[segment][position];
                assert_ne!(new_id, NOT_ASSIGNED);
                cells_write.write_all(&new_id.to_le_bytes()).unwrap();
                cells_write
                    .write_all(&[shape.contains_center as u8])
                    .unwrap();
                cells_write
                    .write_all(&(shape.edge_indices.len() as u32).to_le_bytes())
                    .unwrap();
                for &edge_id in &shape.edge_indices {
                    cells_write.write_all(&edge_id.to_le_bytes()).unwrap();
                }

                let old_doc_id = edge_resolver.doc_id_for(shape.geometry_id);
                let new_doc_id = doc_id_inverse[segment][old_doc_id as usize].unwrap();
                doc_ids_write
                    .write_all(&(new_doc_id as u32).to_le_bytes())
                    .unwrap();
            }
            offsets.push((cell.cell_id.0, offset, doc_id_offset));
        }

        assert_eq!(next_new_id, edge_writer.geometry_count(),);
        edge_writer.finish();

        // Cell index dictionary and footer.
        let dir_offset = cells_write.written_bytes() - cells_base;
        for &(cell_id, offset, doc_id_offset) in &offsets {
            cells_write.write_all(&cell_id.to_le_bytes()).unwrap();
            cells_write.write_all(&offset.to_le_bytes()).unwrap();
            cells_write.write_all(&doc_id_offset.to_le_bytes()).unwrap();
        }
        write_cell_footer(cells_write, cell_count as u32, dir_offset)?;
        cells_write.flush()?;
        edges_write.flush()?;
        doc_ids_write.flush()?;

        Ok(MergeResult {
            old_to_new,
            geometry_count: next_new_id,
            cell_count,
        })
    }

    fn prepare_intersects(&self, geometry: &Geometry<Plane>) -> Box<dyn PreparedSpatialQuery> {
        let set = to_geometry_set(&geometry.project::<S>(), 0);
        Box::new(PreparedQuery::Intersects(Intersects::new(
            set,
            CovererOptions::default(),
        )))
    }

    fn prepare_contains(&self, geometry: &Geometry<Plane>) -> Box<dyn PreparedSpatialQuery> {
        let set = to_geometry_set(&geometry.project::<S>(), 0);
        Box::new(PreparedQuery::Contains(Contains::new(
            set,
            CovererOptions::default(),
        )))
    }

    fn prepare_covers(
        &self,
        geometry: &Geometry<Plane>,
    ) -> crate::Result<Box<dyn PreparedSpatialQuery>> {
        if S::DIMENSIONS != 2 {
            return Err(crate::TantivyError::InvalidArgument(
                "Covers is not supported on this surface".into(),
            ));
        }
        let set = to_geometry_set(&geometry.project::<S>(), 0);
        let query = PlaneQuery::prepare(&set).unwrap();
        Ok(Box::new(PreparedQuery::<S>::Plane(
            query,
            Predicate::Covers,
            false,
        )))
    }

    fn prepare_within(&self, geometry: &Geometry<Plane>) -> Box<dyn PreparedSpatialQuery> {
        let set = to_geometry_set(&geometry.project::<S>(), 0);
        Box::new(PreparedQuery::Within(Within::new(
            set,
            CovererOptions::default(),
        )))
    }

    fn prepare_distance(
        &self,
        geometry: &Geometry<Plane>,
        max_distance: f64,
    ) -> crate::Result<Box<dyn PreparedSpatialQuery>> {
        if S::DIMENSIONS == 2 && max_distance > 0.0 {
            return Err(crate::TantivyError::InvalidArgument(
                "positive planar distance is not supported".into(),
            ));
        }
        let set = to_geometry_set(&geometry.project::<S>(), 0);
        Ok(Box::new(PreparedQuery::Distance(Distance::new(
            set,
            max_distance,
            CovererOptions::default(),
        ))))
    }

    fn value_heads(&self, edges: &[u8], doc_id: u32, heads: &mut Vec<u32>) -> crate::Result<()> {
        heads.extend(EdgeReader::<S>::open(edges)?.value_heads(doc_id)?);
        Ok(())
    }

    fn prepare_join_query(
        &self,
        edges: &[u8],
        head: u32,
        relation: &SpatialRelation,
    ) -> crate::Result<Box<dyn PreparedSpatialQuery>> {
        self.validate_join(relation)?;
        let (_, set) = EdgeReader::<S>::open(edges)?.read_geometry_set(head);
        let options = CovererOptions::default();
        let query = match relation {
            SpatialRelation::Intersects if S::DIMENSIONS == 2 => PreparedQuery::Plane(
                PlaneQuery::prepare(&set).unwrap(),
                Predicate::Intersects,
                true,
            ),
            SpatialRelation::Intersects => PreparedQuery::Intersects(Intersects::new(set, options)),
            SpatialRelation::Near(radius) => {
                if S::DIMENSIONS == 2 && *radius > 0.0 {
                    return Err(crate::TantivyError::InvalidArgument(
                        "positive planar distance is not implemented".into(),
                    ));
                }
                PreparedQuery::Distance(Distance::any_within(set, *radius, options))
            }
            SpatialRelation::Contains | SpatialRelation::Covers | SpatialRelation::CoveredBy => {
                let query = PlaneQuery::prepare(&set).ok_or_else(|| {
                    crate::TantivyError::InvalidArgument(
                        "Coverage joins are not supported on this surface".into(),
                    )
                })?;
                // The probe evaluates (inner, outer), so reverse the outer-first relation.
                let predicate = match relation {
                    SpatialRelation::Contains => Predicate::Within,
                    SpatialRelation::Covers => Predicate::CoveredBy,
                    SpatialRelation::CoveredBy => Predicate::Covers,
                    _ => unreachable!(),
                };
                PreparedQuery::Plane(query, predicate, true)
            }
            SpatialRelation::Between(_, _) => {
                return Err(crate::TantivyError::InvalidArgument(
                    "Between joins are not supported".into(),
                ))
            }
        };
        Ok(Box::new(query))
    }

    fn validate_join(&self, relation: &SpatialRelation) -> crate::Result<()> {
        let supported = match relation {
            SpatialRelation::Intersects => true,
            SpatialRelation::Near(radius) => S::DIMENSIONS == 3 || !(*radius > 0.0),
            SpatialRelation::Contains | SpatialRelation::Covers | SpatialRelation::CoveredBy => {
                S::DIMENSIONS == 2
            }
            SpatialRelation::Between(_, _) => false,
        };
        if supported {
            Ok(())
        } else {
            Err(crate::TantivyError::InvalidArgument(
                "Spatial join relation is not supported on this surface".into(),
            ))
        }
    }
}

enum PreparedQuery<S: Surface> {
    Intersects(Intersects<S>),
    Contains(Contains<S>),
    Within(Within<S>),
    Distance(Distance<S>),
    Plane(PlaneQuery, Predicate, bool),
}

impl<S: Surface> PreparedQuery<S> {
    fn search(
        &self,
        cells: &[u8],
        edges: &[u8],
        doc_ids: &[u8],
        max_doc: u32,
        filter: Option<&BitSet>,
    ) -> io::Result<BitSet> {
        let reader = CellIndexReader::open_with_doc_ids(cells, doc_ids)?;
        let mut edge_resolver = EdgeResolver::new(vec![EdgeReader::<S>::open(edges)?]);
        Ok(match self {
            Self::Intersects(query) => query.search(&reader, filter, &mut edge_resolver, max_doc),
            Self::Contains(query) => query.search(&reader, filter, &mut edge_resolver, max_doc),
            Self::Within(query) => query.search(&reader, filter, &mut edge_resolver, max_doc),
            Self::Distance(query) => query.search(&reader, filter, &mut edge_resolver, max_doc),
            Self::Plane(query, predicate, first_only) => query.search(
                &reader,
                filter,
                &edge_resolver,
                max_doc,
                *predicate,
                *first_only,
            ),
        })
    }
}

impl<S: Surface + 'static> PreparedSpatialQuery for PreparedQuery<S> {
    fn search_segment_bytes(
        &self,
        cells: &[u8],
        edges: &[u8],
        doc_ids: &[u8],
        max_doc: u32,
    ) -> io::Result<BitSet> {
        self.search(cells, edges, doc_ids, max_doc, None)
    }

    fn search_segment_filtered(
        &self,
        cells: &[u8],
        edges: &[u8],
        doc_ids: &[u8],
        max_doc: u32,
        filter: &BitSet,
    ) -> io::Result<BitSet> {
        self.search(cells, edges, doc_ids, max_doc, Some(filter))
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
        old_to_new: Option<&[DocId]>,
    ) -> io::Result<()> {
        let mut sets = self.sets;
        if let Some(old_to_new) = old_to_new {
            for set in &mut sets {
                set.doc_id = old_to_new[set.doc_id as usize];
            }
        }
        if sets.is_empty() {
            return Ok(());
        }

        let builder = Clipper::new(ClipOptions::default());
        let cell_index = builder.build(&sets);

        cell_index.write(cells_write);
        cells_write.flush()?;

        let mut edge_writer = EdgeWriter::<S>::new(edges_write);
        for set in &sets {
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

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
use std::sync::{Arc, RwLock};

use common::{BitSet, CountingWriter, ReadOnlyBitSet};

use crate::directory::WritePtr;
use crate::spatial::cell_index_reader::CellIndexReader;
use crate::spatial::contains::Contains;
use crate::spatial::distance::Distance;
use crate::spatial::edge_reader::EdgeReader;
use crate::spatial::edge_resolver::EdgeResolver;
use crate::spatial::geometry::{to_geometry_set, Geometry};
use crate::spatial::intersects::Intersects;
use crate::spatial::merge_field::{merge_field, MergeSource};
use crate::spatial::plane::query::PlaneQuery;
use crate::spatial::plane::{Plane, Predicate};
use crate::spatial::region_coverer::CovererOptions;
use crate::spatial::sphere::Sphere;
use crate::spatial::surface::Surface;
use crate::spatial::within::Within;
use crate::DocId;

/// Factory for per-field spatial writers, mergers, and queries. Registered in
/// `SpatialIndexManager` by name.
pub trait SpatialIndex: Send + Sync + SpatialIndexClone {
    /// Merge source segments into a new segment.
    fn merge_field<'a>(
        &self,
        sources: &[MergeSource<'a>],
        alive_bitsets: &'a [Option<ReadOnlyBitSet>],
        cells_write: &mut CountingWriter<WritePtr>,
        edges_write: &mut CountingWriter<WritePtr>,
        doc_ids_write: &mut CountingWriter<WritePtr>,
        doc_id_inverse: &[Vec<Option<DocId>>],
    ) -> io::Result<()>;

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
    fn merge_field<'a>(
        &self,
        sources: &[MergeSource<'a>],
        alive_bitsets: &'a [Option<ReadOnlyBitSet>],
        cells_write: &mut CountingWriter<WritePtr>,
        edges_write: &mut CountingWriter<WritePtr>,
        doc_ids_write: &mut CountingWriter<WritePtr>,
        doc_id_inverse: &[Vec<Option<DocId>>],
    ) -> io::Result<()> {
        merge_field::<S, _, _, _>(
            sources,
            alive_bitsets,
            cells_write,
            edges_write,
            doc_ids_write,
            doc_id_inverse,
        )
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

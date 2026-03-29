//! ShapeIndexRegion: Query methods for shape-cell relationships.
//!
//! This module provides methods to determine whether shapes in a CellIndex intersect or contain a
//! given S2Cell target. Used to implement the Region trait over a CellIndex, enabling
//! RegionCoverer to produce coverings.
//!
//! Corresponds to S2ShapeIndexRegion in the C++ implementation.
//! From s2shape_index_region.h
use std::collections::HashMap;

use super::cell_index::{CellIndex, ClippedShape};
use super::cell_index_reader::CellIndexReader;
use super::cell_union::CellUnion;
use super::crossings::S2EdgeCrosser;
use super::edge_cache::EdgeCache;
use super::latlng_rect::S2LatLngRect;
use super::region::Region;
use super::s2cap::S2Cap;
use super::s2cell::S2Cell;
use super::s2cell_id::S2CellId;
use super::s2edge_clipping::{
    clip_to_padded_face, intersects_rect, FACE_CLIP_ERROR_UV_COORD, INTERSECTS_RECT_ERROR_UV_DIST,
};
use super::sphere::Sphere;

/// Provides vertex access for edge intersection tests.
///
/// Implementors resolve a (geometry_id, edge_index) pair to the two endpoints of that edge. For
/// ingest-side indexes backed by EdgeCache, this reads from disk. For query-local indexes, this
/// indexes into an in-memory vertex array.
pub trait EdgeProvider {
    /// Returns the two endpoints of the given edge.
    fn get_edge(&self, geometry_id: u32, edge_idx: u16) -> ([f64; 3], [f64; 3]);
}

/// Minimal clipped shape data for indexed containment. Owned to avoid lifetime mismatches between
/// borrowed in-memory data and deserialized reader data.
pub struct ClippedInfo {
    /// The cell containing the test point.
    pub cell_id: S2CellId,
    /// Whether the geometry contains the cell center.
    pub contains_center: bool,
    /// Edge indices clipped to this cell.
    pub edge_indices: Vec<u16>,
}

/// Abstracts cell lookup and edge resolution for indexed point-in-polygon. Takes &mut self because
/// the segment-side EdgeCache mutates its LRU cache.
pub trait ContainmentIndex {
    /// Finds the cell containing the test point and returns the clipped shape data
    /// for the given geometry, or None if the geometry is not present in that cell.
    fn find_clipped(&mut self, geometry_id: u32, point: &[f64; 3]) -> Option<ClippedInfo>;
    /// Resolves an edge index to its two endpoint vertices.
    fn resolve_edge(&mut self, geometry_id: u32, edge_idx: u16) -> ([f64; 3], [f64; 3]);
}

/// Indexed point-in-polygon through either backend. Finds the cell containing the test point,
/// starts from contains_center, counts crossings of only the edges clipped to that cell.
pub fn contains_point<T: ContainmentIndex>(
    index: &mut T,
    geometry_id: u32,
    point: &[f64; 3],
) -> bool {
    let info = match index.find_clipped(geometry_id, point) {
        Some(c) => c,
        None => return false,
    };
    let mut inside = info.contains_center;
    if !info.edge_indices.is_empty() {
        let center = info.cell_id.to_point();
        let mut crosser = S2EdgeCrosser::new(&center, point);
        for &edge_idx in &info.edge_indices {
            let (v0, v1) = index.resolve_edge(geometry_id, edge_idx);
            inside ^= crosser.edge_or_vertex_crossing_two(&v0, &v1);
        }
    }
    inside
}

/// In-memory containment index wrapping a CellIndex and an EdgeProvider.
pub struct InMemoryIndex<'a, E: EdgeProvider> {
    /// The in-memory cell index.
    pub index: &'a CellIndex,
    /// The edge provider for vertex resolution.
    pub edges: &'a E,
}

impl<E: EdgeProvider> ContainmentIndex for InMemoryIndex<'_, E> {
    fn find_clipped(&mut self, geometry_id: u32, point: &[f64; 3]) -> Option<ClippedInfo> {
        let cell_id = S2CellId::from_point(point);
        let index_cell = self.index.find_cell(cell_id)?;
        let clipped = index_cell.find_shape(geometry_id)?;
        Some(ClippedInfo {
            cell_id: index_cell.cell_id,
            contains_center: clipped.contains_center,
            edge_indices: clipped.edge_indices.clone(),
        })
    }

    fn resolve_edge(&mut self, geometry_id: u32, edge_idx: u16) -> ([f64; 3], [f64; 3]) {
        self.edges.get_edge(geometry_id, edge_idx)
    }
}

/// Segment containment index wrapping a CellIndexReader and an EdgeCache.
pub struct SegmentIndex<'a, 'b> {
    /// The serialized cell index reader.
    pub cell_reader: &'a CellIndexReader<'a>,
    /// The edge cache for vertex resolution.
    pub edge_cache: &'b mut EdgeCache<'a, Sphere>,
}

impl ContainmentIndex for SegmentIndex<'_, '_> {
    fn find_clipped(&mut self, geometry_id: u32, point: &[f64; 3]) -> Option<ClippedInfo> {
        let cell_id = S2CellId::from_point(point);
        let index_cell = self.cell_reader.find(cell_id)?;
        let clipped = index_cell
            .shapes
            .iter()
            .find(|s| s.geometry_id == geometry_id)?;
        Some(ClippedInfo {
            cell_id: index_cell.cell_id,
            contains_center: clipped.contains_center,
            edge_indices: clipped.edge_indices.clone(),
        })
    }

    fn resolve_edge(&mut self, geometry_id: u32, edge_idx: u16) -> ([f64; 3], [f64; 3]) {
        let (_, edge_set) = self.edge_cache.get_edge_set(0, geometry_id);
        let i = edge_idx as usize;
        (edge_set.vertices[i], edge_set.vertices[i + 1])
    }
}

/// Maximum error in edge-cell intersection tests.
///
/// This combines the error from face clipping and rectangle intersection to ensure we don't miss
/// any edges that might intersect the cell boundary.
pub const ANY_EDGE_INTERSECTS_MAX_ERROR: f64 =
    FACE_CLIP_ERROR_UV_COORD + INTERSECTS_RECT_ERROR_UV_DIST;

/// Returns true if any edge of the clipped shape intersects the target cell.
///
/// The test uses UV-space bounds expanded by the maximum clipping error to avoid missing edges
/// that nearly touch the cell boundary.
///
/// This method is conservative: it may return true for edges that are very close to (but don't
/// actually intersect) the cell boundary. The maximum error is less than 10 * DBL_EPSILON radians
/// (about 15 nanometers).
pub fn any_edge_intersects<E: EdgeProvider>(
    clipped: &ClippedShape,
    target: &S2Cell,
    edges: &E,
) -> bool {
    let bound = target
        .get_bound_uv()
        .expanded(ANY_EDGE_INTERSECTS_MAX_ERROR);
    let face = target.face();

    for &edge_idx in &clipped.edge_indices {
        let (v0, v1) = edges.get_edge(clipped.geometry_id, edge_idx);

        if let Some((p0, p1)) = clip_to_padded_face(&v0, &v1, face, ANY_EDGE_INTERSECTS_MAX_ERROR) {
            if intersects_rect(p0, p1, &bound) {
                return true;
            }
        }
    }

    false
}

/// Returns true if the given geometry in the index contains the point. Uses the cell index to
/// narrow the edge set — locates the cell containing the point, starts from contains_center, and
/// counts crossings of only the edges in that cell.
///
/// Port of S2ContainsPointQuery::ShapeContains.
pub fn index_contains_point<E: EdgeProvider>(
    index: &CellIndex,
    edges: &E,
    geometry_id: u32,
    point: &[f64; 3],
) -> bool {
    let cell_id = S2CellId::from_point(point);
    let index_cell = match index.find_cell(cell_id) {
        Some(cell) => cell,
        None => return false,
    };
    let clipped = match index_cell.find_shape(geometry_id) {
        Some(s) => s,
        None => return false,
    };
    clipped_shape_contains_point(index_cell.cell_id, clipped, edges, point)
}

/// Returns true if the given clipped shape contains the point. The cell_id identifies the index
/// cell the clipped shape belongs to — its center is the reference point for the edge crossing
/// test.
fn clipped_shape_contains_point<E: EdgeProvider>(
    cell_id: S2CellId,
    clipped: &ClippedShape,
    edges: &E,
    point: &[f64; 3],
) -> bool {
    let mut inside = clipped.contains_center;
    if !clipped.edge_indices.is_empty() {
        let cell_center = cell_id.to_point();
        let mut crosser = S2EdgeCrosser::new(&cell_center, point);
        for &edge_idx in &clipped.edge_indices {
            let (v0, v1) = edges.get_edge(clipped.geometry_id, edge_idx);
            inside ^= crosser.edge_or_vertex_crossing_two(&v0, &v1);
        }
    }
    inside
}

/// Locates a target cell ID relative to the index.
///
/// Returns the relationship and, for Indexed, the position of the containing cell.
enum CellRelation {
    /// Target does not overlap any index cell.
    Disjoint,
    /// Target contains one or more index cells. The usize is the first overlapping cell's position
    /// in `CellIndex.cells`.
    Subdivided(usize),
    /// Target is within (or equals) an index cell. The usize is that cell's position in
    /// `CellIndex.cells`.
    Indexed(usize),
}

/// Locates a target cell relative to the CellIndex.
///
/// Binary search on the sorted cells array. The three cases mirror S2's
/// ShapeIndexIterator::Locate.
fn locate(index: &CellIndex, target: S2CellId) -> CellRelation {
    let cells = &index.cells;
    if cells.is_empty() {
        return CellRelation::Disjoint;
    }

    // Binary search: find the first cell whose range_max >= target.range_min().
    let pos = cells.partition_point(|c| c.cell_id.range_max() < target.range_min());

    if pos >= cells.len() {
        return CellRelation::Disjoint;
    }

    // If this cell contains the target, or equals it, it's Indexed.
    if cells[pos].cell_id.contains(target) || cells[pos].cell_id == target {
        return CellRelation::Indexed(pos);
    }

    // If the target contains this cell, it's Subdivided.
    if target.contains(cells[pos].cell_id) {
        return CellRelation::Subdivided(pos);
    }

    // The target's range starts after all index cells, or doesn't overlap.
    if cells[pos].cell_id.range_min() > target.range_max() {
        return CellRelation::Disjoint;
    }

    CellRelation::Disjoint
}

/// Visits all shapes that intersect an S2Cell target.  For each shape, calls the visitor with the
/// geometry_id and whether the shape fully contains the target cell.  The visitor returns true to
/// continue or false to terminate early.
pub fn visit_intersecting_shapes<E, F>(
    index: &CellIndex,
    edges: &E,
    target: &S2Cell,
    mut visitor: F,
) -> bool
where
    E: EdgeProvider,
    F: FnMut(u32, bool) -> bool, // (geometry_id, contains_target) -> continue
{
    match locate(index, target.id()) {
        CellRelation::Disjoint => true,

        CellRelation::Subdivided(start) => {
            // Target contains one or more index cells.
            // A shape contains the target iff:
            // - It appears in at least one cell
            // - It has contains_center=true in ALL cells
            // - It has NO edges in ANY cell
            let mut shape_not_contains: HashMap<u32, bool> = HashMap::new();
            let max = target.id().range_max();

            let mut pos = start;
            while pos < index.cells.len() && index.cells[pos].cell_id.range_min() <= max {
                let cell = &index.cells[pos];
                for clipped in &cell.shapes {
                    let not_contains = shape_not_contains
                        .entry(clipped.geometry_id)
                        .or_insert(false);

                    // Mark as not-containing if:
                    // - Has any edges (would intersect cell boundary), OR
                    // - Does not contain center (discontinuous interior)
                    *not_contains |= !clipped.edge_indices.is_empty() || !clipped.contains_center;
                }
                pos += 1;
            }

            // Visit all shapes that appeared in any cell.
            for (geometry_id, not_contains) in shape_not_contains {
                let contains = !not_contains;
                if !visitor(geometry_id, contains) {
                    return false;
                }
            }
            true
        }

        CellRelation::Indexed(pos) => {
            // Target is contained by (or equals) an index cell.
            let cell = &index.cells[pos];
            let index_cell_id = cell.cell_id;
            let target_center = target.get_center();

            for clipped in &cell.shapes {
                let contains = if index_cell_id == target.id() {
                    // Exact match: shape contains target iff no edges AND contains_center
                    clipped.edge_indices.is_empty() && clipped.contains_center
                } else {
                    // Target is smaller than index cell — need finer testing
                    if any_edge_intersects(clipped, target, edges) {
                        // Edge intersects target: shape intersects but doesn't contain
                        false
                    } else {
                        // No edges intersect: check if shape contains target's center by
                        // ray-casting from the index cell's center (where we know contains_center)
                        // to the target center, counting crossings of only the edges in this cell.
                        if !clipped_shape_contains_point(
                            index_cell_id,
                            clipped,
                            edges,
                            &target_center,
                        ) {
                            continue;
                        }
                        true
                    }
                };

                if !visitor(clipped.geometry_id, contains) {
                    return false;
                }
            }
            true
        }
    }
}

/// Convenience method: Returns all geometry IDs that intersect the target.
pub fn get_intersecting_shapes<E: EdgeProvider>(
    index: &CellIndex,
    edges: &E,
    target: &S2Cell,
) -> Vec<(u32, bool)> {
    let mut results = Vec::new();
    visit_intersecting_shapes(index, edges, target, |geo_id, contains| {
        results.push((geo_id, contains));
        true
    });
    results
}

/// Returns true if any shape in the index intersects the target cell.
///
/// This is equivalent to S2ShapeIndexRegion::MayIntersect.
pub fn may_intersect<E: EdgeProvider>(index: &CellIndex, edges: &E, target: &S2Cell) -> bool {
    let mut intersects = false;
    visit_intersecting_shapes(index, edges, target, |_geo_id, _contains| {
        intersects = true;
        false // Stop after first match
    });
    intersects
}

/// Returns true if any shape in the index contains the target cell.
///
/// This is equivalent to S2ShapeIndexRegion::Contains(S2Cell).
pub fn any_shape_contains<E: EdgeProvider>(index: &CellIndex, edges: &E, target: &S2Cell) -> bool {
    let mut contains = false;
    visit_intersecting_shapes(index, edges, target, |_geo_id, shape_contains| {
        if shape_contains {
            contains = true;
            return false; // Stop after first containing shape
        }
        true // Continue looking
    });
    contains
}

/// Region adapter over a CellIndex.
///
/// Wraps a CellIndex and an EdgeProvider to implement the Region trait, enabling RegionCoverer to
/// produce coverings of the indexed geometry. For the contains query, this wraps the query-local
/// CellIndex built from the query polygon.
pub struct CellIndexRegion<'a, E: EdgeProvider> {
    index: &'a CellIndex,
    edges: &'a E,
}

impl<'a, E: EdgeProvider> CellIndexRegion<'a, E> {
    /// Creates a region backed by a cell index and an edge provider.
    pub fn new(index: &'a CellIndex, edges: &'a E) -> Self {
        Self { index, edges }
    }
}

impl<'a, E: EdgeProvider> Region for CellIndexRegion<'a, E> {
    fn get_cap_bound(&self) -> S2Cap {
        let cell_ids: Vec<S2CellId> = self.index.cells.iter().map(|c| c.cell_id).collect();
        let cu = CellUnion::from_cell_ids(cell_ids);
        cu.get_cap_bound()
    }

    fn get_rect_bound(&self) -> S2LatLngRect {
        let cell_ids: Vec<S2CellId> = self.index.cells.iter().map(|c| c.cell_id).collect();
        let cu = CellUnion::from_cell_ids(cell_ids);
        cu.get_rect_bound()
    }

    fn contains_cell(&self, cell: &S2Cell) -> bool {
        any_shape_contains(self.index, self.edges, cell)
    }

    fn may_intersect(&self, cell: &S2Cell) -> bool {
        may_intersect(self.index, self.edges, cell)
    }

    fn contains_point(&self, p: &[f64; 3]) -> bool {
        index_contains_point(self.index, self.edges, 0, p)
    }
}

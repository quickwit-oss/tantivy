//! Branch-and-bound closest edge query over the cell index.
//!
//! Finds geometries closest to a query geometry by traversing the S2 cell hierarchy in distance
//! order. The query geometry has its own CellIndex for bilateral pruning -- a stored cell is only
//! checked against the query cells whose bounding caps could be closer than the best distance so
//! far, not against every query edge.
//!
//! Interiors count, as with S2ClosestEdgeQuery's include_interiors: a polygon that contains part
//! of the other geometry is at distance zero, however far away its edges are.
//!
//! Three modes from one algorithm:
//!   kNN          -- dynamic threshold, tightens to Kth-best
//!   within-D     -- fixed threshold, collect all within D
//!   boolean      -- fixed threshold, exit on first witness

use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet};

use common::BitSet;

use super::cell_index::{
    BuildOptions, CellIndex, CellRelation, GeometryId, IndexBuilder, IndexCell,
};
use super::cell_index_reader::CellIndexReader;
use super::contains_query::QueryEdgeProvider;
use super::edge_cache::EdgeCache;
use super::geometry_set::GeometrySet;
use super::s1chord_angle::S1ChordAngle;
use super::s2cell::S2Cell;
use super::s2cell_id::S2CellId;
use super::s2edge_distances::{update_edge_pair_min_distance, update_min_distance};
use super::shape_index_region::{contains_point, index_contains_point, SegmentIndex};
use super::sphere::Sphere;

/// A result from the closest edge query: document ID and distance.
#[derive(Clone, Debug)]
pub struct ClosestEdgeResult {
    /// The document containing the closest geometry.
    pub doc_id: u32,
    /// The minimum distance from the query geometry to this document's geometry.
    pub distance: S1ChordAngle,
}

/// Prepared closest-edge query. The query geometry is smashed into a GeometrySet and spatially
/// indexed. The same CellIndex that contains and intersects build for the query polygon.
pub struct ClosestEdgeQuery {
    query_index: CellIndex,
    query_edges: QueryEdgeProvider,
    /// Bounding cap of each query index cell, parallel to `query_index.cells`.
    query_bounds: Vec<CellBound>,
    max_results: usize,
    max_distance: S1ChordAngle,
    min_distance: S1ChordAngle,
    first_only: bool,
}

impl ClosestEdgeQuery {
    /// Build a kNN query from a smashed query geometry.
    pub fn knn(set: GeometrySet, k: usize) -> Self {
        Self::build(set, k, S1ChordAngle::infinity(), false)
    }

    /// Build a within-D query from a smashed query geometry.
    pub fn within(set: GeometrySet, max_distance: S1ChordAngle) -> Self {
        Self::build(set, usize::MAX, max_distance, false)
    }

    /// Build a boolean distance query. Returns at most one result.
    pub fn any_within(set: GeometrySet, max_distance: S1ChordAngle) -> Self {
        Self::build(set, 1, max_distance, true)
    }

    /// Build a range distance query: results between inner and outer distance.
    ///
    /// There is no boolean form that stops at the first witness. A geometry's distance is final
    /// only once every closer cell has been visited, and the first geometry found may turn out
    /// to lie inside the inner distance while another lies in range.
    pub fn between(
        set: GeometrySet,
        min_distance: S1ChordAngle,
        max_distance: S1ChordAngle,
    ) -> Self {
        let mut q = Self::build(set, usize::MAX, max_distance, false);
        q.min_distance = min_distance;
        q
    }

    fn build(
        set: GeometrySet,
        max_results: usize,
        max_distance: S1ChordAngle,
        first_only: bool,
    ) -> Self {
        let builder = IndexBuilder::new(BuildOptions::default());
        let query_index = builder.build_from_sets(std::slice::from_ref(&set));
        let query_bounds = query_index
            .cells
            .iter()
            .map(|cell| CellBound::new(&S2Cell::new(cell.cell_id)))
            .collect();
        let query_edges = QueryEdgeProvider { set };
        Self {
            query_index,
            query_edges,
            query_bounds,
            max_results,
            max_distance,
            min_distance: S1ChordAngle::zero(),
            first_only,
        }
    }

    /// Search one segment.
    pub fn search_segment<'a>(
        &self,
        cell_reader: &'a CellIndexReader<'a>,
        edge_cache: &mut EdgeCache<'a, Sphere>,
    ) -> Vec<ClosestEdgeResult> {
        self.search_impl(cell_reader, edge_cache, None)
    }

    /// Search one segment, skipping shapes whose doc_id is not in the filter bitset.
    pub fn search_segment_filtered<'a>(
        &self,
        cell_reader: &'a CellIndexReader<'a>,
        edge_cache: &mut EdgeCache<'a, Sphere>,
        terms_filter: &BitSet,
    ) -> Vec<ClosestEdgeResult> {
        self.search_impl(cell_reader, edge_cache, Some(terms_filter))
    }

    fn search_impl<'a>(
        &self,
        cell_reader: &'a CellIndexReader<'a>,
        edge_cache: &mut EdgeCache<'a, Sphere>,
        terms_filter: Option<&BitSet>,
    ) -> Vec<ClosestEdgeResult> {
        if cell_reader.cell_count() == 0 || self.max_results == 0 {
            return Vec::new();
        }

        let mut best: HashMap<u32, S1ChordAngle> = HashMap::new();
        let mut distance_limit = self.max_distance;

        self.add_containing_polygons(
            cell_reader,
            edge_cache,
            terms_filter,
            &mut best,
            &mut distance_limit,
        );
        if self.first_only && !best.is_empty() {
            return self.collect_results(&best);
        }

        // Stored geometries already tested for lying inside the query geometry.
        let mut tested_interiors: HashSet<GeometryId> = HashSet::new();
        let mut queue: BinaryHeap<QueueEntry> = BinaryHeap::new();

        // Seed with the six face-level cells.
        for face in 0..6 {
            self.enqueue(
                &S2Cell::from_face(face),
                cell_reader,
                distance_limit,
                &mut queue,
            );
        }

        while let Some(entry) = queue.pop() {
            if entry.distance >= distance_limit {
                break;
            }

            match entry.relation {
                // The queue cell is the index cell: any coarser index cell containing it would
                // have been processed in its place.
                CellRelation::Indexed(position) => {
                    self.process_edges(
                        &cell_reader.cell(position),
                        edge_cache,
                        terms_filter,
                        &mut tested_interiors,
                        &mut best,
                        &mut distance_limit,
                    );
                    if self.first_only && !best.is_empty() {
                        return self.collect_results(&best);
                    }
                }
                CellRelation::Subdivided(_) => {
                    let cell = S2Cell::new(entry.cell_id);
                    if let Some(children) = cell.subdivide() {
                        for child in &children {
                            self.enqueue(child, cell_reader, distance_limit, &mut queue);
                        }
                    }
                }
                CellRelation::Disjoint => {}
            }
        }

        self.collect_results(&best)
    }

    /// Queues a cell that overlaps the index and may be closer to the query than the limit.
    fn enqueue(
        &self,
        cell: &S2Cell,
        cell_reader: &CellIndexReader,
        distance_limit: S1ChordAngle,
        queue: &mut BinaryHeap<QueueEntry>,
    ) {
        let relation = cell_reader.locate(cell.id());
        if relation == CellRelation::Disjoint {
            return;
        }
        let distance = self.cell_to_query_distance(cell);
        if distance < distance_limit {
            queue.push(QueueEntry {
                distance,
                cell_id: cell.id(),
                relation,
            });
        }
    }

    /// Stored polygons that contain part of the query geometry are at distance zero. Testing
    /// one vertex of each query member suffices: a polygon that contains part of a member but
    /// not that vertex has an edge that crosses the member, which the edge distances find.
    fn add_containing_polygons<'a>(
        &self,
        cell_reader: &'a CellIndexReader<'a>,
        edge_cache: &mut EdgeCache<'a, Sphere>,
        terms_filter: Option<&BitSet>,
        best: &mut HashMap<u32, S1ChordAngle>,
        distance_limit: &mut S1ChordAngle,
    ) {
        for member in &self.query_edges.set.members {
            let Some(point) = member.vertices.first() else {
                continue;
            };
            let Some(index_cell) = cell_reader.find(S2CellId::from_point(point)) else {
                continue;
            };
            for clipped in &index_cell.shapes {
                if let Some(filter) = terms_filter {
                    if !filter.contains(edge_cache.doc_id_for(clipped.geometry_id)) {
                        continue;
                    }
                }
                let entry = edge_cache.get(clipped.geometry_id);
                if !entry.edge_set().closed {
                    continue;
                }
                let mut segment = SegmentIndex {
                    cell_reader,
                    edge_cache: &mut *edge_cache,
                };
                if contains_point(&mut segment, clipped.geometry_id, point) {
                    self.update_best(entry.doc_id(), S1ChordAngle::zero(), best, distance_limit);
                }
            }
        }
    }

    /// Whether a closed member of the query geometry contains the point.
    fn query_contains(&self, point: &[f64; 3]) -> bool {
        self.query_edges
            .set
            .members
            .iter()
            .enumerate()
            .any(|(member, edge_set)| {
                edge_set.closed
                    && index_contains_point(
                        &self.query_index,
                        &self.query_edges,
                        (0, member as u32),
                        point,
                    )
            })
    }

    /// Minimum distance from a stored cell to the query geometry, its interior included: a
    /// lower bound on the distance of anything stored in the cell.
    fn cell_to_query_distance(&self, cell: &S2Cell) -> S1ChordAngle {
        if self.query_contains(&cell.get_center()) {
            return S1ChordAngle::zero();
        }
        let bound = CellBound::new(cell);
        let mut min_dist = S1ChordAngle::infinity();
        for (query_cell, query_bound) in self.query_index.cells.iter().zip(&self.query_bounds) {
            if bound.distance(query_bound) >= min_dist {
                continue;
            }
            for clipped in &query_cell.shapes {
                let vertices = &self.query_edges.get_edge_set(clipped.geometry_id).vertices;
                for &edge_idx in &clipped.edge_indices {
                    let distance = match edge(vertices, edge_idx) {
                        (point, other) if point == other => cell.get_distance_to_point(point),
                        (v0, v1) => cell.get_distance_to_edge(v0, v1),
                    };
                    min_dist = min_dist.min(distance);
                }
            }
        }
        min_dist
    }

    /// Lowers `min_dist` to the distance between the stored edge (a0, a1), clipped to the cell
    /// bounded by `bound`, and the query edges in query cells that could be closer.
    fn update_min_distance_to_query(
        &self,
        bound: &CellBound,
        a0: &[f64; 3],
        a1: &[f64; 3],
        min_dist: &mut S1ChordAngle,
    ) {
        for (query_cell, query_bound) in self.query_index.cells.iter().zip(&self.query_bounds) {
            if bound.distance(query_bound) >= *min_dist {
                continue;
            }
            for clipped in &query_cell.shapes {
                let vertices = &self.query_edges.get_edge_set(clipped.geometry_id).vertices;
                for &edge_idx in &clipped.edge_indices {
                    let (b0, b1) = edge(vertices, edge_idx);
                    if a0 == a1 {
                        update_min_distance(a0, b0, b1, min_dist);
                    } else if b0 == b1 {
                        update_min_distance(b0, a0, a1, min_dist);
                    } else {
                        update_edge_pair_min_distance(a0, a1, b0, b1, min_dist);
                    }
                }
            }
        }
    }

    fn process_edges(
        &self,
        index_cell: &IndexCell,
        edge_cache: &mut EdgeCache<'_, Sphere>,
        terms_filter: Option<&BitSet>,
        tested_interiors: &mut HashSet<GeometryId>,
        best: &mut HashMap<u32, S1ChordAngle>,
        distance_limit: &mut S1ChordAngle,
    ) {
        let bound = CellBound::new(&S2Cell::new(index_cell.cell_id));

        for clipped in &index_cell.shapes {
            let geometry_id = clipped.geometry_id;

            if let Some(filter) = terms_filter {
                let doc_id = edge_cache.doc_id_for(geometry_id);
                if !filter.contains(doc_id) {
                    continue;
                }
            }

            let cache_entry = edge_cache.get(geometry_id);
            let doc_id = cache_entry.doc_id();
            let candidate_vertices = cache_entry.vertices();
            if candidate_vertices.is_empty() {
                continue;
            }

            let mut min_dist = *distance_limit;
            if let Some(&prev) = best.get(&doc_id) {
                if prev < min_dist {
                    min_dist = prev;
                }
            }

            // A stored geometry inside the query polygon is at distance zero wherever its edges
            // are. If only part of it is inside, its edges cross the query's, which the edge
            // distances find.
            if tested_interiors.insert(geometry_id) && self.query_contains(&candidate_vertices[0]) {
                min_dist = S1ChordAngle::zero();
            } else {
                // A shape with no edges in this cell covers it. Its distance comes from the cells
                // its edges cross, or from add_containing_polygons when it contains the query.
                for &edge_idx in &clipped.edge_indices {
                    let (cv0, cv1) = cache_entry.edge(edge_idx);
                    self.update_min_distance_to_query(&bound, &cv0, &cv1, &mut min_dist);
                }
            }

            if min_dist < *distance_limit {
                self.update_best(doc_id, min_dist, best, distance_limit);
            }
        }
    }

    fn update_best(
        &self,
        doc_id: u32,
        distance: S1ChordAngle,
        best: &mut HashMap<u32, S1ChordAngle>,
        distance_limit: &mut S1ChordAngle,
    ) {
        match best.get(&doc_id) {
            Some(&prev) if distance >= prev => return,
            _ => {}
        }
        best.insert(doc_id, distance);

        if self.max_results < usize::MAX && best.len() >= self.max_results {
            let mut distances: Vec<S1ChordAngle> = best.values().copied().collect();
            distances.sort();
            if distances.len() >= self.max_results {
                *distance_limit = distances[self.max_results - 1];
            }
        }
    }

    fn collect_results(&self, best: &HashMap<u32, S1ChordAngle>) -> Vec<ClosestEdgeResult> {
        let mut out: Vec<ClosestEdgeResult> = best
            .iter()
            .filter(|(_, &distance)| distance >= self.min_distance)
            .map(|(&doc_id, &distance)| ClosestEdgeResult { doc_id, distance })
            .collect();
        out.sort_by_key(|result| result.distance);
        if self.max_results < out.len() {
            out.truncate(self.max_results);
        }
        out
    }
}

/// The endpoints of an edge in a smashed vertex array. A point is an edge from itself to itself.
fn edge(vertices: &[[f64; 3]], edge_idx: u16) -> (&[f64; 3], &[f64; 3]) {
    let i = edge_idx as usize;
    (&vertices[i], vertices.get(i + 1).unwrap_or(&vertices[i]))
}

/// A cell's bounding cap, for a cheap lower bound on the distance between two cells.
struct CellBound {
    center: [f64; 3],
    radius: f64,
}

impl CellBound {
    fn new(cell: &S2Cell) -> Self {
        let cap = cell.get_cap_bound();
        CellBound {
            center: cap.center(),
            radius: cap.get_radius_radians(),
        }
    }

    /// Lower bound on the distance between any point of one cap and any point of the other.
    /// Rounded down by more than the error of the computation, so that it stays a lower bound.
    fn distance(&self, other: &CellBound) -> S1ChordAngle {
        let centers = S1ChordAngle::from_points(&self.center, &other.center).to_radians();
        let gap = centers - self.radius - other.radius - 1e-14;
        if gap > 0.0 {
            S1ChordAngle::from_radians(gap)
        } else {
            S1ChordAngle::zero()
        }
    }
}

struct QueueEntry {
    distance: S1ChordAngle,
    cell_id: S2CellId,
    /// Where the cell lies relative to the index cells.
    relation: CellRelation,
}

impl PartialEq for QueueEntry {
    fn eq(&self, other: &Self) -> bool {
        self.distance == other.distance
    }
}

impl Eq for QueueEntry {}

impl PartialOrd for QueueEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

// Reversed ordering for min-heap (BinaryHeap is a max-heap).
impl Ord for QueueEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        other.distance.cmp(&self.distance)
    }
}

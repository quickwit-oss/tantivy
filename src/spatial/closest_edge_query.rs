//! Branch-and-bound closest edge query over the cell index.
//!
//! Finds geometries closest to a query geometry by traversing the S2 cell hierarchy in distance
//! order. The query geometry has its own CellIndex for bilateral pruning -- only query edges in
//! cells that overlap the stored cell are checked, not all of them.
//!
//! Three modes from one algorithm:
//!   kNN          -- dynamic threshold, tightens to Kth-best
//!   within-D     -- fixed threshold, collect all within D
//!   boolean      -- fixed threshold, exit on first witness

use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};

use common::BitSet;

use super::cell_index::{BuildOptions, CellIndex, IndexBuilder};
use super::cell_index_reader::CellIndexReader;
use super::contains_query::QueryEdgeProvider;
use super::edge_cache::EdgeCache;
use super::geometry_set::GeometrySet;
use super::s1chord_angle::S1ChordAngle;
use super::s2cell::S2Cell;
use super::s2cell_id::S2CellId;
use super::s2edge_distances::{update_edge_pair_min_distance, update_min_distance};
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
    pub fn between(
        set: GeometrySet,
        min_distance: S1ChordAngle,
        max_distance: S1ChordAngle,
    ) -> Self {
        let mut q = Self::build(set, usize::MAX, max_distance, false);
        q.min_distance = min_distance;
        q
    }

    /// Build a boolean range distance query. Returns at most one result.
    pub fn any_between(
        set: GeometrySet,
        min_distance: S1ChordAngle,
        max_distance: S1ChordAngle,
    ) -> Self {
        let mut q = Self::build(set, 1, max_distance, true);
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
        let query_index = builder.build(std::slice::from_ref(&set));
        let query_edges = QueryEdgeProvider { set };
        Self {
            query_index,
            query_edges,
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
        if cell_reader.cell_count() == 0 {
            return Vec::new();
        }

        let mut best: HashMap<u32, S1ChordAngle> = HashMap::new();
        let mut distance_limit = self.max_distance;
        let mut queue: BinaryHeap<QueueEntry> = BinaryHeap::new();

        // Seed with the six face-level cells.
        for face in 0..6 {
            let cell = S2Cell::from_face(face);
            let dist = self.cell_to_query_distance(&cell);
            if dist < distance_limit {
                queue.push(QueueEntry {
                    distance: dist,
                    cell_id: cell.id(),
                });
            }
        }

        while let Some(entry) = queue.pop() {
            if entry.distance >= distance_limit {
                break;
            }

            let index_cells: Vec<_> = cell_reader.scan_range(entry.cell_id).collect();
            if index_cells.is_empty() {
                continue;
            }

            // Process if any index cell contains this queue cell (queue cell is at or
            // below the index cell level).
            let should_process = index_cells.iter().any(|ic| {
                ic.cell_id.range_min() <= entry.cell_id.range_min()
                    && ic.cell_id.range_max() >= entry.cell_id.range_max()
            });

            if should_process {
                for index_cell in &index_cells {
                    self.process_edges(
                        index_cell,
                        edge_cache,
                        terms_filter,
                        &mut best,
                        &mut distance_limit,
                    );
                    if self.first_only && !best.is_empty() {
                        return self.collect_results(&best);
                    }
                }
            } else {
                let cell = S2Cell::new(entry.cell_id);
                if let Some(children) = cell.subdivide() {
                    for child in &children {
                        let dist = self.cell_to_query_distance(child);
                        if dist < distance_limit {
                            let mut has_content = false;
                            for _ in cell_reader.scan_range(child.id()) {
                                has_content = true;
                                break;
                            }
                            if has_content {
                                queue.push(QueueEntry {
                                    distance: dist,
                                    cell_id: child.id(),
                                });
                            }
                        }
                    }
                }
            }
        }

        self.collect_results(&best)
    }

    /// Minimum distance from a stored cell to the query geometry. Uses the query-side CellIndex
    /// to find only the query edges that overlap the stored cell's range.
    fn cell_to_query_distance(&self, cell: &S2Cell) -> S1ChordAngle {
        let mut min_dist = S1ChordAngle::infinity();

        for query_cell in self.query_cells_overlapping(cell.id()) {
            for clipped in &query_cell.shapes {
                let edge_set = self.query_edges.get_edge_set(clipped.geometry_id);
                let vertices = &edge_set.vertices;
                if vertices.len() < 2 {
                    if vertices.len() == 1 {
                        let d = cell.get_distance_to_point(&vertices[0]);
                        if d < min_dist {
                            min_dist = d;
                        }
                    }
                    continue;
                }
                for &edge_idx in &clipped.edge_indices {
                    let i = edge_idx as usize;
                    if i + 1 < vertices.len() {
                        let d = cell.get_distance_to_edge(&vertices[i], &vertices[i + 1]);
                        if d < min_dist {
                            min_dist = d;
                        }
                    }
                }
            }
        }

        // If no query cells overlap, fall back to checking all query edges.
        if min_dist == S1ChordAngle::infinity() {
            for member in &self.query_edges.set.members {
                let vertices = &member.vertices;
                for i in 0..vertices.len().saturating_sub(1) {
                    let d = cell.get_distance_to_edge(&vertices[i], &vertices[i + 1]);
                    if d < min_dist {
                        min_dist = d;
                    }
                }
                if vertices.len() == 1 {
                    let d = cell.get_distance_to_point(&vertices[0]);
                    if d < min_dist {
                        min_dist = d;
                    }
                }
            }
        }

        min_dist
    }

    /// Find query CellIndex cells that overlap a given stored cell's range.
    fn query_cells_overlapping(&self, target: S2CellId) -> &[super::cell_index::IndexCell] {
        let range_min = target.range_min();
        let range_max = target.range_max();
        let cells = &self.query_index.cells;
        let start = cells.partition_point(|c| c.cell_id.range_max() < range_min);
        let end = cells.partition_point(|c| c.cell_id.range_min() <= range_max);
        &cells[start..end]
    }

    fn process_edges<'a>(
        &self,
        index_cell: &super::cell_index::IndexCell,
        edge_cache: &mut EdgeCache<'a, Sphere>,
        terms_filter: Option<&BitSet>,
        best: &mut HashMap<u32, S1ChordAngle>,
        distance_limit: &mut S1ChordAngle,
    ) {
        // Find which query edges overlap this stored cell.
        let query_cells = self.query_cells_overlapping(index_cell.cell_id);

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

            // Skip shapes with no edges and no vertices to check. A ClippedShape with
            // contains_center but empty edge_indices means the cell center is inside the
            // geometry but no edges cross the cell. Without edges to compare, we cannot
            // compute an actual distance.
            if clipped.edge_indices.is_empty() && candidate_vertices.len() > 1 {
                continue;
            }

            // For each stored edge, check against query edges. Use overlapping query cells
            // when available; fall back to all query edges when no query cells overlap
            // this stored cell (common for point queries where the query CellIndex has
            // one cell at a fine level).
            for &edge_idx in &clipped.edge_indices {
                let ci = edge_idx as usize;
                if ci + 1 >= candidate_vertices.len() {
                    continue;
                }
                let cv0 = &candidate_vertices[ci];
                let cv1 = &candidate_vertices[ci + 1];

                if !query_cells.is_empty() {
                    for query_cell in query_cells {
                        for qclipped in &query_cell.shapes {
                            let qedges = self.query_edges.get_edge_set(qclipped.geometry_id);
                            let qverts = &qedges.vertices;
                            for &qeidx in &qclipped.edge_indices {
                                let qi = qeidx as usize;
                                if qi + 1 < qverts.len() {
                                    update_edge_pair_min_distance(
                                        cv0,
                                        cv1,
                                        &qverts[qi],
                                        &qverts[qi + 1],
                                        &mut min_dist,
                                    );
                                }
                            }
                        }
                    }
                } else {
                    for member in &self.query_edges.set.members {
                        let qverts = &member.vertices;
                        for i in 0..qverts.len().saturating_sub(1) {
                            update_edge_pair_min_distance(
                                cv0,
                                cv1,
                                &qverts[i],
                                &qverts[i + 1],
                                &mut min_dist,
                            );
                        }
                        if qverts.len() == 1 {
                            update_min_distance(&qverts[0], cv0, cv1, &mut min_dist);
                        }
                    }
                }
            }

            // Single-vertex geometries (points).
            if candidate_vertices.len() == 1 {
                for member in &self.query_edges.set.members {
                    let qverts = &member.vertices;
                    for i in 0..qverts.len().saturating_sub(1) {
                        update_min_distance(
                            &candidate_vertices[0],
                            &qverts[i],
                            &qverts[i + 1],
                            &mut min_dist,
                        );
                    }
                    if qverts.len() == 1 {
                        let d = S1ChordAngle::from_points(&candidate_vertices[0], &qverts[0]);
                        if d < min_dist {
                            min_dist = d;
                        }
                    }
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
        out.sort_by(|a, b| a.distance.cmp(&b.distance));
        if self.max_results < out.len() {
            out.truncate(self.max_results);
        }
        out
    }
}

struct QueueEntry {
    distance: S1ChordAngle,
    cell_id: S2CellId,
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

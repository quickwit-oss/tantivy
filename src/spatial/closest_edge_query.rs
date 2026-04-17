//! Branch-and-bound closest edge query over the cell index.
//!
//! Finds geometries closest to a query geometry by traversing the S2 cell hierarchy in distance
//! order. Seeds the priority queue from the intersection of a coarse index covering and a search
//! disc covering, then subdivides using two seeks per level. Faithful to the structure of
//! S2ClosestEdgeQueryBase in s2closest_edge_query_base.h.
//!
//! Two modes:
//!   kNN          -- dynamic threshold, tightens to Kth-best
//!   within-D     -- fixed threshold, collect all within D

use std::cmp::Ordering;
use std::collections::BinaryHeap;

use common::BitSet;

use super::cell_index_reader::{CellIndexReader, CellRelation};
use super::edge_cache::EdgeCache;
use super::geometry_set::GeometrySet;
use super::region_coverer::{CovererOptions, RegionCoverer};
use super::s1chord_angle::S1ChordAngle;
use super::s2cap::S2Cap;
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

struct QueueEntry {
    distance: S1ChordAngle,
    cell_id: S2CellId,
    is_index_cell: bool,
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

// Reversed ordering for min-heap.
impl Ord for QueueEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        other.distance.cmp(&self.distance)
    }
}

// Minimum distance from an S2Cell to the query geometry.
fn cell_to_query_distance(cell: &S2Cell, query: &GeometrySet) -> S1ChordAngle {
    let mut min_dist = S1ChordAngle::infinity();
    for member in &query.members {
        let vertices = &member.vertices;
        if vertices.len() < 2 {
            if vertices.len() == 1 {
                let d = cell.get_distance_to_point(&vertices[0]);
                if d < min_dist {
                    min_dist = d;
                }
            }
            continue;
        }
        for i in 0..vertices.len() - 1 {
            let d = cell.get_distance_to_edge(&vertices[i], &vertices[i + 1]);
            if d < min_dist {
                min_dist = d;
            }
        }
    }
    min_dist
}

/// Prepared closest-edge query. Built once from a query geometry, applied per segment.
pub struct ClosestEdgeQuery {
    query_set: GeometrySet,
    max_results: usize,
    max_distance: S1ChordAngle,
    min_distance: Option<S1ChordAngle>,
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
        // Between is composable from within, filtering by min_distance in collect_results.
        let mut q = Self::build(set, usize::MAX, max_distance, false);
        q.min_distance = Some(min_distance);
        q
    }

    /// Build a boolean range distance query. Returns at most one result.
    pub fn any_between(
        set: GeometrySet,
        min_distance: S1ChordAngle,
        max_distance: S1ChordAngle,
    ) -> Self {
        let mut q = Self::build(set, 1, max_distance, true);
        q.min_distance = Some(min_distance);
        q
    }

    fn build(
        set: GeometrySet,
        max_results: usize,
        max_distance: S1ChordAngle,
        first_only: bool,
    ) -> Self {
        Self {
            query_set: set,
            max_results,
            max_distance,
            min_distance: None,
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
        if cell_reader.cell_count() < 2 {
            return Vec::new();
        }

        let geometry_count = edge_cache.geometry_count(0) as usize;
        let mut distance_limit = self.max_distance;

        // Compute the index covering.
        let index_covering = self.compute_index_covering(cell_reader);
        if index_covering.is_empty() {
            return Vec::new();
        }

        // Compute the search disc covering and intersect with the index covering.
        let initial_cells = self.compute_initial_cells(&index_covering);

        // Seed the priority queue.
        let mut queue: BinaryHeap<QueueEntry> = BinaryHeap::new();
        let mut cursor = cell_reader.cursor();

        for &cell_id in &initial_cells {
            let relation = cursor.locate(cell_id);
            match relation {
                CellRelation::ContainedBy => {
                    let index_cell_id = cursor.id().unwrap();
                    let cell = S2Cell::new(index_cell_id);
                    let dist = cell_to_query_distance(&cell, &self.query_set);
                    if dist < distance_limit {
                        queue.push(QueueEntry {
                            distance: dist,
                            cell_id: index_cell_id,
                            is_index_cell: true,
                        });
                    }
                }
                CellRelation::Contains => {
                    let cell = S2Cell::new(cell_id);
                    let dist = cell_to_query_distance(&cell, &self.query_set);
                    if dist < distance_limit {
                        queue.push(QueueEntry {
                            distance: dist,
                            cell_id,
                            is_index_cell: false,
                        });
                    }
                }
                CellRelation::Disjoint => {}
            }
        }

        // Main loop.
        let mut best: Vec<S1ChordAngle> =
            vec![S1ChordAngle::infinity(); geometry_count];

        while let Some(entry) = queue.pop() {
            if !(entry.distance < distance_limit) {
                break;
            }

            if entry.is_index_cell {
                self.process_cell(
                    &entry,
                    cell_reader,
                    edge_cache,
                    terms_filter,
                    &mut best,
                    &mut distance_limit,
                );
                if self.first_only && best.iter().any(|d| *d < S1ChordAngle::infinity()) {
                    break;
                }
                continue;
            }

            // Subdivide into four children using two seeks.
            let id = entry.cell_id;

            cursor = cell_reader.cursor();
            cursor.seek(id.child(1).range_min());

            if !cursor.done() {
                let cur_id = cursor.id().unwrap();
                if cur_id <= id.child(1).range_max() {
                    let is_index = cur_id == id.child(1);
                    let child_cell = S2Cell::new(id.child(1));
                    let dist = cell_to_query_distance(&child_cell, &self.query_set);
                    if dist < distance_limit {
                        queue.push(QueueEntry {
                            distance: dist,
                            cell_id: if is_index { cur_id } else { id.child(1) },
                            is_index_cell: is_index,
                        });
                    }
                }
            }

            if cursor.prev() {
                let cur_id = cursor.id().unwrap();
                if cur_id >= id.range_min() {
                    let is_index = cur_id == id.child(0);
                    let child_cell = S2Cell::new(id.child(0));
                    let dist = cell_to_query_distance(&child_cell, &self.query_set);
                    if dist < distance_limit {
                        queue.push(QueueEntry {
                            distance: dist,
                            cell_id: if is_index { cur_id } else { id.child(0) },
                            is_index_cell: is_index,
                        });
                    }
                }
            }

            cursor.seek(id.child(3).range_min());

            if !cursor.done() {
                let cur_id = cursor.id().unwrap();
                if cur_id <= id.range_max() {
                    let is_index = cur_id == id.child(3);
                    let child_cell = S2Cell::new(id.child(3));
                    let dist = cell_to_query_distance(&child_cell, &self.query_set);
                    if dist < distance_limit {
                        queue.push(QueueEntry {
                            distance: dist,
                            cell_id: if is_index { cur_id } else { id.child(3) },
                            is_index_cell: is_index,
                        });
                    }
                }
            }

            if cursor.prev() {
                let cur_id = cursor.id().unwrap();
                if cur_id >= id.child(2).range_min() {
                    let is_index = cur_id == id.child(2);
                    let child_cell = S2Cell::new(id.child(2));
                    let dist = cell_to_query_distance(&child_cell, &self.query_set);
                    if dist < distance_limit {
                        queue.push(QueueEntry {
                            distance: dist,
                            cell_id: if is_index { cur_id } else { id.child(2) },
                            is_index_cell: is_index,
                        });
                    }
                }
            }
        }

        self.collect_results(&best, edge_cache)
    }

    fn compute_index_covering(&self, cell_reader: &CellIndexReader) -> Vec<S2CellId> {
        let mut cursor = cell_reader.cursor();
        let first_id = match cursor.id() {
            Some(id) => id,
            None => return Vec::new(),
        };

        cursor.seek(S2CellId(u64::MAX));
        cursor.prev();
        let last_id = cursor.id().unwrap();

        let ancestor_level = first_id.common_ancestor_level(last_id);
        let level = ancestor_level as i32 + 1;

        let mut index_covering: Vec<S2CellId> = Vec::new();
        cursor = cell_reader.cursor();
        let last_top = last_id.parent(level);

        let mut top = first_id.parent(level);
        while top != last_top {
            if top.range_max() < cursor.id().unwrap() {
                top = top.next();
                continue;
            }

            let range_first = cursor.id().unwrap();
            cursor.seek(top.range_max().next());
            cursor.prev();
            let range_last = cursor.id().unwrap();

            if range_first == range_last {
                index_covering.push(range_first);
            } else {
                let shrink_level = range_first.common_ancestor_level(range_last);
                index_covering.push(range_first.parent(shrink_level as i32));
            }

            cursor.seek(top.range_max().next());
            top = top.next();
        }

        if !cursor.done() {
            let range_first = cursor.id().unwrap();
            cursor.seek(S2CellId(u64::MAX));
            cursor.prev();
            let range_last = cursor.id().unwrap();

            if range_first == range_last {
                index_covering.push(range_first);
            } else {
                let shrink_level = range_first.common_ancestor_level(range_last);
                index_covering.push(range_first.parent(shrink_level as i32));
            }
        }

        index_covering
    }

    fn compute_initial_cells(&self, index_covering: &[S2CellId]) -> Vec<S2CellId> {
        let mut query_cap = S2Cap::empty();
        for member in &self.query_set.members {
            for v in &member.vertices {
                query_cap.add_point(v);
            }
        }

        let radius_radians = self.max_distance.to_radians();
        let search_cap = query_cap.expanded(radius_radians);

        let mut search_covering: Vec<S2CellId> = Vec::new();
        let coverer = RegionCoverer::new(CovererOptions::new().max_cells(4));
        coverer.get_fast_covering(&search_cap, &mut search_covering);
        search_covering.sort();

        let mut initial_cells: Vec<S2CellId> = Vec::new();
        for &sc in &search_covering {
            for &ic in index_covering {
                if sc.range_min() <= ic.range_max() && ic.range_min() <= sc.range_max() {
                    if sc.level() >= ic.level() {
                        initial_cells.push(sc);
                    } else {
                        initial_cells.push(ic);
                    }
                    break;
                }
            }
        }
        initial_cells.sort();
        initial_cells.dedup();
        initial_cells
    }

    fn process_cell<'a>(
        &self,
        entry: &QueueEntry,
        cell_reader: &'a CellIndexReader<'a>,
        edge_cache: &mut EdgeCache<'a, Sphere>,
        terms_filter: Option<&BitSet>,
        best: &mut [S1ChordAngle],
        distance_limit: &mut S1ChordAngle,
    ) {
        let cell = cell_reader.find(entry.cell_id).unwrap();

        for clipped in &cell.shapes {
            let gid = clipped.geometry_id.1 as usize;

            if let Some(filter) = terms_filter {
                let doc_id = edge_cache.doc_id_for(clipped.geometry_id);
                if !filter.contains(doc_id) {
                    continue;
                }
            }

            let cache_entry = edge_cache.get(clipped.geometry_id);
            let candidate_vertices = cache_entry.vertices();
            let mut min_dist = best[gid];

            if clipped.edge_indices.is_empty() {
                if candidate_vertices.len() == 1 {
                    for member in &self.query_set.members {
                        let qverts = &member.vertices;
                        for qi in 0..qverts.len().saturating_sub(1) {
                            update_min_distance(
                                &candidate_vertices[0],
                                &qverts[qi],
                                &qverts[qi + 1],
                                &mut min_dist,
                            );
                        }
                    }
                }
            } else if candidate_vertices.len() >= 2 {
                for &edge_idx in &clipped.edge_indices {
                    let ci = edge_idx as usize;
                    if ci + 1 >= candidate_vertices.len() {
                        continue;
                    }
                    let cv0 = &candidate_vertices[ci];
                    let cv1 = &candidate_vertices[ci + 1];
                    for member in &self.query_set.members {
                        let qverts = &member.vertices;
                        for qi in 0..qverts.len().saturating_sub(1) {
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

            if min_dist < best[gid] {
                best[gid] = min_dist;
                self.maybe_tighten_limit(best, distance_limit);
            }
        }
    }

    fn maybe_tighten_limit(&self, best: &[S1ChordAngle], distance_limit: &mut S1ChordAngle) {
        if self.max_results >= best.len() {
            return;
        }
        let mut distances: Vec<S1ChordAngle> = best
            .iter()
            .copied()
            .filter(|d| *d < S1ChordAngle::infinity())
            .collect();
        if distances.len() >= self.max_results {
            distances.sort();
            *distance_limit = distances[self.max_results - 1];
        }
    }

    fn collect_results(
        &self,
        best: &[S1ChordAngle],
        edge_cache: &EdgeCache<'_, Sphere>,
    ) -> Vec<ClosestEdgeResult> {
        let min_distance = self.min_distance.unwrap_or(S1ChordAngle::zero());
        let mut results: Vec<ClosestEdgeResult> = Vec::new();
        for (gid, &distance) in best.iter().enumerate() {
            if distance < self.max_distance && distance >= min_distance {
                let doc_id = edge_cache.doc_id_for((0, gid as u32));
                results.push(ClosestEdgeResult { doc_id, distance });
            }
        }
        results.sort_by(|a, b| a.distance.cmp(&b.distance));
        if self.max_results < results.len() {
            results.truncate(self.max_results);
        }
        results
    }
}

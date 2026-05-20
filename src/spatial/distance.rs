//! Distance query: find indexed geometries within a distance of the query polygon.
//!
//! At distance zero this is equivalent to intersects. At distance D it finds all geometries whose
//! edges are within D of the query polygon's edges. Uses the query CellIndex for the polygon
//! interior (intersects phase) and a flood fill with kd-tree pruning for the distance band around
//! the boundary.

use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashSet};

use common::BitSet;

use super::boundary_tree::{BoundaryNode, BoundaryTree};
use super::cell_index_reader::CellIndexReader;
use super::covering::{covering_contains_center, covering_split, CoveringCell};
use super::edge_cache::EdgeCache;
use super::edge_crosser::EdgeCrosser;
use super::edge_provider::EdgeProvider;
use super::geometry_set::GeometrySet;
use super::query_edge_provider::QueryEdgeProvider;
use super::region_coverer::CovererOptions;
use super::s1chord_angle::S1ChordAngle;
use super::s2cell::S2Cell;
use super::s2cell_id::S2CellId;
use super::s2edge_distances::update_edge_pair_min_distance;
use super::s2padded_cell::S2PaddedCell;
use super::shape_index_region::index_contains_point;
use super::surface::Surface;
use crate::spatial::clip_options::ClipOptions;
use crate::spatial::clipper::Clipper;
use crate::spatial::shape_index::ShapeIndex;

/// A sweep cell in the flood fill priority queue.
struct SweepEntry {
    chord_to_nearest: f64,
    cell_id: S2CellId,
}

impl PartialEq for SweepEntry {
    fn eq(&self, other: &Self) -> bool {
        self.chord_to_nearest == other.chord_to_nearest
    }
}
impl Eq for SweepEntry {}

impl PartialOrd for SweepEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SweepEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .chord_to_nearest
            .partial_cmp(&self.chord_to_nearest)
            .unwrap_or(Ordering::Equal)
    }
}

/// Prepared distance query, built once from a query polygon and applied per-segment.
pub struct Distance<S: Surface> {
    query_index: ShapeIndex,
    query_edges: QueryEdgeProvider<S>,
    max_distance: f64,
    first_only: bool,
}

impl<S: Surface> Distance<S> {
    /// Build the query from a smashed GeometrySet. Distance is in radians on the sphere or in UV
    /// units on the plane.
    pub fn new(set: GeometrySet<S>, max_distance: f64, _options: CovererOptions) -> Self {
        let builder = Clipper::new(ClipOptions::default());
        let query_index = builder.build(std::slice::from_ref(&set));
        let query_edges = QueryEdgeProvider { set };
        Self {
            query_index,
            query_edges,
            max_distance,
            first_only: false,
        }
    }

    /// Build a boolean distance query that returns after the first hit.
    pub fn any_within(set: GeometrySet<S>, max_distance: f64, options: CovererOptions) -> Self {
        let mut q = Self::new(set, max_distance, options);
        q.first_only = true;
        q
    }

    /// Search one segment for geometries within max_distance of the query polygon.
    pub fn search<'a>(
        &self,
        reader: &'a CellIndexReader<'a>,
        terms_filter: Option<&BitSet>,
        edge_cache: &mut EdgeCache<'a, S>,
        max_doc: u32,
    ) -> BitSet {
        let geometry_count = edge_cache.geometry_count(0);
        let mut seen = BitSet::with_max_value(geometry_count);
        let mut doc_ids = BitSet::with_max_value(max_doc);
        let mut containment_tested = BitSet::with_max_value(geometry_count);
        let mut boundary_entries: Vec<(S2CellId, Vec<u32>)> = Vec::new();

        // We perform intersection since it is equivalent to within dinstance accordint to
        // ST_DWithin.
        let query_vertex = &self.query_edges.get_edge_set((0, 0)).vertices[0];
        let query_point_cell_id = S::cell_id_from_point(query_vertex);
        if let Some(qpc) = reader.find(query_point_cell_id) {
            for shape in &qpc.shapes {
                let gid = shape.geometry_id.1;
                if shape.contains_center && shape.edge_indices.is_empty() {
                    let located = edge_cache.locate(shape.geometry_id);
                    if !located.closed {
                        continue;
                    }
                    if let Some(filter) = terms_filter {
                        if !filter.contains(located.doc_id) {
                            seen.insert(gid);
                            continue;
                        }
                    }
                    seen.insert(gid);
                    doc_ids.insert(located.doc_id);
                    if self.first_only {
                        return doc_ids;
                    }
                }
            }
        }

        let mut heap = BinaryHeap::new();

        for query_cell in &self.query_index.cells {
            let cell_id = query_cell.cell_id;

            let mut query_edge_indices: Vec<u32> = Vec::new();
            let mut contains_center = false;
            for shape in &query_cell.shapes {
                query_edge_indices.extend_from_slice(&shape.edge_indices);
                if shape.contains_center {
                    contains_center = true;
                }
            }

            let (index_start, index_end) = reader.range_for_cell(cell_id, 0, reader.cell_count());

            if index_start == index_end && query_edge_indices.is_empty() && !contains_center {
                continue;
            }

            let first_index_level = if index_start < index_end {
                reader.cell_id_at(index_start).level()
            } else {
                0
            };

            heap.push(CoveringCell {
                pcell: S2PaddedCell::new(cell_id, S::CELL_PADDING),
                query_edges: query_edge_indices,
                contains_center,
                index_start,
                index_end,
                first_index_level,
            });
        }

        let mut found = false;

        while let Some(entry) = heap.pop() {
            if self.first_only && found {
                break;
            }
            if entry.query_edges.is_empty()
                && entry.contains_center
                && entry.first_index_level > entry.pcell.level()
            {
                if !reader.doc_ids_data.is_empty() && terms_filter.is_none() {
                    if self.first_only && entry.index_start < entry.index_end {
                        found = true;
                        continue;
                    }
                    reader.visit_doc_ids(entry.index_start, entry.index_end, |doc_id| {
                        doc_ids.insert(doc_id);
                        true
                    });
                } else if self.first_only && !reader.doc_ids_data.is_empty() {
                    let filter = terms_filter.unwrap();
                    reader.visit_doc_ids(entry.index_start, entry.index_end, |doc_id| {
                        if filter.contains(doc_id) {
                            found = true;
                            return false;
                        }
                        true
                    });
                } else {
                    for pos in entry.index_start..entry.index_end {
                        let cell = reader.cell_at(pos);
                        for clipped in &cell.shapes {
                            let gid = clipped.geometry_id.1;
                            if seen.contains(gid) {
                                continue;
                            }
                            seen.insert(gid);
                            let doc_id = edge_cache.doc_id_for(clipped.geometry_id);
                            if let Some(filter) = terms_filter {
                                if !filter.contains(doc_id) {
                                    continue;
                                }
                            }
                            doc_ids.insert(doc_id);
                        }
                    }
                }
            } else if entry.pcell.level() < entry.first_index_level && !entry.pcell.id().is_leaf() {
                let get_edge = |idx: u32| self.query_edges.get_edge((0, 0), idx);
                let start_for_cell = |cell_id: S2CellId, lo: u32, hi: u32| {
                    let start = reader.start_for_cell(cell_id, lo, hi);
                    let level = if start < hi {
                        reader.cell_id_at(start).level()
                    } else {
                        0
                    };
                    (start, level)
                };
                let parent_center = entry.pcell.get_center();
                let mut children = covering_split(&entry, &get_edge, &start_for_cell);
                covering_contains_center::<S>(&parent_center, &mut children, &get_edge);
                for child in children {
                    heap.push(child);
                }
            } else {
                if !entry.query_edges.is_empty() {
                    boundary_entries.push((entry.pcell.id(), entry.query_edges.clone()));
                }
                let query_vertices = &self.query_edges.get_edge_set((0, 0)).vertices;
                for pos in entry.index_start..entry.index_end {
                    let cell = reader.cell_at(pos);
                    for clipped in &cell.shapes {
                        let gid = clipped.geometry_id.1;

                        if seen.contains(gid) {
                            continue;
                        }

                        let doc_id = edge_cache.doc_id_for(clipped.geometry_id);
                        if let Some(filter) = terms_filter {
                            if !filter.contains(doc_id) {
                                seen.insert(gid);
                                continue;
                            }
                        }

                        if !containment_tested.contains(gid) {
                            containment_tested.insert(gid);

                            let located = edge_cache.locate(clipped.geometry_id);
                            let first_vertex = located.vertex(0);
                            if index_contains_point::<S, QueryEdgeProvider<S>>(
                                &self.query_index,
                                &self.query_edges,
                                (0, 0),
                                &first_vertex,
                            ) {
                                seen.insert(gid);
                                doc_ids.insert(located.doc_id);
                                if self.first_only {
                                    return doc_ids;
                                }
                                continue;
                            }
                        }

                        if !clipped.edge_indices.is_empty() && !entry.query_edges.is_empty() {
                            let located = edge_cache.locate(clipped.geometry_id);
                            if located.vertex_count >= 2 {
                                let crossed = 'crossing: {
                                    for &candidate_edge_idx in &clipped.edge_indices {
                                        let (cv0, cv1) = located.edge(candidate_edge_idx);
                                        let mut crosser = S::EdgeCrosser::new(&cv0, &cv1);
                                        for &query_edge_idx in &entry.query_edges {
                                            let qi = query_edge_idx as usize;
                                            let qv0 = &query_vertices[qi];
                                            let qv1 = &query_vertices[qi + 1];
                                            if crosser.crossing_sign_two(qv0, qv1) > 0 {
                                                break 'crossing true;
                                            }
                                        }
                                    }
                                    false
                                };
                                if crossed {
                                    seen.insert(gid);
                                    doc_ids.insert(doc_id);
                                    if self.first_only {
                                        return doc_ids;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        if self.first_only && found {
            return doc_ids;
        }

        // Phase 2: flood fill for the distance band.
        if self.max_distance > 0.0 && !boundary_entries.is_empty() {
            self.flood_fill(
                &boundary_entries,
                reader,
                terms_filter,
                edge_cache,
                &mut seen,
                &mut doc_ids,
            );
        }

        doc_ids
    }

    fn flood_fill<'a>(
        &self,
        boundary_entries: &[(S2CellId, Vec<u32>)],
        reader: &'a CellIndexReader<'a>,
        terms_filter: Option<&BitSet>,
        edge_cache: &mut EdgeCache<'a, S>,
        seen: &mut BitSet,
        doc_ids: &mut BitSet,
    ) {
        let max_chord = S1ChordAngle::from_radians(self.max_distance).length2() as f64;

        // Build the kd-tree from boundary cell centers.
        let boundary_nodes: Vec<BoundaryNode> = boundary_entries
            .iter()
            .map(|(cell_id, edge_indices)| {
                let center = S::cell_center(*cell_id);
                let p = center.as_ref();
                BoundaryNode {
                    point: [p[0], p[1], p[2]],
                    edge_indices: edge_indices.clone(),
                }
            })
            .collect();
        let boundary_tree = BoundaryTree::build(boundary_nodes);

        // Compute the cell radius for the boundary level. All boundary cells are at the same level
        // after the uniform split.
        let boundary_level = boundary_entries[0].0.level();
        let boundary_cell = S2Cell::new(boundary_entries[0].0);
        let boundary_cap = boundary_cell.get_cap_bound();
        let boundary_radius = boundary_cap.radius().length2() as f64;

        // The search threshold accounts for both cell radii.
        let search_threshold = max_chord + 2.0 * boundary_radius;

        // Seed the flood fill with neighbors of boundary cells.
        let mut visited: HashSet<S2CellId> = HashSet::new();
        let mut flood_heap: BinaryHeap<SweepEntry> = BinaryHeap::new();

        for (cell_id, _) in boundary_entries {
            visited.insert(*cell_id);
        }

        // Seed the flood fill. For each boundary cell, visit its 8 neighbors. Neighbors that are
        // within the search threshold (max distance plus cell radii) go onto the heap ordered by
        // chord angle to the nearest boundary cell. The heap processes closest cells first so
        // positives are found early. Each processed cell expands its own 8 neighbors, and the
        // flood terminates when no reachable cell is within range.
        for (cell_id, _) in boundary_entries {
            let (_, i, j, _) = cell_id.to_face_ij_orientation();
            let cell_size = S2CellId::size_ij_for_level(boundary_level);
            for di in -1i32..=1 {
                for dj in -1i32..=1 {
                    if di == 0 && dj == 0 {
                        continue;
                    }
                    let ni = i + di * cell_size;
                    let nj = j + dj * cell_size;
                    let neighbor =
                        S2CellId::from_face_ij_wrap(cell_id.face(), ni, nj).parent(boundary_level);
                    if !visited.insert(neighbor) {
                        continue;
                    }
                    let center = S::cell_center(neighbor);
                    let p = center.as_ref();
                    let query = [p[0], p[1], p[2]];
                    if let Some((chord, _)) = boundary_tree.nearest(&query) {
                        if chord <= search_threshold {
                            flood_heap.push(SweepEntry {
                                chord_to_nearest: chord,
                                cell_id: neighbor,
                            });
                        }
                    }
                }
            }
        }

        let query_vertices = &self.query_edges.get_edge_set((0, 0)).vertices;

        // Process the flood fill.
        let mut flood_found = false;
        while let Some(sweep) = flood_heap.pop() {
            if self.first_only && flood_found {
                return;
            }
            let sweep_cell = S2Cell::new(sweep.cell_id);
            let sweep_cap = sweep_cell.get_cap_bound();
            let sweep_radius = sweep_cap.radius().length2() as f64;

            // Skip if definitely out of range.
            if sweep.chord_to_nearest - sweep_radius - boundary_radius > max_chord {
                continue;
            }

            // Bulk include if definitely in range.
            let bulk_include = sweep.chord_to_nearest + sweep_radius + boundary_radius < max_chord;

            // Look up index cells in this sweep cell.
            let (index_start, index_end) =
                reader.range_for_cell(sweep.cell_id, 0, reader.cell_count());

            if bulk_include {
                if self.first_only {
                    if terms_filter.is_none() && index_start < index_end {
                        flood_found = true;
                        continue;
                    }
                    if let Some(filter) = terms_filter {
                        if !reader.doc_ids_data.is_empty() {
                            reader.visit_doc_ids(index_start, index_end, |doc_id| {
                                if filter.contains(doc_id) {
                                    flood_found = true;
                                    return false;
                                }
                                true
                            });
                            continue;
                        }
                    }
                }
                if !reader.doc_ids_data.is_empty() && terms_filter.is_none() {
                    reader.visit_doc_ids(index_start, index_end, |doc_id| {
                        doc_ids.insert(doc_id);
                        true
                    });
                } else {
                    for pos in index_start..index_end {
                        let cell = reader.cell_at(pos);
                        for clipped in &cell.shapes {
                            let gid = clipped.geometry_id.1;
                            if seen.contains(gid) {
                                continue;
                            }
                            seen.insert(gid);
                            let doc_id = edge_cache.doc_id_for(clipped.geometry_id);
                            if let Some(filter) = terms_filter {
                                if !filter.contains(doc_id) {
                                    continue;
                                }
                            }
                            doc_ids.insert(doc_id);
                        }
                    }
                }
            } else {
                // Drop down: edge-pair distance test.
                let center = S::cell_center(sweep.cell_id);
                let cp = center.as_ref();
                let query_point = [cp[0], cp[1], cp[2]];

                for pos in index_start..index_end {
                    let cell = reader.cell_at(pos);
                    for clipped in &cell.shapes {
                        let gid = clipped.geometry_id.1;
                        if seen.contains(gid) {
                            continue;
                        }

                        let doc_id = edge_cache.doc_id_for(clipped.geometry_id);
                        if let Some(filter) = terms_filter {
                            if !filter.contains(doc_id) {
                                seen.insert(gid);
                                continue;
                            }
                        }

                        let located = edge_cache.locate(clipped.geometry_id);
                        if located.vertex_count < 2 && clipped.edge_indices.is_empty() {
                            // Point geometry: test distance to nearest boundary edge.
                            let v = located.vertex(0);
                            let vp = v.as_ref();
                            let vxyz: [f64; 3] =
                                [vp[0], vp[1], if vp.len() > 2 { vp[2] } else { 0.0 }];
                            let mut found = false;
                            boundary_tree.search_within(
                                &vxyz,
                                search_threshold,
                                &mut |edge_indices| {
                                    for &qi in edge_indices {
                                        let i = qi as usize;
                                        let qv0 = &query_vertices[i];
                                        let qv1 = &query_vertices[i + 1];
                                        let q0 = qv0.as_ref();
                                        let q1 = qv1.as_ref();
                                        let q0xyz: [f64; 3] =
                                            [q0[0], q0[1], if q0.len() > 2 { q0[2] } else { 0.0 }];
                                        let q1xyz: [f64; 3] =
                                            [q1[0], q1[1], if q1.len() > 2 { q1[2] } else { 0.0 }];
                                        let mut dist = S1ChordAngle::infinity();
                                        super::s2edge_distances::update_min_distance(
                                            &vxyz, &q0xyz, &q1xyz, &mut dist,
                                        );
                                        if (dist.length2() as f64) <= max_chord {
                                            found = true;
                                            return false;
                                        }
                                    }
                                    true
                                },
                            );
                            if found {
                                seen.insert(gid);
                                doc_ids.insert(doc_id);
                                if self.first_only {
                                    return;
                                }
                            }
                            continue;
                        }

                        // Edge geometry: test edge-pair distances.
                        let mut found = false;
                        boundary_tree.search_within(
                            &query_point,
                            search_threshold,
                            &mut |edge_indices| {
                                for &candidate_edge_idx in &clipped.edge_indices {
                                    let (cv0, cv1) = edge_cache
                                        .locate(clipped.geometry_id)
                                        .edge(candidate_edge_idx);
                                    let c0 = cv0.as_ref();
                                    let c1 = cv1.as_ref();
                                    let c0xyz: [f64; 3] =
                                        [c0[0], c0[1], if c0.len() > 2 { c0[2] } else { 0.0 }];
                                    let c1xyz: [f64; 3] =
                                        [c1[0], c1[1], if c1.len() > 2 { c1[2] } else { 0.0 }];
                                    for &qi in edge_indices {
                                        let i = qi as usize;
                                        let qv0 = &query_vertices[i];
                                        let qv1 = &query_vertices[i + 1];
                                        let q0 = qv0.as_ref();
                                        let q1 = qv1.as_ref();
                                        let q0xyz: [f64; 3] =
                                            [q0[0], q0[1], if q0.len() > 2 { q0[2] } else { 0.0 }];
                                        let q1xyz: [f64; 3] =
                                            [q1[0], q1[1], if q1.len() > 2 { q1[2] } else { 0.0 }];
                                        let mut dist = S1ChordAngle::infinity();
                                        update_edge_pair_min_distance(
                                            &c0xyz, &c1xyz, &q0xyz, &q1xyz, &mut dist,
                                        );
                                        if (dist.length2() as f64) <= max_chord {
                                            found = true;
                                            return false;
                                        }
                                    }
                                }
                                true
                            },
                        );
                        if found {
                            seen.insert(gid);
                            doc_ids.insert(doc_id);
                            if self.first_only {
                                return;
                            }
                        }
                    }
                }
            }

            // Expand neighbors.
            let (_, i, j, _) = sweep.cell_id.to_face_ij_orientation();
            let cell_size = S2CellId::size_ij_for_level(boundary_level);
            for di in -1i32..=1 {
                for dj in -1i32..=1 {
                    if di == 0 && dj == 0 {
                        continue;
                    }
                    let ni = i + di * cell_size;
                    let nj = j + dj * cell_size;
                    let neighbor = S2CellId::from_face_ij_wrap(sweep.cell_id.face(), ni, nj)
                        .parent(boundary_level);
                    if !visited.insert(neighbor) {
                        continue;
                    }
                    let nc = S::cell_center(neighbor);
                    let np = nc.as_ref();
                    let nq = [np[0], np[1], np[2]];
                    if let Some((chord, _)) = boundary_tree.nearest(&nq) {
                        if chord <= search_threshold {
                            flood_heap.push(SweepEntry {
                                chord_to_nearest: chord,
                                cell_id: neighbor,
                            });
                        }
                    }
                }
            }
        }
    }
}

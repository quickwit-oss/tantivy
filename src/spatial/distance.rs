//! Find indexed geometries within a distance of the query geometry.
//!
//! A covering descent searches the query interior, and a flood at one cell level searches
//! around its boundary. Index cells can be larger than flood cells, so a cell overlap only
//! identifies candidates. Containment and edge distances determine which geometries match.

use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashSet};

use common::BitSet;

use super::cell_index_reader::CellIndexReader;
use super::clip_options::ClipOptions;
use super::clipper::Clipper;
use super::covering::{split_edges, CoveringCell};
use super::distance_boundary::{xyz, DistanceBoundary};
use super::edge_cache::EdgeCache;
use super::edge_crosser::EdgeCrosser;
use super::edge_provider::EdgeProvider;
use super::geometry_set::GeometrySet;
use super::query_edge_provider::QueryEdgeProvider;
use super::region_coverer::CovererOptions;
use super::s1chord_angle::S1ChordAngle;
use super::s2cell_id::S2CellId;
use super::s2edge_distances::update_edge_pair_min_distance;
use super::s2padded_cell::S2PaddedCell;
use super::shape_index::{ShapeCell, ShapeIndex};
use super::shape_index_region::index_contains_point;
use super::surface::Surface;

/// A sweep cell in the flood fill priority queue.
struct SweepEntry {
    chord_to_nearest: f64,
    cell_id: S2CellId,
    search_threshold: f64,
}

impl PartialEq for SweepEntry {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
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
            .total_cmp(&self.chord_to_nearest)
            .then_with(|| other.cell_id.cmp(&self.cell_id))
    }
}

struct Matches {
    seen: BitSet,
    containment_tested: BitSet,
    doc_ids: BitSet,
}

/// Prepared distance query, built once from query geometry and applied per segment.
pub struct Distance<S: Surface> {
    query_index: ShapeIndex,
    query_edges: QueryEdgeProvider<S>,
    // Each edge has a member number and a vertex offset within that member. Ring gaps are skipped.
    edges: Vec<(u32, u32)>,
    edge_ids: Vec<Vec<u32>>,
    boundary: Option<DistanceBoundary>,
    max_distance: Option<S1ChordAngle>,
    first_only: bool,
}

impl<S: Surface> Distance<S> {
    /// Build the query from a smashed GeometrySet. Distance is in radians.
    /// Nonpositive values and NaN select the intersection path. Positive values are capped at PI.
    /// Panics if a positive distance is requested on the plane.
    pub fn new(set: GeometrySet<S>, max_distance: f64, _options: CovererOptions) -> Self {
        let query_index = Clipper::new(ClipOptions::default()).build(std::slice::from_ref(&set));
        let mut edges = Vec::new();
        let mut edge_ids = Vec::new();
        for (member_id, member) in set.members.iter().enumerate() {
            let mut ids = vec![u32::MAX; member.vertices.len()];
            for ring in member.ring_offsets.windows(2) {
                let start = ring[0];
                let end = ring[1];
                let edge_end = if end == start + 1 {
                    end
                } else {
                    end.saturating_sub(1)
                };
                for edge in start..edge_end {
                    ids[edge] = edges.len() as u32;
                    edges.push((member_id as u32, edge as u32));
                }
            }
            edge_ids.push(ids);
        }
        let query_edges = QueryEdgeProvider { set };
        let max_distance = (max_distance > 0.0)
            .then(|| S1ChordAngle::from_radians(max_distance.min(std::f64::consts::PI)));
        let boundary = max_distance.map(|distance| {
            assert_eq!(
                S::DIMENSIONS,
                3,
                "positive planar distance is not implemented"
            );
            let seeds = query_index.cells.iter().map(|cell| {
                let ids = cell
                    .shapes
                    .iter()
                    .flat_map(|shape| {
                        let member = shape.geometry_id.1 as usize;
                        let ids = &edge_ids[member];
                        shape
                            .edge_indices
                            .iter()
                            .map(move |&edge| ids[edge as usize])
                    })
                    .collect();
                (cell.cell_id, ids)
            });
            DistanceBoundary::build::<S>(distance, seeds, &|id| {
                let (member, edge) = edges[id as usize];
                query_edges.get_edge((0, member), edge)
            })
        });
        Self {
            query_index,
            query_edges,
            edges,
            edge_ids,
            boundary,
            max_distance,
            first_only: false,
        }
    }

    /// Build a distance query that stops after the first matching document.
    pub fn any_within(set: GeometrySet<S>, max_distance: f64, options: CovererOptions) -> Self {
        let mut query = Self::new(set, max_distance, options);
        query.first_only = true;
        query
    }

    fn edge(&self, id: u32) -> (S::Point, S::Point) {
        let (member, edge) = self.edges[id as usize];
        self.query_edges.get_edge((0, member), edge)
    }

    fn member_contains(&self, member: u32, point: &S::Point) -> bool {
        self.query_edges.set.members[member as usize].closed
            && index_contains_point::<S, _>(
                &self.query_index,
                &self.query_edges,
                (0, member),
                point,
            )
    }

    fn contains(&self, point: &S::Point) -> bool {
        (0..self.query_edges.set.members.len() as u32)
            .any(|member| self.member_contains(member, point))
    }

    /// Search one segment. The caller must exclude deleted documents through terms_filter,
    /// since their geometry may still be present in the spatial files.
    pub fn search<'a>(
        &self,
        reader: &'a CellIndexReader<'a>,
        terms_filter: Option<&BitSet>,
        edge_cache: &mut EdgeCache<'a, S>,
        max_doc: u32,
    ) -> BitSet {
        let geometry_count = edge_cache.geometry_count(0);
        let mut matches = Matches {
            seen: BitSet::with_max_value(geometry_count),
            containment_tested: BitSet::with_max_value(geometry_count),
            doc_ids: BitSet::with_max_value(max_doc),
        };
        if reader.cell_count() == 0 || terms_filter.is_some_and(|filter| filter.len() == 0) {
            return matches.doc_ids;
        }

        // A candidate containing a query component need not have an edge in the query region.
        for member in &self.query_edges.set.members {
            let Some(query_vertex) = member.vertices.first() else {
                continue;
            };
            if let Some(cell) = reader.find(S::cell_id_from_point(query_vertex)) {
                let center = S::cell_center(cell.cell_id);
                for shape in &cell.shapes {
                    let gid = shape.geometry_id.1;
                    if matches.seen.contains(gid) {
                        continue;
                    }
                    let located = edge_cache.locate(shape.geometry_id);
                    if terms_filter.is_some_and(|filter| !filter.contains(located.doc_id)) {
                        matches.seen.insert(gid);
                        continue;
                    }
                    if !located.closed {
                        continue;
                    }
                    let mut inside = shape.contains_center;
                    let mut crosser = S::EdgeCrosser::new(&center, query_vertex);
                    for &edge in &shape.edge_indices {
                        let (a, b) = located.edge(edge);
                        inside ^= crosser.edge_or_vertex_crossing_two(&a, &b);
                    }
                    if inside {
                        matches.seen.insert(gid);
                        matches.doc_ids.insert(located.doc_id);
                        if self.first_only {
                            return matches.doc_ids;
                        }
                    }
                }
            }
        }

        // Keep covering entries separate for each member. Counting crossings across members
        // together would exclude points inside two overlapping polygons.
        let mut heap = BinaryHeap::new();
        for cell in &self.query_index.cells {
            let (start, end) = reader.range_for_cell(cell.cell_id, 0, reader.cell_count());
            if start == end {
                continue;
            }
            for shape in &cell.shapes {
                let member = shape.geometry_id.1;
                heap.push((
                    CoveringCell {
                        pcell: S2PaddedCell::<S>::new(cell.cell_id, S::CELL_PADDING),
                        query_edges: shape
                            .edge_indices
                            .iter()
                            .map(|&edge| self.edge_ids[member as usize][edge as usize])
                            .collect(),
                        contains_center: shape.contains_center,
                        index_start: start,
                        index_end: end,
                        first_index_level: reader.cell_id_at(start).level(),
                    },
                    member,
                ));
            }
        }

        while let Some((entry, member)) = heap.pop() {
            if !entry.query_edges.is_empty() && entry.pcell.level() < entry.first_index_level {
                let mut children =
                    split_edges(&entry.pcell, &entry.query_edges, &|id| self.edge(id));
                for pos in 0..4 {
                    let (i, j) = entry.pcell.get_child_ij(pos);
                    let pcell = S2PaddedCell::from_parent(&entry.pcell, i, j);
                    let (start, end) =
                        reader.range_for_cell(pcell.id(), entry.index_start, entry.index_end);
                    if start == end {
                        continue;
                    }
                    let query_edges = std::mem::take(&mut children[i][j]);
                    // A child can be inside the polygon even when its parent's center is outside.
                    // Check the child's center before discarding a child without edges.
                    let contains_center = self.member_contains(member, &pcell.get_center());
                    if query_edges.is_empty() && !contains_center {
                        continue;
                    }
                    heap.push((
                        CoveringCell {
                            pcell,
                            query_edges,
                            contains_center,
                            index_start: start,
                            index_end: end,
                            first_index_level: reader.cell_id_at(start).level(),
                        },
                        member,
                    ));
                }
            } else {
                for pos in entry.index_start..entry.index_end {
                    if self.visit_cell(
                        &reader.cell_at(pos),
                        &entry.query_edges,
                        terms_filter,
                        edge_cache,
                        &mut matches,
                    ) {
                        return matches.doc_ids;
                    }
                }
            }
        }

        if let Some(boundary) = &self.boundary {
            self.flood_fill(boundary, reader, terms_filter, edge_cache, &mut matches);
        }
        matches.doc_ids
    }

    /// Check candidate geometries for containment or an edge pair within the requested distance.
    /// A stored cell may extend beyond the flood cell, so overlap alone cannot establish a match.
    /// A failed check leaves the geometry eligible for later cells with different edge pairs.
    fn visit_cell<'a>(
        &self,
        cell: &ShapeCell,
        query_edges: &[u32],
        terms_filter: Option<&BitSet>,
        edge_cache: &mut EdgeCache<'a, S>,
        matches: &mut Matches,
    ) -> bool {
        for clipped in &cell.shapes {
            let gid = clipped.geometry_id.1;
            if matches.seen.contains(gid) {
                continue;
            }
            let located = edge_cache.locate(clipped.geometry_id);
            if terms_filter.is_some_and(|filter| !filter.contains(located.doc_id)) {
                matches.seen.insert(gid);
                continue;
            }
            if matches.doc_ids.contains(located.doc_id) {
                matches.seen.insert(gid);
                continue;
            }
            if located.vertex_count == 0 {
                continue;
            }
            let mut found = false;
            if !matches.containment_tested.contains(gid) {
                matches.containment_tested.insert(gid);
                found = self.contains(&located.vertex(0));
            }
            if !found {
                'pairs: for &candidate_edge in &clipped.edge_indices {
                    let (a, b) = located.edge(candidate_edge);
                    for &query_edge in query_edges {
                        let (c, d) = self.edge(query_edge);
                        if let Some(max_distance) = self.max_distance {
                            let mut distance = S1ChordAngle::infinity();
                            update_edge_pair_min_distance(
                                &xyz(&a),
                                &xyz(&b),
                                &xyz(&c),
                                &xyz(&d),
                                &mut distance,
                            );
                            found = distance <= max_distance;
                        } else {
                            found = S::EdgeCrosser::new(&a, &b).crossing_sign_two(&c, &d) > 0;
                        }
                        if found {
                            break 'pairs;
                        }
                    }
                }
            }
            if found {
                matches.seen.insert(gid);
                matches.doc_ids.insert(located.doc_id);
                if self.first_only {
                    return true;
                }
            }
        }
        false
    }

    fn flood_fill<'a>(
        &self,
        boundary: &DistanceBoundary,
        reader: &'a CellIndexReader<'a>,
        terms_filter: Option<&BitSet>,
        edge_cache: &mut EdgeCache<'a, S>,
        matches: &mut Matches,
    ) {
        let mut visited = HashSet::new();
        let mut heap = BinaryHeap::new();
        for &cell_id in &boundary.cells {
            visited.insert(cell_id);
            heap.push(SweepEntry {
                chord_to_nearest: 0.0,
                cell_id,
                search_threshold: boundary.search_threshold::<S>(cell_id),
            });
        }
        let mut query_edges = Vec::new();
        while let Some(sweep) = heap.pop() {
            let center = xyz(&S::cell_center(sweep.cell_id));
            let threshold = sweep.search_threshold;
            let (start, end) = reader.range_for_cell(sweep.cell_id, 0, reader.cell_count());
            if start != end {
                query_edges.clear();
                boundary
                    .tree
                    .search_within(&center, threshold, &mut |edges| {
                        query_edges.extend_from_slice(edges);
                        true
                    });
                query_edges.sort_unstable();
                query_edges.dedup();
                for pos in start..end {
                    if self.visit_cell(
                        &reader.cell_at(pos),
                        &query_edges,
                        terms_filter,
                        edge_cache,
                        matches,
                    ) {
                        return;
                    }
                }
            }

            // A shortest arc from the query to a point within D stays within D along its length.
            // Every grid cell on that arc passes the distance bound, so the flood can reach it.
            // The four edge neighbors maintain connectivity across cube faces. Wrapped diagonals
            // may add extra cells at cube corners; visit_cell still checks their geometry.
            let (_, i, j, _) = sweep.cell_id.to_face_ij_orientation();
            let size = S2CellId::size_ij_for_level(boundary.level);
            for di in -1i32..=1 {
                for dj in -1i32..=1 {
                    if di == 0 && dj == 0 {
                        continue;
                    }
                    let neighbor = S2CellId::from_face_ij_wrap(
                        sweep.cell_id.face(),
                        i + di * size,
                        j + dj * size,
                    )
                    .parent(boundary.level);
                    if !visited.insert(neighbor) {
                        continue;
                    }
                    let center = xyz(&S::cell_center(neighbor));
                    if let Some((chord, _)) = boundary.tree.nearest(&center) {
                        let search_threshold = boundary.search_threshold::<S>(neighbor);
                        if chord <= search_threshold {
                            heap.push(SweepEntry {
                                chord_to_nearest: chord,
                                cell_id: neighbor,
                                search_threshold,
                            });
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
#[path = "tests/distance_tests.rs"]
mod tests;

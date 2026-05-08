//! Polygon-intersects-polygon query.
//!
//! Single-pass query using bitsets for bookkeeping. For each candidate geometry encountered during
//! the covering walk: first-encounter containment tests (both directions), then edge crossing tests
//! in every cell where the geometry appears. A hit bitset skips confirmed geometries. A
//! containment-tested bitset ensures the containment tests run at most once per geometry.

use std::collections::BinaryHeap;

use common::BitSet;

use super::cell_index_reader::CellIndexReader;
use super::contains_query::QueryEdgeProvider;
use super::edge_cache::EdgeCache;
use super::edge_crosser::EdgeCrosser;
use super::geometry_set::GeometrySet;
use super::r2rect::R2Rect;
use super::region_coverer::CovererOptions;
use super::s2cell_id::S2CellId;
use super::s2padded_cell::S2PaddedCell;
use super::shape_index_region::{index_contains_point, EdgeProvider};
use super::surface::Surface;
use crate::spatial::clip_options::ClipOptions;
use crate::spatial::clipper::{Clipper, CELL_PADDING};
use crate::spatial::s2edge_clipping::clip_edge_bound;
use crate::spatial::shape_index::ShapeIndex;

/// Prepared intersects query, built once from a query polygon and applied per-segment.
pub struct IntersectsQuery<S: Surface> {
    query_index: ShapeIndex,
    query_edges: QueryEdgeProvider<S>,
}

impl<S: Surface> IntersectsQuery<S> {
    /// Build the query from a smashed GeometrySet.
    pub fn new(set: GeometrySet<S>, _options: CovererOptions) -> Self {
        let builder = Clipper::new(ClipOptions::default());
        let query_index = builder.build(std::slice::from_ref(&set));

        let query_edges = QueryEdgeProvider { set };

        Self {
            query_index,
            query_edges,
        }
    }

    /// Search one segment for geometries that intersect the query polygon.
    pub fn search_segment<'a>(
        &self,
        cell_reader: &'a CellIndexReader<'a>,
        edge_cache: &mut EdgeCache<'a, S>,
    ) -> Vec<u32> {
        self.search_segment_inner(cell_reader, None, edge_cache)
    }

    /// Search one segment, skipping shapes whose doc_id is not in the filter bitset.
    pub fn search_segment_filtered<'a>(
        &self,
        cell_reader: &'a CellIndexReader<'a>,
        edge_cache: &mut EdgeCache<'a, S>,
        terms_filter: &BitSet,
    ) -> Vec<u32> {
        self.search_segment_inner(cell_reader, Some(terms_filter), edge_cache)
    }

    fn search_segment_inner<'a>(
        &self,
        reader: &'a CellIndexReader<'a>,
        terms_filter: Option<&BitSet>,
        edge_cache: &mut EdgeCache<'a, S>,
    ) -> Vec<u32> {
        let geometry_count = edge_cache.geometry_count(0);
        let mut seen = BitSet::with_max_value(geometry_count);
        let mut containment_tested = BitSet::with_max_value(geometry_count);
        let mut doc_ids = Vec::new();
        let mut hits_candidate_contains = 0u64;
        let mut hits_query_contains = 0u64;
        let mut hits_crossing = 0u64;
        let mut total_tested = 0u64;
        let mut crossing_tests = 0u64;
        let mut index_cells_visited = 0u64;

        eprintln!("query index: {} cells", self.query_index.cells.len());
        for qc in &self.query_index.cells {
            let edges: usize = qc.shapes.iter().map(|s| s.edge_indices.len()).sum();
            let cc = qc.shapes.iter().any(|s| s.contains_center);
            eprintln!(
                "  cell {} face {} level {} edges {} contains_center {}",
                qc.cell_id.0, qc.cell_id.face(), qc.cell_id.level(), edges, cc,
            );
        }

        // Pre-scan: find geometries that contain the query point. A closed geometry
        // with contains_center and no edges in the query point cell fully contains the
        // query point. Include these before the covering walk.
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
                    hits_candidate_contains += 1;
                    seen.insert(gid);
                    doc_ids.push(located.doc_id);
                }
            }
        }

        let mut interior_hits = 0u64;
        let mut dropped = 0u64;
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

            let (index_start, index_end) =
                reader.range_for_cell(cell_id, 0, reader.cell_count());

            if index_start == index_end && query_edge_indices.is_empty() && !contains_center {
                continue;
            }

            let first_index_level = if index_start < index_end {
                reader.cell_id_at(index_start).level()
            } else {
                0
            };

            heap.push(CoveringEntry {
                pcell: S2PaddedCell::new(cell_id, CELL_PADDING),
                query_edges: query_edge_indices,
                contains_center,
                index_start,
                index_end,
                first_index_level,
            });
        }

        while let Some(entry) = heap.pop() {
            let is_interior = entry.query_edges.is_empty() && entry.contains_center;
            if is_interior {
                for pos in entry.index_start..entry.index_end {
                    index_cells_visited += 1;
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
                        doc_ids.push(doc_id);
                        interior_hits += 1;
                    }
                }
            } else if entry.pcell.level() + 1 < entry.first_index_level
                && !entry.pcell.id().is_leaf()
            {
                for child in subdivide_entry(&entry, &self.query_edges, reader, &mut dropped) {
                    heap.push(child);
                }
            } else {
                let query_vertices = &self.query_edges.get_edge_set((0, 0)).vertices;
                for pos in entry.index_start..entry.index_end {
                    let cell = reader.cell_at(pos);
                    index_cells_visited += 1;
                    for clipped in &cell.shapes {
                        let gid = clipped.geometry_id.1;

                        if seen.contains(gid) {
                            continue;
                        }

                        // Filter check.
                        if let Some(filter) = terms_filter {
                            let doc_id = edge_cache.doc_id_for(clipped.geometry_id);
                            if !filter.contains(doc_id) {
                                seen.insert(gid);
                                continue;
                            }
                        }

                        // Containment test: run once per geometry.
                        if !containment_tested.contains(gid) {
                            containment_tested.insert(gid);
                            total_tested += 1;

                            let located = edge_cache.locate(clipped.geometry_id);
                            let first_vertex = located.vertex(0);
                            if index_contains_point::<S, QueryEdgeProvider<S>>(
                                &self.query_index,
                                &self.query_edges,
                                (0, 0),
                                &first_vertex,
                            ) {
                                hits_query_contains += 1;
                                seen.insert(gid);
                                doc_ids.push(located.doc_id);
                                continue;
                            }
                        }

                        // Edge crossing test: runs in each boundary child.
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
                                            crossing_tests += 1;
                                            if crosser.crossing_sign_two(qv0, qv1) > 0 {
                                                break 'crossing true;
                                            }
                                        }
                                    }
                                    false
                                };
                                if crossed {
                                    hits_crossing += 1;
                                    seen.insert(gid);
                                    doc_ids.push(located.doc_id);
                                }
                            }
                        }
                    }
                }
            }
        }


        eprintln!(
            "intersects: {} index cells, {} tested, {} interior, {} dropped, {} candidate_contains, {} query_contains, \
             {} crossing ({} tests), {} total hits",
            index_cells_visited,
            total_tested,
            interior_hits,
            dropped,
            hits_candidate_contains,
            hits_query_contains,
            hits_crossing,
            crossing_tests,
            hits_candidate_contains + hits_query_contains + hits_crossing + interior_hits,
        );

        doc_ids
    }
}

/// An entry in the covering descent. Carries the padded cell for splitting, the query edge
/// indices that overlap it, and the range of index cells it covers in the segment directory.
struct CoveringEntry<S: Surface> {
    /// The padded cell. Provides UV bounds, middle for splitting, child traversal order.
    pcell: S2PaddedCell<S>,
    /// Indices of query polygon edges that overlap this cell.
    query_edges: Vec<u32>,
    /// Whether the query polygon contains the center of this cell.
    contains_center: bool,
    /// Start position in the segment cell index directory (inclusive).
    index_start: u32,
    /// End position in the segment cell index directory (exclusive).
    index_end: u32,
    /// Level of the first index cell in the range. Used to decide whether to split.
    first_index_level: i32,
}


impl<S: Surface> PartialEq for CoveringEntry<S> {
    fn eq(&self, other: &Self) -> bool {
        self.pcell.id() == other.pcell.id()
    }
}

impl<S: Surface> Eq for CoveringEntry<S> {}

impl<S: Surface> PartialOrd for CoveringEntry<S> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

// BinaryHeap is a max-heap. Interior cells first, then smallest cell ID.
// Interior: no query edges and contains_center.
impl<S: Surface> Ord for CoveringEntry<S> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let self_interior = self.query_edges.is_empty() && self.contains_center;
        let other_interior = other.query_edges.is_empty() && other.contains_center;
        self_interior.cmp(&other_interior)
            .then(other.pcell.id().cmp(&self.pcell.id()))
    }
}

/// Subdivide a covering entry into four children. Distributes query edge indices into child
/// quadrants using UV bounds. Computes the index range for each child from the parent range.
/// Discards exterior children (no query edges and not interior).
fn subdivide_entry<S: Surface>(
    entry: &CoveringEntry<S>,
    query_edges: &QueryEdgeProvider<S>,
    reader: &CellIndexReader,
    dropped: &mut u64,
) -> Vec<CoveringEntry<S>> {
    if entry.pcell.id().is_leaf() {
        return vec![];
    }

    let face = entry.pcell.id().face();
    let cell_bound = entry.pcell.bound();
    let middle = entry.pcell.middle();
    let u_mid_lo = middle[0].lo();
    let u_mid_hi = middle[0].hi();
    let v_mid_lo = middle[1].lo();
    let v_mid_hi = middle[1].hi();

    // Distribute query edge indices into child quadrants. Clip each edge's UV bound to the
    // cell's padded bound before classifying, matching the interleaver's tightening step.
    let mut child_edges: [[Vec<u32>; 2]; 2] = Default::default();

    for &edge_idx in &entry.query_edges {
        let (v0, v1) = query_edges.get_edge((0, 0), edge_idx);
        let (a_uv, b_uv) = match S::clip_to_face(&v0, &v1, face, CELL_PADDING) {
            Some(ab) => ab,
            None => continue,
        };
        let mut bound = R2Rect::from_point_pair(a_uv, b_uv);
        if !clip_edge_bound(a_uv, b_uv, &cell_bound, &mut bound) {
            continue;
        }

        if bound[0].lo() < u_mid_hi && bound[1].lo() < v_mid_hi {
            child_edges[0][0].push(edge_idx);
        }
        if bound[0].lo() < u_mid_hi && bound[1].hi() > v_mid_lo {
            child_edges[0][1].push(edge_idx);
        }
        if bound[0].hi() > u_mid_lo && bound[1].lo() < v_mid_hi {
            child_edges[1][0].push(edge_idx);
        }
        if bound[0].hi() > u_mid_lo && bound[1].hi() > v_mid_lo {
            child_edges[1][1].push(edge_idx);
        }
    }

    // Find the traversal order and child cell IDs.
    let mut child_ij: [(usize, usize); 4] = [(0, 0); 4];
    let mut child_ids: [S2CellId; 4] = [S2CellId(0); 4];
    for pos in 0..4 {
        let (i, j) = entry.pcell.get_child_ij(pos);
        child_ij[pos as usize] = (i, j);
        child_ids[pos as usize] = entry.pcell.id().child(pos);
    }

    // Find the 3 interior range boundaries. starts[0] is the parent's start.
    let mut starts = [0u32; 4];
    starts[0] = entry.index_start;
    for k in 1..4 {
        starts[k] = reader.start_for_cell(child_ids[k], starts[k - 1], entry.index_end);
    }

    let parent_center = entry.pcell.get_center();

    let mut result = Vec::with_capacity(4);
    for k in 0..4 {
        let (i, j) = child_ij[k];
        let edges = std::mem::take(&mut child_edges[i][j]);
        let start = starts[k];
        let end = if k < 3 { starts[k + 1] } else { entry.index_end };

        // Determine contains_center for this child by testing edge crossings from
        // parent center to child center, starting from the parent's contains_center.
        let child_pcell = S2PaddedCell::from_parent(&entry.pcell, i, j);
        let child_center = child_pcell.get_center();
        let mut contains_center = entry.contains_center;
        if !edges.is_empty() {
            let mut crosser = S::EdgeCrosser::new(&parent_center, &child_center);
            for &edge_idx in &edges {
                let (v0, v1) = query_edges.get_edge((0, 0), edge_idx);
                if crosser.edge_or_vertex_crossing_two(&v0, &v1) {
                    contains_center = !contains_center;
                }
            }
        }

        let is_interior = edges.is_empty() && contains_center;

        // Skip: empty index range, or exterior (no query edges and not interior).
        if start == end || (edges.is_empty() && !is_interior) {
            *dropped += 1;
            continue;
        }

        let first_index_level = if start < end {
            reader.cell_id_at(start).level()
        } else {
            0
        };

        result.push(CoveringEntry {
            pcell: child_pcell,
            query_edges: edges,
            contains_center,
            index_start: start,
            index_end: end,
            first_index_level,
        });
    }

    result
}

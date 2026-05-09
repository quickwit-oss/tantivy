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

    /// Search one segment using the contains predicate.
    pub fn contains_segment<'a>(
        &self,
        cell_reader: &'a CellIndexReader<'a>,
        edge_cache: &mut EdgeCache<'a, S>,
    ) -> Vec<u32> {
        let geometry_count = edge_cache.geometry_count(0);
        let mut predicate = ContainsPredicate::new(geometry_count);
        self.search_with_predicate(cell_reader, None, edge_cache, &mut predicate);
        predicate.into_doc_ids()
    }

    fn search_segment_inner<'a>(
        &self,
        reader: &'a CellIndexReader<'a>,
        terms_filter: Option<&BitSet>,
        edge_cache: &mut EdgeCache<'a, S>,
    ) -> Vec<u32> {
        let geometry_count = edge_cache.geometry_count(0);
        let mut predicate = IntersectsPredicate::new(geometry_count);
        self.search_with_predicate(reader, terms_filter, edge_cache, &mut predicate);
        predicate.doc_ids().to_vec()
    }

    fn search_with_predicate<'a>(
        &self,
        reader: &'a CellIndexReader<'a>,
        terms_filter: Option<&BitSet>,
        edge_cache: &mut EdgeCache<'a, S>,
        predicate: &mut impl Predicate,
    ) {
        let geometry_count = edge_cache.geometry_count(0);
        let mut containment_tested = BitSet::with_max_value(geometry_count);

        let query_vertex = &self.query_edges.get_edge_set((0, 0)).vertices[0];
        let query_point_cell_id = S::cell_id_from_point(query_vertex);
        predicate.scan::<S>(query_point_cell_id, reader, edge_cache, terms_filter);

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
                    let cell = reader.cell_at(pos);
                    for clipped in &cell.shapes {
                        let gid = clipped.geometry_id.1;
                        if predicate.is_resolved(gid) {
                            continue;
                        }
                        let doc_id = edge_cache.doc_id_for(clipped.geometry_id);
                        if let Some(filter) = terms_filter {
                            if !filter.contains(doc_id) {
                                predicate.exclude(gid);
                                continue;
                            }
                        }
                        if doc_id == 5181405 {
                            eprintln!(
                                "TRACE doc_id=5181405 gid={} edges={} contains_center={} cell={} level={}",
                                gid, clipped.edge_indices.len(), clipped.contains_center,
                                cell.cell_id.0, cell.cell_id.level(),
                            );
                        }
                        predicate.interior(gid, doc_id, !clipped.edge_indices.is_empty(), clipped.contains_center);
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
                    for clipped in &cell.shapes {
                        let gid = clipped.geometry_id.1;

                        if predicate.is_resolved(gid) {
                            continue;
                        }

                        if let Some(filter) = terms_filter {
                            let doc_id = edge_cache.doc_id_for(clipped.geometry_id);
                            if !filter.contains(doc_id) {
                                predicate.exclude(gid);
                                continue;
                            }
                        }

                        if predicate.test_containment() && !containment_tested.contains(gid) {
                            containment_tested.insert(gid);

                            let located = edge_cache.locate(clipped.geometry_id);
                            let first_vertex = located.vertex(0);
                            if index_contains_point::<S, QueryEdgeProvider<S>>(
                                &self.query_index,
                                &self.query_edges,
                                (0, 0),
                                &first_vertex,
                            ) {
                                predicate.interior(gid, located.doc_id, false, true);
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
                                predicate.crossing(gid, located.doc_id, crossed);
                            }
                        }
                    }
                }
            }
        }

    }
}

/// Predicate for the covering descent. Implementations decide what to do with geometries
/// encountered in interior cells, boundary cells with crossings, and filtered geometries.
trait Predicate {
    /// Pre-scan the segment for geometries that contain the query point.
    fn scan<S: Surface>(
        &mut self,
        query_point_cell_id: S2CellId,
        reader: &CellIndexReader,
        edge_cache: &EdgeCache<S>,
        terms_filter: Option<&BitSet>,
    );
    /// A geometry was found in an interior cell.
    fn interior(&mut self, gid: u32, doc_id: u32, has_edges: bool, contains_center: bool);
    /// A geometry's edge crossed a query edge in a boundary cell.
    fn crossing(&mut self, gid: u32, doc_id: u32, crossed: bool);
    /// Whether to run the containment test (first vertex inside query).
    fn test_containment(&self) -> bool;
    /// A geometry was rejected by the terms filter or other condition.
    fn exclude(&mut self, gid: u32);
    /// Whether this geometry has already been resolved (hit or rejected).
    fn is_resolved(&self, gid: u32) -> bool;
}

/// Intersects predicate: interior and crossing are both hits.
struct IntersectsPredicate {
    seen: BitSet,
    doc_ids: Vec<u32>,
}

impl IntersectsPredicate {
    fn new(geometry_count: u32) -> Self {
        IntersectsPredicate {
            seen: BitSet::with_max_value(geometry_count),
            doc_ids: Vec::new(),
        }
    }
}

impl Predicate for IntersectsPredicate {
    fn scan<S: Surface>(
        &mut self,
        query_point_cell_id: S2CellId,
        reader: &CellIndexReader,
        edge_cache: &EdgeCache<S>,
        terms_filter: Option<&BitSet>,
    ) {
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
                            self.exclude(gid);
                            continue;
                        }
                    }
                    self.interior(gid, located.doc_id, false, true);
                }
            }
        }
    }

    fn interior(&mut self, gid: u32, doc_id: u32, _has_edges: bool, _contains_center: bool) {
        self.seen.insert(gid);
        self.doc_ids.push(doc_id);
    }

    fn crossing(&mut self, gid: u32, doc_id: u32, crossed: bool) {
        if crossed {
            self.seen.insert(gid);
            self.doc_ids.push(doc_id);
        }
    }

    fn test_containment(&self) -> bool {
        true
    }

    fn exclude(&mut self, gid: u32) {
        self.seen.insert(gid);
    }

    fn is_resolved(&self, gid: u32) -> bool {
        self.seen.contains(gid)
    }
}

impl IntersectsPredicate {
    fn doc_ids(&self) -> &[u32] {
        &self.doc_ids
    }
}

/// Contains predicate (Lucene CONTAINS / PostGIS ST_Contains): the query polygon fully
/// contains the indexed geometry. Interior cells include geometries with no edges. A crossing
/// in any boundary cell rejects the geometry.
/// Contains predicate (Lucene CONTAINS / PostGIS ST_Contains): the query polygon fully
/// contains the indexed geometry. Interior cells include geometries with no edges — a geometry
/// with edges in an interior cell has its boundary inside the query, so it extends beyond.
/// A crossing in any boundary cell rejects the geometry.
struct ContainsPredicate {
    included: BitSet,
    excluded: BitSet,
    candidates: Vec<(u32, u32)>,
}

impl ContainsPredicate {
    fn new(geometry_count: u32) -> Self {
        ContainsPredicate {
            included: BitSet::with_max_value(geometry_count),
            excluded: BitSet::with_max_value(geometry_count),
            candidates: Vec::new(),
        }
    }
}

impl Predicate for ContainsPredicate {
    fn scan<S: Surface>(
        &mut self,
        _query_point_cell_id: S2CellId,
        _reader: &CellIndexReader,
        _edge_cache: &EdgeCache<S>,
        _terms_filter: Option<&BitSet>,
    ) {
    }

    fn interior(&mut self, gid: u32, doc_id: u32, has_edges: bool, contains_center: bool) {
        if has_edges {
            self.excluded.insert(gid);
        } else if contains_center && !self.excluded.contains(gid) && !self.included.contains(gid) {
            self.included.insert(gid);
            self.candidates.push((gid, doc_id));
        }
    }

    fn crossing(&mut self, gid: u32, _doc_id: u32, crossed: bool) {
        if crossed {
            self.excluded.insert(gid);
        }
    }

    fn test_containment(&self) -> bool {
        false
    }

    fn exclude(&mut self, gid: u32) {
        self.excluded.insert(gid);
    }

    fn is_resolved(&self, gid: u32) -> bool {
        self.excluded.contains(gid)
    }
}

impl ContainsPredicate {
    fn into_doc_ids(self) -> Vec<u32> {
        self.candidates
            .into_iter()
            .filter(|(gid, _)| !self.excluded.contains(*gid))
            .map(|(_, doc_id)| doc_id)
            .collect()
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

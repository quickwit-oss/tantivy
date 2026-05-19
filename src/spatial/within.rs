//! Within query: find indexed geometries entirely inside the query polygon.

use std::collections::BinaryHeap;

use common::BitSet;

use super::cell_index_reader::CellIndexReader;
use super::covering::{covering_contains_center, covering_split, CoveringCell};
use super::edge_cache::EdgeCache;
use super::edge_crosser::EdgeCrosser;
use super::edge_provider::EdgeProvider;
use super::geometry_set::GeometrySet;
use super::query_edge_provider::QueryEdgeProvider;
use super::region_coverer::CovererOptions;
use super::s2cell_id::S2CellId;
use super::s2padded_cell::S2PaddedCell;
use super::shape_index_region::index_contains_point;
use super::surface::Surface;
use crate::spatial::clip_options::ClipOptions;
use crate::spatial::clipper::Clipper;
use crate::spatial::shape_index::ShapeIndex;

/// One query polygon with its CellIndex and edge provider.
struct QueryPolygon<S: Surface> {
    index: ShapeIndex,
    edges: QueryEdgeProvider<S>,
}

/// Prepared within query. For a single polygon query, there is one QueryPolygon. For a
/// multi-polygon, there is one per member, sorted by vertex count descending.
pub struct Within<S: Surface> {
    polygons: Vec<QueryPolygon<S>>,
}

impl<S: Surface> Within<S> {
    /// Build the query from a smashed GeometrySet.
    pub fn new(set: GeometrySet<S>, _options: CovererOptions) -> Self {
        let mut members: Vec<_> = set.members.into_iter().enumerate().collect();
        members.sort_by(|a, b| b.1.vertices.len().cmp(&a.1.vertices.len()));

        let mut polygons = Vec::with_capacity(members.len());
        for (_original_index, member) in members {
            let member_set = GeometrySet {
                members: vec![member],
                doc_id: set.doc_id,
            };
            let builder = Clipper::new(ClipOptions::default());
            let index = builder.build(std::slice::from_ref(&member_set));
            let edges = QueryEdgeProvider { set: member_set };
            polygons.push(QueryPolygon { index, edges });
        }

        Self { polygons }
    }

    /// Search one segment for geometries entirely within the query polygon.
    pub fn search<'a>(
        &'a self,
        reader: &'a CellIndexReader<'a>,
        terms_filter: Option<&'a BitSet>,
        edge_cache: &mut EdgeCache<'a, S>,
        max_doc: u32,
    ) -> BitSet {
        let geometry_count = edge_cache.geometry_count(0);
        let included: Vec<BitSet> = (0..self.polygons.len())
            .map(|_| BitSet::with_max_value(geometry_count))
            .collect();
        let mut search = WithinSearch {
            polygons: &self.polygons,
            reader,
            edge_cache,
            terms_filter,
            doc_ids: BitSet::with_max_value(max_doc),
            excluded: BitSet::with_max_value(geometry_count),
            included,
        };

        for poly_idx in 0..self.polygons.len() {
            search.search_polygon(poly_idx);
        }

        search.doc_ids
    }
}

/// Mutable search state for a within query against one segment.
struct WithinSearch<'a, 'b, S: Surface> {
    polygons: &'a [QueryPolygon<S>],
    reader: &'a CellIndexReader<'a>,
    edge_cache: &'b mut EdgeCache<'a, S>,
    terms_filter: Option<&'a BitSet>,
    doc_ids: BitSet,
    excluded: BitSet,
    included: Vec<BitSet>,
}

impl<'a, 'b, S: Surface> WithinSearch<'a, 'b, S> {
    fn exclude_set(&mut self, geometry_id: (u16, u32)) {
        let entry = self.edge_cache.get(geometry_id);
        let doc_id = entry.doc_id();
        let head = entry.head();
        let count = entry.member_count();
        for i in 0..count {
            self.excluded.insert(head + i);
        }
        self.doc_ids.remove(doc_id);
    }

    // When a geometry is identified as a candidate, check its siblings. For single-member sets,
    // just insert the doc_id. For multi-member sets, dispatch each sibling to a query polygon via
    // first-vertex containment.
    fn include_candidate(&mut self, geometry_id: (u16, u32), poly_idx: usize) {
        let entry = self.edge_cache.get(geometry_id);
        let doc_id = entry.doc_id();
        let head = entry.head();
        let count = entry.member_count();

        self.included[poly_idx].insert(geometry_id.1);
        self.doc_ids.insert(doc_id);

        if count == 1 {
            return;
        }

        for i in 0..count {
            let sibling_gid = head + i;
            if sibling_gid == geometry_id.1 {
                continue;
            }
            if self.excluded.contains(sibling_gid) {
                self.exclude_set(geometry_id);
                return;
            }

            let sibling_id = (geometry_id.0, sibling_gid);
            let mut included = false;
            for pi in 0..self.polygons.len() {
                if self.included[pi].contains(sibling_gid) {
                    included = true;
                    break;
                }
                if pi < poly_idx {
                    continue;
                }
                let located = self.edge_cache.locate(sibling_id);
                let first_vertex = located.vertex(0);
                if index_contains_point::<S, QueryEdgeProvider<S>>(
                    &self.polygons[pi].index,
                    &self.polygons[pi].edges,
                    (0, 0),
                    &first_vertex,
                ) {
                    self.included[pi].insert(sibling_gid);
                    included = true;
                    break;
                }
            }

            if !included {
                self.exclude_set(geometry_id);
                return;
            }
        }
    }

    fn search_polygon(&mut self, poly_idx: usize) {
        let polygon = &self.polygons[poly_idx];
        let geometry_count = self.edge_cache.geometry_count(0);
        let mut containment_tested = BitSet::with_max_value(geometry_count);

        let mut heap = BinaryHeap::new();

        for query_cell in &polygon.index.cells {
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
                self.reader
                    .range_for_cell(cell_id, 0, self.reader.cell_count());

            if index_start == index_end && query_edge_indices.is_empty() && !contains_center {
                continue;
            }

            let first_index_level = if index_start < index_end {
                self.reader.cell_id_at(index_start).level()
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

        while let Some(entry) = heap.pop() {
            if entry.query_edges.is_empty()
                && entry.contains_center
                && entry.first_index_level > entry.pcell.level()
            {
                for pos in entry.index_start..entry.index_end {
                    let cell = self.reader.cell_at(pos);
                    for clipped in &cell.shapes {
                        if clipped.edge_indices.is_empty() {
                            continue;
                        }
                        let gid = clipped.geometry_id.1;
                        if self.excluded.contains(gid) || self.included[poly_idx].contains(gid) {
                            continue;
                        }
                        let doc_id = self.edge_cache.doc_id_for(clipped.geometry_id);
                        if let Some(filter) = self.terms_filter {
                            if !filter.contains(doc_id) {
                                continue;
                            }
                        }
                        self.include_candidate(clipped.geometry_id, poly_idx);
                    }
                }
            } else if entry.pcell.level() < entry.first_index_level && !entry.pcell.id().is_leaf() {
                let get_edge = |idx: u32| polygon.edges.get_edge((0, 0), idx);
                let start_for_cell = |cell_id: S2CellId, lo: u32, hi: u32| {
                    let start = self.reader.start_for_cell(cell_id, lo, hi);
                    let level = if start < hi {
                        self.reader.cell_id_at(start).level()
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
                let query_vertices = &polygon.edges.get_edge_set((0, 0)).vertices;
                for pos in entry.index_start..entry.index_end {
                    let cell = self.reader.cell_at(pos);
                    for clipped in &cell.shapes {
                        if clipped.edge_indices.is_empty() {
                            continue;
                        }
                        let gid = clipped.geometry_id.1;

                        if self.excluded.contains(gid) {
                            continue;
                        }

                        let doc_id = self.edge_cache.doc_id_for(clipped.geometry_id);
                        if let Some(filter) = self.terms_filter {
                            if !filter.contains(doc_id) {
                                continue;
                            }
                        }

                        if !self.included[poly_idx].contains(gid)
                            && !containment_tested.contains(gid)
                        {
                            containment_tested.insert(gid);
                            let located = self.edge_cache.locate(clipped.geometry_id);
                            let first_vertex = located.vertex(0);
                            if index_contains_point::<S, QueryEdgeProvider<S>>(
                                &polygon.index,
                                &polygon.edges,
                                (0, 0),
                                &first_vertex,
                            ) {
                                self.include_candidate(clipped.geometry_id, poly_idx);
                                if self.excluded.contains(gid) {
                                    continue;
                                }
                            }
                        }

                        if !entry.query_edges.is_empty() {
                            let located = self.edge_cache.locate(clipped.geometry_id);
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
                                    self.exclude_set(clipped.geometry_id);
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

//! Polygon-intersects-polygon query.
//!
//! Single-pass query using bitsets for bookkeeping. For each candidate geometry encountered during
//! the covering walk: first-encounter containment tests (both directions), then edge crossing tests
//! in every cell where the geometry appears. A hit bitset skips confirmed geometries. A
//! containment-tested bitset ensures the containment tests run at most once per geometry.

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

/// Prepared intersects query, built once from a query polygon and applied per-segment.
pub struct Intersects<S: Surface> {
    query_index: ShapeIndex,
    query_edges: QueryEdgeProvider<S>,
}

impl<S: Surface> Intersects<S> {
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

        while let Some(entry) = heap.pop() {
            if entry.query_edges.is_empty()
                && entry.contains_center
                && entry.first_index_level > entry.pcell.level()
            {
                if !reader.doc_ids_data.is_empty() && terms_filter.is_none() {
                    reader.visit_doc_ids(entry.index_start, entry.index_end, |doc_id| {
                        doc_ids.insert(doc_id);
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
                                }
                            }
                        }
                    }
                }
            }
        }

        doc_ids
    }
}

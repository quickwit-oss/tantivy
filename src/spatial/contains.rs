//! Contains query: find indexed geometries that contain the query polygon.

use std::collections::VecDeque;

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
use super::surface::Surface;
use crate::spatial::clip_options::ClipOptions;
use crate::spatial::clipped_shape::ClippedShape;
use crate::spatial::clipper::Clipper;
use crate::spatial::shape_index::ShapeIndex;

/// Prepared contains query, built once from a query polygon and applied per-segment.
pub struct Contains<S: Surface> {
    query_index: ShapeIndex,
    query_edges: QueryEdgeProvider<S>,
}

impl<S: Surface> Contains<S> {
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

    /// Search one segment for geometries that contain the query polygon.
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

        let candidates = self.find_candidates(reader, terms_filter, edge_cache, &mut seen);
        if candidates.is_empty() {
            return doc_ids;
        }

        for &(_gid, doc_id) in &candidates {
            doc_ids.insert(doc_id);
        }

        self.exclude_by_crossings(reader, edge_cache, &candidates, &mut seen, &mut doc_ids);

        doc_ids
    }

    /// Find candidate geometries by locating an interior covering cell and reading
    /// the segment's cell index there. Geometries with contains_center and no edges
    /// contain that cell, which is inside the query, so they contain a query point.
    fn find_candidates<'a>(
        &self,
        reader: &'a CellIndexReader<'a>,
        terms_filter: Option<&BitSet>,
        edge_cache: &mut EdgeCache<'a, S>,
        seen: &mut BitSet,
    ) -> Vec<(u32, u32)> {
        let interior_cell = match self.find_interior_cell(reader) {
            Some(cell) => cell,
            None => return vec![],
        };

        let target_id = interior_cell.pcell.id();
        let mut candidates = Vec::new();

        for pos in interior_cell.index_start..interior_cell.index_end {
            let cell = reader.cell_at(pos);
            let index_level = cell.cell_id.level();

            for clipped in &cell.shapes {
                let gid = clipped.geometry_id.1;
                if seen.contains(gid) {
                    continue;
                }
                seen.insert(gid);

                let located = edge_cache.locate(clipped.geometry_id);
                if !located.closed {
                    continue;
                }
                if let Some(filter) = terms_filter {
                    if !filter.contains(located.doc_id) {
                        continue;
                    }
                }

                if clipped.edge_indices.is_empty() {
                    if clipped.contains_center {
                        candidates.push((gid, located.doc_id));
                    }
                    continue;
                }

                if index_level >= target_id.level() {
                    continue;
                }

                if let Some(doc_id) =
                    self.split_to_interior(clipped, cell.cell_id, target_id, edge_cache)
                {
                    candidates.push((gid, doc_id));
                }
            }
        }

        candidates
    }

    /// Split a single geometry from a coarse index cell down toward the target
    /// covering cell. At each level, only chase the child on the path to target_id.
    /// If the geometry's edges drop away before reaching the target, contains_center
    /// is the answer. If edges remain at the target level, not a candidate.
    fn split_to_interior<'a>(
        &self,
        clipped: &ClippedShape,
        index_cell_id: S2CellId,
        target_id: S2CellId,
        edge_cache: &mut EdgeCache<'a, S>,
    ) -> Option<u32> {
        let edge_indices: Vec<u32> = clipped.edge_indices.clone();
        let located = edge_cache.locate(clipped.geometry_id);
        let doc_id = located.doc_id;

        let get_edge = |idx: u32| {
            let entry = edge_cache.locate(clipped.geometry_id);
            entry.edge(idx)
        };
        let no_op_start = |_cell_id: S2CellId, _lo: u32, _hi: u32| -> (u32, i32) { (0, 0) };

        let mut current = CoveringCell {
            pcell: S2PaddedCell::<S>::new(index_cell_id, S::CELL_PADDING),
            query_edges: edge_indices,
            contains_center: clipped.contains_center,
            index_start: 0,
            index_end: 0,
            first_index_level: 0,
        };

        loop {
            if current.query_edges.is_empty() {
                return if current.contains_center {
                    Some(doc_id)
                } else {
                    None
                };
            }

            if current.pcell.id().level() >= target_id.level() || current.pcell.id().is_leaf() {
                return None;
            }

            let parent_center = current.pcell.get_center();
            let mut children = covering_split(&current, &get_edge, &no_op_start);
            covering_contains_center::<S>(&parent_center, &mut children, &get_edge);

            let mut found_child = false;
            for child in children {
                if child.pcell.id().contains(target_id) || child.pcell.id() == target_id {
                    current = child;
                    found_child = true;
                    break;
                }
            }

            if !found_child {
                return None;
            }
        }
    }

    /// Find an interior covering cell. First scan the query CellIndex for one that
    /// already exists. If none, breadth-first split boundary cells until an interior
    /// child appears.
    fn find_interior_cell(&self, reader: &CellIndexReader) -> Option<CoveringCell<S>> {
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

        let mut queue: VecDeque<CoveringCell<S>> = VecDeque::new();

        for query_cell in &self.query_index.cells {
            let mut query_edge_indices: Vec<u32> = Vec::new();
            let mut contains_center = false;
            for shape in &query_cell.shapes {
                query_edge_indices.extend_from_slice(&shape.edge_indices);
                if shape.contains_center {
                    contains_center = true;
                }
            }

            if query_edge_indices.is_empty() && contains_center {
                let (index_start, index_end) =
                    reader.range_for_cell(query_cell.cell_id, 0, reader.cell_count());
                if index_start < index_end {
                    return Some(CoveringCell {
                        pcell: S2PaddedCell::new(query_cell.cell_id, S::CELL_PADDING),
                        query_edges: query_edge_indices,
                        contains_center,
                        index_start,
                        index_end,
                        first_index_level: reader.cell_id_at(index_start).level(),
                    });
                }
            }

            let (index_start, index_end) =
                reader.range_for_cell(query_cell.cell_id, 0, reader.cell_count());
            if index_start == index_end {
                continue;
            }

            queue.push_back(CoveringCell {
                pcell: S2PaddedCell::new(query_cell.cell_id, S::CELL_PADDING),
                query_edges: query_edge_indices,
                contains_center,
                index_start,
                index_end,
                first_index_level: reader.cell_id_at(index_start).level(),
            });
        }

        while let Some(entry) = queue.pop_front() {
            let parent_center = entry.pcell.get_center();
            let mut children = covering_split(&entry, &get_edge, &start_for_cell);
            covering_contains_center::<S>(&parent_center, &mut children, &get_edge);

            for child in children {
                if child.query_edges.is_empty() && child.contains_center {
                    if child.index_start < child.index_end {
                        return Some(child);
                    }
                } else if !child.query_edges.is_empty() && !child.pcell.id().is_leaf() {
                    queue.push_back(child);
                }
            }
        }

        None
    }

    /// Walk all boundary covering cells and exclude candidates whose edges cross
    /// query edges.
    fn exclude_by_crossings<'a>(
        &self,
        reader: &'a CellIndexReader<'a>,
        edge_cache: &mut EdgeCache<'a, S>,
        candidates: &[(u32, u32)],
        _seen: &mut BitSet,
        doc_ids: &mut BitSet,
    ) {
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

        let geometry_count = edge_cache.geometry_count(0);
        let mut candidate_gids = BitSet::with_max_value(geometry_count);
        for &(gid, _) in candidates {
            candidate_gids.insert(gid);
        }

        let mut heap = std::collections::BinaryHeap::new();

        for query_cell in &self.query_index.cells {
            let mut query_edge_indices: Vec<u32> = Vec::new();
            let mut contains_center = false;
            for shape in &query_cell.shapes {
                query_edge_indices.extend_from_slice(&shape.edge_indices);
                if shape.contains_center {
                    contains_center = true;
                }
            }

            if query_edge_indices.is_empty() {
                continue;
            }

            let (index_start, index_end) =
                reader.range_for_cell(query_cell.cell_id, 0, reader.cell_count());
            if index_start == index_end {
                continue;
            }

            heap.push(CoveringCell {
                pcell: S2PaddedCell::new(query_cell.cell_id, S::CELL_PADDING),
                query_edges: query_edge_indices,
                contains_center,
                index_start,
                index_end,
                first_index_level: reader.cell_id_at(index_start).level(),
            });
        }

        let query_vertices = &self.query_edges.get_edge_set((0, 0)).vertices;

        while let Some(entry) = heap.pop() {
            if entry.query_edges.is_empty() {
                continue;
            }

            if entry.pcell.level() < entry.first_index_level && !entry.pcell.id().is_leaf() {
                let parent_center = entry.pcell.get_center();
                let mut children = covering_split(&entry, &get_edge, &start_for_cell);
                covering_contains_center::<S>(&parent_center, &mut children, &get_edge);
                for child in children {
                    heap.push(child);
                }
            } else {
                for pos in entry.index_start..entry.index_end {
                    let cell = reader.cell_at(pos);
                    for clipped in &cell.shapes {
                        let gid = clipped.geometry_id.1;
                        if !candidate_gids.contains(gid) {
                            continue;
                        }
                        if clipped.edge_indices.is_empty() {
                            continue;
                        }

                        let located = edge_cache.locate(clipped.geometry_id);
                        if located.vertex_count < 2 {
                            continue;
                        }

                        for &candidate_edge_idx in &clipped.edge_indices {
                            let (cv0, cv1) = located.edge(candidate_edge_idx);
                            let mut crosser = S::EdgeCrosser::new(&cv0, &cv1);
                            for &query_edge_idx in &entry.query_edges {
                                let qi = query_edge_idx as usize;
                                let qv0 = &query_vertices[qi];
                                let qv1 = &query_vertices[qi + 1];
                                if crosser.crossing_sign_two(qv0, qv1) > 0 {
                                    let doc_id = edge_cache.doc_id_for(clipped.geometry_id);
                                    doc_ids.remove(doc_id);
                                    candidate_gids.remove(gid);
                                    break;
                                }
                            }
                            if !candidate_gids.contains(gid) {
                                break;
                            }
                        }
                    }
                }
            }
        }
    }
}

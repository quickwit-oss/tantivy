//! Polygon-intersects-polygon query.
//!
//! Shares infrastructure with contains_query. Differs in verification: any overlap between the
//! query polygon and a candidate is sufficient.

use std::collections::HashMap;

use common::BitSet;

use super::cell_index::{BuildOptions, CellIndex, IndexBuilder};
use super::cell_index_reader::CellIndexReader;
use super::contains_query::{BoundaryEdges, CandidateInfo, QueryEdgeProvider};
use super::crossings::S2EdgeCrosser;
use super::edge_reader::EdgeReader;
use super::geometry_set::GeometrySet;
use super::region::Region;
use super::region_coverer::{CovererOptions, RegionCoverer};
use super::s2cell::S2Cell;
use super::s2cell_id::S2CellId;
use super::shape_index_region::{
    contains_point, index_contains_point, CellIndexRegion, SegmentIndex,
};

/// Prepared intersects query, built once from a query polygon and applied per-segment.
pub struct IntersectsQuery {
    query_index: CellIndex,
    query_edges: QueryEdgeProvider,
    covering: Vec<S2CellId>,
    interior: Vec<bool>,
}

impl IntersectsQuery {
    /// Build the query from a smashed GeometrySet.
    pub fn new(set: GeometrySet, options: CovererOptions) -> Self {
        let builder = IndexBuilder::new(BuildOptions::default());
        let query_index = builder.build_from_sets(std::slice::from_ref(&set));
        let query_edges = QueryEdgeProvider { set };

        let region = CellIndexRegion::new(&query_index, &query_edges);
        let coverer = RegionCoverer::new(options);
        let covering = coverer.get_covering(&region);

        let interior: Vec<bool> = covering
            .cell_ids()
            .iter()
            .map(|&id| region.contains_cell(&S2Cell::new(id)))
            .collect();

        let covering = covering.into_cell_ids();

        Self {
            query_index,
            query_edges,
            covering,
            interior,
        }
    }

    /// Search one segment for geometries that intersect the query polygon.
    pub fn search_segment<'a>(
        &self,
        cell_reader: &'a CellIndexReader<'a>,
        edge_reader: &mut EdgeReader<'a>,
    ) -> Vec<u32> {
        let candidates = self.collect_candidates(cell_reader, None, edge_reader);
        self.verify_candidates(candidates, cell_reader, edge_reader)
    }

    /// Search one segment, skipping shapes whose doc_id is not in the filter bitset.
    pub fn search_segment_filtered<'a>(
        &self,
        cell_reader: &'a CellIndexReader<'a>,
        edge_reader: &mut EdgeReader<'a>,
        terms_filter: &BitSet,
    ) -> Vec<u32> {
        let candidates = self.collect_candidates(cell_reader, Some(terms_filter), edge_reader);
        self.verify_candidates(candidates, cell_reader, edge_reader)
    }

    fn collect_candidates(
        &self,
        reader: &CellIndexReader,
        terms_filter: Option<&BitSet>,
        edge_reader: &EdgeReader,
    ) -> HashMap<u32, CandidateInfo> {
        let mut candidates: HashMap<u32, CandidateInfo> = HashMap::new();

        for (i, &covering_cell_id) in self.covering.iter().enumerate() {
            let is_interior = self.interior[i];

            for index_cell in reader.scan_range(covering_cell_id) {
                let cell_is_interior = is_interior && covering_cell_id.contains(index_cell.cell_id);

                for clipped in &index_cell.shapes {
                    if let Some(filter) = terms_filter {
                        let doc_id = edge_reader.doc_id_for(clipped.geometry_id);
                        if !filter.contains(doc_id) {
                            continue;
                        }
                    }

                    let info = candidates
                        .entry(clipped.geometry_id)
                        .or_insert_with(CandidateInfo::new);

                    if cell_is_interior {
                        info.has_interior = true;
                    } else {
                        info.has_boundary = true;
                        if !clipped.edge_indices.is_empty() {
                            info.boundary_edges.push(BoundaryEdges {
                                covering_cell_id,
                                edge_indices: clipped.edge_indices.clone(),
                            });
                        }
                    }
                }
            }
        }

        candidates
    }

    fn verify_candidates<'a>(
        &self,
        candidates: HashMap<u32, CandidateInfo>,
        cell_reader: &'a CellIndexReader<'a>,
        edge_reader: &mut EdgeReader<'a>,
    ) -> Vec<u32> {
        let mut doc_ids = Vec::new();

        for (geometry_id, info) in candidates {
            if self.verify_one(geometry_id, &info, cell_reader, edge_reader) {
                let (doc_id, _) = edge_reader.get_edge_set(geometry_id);
                doc_ids.push(doc_id);
            }
        }

        doc_ids
    }

    fn verify_one<'a>(
        &self,
        geometry_id: u32,
        info: &CandidateInfo,
        cell_reader: &'a CellIndexReader<'a>,
        edge_reader: &mut EdgeReader<'a>,
    ) -> bool {
        if info.has_interior {
            let (_, edge_set) = edge_reader.get_edge_set(geometry_id);
            let vertices = &edge_set.vertices;
            if !vertices.is_empty()
                && index_contains_point(&self.query_index, &self.query_edges, 0, &vertices[0])
            {
                return true;
            }
        }

        if info.has_boundary && self.has_crossing(geometry_id, info, edge_reader) {
            return true;
        }

        let (_, edge_set) = edge_reader.get_edge_set(geometry_id);
        let vertices = &edge_set.vertices;
        let closed = edge_set.closed;

        if vertices.is_empty() {
            return false;
        }

        if index_contains_point(&self.query_index, &self.query_edges, 0, &vertices[0]) {
            return true;
        }

        if closed {
            let mut seg = SegmentIndex {
                cell_reader,
                edge_reader,
            };
            if contains_point(&mut seg, geometry_id, &self.query_edges.get_edge_set(0).vertices[0])
            {
                return true;
            }
        }

        false
    }

    fn has_crossing(
        &self,
        geometry_id: u32,
        info: &CandidateInfo,
        edge_reader: &mut EdgeReader,
    ) -> bool {
        let (_, edge_set) = edge_reader.get_edge_set(geometry_id);
        let candidate_vertices = &edge_set.vertices;
        let n_candidate = candidate_vertices.len();

        if n_candidate < 2 {
            return false;
        }

        let query_vertices = &self.query_edges.get_edge_set(0).vertices;

        for boundary in &info.boundary_edges {
            let query_cell = self.query_index.find_cell(boundary.covering_cell_id);
            let query_edge_indices: Vec<u16> = match query_cell {
                Some(cell) => cell
                    .shapes
                    .iter()
                    .flat_map(|s| s.edge_indices.iter().copied())
                    .collect(),
                None => continue,
            };

            if query_edge_indices.is_empty() {
                continue;
            }

            for &candidate_edge_idx in &boundary.edge_indices {
                let ci = candidate_edge_idx as usize;
                let cv0 = &candidate_vertices[ci];
                let cv1 = &candidate_vertices[ci + 1];

                let mut crosser = S2EdgeCrosser::new(cv0, cv1);

                for &query_edge_idx in &query_edge_indices {
                    let qi = query_edge_idx as usize;
                    let qv0 = &query_vertices[qi];
                    let qv1 = &query_vertices[qi + 1];

                    if crosser.crossing_sign_two(qv0, qv1) > 0 {
                        return true;
                    }
                }
            }
        }

        false
    }
}

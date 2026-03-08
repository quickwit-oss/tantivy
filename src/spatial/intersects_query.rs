//! Polygon-intersects-polygon query.
//!
//! Sibling of contains_query. Shares the same infrastructure -- query-local CellIndex,
//! RegionCoverer, covering classification, candidate collection -- and differs only in
//! verification logic. Any overlap between the query polygon and a candidate is sufficient.

use std::collections::HashMap;

use super::cell_index::{BuildOptions, CellIndex, GeometryData, IndexBuilder};
use super::cell_index_reader::CellIndexReader;
use super::containment::{brute_force_contains, compute_origin_inside};
use super::contains_query::{BoundaryEdges, CandidateInfo, QueryEdgeProvider};
use super::crossings::S2EdgeCrosser;
use super::edge_reader::EdgeReader;
use super::region::Region;
use super::region_coverer::{CovererOptions, RegionCoverer};
use super::s2cell::S2Cell;
use super::s2cell_id::S2CellId;
use super::shape_index_region::{index_contains_point, CellIndexRegion};

/// Prepared intersects query, built once from a query polygon and applied per-segment.
pub struct IntersectsQuery {
    query_index: CellIndex,
    query_edges: QueryEdgeProvider,
    covering: Vec<S2CellId>,
    interior: Vec<bool>,
}

impl IntersectsQuery {
    /// Build the query from a polygon's vertices on the unit sphere.
    pub fn new(vertices: Vec<[f64; 3]>, options: CovererOptions) -> Self {
        let query_edges = QueryEdgeProvider::new(vertices.clone());

        let geo = GeometryData {
            rings: vec![vertices],
            origin_inside: vec![query_edges.origin_inside],
            dimension: 2,
        };
        let mut builder = IndexBuilder::new(BuildOptions::default());
        builder.add(0, vec![geo]);
        let query_index = builder.build();

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
    pub fn search_segment(
        &self,
        cell_reader: &CellIndexReader,
        edge_reader: &mut EdgeReader,
    ) -> Vec<u32> {
        let candidates = self.collect_candidates(cell_reader);
        self.verify_candidates(candidates, edge_reader)
    }

    fn collect_candidates(&self, reader: &CellIndexReader) -> HashMap<u32, CandidateInfo> {
        let mut candidates: HashMap<u32, CandidateInfo> = HashMap::new();

        for (i, &covering_cell_id) in self.covering.iter().enumerate() {
            let is_interior = self.interior[i];

            for index_cell in reader.scan_range(covering_cell_id) {
                // The interior shortcut is only valid when the covering cell
                // contains the index cell — the index cell is entirely within
                // the query polygon's interior. When the index cell is coarser
                // (contains the covering cell), geometries in the far reaches
                // of the index cell may not intersect the query at all.
                let cell_is_interior = is_interior && covering_cell_id.contains(index_cell.cell_id);

                for clipped in &index_cell.shapes {
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

    fn verify_candidates(
        &self,
        candidates: HashMap<u32, CandidateInfo>,
        edge_reader: &mut EdgeReader,
    ) -> Vec<u32> {
        let mut doc_ids = Vec::new();

        for (geometry_id, info) in candidates {
            if self.verify_one(geometry_id, &info, edge_reader) {
                let set = edge_reader.get(geometry_id);
                doc_ids.push(set.doc_id);
            }
        }

        doc_ids
    }

    /// Verify a single candidate geometry for intersection.
    ///
    /// Interior cell hit is an immediate match. Boundary candidates get crossing tests -- any
    /// crossing means intersection. If no crossings, test vertex containment in both directions:
    /// candidate vertex inside query, and query vertex inside candidate.
    fn verify_one(
        &self,
        geometry_id: u32,
        info: &CandidateInfo,
        edge_reader: &mut EdgeReader,
    ) -> bool {
        // Interior cell hit: a covering cell entirely inside the query polygon contains an
        // index cell where this geometry appears. The CellIndex assigns edges to cells using
        // conservative bounding box tests, so confirm with a vertex containment check.
        if info.has_interior {
            let set = edge_reader.get(geometry_id);
            let member_idx = (geometry_id - set.geometry_id) as usize;
            let vertices = &set.vertices[member_idx];
            if !vertices.is_empty()
                && index_contains_point(&self.query_index, &self.query_edges, 0, &vertices[0])
            {
                return true;
            }
        }

        // Boundary: any edge crossing means intersection.
        if info.has_boundary && self.has_crossing(geometry_id, info, edge_reader) {
            return true;
        }

        // No crossings: check nesting in both directions.
        let set = edge_reader.get(geometry_id);
        let member_idx = (geometry_id - set.geometry_id) as usize;
        let vertices = &set.vertices[member_idx];

        if vertices.is_empty() {
            return false;
        }

        // Forward: candidate vertex inside query polygon.
        if index_contains_point(&self.query_index, &self.query_edges, 0, &vertices[0]) {
            return true;
        }

        // Reverse: query vertex inside candidate polygon.
        let origin_inside = compute_origin_inside(vertices);
        brute_force_contains(&self.query_edges.vertices[0], vertices, origin_inside)
    }

    /// Tests whether any candidate edge crosses any query polygon edge, narrowed to edges sharing
    /// a boundary covering cell. Same evidence gathering as contains_query; the caller interprets
    /// the result differently.
    fn has_crossing(
        &self,
        geometry_id: u32,
        info: &CandidateInfo,
        edge_reader: &mut EdgeReader,
    ) -> bool {
        let set = edge_reader.get(geometry_id);
        let member_idx = (geometry_id - set.geometry_id) as usize;
        let candidate_vertices = &set.vertices[member_idx];
        let n_candidate = candidate_vertices.len();

        if n_candidate < 3 {
            return false;
        }

        let n_query = self.query_edges.vertices.len();

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
                    let qv0 = &self.query_edges.vertices[qi];
                    let qv1 = &self.query_edges.vertices[(qi + 1) % n_query];

                    if crosser.crossing_sign_two(qv0, qv1) > 0 {
                        return true;
                    }
                }
            }
        }

        false
    }
}

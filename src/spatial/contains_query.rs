//! Polygon-contains-polygon query using the verification cascade.
//!
//! Given a query polygon, finds all indexed geometries contained by it. Builds a query-local
//! CellIndex using the same IndexBuilder as ingest, produces a covering via RegionCoverer, then
//! verifies candidates per segment using bilateral edge narrowing.
//!
//! This module does not port any single S2 class. It composes S2 primitives with the serialized
//! segment index and query-local CellIndex.

use std::collections::HashMap;

use super::cell_index::{BuildOptions, CellIndex, GeometryData, IndexBuilder};
use super::cell_index_reader::CellIndexReader;
use super::containment::compute_origin_inside;
use super::crossings::S2EdgeCrosser;
use super::edge_reader::EdgeReader;
use super::region::Region;
use super::region_coverer::{CovererOptions, RegionCoverer};
use super::s2cell::S2Cell;
use super::s2cell_id::S2CellId;
use super::shape_index_region::{index_contains_point, CellIndexRegion, EdgeProvider};

/// In-memory edge provider for the query polygon.
///
/// Resolves edge indices into the vertex array held in memory. The query polygon is always
/// geometry_id 0 with edges wrapping from last to first.
pub struct QueryEdgeProvider {
    pub(crate) vertices: Vec<[f64; 3]>,
    pub(crate) origin_inside: bool,
}

impl QueryEdgeProvider {
    /// Creates a new edge provider from a list of vertices.
    pub fn new(vertices: Vec<[f64; 3]>) -> Self {
        let origin_inside = compute_origin_inside(&vertices);
        Self {
            vertices,
            origin_inside,
        }
    }
}

impl EdgeProvider for QueryEdgeProvider {
    fn get_edge(&self, _geometry_id: u32, edge_idx: u16) -> ([f64; 3], [f64; 3]) {
        let i = edge_idx as usize;
        let n = self.vertices.len();
        (self.vertices[i], self.vertices[(i + 1) % n])
    }

    fn get_vertices(&self, _geometry_id: u32) -> (&[[f64; 3]], bool) {
        (&self.vertices, self.origin_inside)
    }
}

/// Prepared contains query, built once from a query polygon and applied per-segment. Holds the
/// query-local CellIndex, the covering, and the interior/boundary classification.
pub struct ContainsQuery {
    query_index: CellIndex,
    query_edges: QueryEdgeProvider,
    covering: Vec<S2CellId>,
    interior: Vec<bool>,
}

impl ContainsQuery {
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

        // Classify each covering cell as interior or boundary by re-testing against the Region.
        // Interior cells are entirely inside the query polygon; boundary cells straddle the edge.
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

    /// Search one segment for geometries contained by the query polygon.
    pub fn search_segment(
        &self,
        cell_reader: &CellIndexReader,
        edge_reader: &mut EdgeReader,
    ) -> Vec<u32> {
        let candidates = self.collect_candidates(cell_reader);
        self.verify_candidates(candidates, edge_reader)
    }

    /// Scan covering cells against the segment's cell index to collect candidate geometries. For
    /// each candidate, track whether it appeared in boundary cells and which edges it has there.
    fn collect_candidates(&self, reader: &CellIndexReader) -> HashMap<u32, CandidateInfo> {
        let mut candidates: HashMap<u32, CandidateInfo> = HashMap::new();

        for (i, &covering_cell_id) in self.covering.iter().enumerate() {
            let is_interior = self.interior[i];

            for index_cell in reader.scan_range(covering_cell_id) {
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

    /// Verify each candidate using the cascade.
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

    /// Verify a single candidate geometry.
    ///
    /// If the candidate has edges in boundary covering cells, test those edges against the query
    /// polygon's edges in the same cells. Any interior crossing means the candidate is not
    /// contained.
    ///
    /// If no crossings are found (or the candidate is interior-only), classify by testing a
    /// candidate vertex against the query polygon.
    fn verify_one(
        &self,
        geometry_id: u32,
        info: &CandidateInfo,
        edge_reader: &mut EdgeReader,
    ) -> bool {
        // Check for edge crossings in boundary cells.
        if info.has_boundary && self.has_crossing(geometry_id, info, edge_reader) {
            return false;
        }

        // No crossings: classify by vertex containment.
        let set = edge_reader.get(geometry_id);
        let member_idx = (geometry_id - set.geometry_id) as usize;
        let vertices = &set.vertices[member_idx];

        if vertices.is_empty() {
            return false;
        }

        index_contains_point(&self.query_index, &self.query_edges, 0, &vertices[0])
    }

    /// Tests whether any candidate edge crosses any query polygon edge, narrowed to edges sharing
    /// a boundary covering cell.
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

        if n_candidate < 2 {
            return false;
        }

        let n_query = self.query_edges.vertices.len();

        for boundary in &info.boundary_edges {
            // Get query polygon edges in this covering cell.
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

            // Test each candidate edge against each query edge in this cell.
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

/// Tracks what we know about a candidate geometry from the covering scan.
pub(crate) struct CandidateInfo {
    /// Whether the candidate appeared in any interior covering cell.
    pub(crate) has_interior: bool,
    /// Whether the candidate appeared in any boundary covering cell.
    pub(crate) has_boundary: bool,
    /// Candidate edges grouped by the boundary covering cell they appeared in.
    pub(crate) boundary_edges: Vec<BoundaryEdges>,
}

impl CandidateInfo {
    pub(crate) fn new() -> Self {
        Self {
            has_interior: false,
            has_boundary: false,
            boundary_edges: Vec::new(),
        }
    }
}

/// Candidate edge indices from a single boundary covering cell.
pub(crate) struct BoundaryEdges {
    /// The covering cell these edges appeared in, used to look up the query polygon's edges in the
    /// same cell.
    pub(crate) covering_cell_id: S2CellId,
    /// Edge indices into the candidate geometry's vertex array.
    pub(crate) edge_indices: Vec<u16>,
}

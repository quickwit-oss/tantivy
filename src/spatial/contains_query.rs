//! Polygon-contains-polygon query using the verification cascade.
//!
//! Given a query polygon, finds all indexed geometries contained by it. Builds a query-local
//! CellIndex using the same IndexBuilder as ingest, produces a covering via RegionCoverer, then
//! verifies candidates per segment using bilateral edge narrowing.
//!
//! This module does not port any single S2 class. It composes S2 primitives with the serialized
//! segment index and query-local CellIndex.

use std::collections::HashMap;

use common::BitSet;

use super::cell_index::{BuildOptions, CellIndex, GeometryId, IndexBuilder};
use super::cell_index_reader::CellIndexReader;
use super::crossings::S2EdgeCrosser;
use super::edge_cache::EdgeCache;
use super::geometry_set::{EdgeSet, GeometrySet};
use super::region::Region;
use super::region_coverer::{CovererOptions, RegionCoverer};
use super::s2cell::S2Cell;
use super::s2cell_id::S2CellId;
use super::shape_index_region::{index_contains_point, CellIndexRegion, EdgeProvider};
use super::sphere::Sphere;

/// In-memory edge provider wrapping a smashed GeometrySet. Resolves edge indices into the
/// smashed vertex arrays. No modulo wrapping — smashed vertices include the closure vertex.
pub struct QueryEdgeProvider {
    pub(crate) set: GeometrySet,
}

impl QueryEdgeProvider {
    /// Returns the EdgeSet for the given geometry_id.
    pub fn get_edge_set(&self, geometry_id: GeometryId) -> &EdgeSet {
        &self.set.members[geometry_id.1 as usize]
    }
}

impl EdgeProvider for QueryEdgeProvider {
    fn get_edge(&self, geometry_id: GeometryId, edge_idx: u16) -> ([f64; 3], [f64; 3]) {
        let vertices = &self.set.members[geometry_id.1 as usize].vertices;
        let i = edge_idx as usize;
        (vertices[i], vertices[i + 1])
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

    /// Search one segment for geometries contained by the query polygon.
    pub fn search_segment(
        &self,
        cell_reader: &CellIndexReader,
        edge_cache: &mut EdgeCache<'_, Sphere>,
    ) -> Vec<u32> {
        let candidates = self.collect_candidates(cell_reader, None, edge_cache);
        self.verify_candidates(candidates, edge_cache)
    }

    /// Search one segment, skipping shapes whose doc_id is not in the filter bitset.
    pub fn search_segment_filtered(
        &self,
        cell_reader: &CellIndexReader,
        edge_cache: &mut EdgeCache<'_, Sphere>,
        terms_filter: &BitSet,
    ) -> Vec<u32> {
        let candidates = self.collect_candidates(cell_reader, Some(terms_filter), edge_cache);
        self.verify_candidates(candidates, edge_cache)
    }

    fn collect_candidates(
        &self,
        reader: &CellIndexReader,
        terms_filter: Option<&BitSet>,
        edge_cache: &EdgeCache<'_, Sphere>,
    ) -> HashMap<GeometryId, CandidateInfo> {
        let mut candidates: HashMap<GeometryId, CandidateInfo> = HashMap::new();

        for (i, &covering_cell_id) in self.covering.iter().enumerate() {
            let is_interior = self.interior[i];

            for index_cell in reader.scan_range(covering_cell_id) {
                let cell_is_interior = is_interior && covering_cell_id.contains(index_cell.cell_id);

                for clipped in &index_cell.shapes {
                    if let Some(filter) = terms_filter {
                        let doc_id = edge_cache.doc_id_for(clipped.geometry_id);
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

    /// Verify each candidate using the cascade.
    fn verify_candidates(
        &self,
        candidates: HashMap<GeometryId, CandidateInfo>,
        edge_cache: &mut EdgeCache<'_, Sphere>,
    ) -> Vec<u32> {
        let mut doc_ids = Vec::new();

        for (geometry_id, info) in candidates {
            if self.verify_one(geometry_id, &info, edge_cache) {
                let (doc_id, _) = edge_cache.get_edge_set(geometry_id);
                doc_ids.push(doc_id);
            }
        }

        doc_ids
    }

    /// Verify a single candidate geometry. If the candidate has edges in boundary covering cells,
    /// test those edges against the query polygon's edges in the same cells. Any interior crossing
    /// means the candidate is not contained. If no crossings are found (or the candidate is
    /// interior-only), classify by testing a candidate vertex against the query polygon.
    fn verify_one(
        &self,
        geometry_id: GeometryId,
        info: &CandidateInfo,
        edge_cache: &mut EdgeCache<'_, Sphere>,
    ) -> bool {
        if info.has_boundary && self.has_crossing(geometry_id, info, edge_cache) {
            return false;
        }

        let (_, edge_set) = edge_cache.get_edge_set(geometry_id);
        let vertices = &edge_set.vertices;

        if vertices.is_empty() {
            return false;
        }

        index_contains_point(&self.query_index, &self.query_edges, (0, 0), &vertices[0])
    }

    /// Tests whether any candidate edge crosses any query polygon edge, narrowed to edges sharing
    /// a boundary covering cell.
    fn has_crossing(
        &self,
        geometry_id: GeometryId,
        info: &CandidateInfo,
        edge_cache: &mut EdgeCache<'_, Sphere>,
    ) -> bool {
        let (_, edge_set) = edge_cache.get_edge_set(geometry_id);
        let candidate_vertices = &edge_set.vertices;
        let n_candidate = candidate_vertices.len();

        if n_candidate < 2 {
            return false;
        }

        let query_vertices = &self.query_edges.get_edge_set((0, 0)).vertices;

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

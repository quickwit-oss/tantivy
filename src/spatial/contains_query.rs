//! Polygon-contains-polygon query using the verification cascade.
//!
//! Given a query polygon, finds all indexed geometries contained by it. Builds a query-local
//! CellIndex using the same IndexBuilder as ingest, produces a covering via RegionCoverer, then
//! verifies candidates per segment using bilateral edge narrowing.
//!
//! This module does not port any single S2 class. It composes S2 primitives with the serialized
//! segment index and query-local CellIndex.

use std::collections::{HashMap, HashSet};
use std::ops::Range;

use common::BitSet;

use super::cell_index::{BuildOptions, CellIndex, GeometryId, IndexBuilder};
use super::cell_index_reader::CellIndexReader;
use super::crossings::S2EdgeCrosser;
use super::edge_cache::EdgeCache;
use super::geometry_set::{EdgeSet, GeometrySet};
use super::math::{cross, norm2, normalize, sub};
use super::region::Region;
use super::region_coverer::{CovererOptions, RegionCoverer};
use super::s2cell::S2Cell;
use super::s2cell_id::S2CellId;
use super::shape_index_region::{
    any_shape_contains_point, contains_point, CellIndexRegion, EdgeProvider, SegmentIndex,
};
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
    fn get_edge(&self, geometry_id: GeometryId, edge_idx: u32) -> ([f64; 3], [f64; 3]) {
        let vertices = &self.get_edge_set(geometry_id).vertices;
        let i = edge_idx as usize;
        (vertices[i], *vertices.get(i + 1).unwrap_or(&vertices[i]))
    }

    fn is_closed(&self, geometry_id: GeometryId) -> bool {
        self.get_edge_set(geometry_id).closed
    }
}

/// A query geometry prepared for searching segments: its cell index, its covering, and for each
/// covering cell whether the geometry contains it and which query edges lie in it. Shared by the
/// contains and intersects queries.
pub(crate) struct QueryRegion {
    query_index: CellIndex,
    query_edges: QueryEdgeProvider,
    covering: Vec<S2CellId>,
    interior: Vec<bool>,
    /// The query edges in each covering cell, parallel to `covering`.
    cell_edges: Vec<Vec<QueryEdge>>,
}

/// An edge of the query geometry, and the member and edge index that identify it. A point is an
/// edge from itself to itself.
struct QueryEdge {
    member: u32,
    edge_idx: u32,
    v0: [f64; 3],
    v1: [f64; 3],
}

/// How a candidate's boundary meets the query geometry's boundary.
pub(crate) enum Contact {
    /// An edge of each crosses the other at a point interior to both.
    Cross,
    /// The boundaries meet at most at shared vertices and along shared edges.
    Touch(Touch),
}

/// Where the boundaries of a candidate and the query geometry meet without crossing. A ring of
/// either that meets nothing lies wholly inside or wholly outside the other geometry, as does a
/// touching edge, but for its endpoints. Vertices where they meet may lie on either side, so they
/// decide nothing.
#[derive(Default)]
pub(crate) struct Touch {
    /// Candidate edges that share a vertex with a query edge, other than those in `along`.
    pub(crate) touching: HashSet<u32>,
    /// Candidate edges that run along a query edge, in either direction.
    pub(crate) along: HashSet<u32>,
    /// Query edges, as (member, edge index), that share a vertex with a candidate edge, other
    /// than those in `query_along`.
    pub(crate) query_touching: HashSet<(u32, u32)>,
    /// Query edges that run along a candidate edge.
    pub(crate) query_along: HashSet<(u32, u32)>,
}

impl Touch {
    /// Whether an edge of the candidate ring meets the query geometry's boundary.
    pub(crate) fn candidate_ring_meets(&self, ring: Range<usize>) -> bool {
        self.touching
            .iter()
            .chain(&self.along)
            .any(|&edge_idx| ring.contains(&(edge_idx as usize)))
    }

    /// The edges of the query member's ring that touch the candidate's boundary.
    fn query_ring_touching(
        &self,
        member: u32,
        ring: Range<usize>,
    ) -> impl Iterator<Item = u32> + '_ {
        self.query_touching
            .iter()
            .filter(move |&&(m, edge_idx)| m == member && ring.contains(&(edge_idx as usize)))
            .map(|&(_, edge_idx)| edge_idx)
    }

    /// Whether an edge of the query member's ring meets the candidate's boundary.
    pub(crate) fn query_ring_meets(&self, member: u32, ring: Range<usize>) -> bool {
        self.query_touching
            .iter()
            .chain(&self.query_along)
            .any(|&(m, edge_idx)| m == member && ring.contains(&(edge_idx as usize)))
    }
}

/// The midpoint of an edge.
pub(crate) fn midpoint((v0, v1): ([f64; 3], [f64; 3])) -> [f64; 3] {
    normalize(&[v0[0] + v1[0], v0[1] + v1[1], v0[2] + v1[2]])
}

/// Two points just to either side of the middle of an edge, to tell on which side of a shared
/// edge each geometry's interior lies.
pub(crate) fn beside((v0, v1): ([f64; 3], [f64; 3])) -> [[f64; 3]; 2] {
    let middle = midpoint((v0, v1));
    let normal = normalize(&cross(&v0, &v1));
    let offset = 1e-6 * norm2(&sub(&v0, &v1)).sqrt();
    [offset, -offset].map(|offset| {
        normalize(&[
            middle[0] + offset * normal[0],
            middle[1] + offset * normal[1],
            middle[2] + offset * normal[2],
        ])
    })
}

impl QueryRegion {
    /// Index and cover a smashed query geometry.
    pub(crate) fn new(set: GeometrySet, options: CovererOptions) -> Self {
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

        // A covering cell is usually coarser than the query index cells, so it holds the edges of
        // every index cell it overlaps.
        let cell_edges = covering
            .iter()
            .map(|&cell_id| {
                let mut edges = Vec::new();
                for cell in query_index.cells_overlapping(cell_id) {
                    for clipped in &cell.shapes {
                        for &edge_idx in &clipped.edge_indices {
                            let (v0, v1) = query_edges.get_edge(clipped.geometry_id, edge_idx);
                            edges.push(QueryEdge {
                                member: clipped.geometry_id.1,
                                edge_idx,
                                v0,
                                v1,
                            });
                        }
                    }
                }
                edges
            })
            .collect();

        Self {
            query_index,
            query_edges,
            covering,
            interior,
            cell_edges,
        }
    }

    /// The members of the query geometry.
    pub(crate) fn members(&self) -> &[EdgeSet] {
        &self.query_edges.set.members
    }

    /// The endpoints of an edge of a query member.
    pub(crate) fn query_edge(&self, member: u32, edge_idx: u32) -> ([f64; 3], [f64; 3]) {
        self.query_edges.get_edge((0, member), edge_idx)
    }

    /// Whether a member of the query geometry that has an interior contains the point.
    pub(crate) fn contains(&self, point: &[f64; 3]) -> bool {
        any_shape_contains_point(&self.query_index, &self.query_edges, point)
    }

    /// Collect every stored geometry in the covering, recording which covering cells it lies in.
    /// Skips geometries whose doc_id is not in the filter bitset.
    pub(crate) fn collect_candidates(
        &self,
        reader: &CellIndexReader,
        terms_filter: Option<&BitSet>,
        edge_cache: &EdgeCache<'_, Sphere>,
    ) -> HashMap<GeometryId, CandidateInfo> {
        let mut candidates: HashMap<GeometryId, CandidateInfo> = HashMap::new();

        for (covering_index, &covering_cell_id) in self.covering.iter().enumerate() {
            let is_interior = self.interior[covering_index];

            for index_cell in reader.scan_range(covering_cell_id) {
                let cell_is_interior = is_interior && covering_cell_id.contains(index_cell.cell_id);

                for clipped in index_cell.shapes {
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
                                covering_index,
                                edge_indices: clipped.edge_indices,
                            });
                        }
                    }
                }
            }
        }

        candidates
    }

    /// Tests the candidate's edges against the query edges, narrowed to edges sharing a boundary
    /// covering cell.
    pub(crate) fn boundary_contact(&self, info: &CandidateInfo, candidate: &EdgeSet) -> Contact {
        let mut touch = Touch::default();
        let vertices = &candidate.vertices;
        if vertices.len() < 2 {
            return Contact::Touch(touch);
        }

        for boundary in &info.boundary_edges {
            let query_edges = &self.cell_edges[boundary.covering_index];
            if query_edges.is_empty() {
                continue;
            }

            for &candidate_edge_idx in &boundary.edge_indices {
                let ci = candidate_edge_idx as usize;
                let (cv0, cv1) = (&vertices[ci], &vertices[ci + 1]);
                if cv0 == cv1 {
                    continue;
                }
                let mut crosser = S2EdgeCrosser::new(cv0, cv1);

                for query_edge in query_edges {
                    let (qv0, qv1) = (&query_edge.v0, &query_edge.v1);
                    if qv0 == qv1 {
                        continue;
                    }
                    match crosser.crossing_sign_two(qv0, qv1) {
                        1 => return Contact::Cross,
                        0 => {
                            let query_edge = (query_edge.member, query_edge.edge_idx);
                            if (cv0, cv1) == (qv0, qv1) || (cv0, cv1) == (qv1, qv0) {
                                touch.along.insert(candidate_edge_idx);
                                touch.query_along.insert(query_edge);
                            } else {
                                touch.touching.insert(candidate_edge_idx);
                                touch.query_touching.insert(query_edge);
                            }
                        }
                        _ => {}
                    }
                }
            }
        }

        let Touch {
            touching,
            along,
            query_touching,
            query_along,
        } = &mut touch;
        touching.retain(|edge_idx| !along.contains(edge_idx));
        query_touching.retain(|edge| !query_along.contains(edge));
        Contact::Touch(touch)
    }
}

/// Prepared contains query, built once from a query polygon and applied per-segment. Holds the
/// query-local CellIndex, the covering, and the interior/boundary classification.
pub struct ContainsQuery {
    region: QueryRegion,
}

impl ContainsQuery {
    /// Build the query from a smashed GeometrySet.
    pub fn new(set: GeometrySet, options: CovererOptions) -> Self {
        Self {
            region: QueryRegion::new(set, options),
        }
    }

    /// Search one segment for geometries contained by the query polygon.
    pub fn search_segment(
        &self,
        cell_reader: &CellIndexReader,
        edge_cache: &mut EdgeCache<'_, Sphere>,
    ) -> Vec<u32> {
        let candidates = self
            .region
            .collect_candidates(cell_reader, None, edge_cache);
        self.verify_candidates(candidates, cell_reader, edge_cache)
    }

    /// Search one segment, skipping shapes whose doc_id is not in the filter bitset.
    pub fn search_segment_filtered(
        &self,
        cell_reader: &CellIndexReader,
        edge_cache: &mut EdgeCache<'_, Sphere>,
        terms_filter: &BitSet,
    ) -> Vec<u32> {
        let candidates =
            self.region
                .collect_candidates(cell_reader, Some(terms_filter), edge_cache);
        self.verify_candidates(candidates, cell_reader, edge_cache)
    }

    /// Verify each candidate using the cascade. A geometry set is contained only when every one
    /// of its members is: a member that is not a candidate lies outside the covering.
    fn verify_candidates(
        &self,
        candidates: HashMap<GeometryId, CandidateInfo>,
        cell_reader: &CellIndexReader,
        edge_cache: &mut EdgeCache<'_, Sphere>,
    ) -> Vec<u32> {
        // Keyed by the set's head: its document and the members not yet verified.
        let mut unverified: HashMap<GeometryId, (u32, u32)> = HashMap::new();
        let mut rejected: HashSet<GeometryId> = HashSet::new();

        for (geometry_id, info) in candidates {
            let entry = edge_cache.get(geometry_id);
            let head = (geometry_id.0, entry.head());
            if rejected.contains(&head) {
                continue;
            }
            if !self.verify_one(geometry_id, &info, cell_reader, edge_cache) {
                rejected.insert(head);
                continue;
            }
            unverified
                .entry(head)
                .or_insert((entry.doc_id(), entry.member_count()))
                .1 -= 1;
        }

        unverified
            .into_iter()
            .filter(|(head, (_, members))| *members == 0 && !rejected.contains(head))
            .map(|(_, (doc_id, _))| doc_id)
            .collect()
    }

    /// Verify a single candidate geometry. If the candidate has edges in boundary covering cells,
    /// test those edges against the query polygon's edges in the same cells. Any interior crossing
    /// means the candidate is not contained. Otherwise every part of the candidate's boundary
    /// must lie inside the query polygon, and no hole of the query polygon inside the candidate.
    fn verify_one(
        &self,
        geometry_id: GeometryId,
        info: &CandidateInfo,
        cell_reader: &CellIndexReader,
        edge_cache: &mut EdgeCache<'_, Sphere>,
    ) -> bool {
        let entry = edge_cache.get(geometry_id);
        let candidate = entry.edge_set();
        if candidate.vertices.is_empty() {
            return false;
        }
        let touch = match self.region.boundary_contact(info, candidate) {
            Contact::Cross => return false,
            Contact::Touch(touch) => touch,
        };

        let touches_outside = touch
            .touching
            .iter()
            .any(|&edge_idx| !self.region.contains(&midpoint(entry.edge(edge_idx))));
        if touches_outside {
            return false;
        }
        for ring in candidate.ring_offsets.windows(2) {
            if !touch.candidate_ring_meets(ring[0]..ring[1])
                && !self.region.contains(&candidate.vertices[ring[0]])
            {
                return false;
            }
        }
        if !candidate.closed {
            return true;
        }

        let mut segment = SegmentIndex {
            cell_reader,
            edge_cache,
        };
        let mut candidate_contains =
            |point: &[f64; 3]| contains_point(&mut segment, geometry_id, point);

        // Where the boundaries run together, the candidate's interior must lie on the query
        // polygon's side.
        for &edge_idx in &touch.along {
            for point in beside(entry.edge(edge_idx)) {
                if candidate_contains(&point) && !self.region.contains(&point) {
                    return false;
                }
            }
        }

        // A hole of the query polygon inside the candidate leaves part of it uncovered.
        for (member, edge_set) in self.region.members().iter().enumerate() {
            let member = member as u32;
            if !edge_set.closed {
                continue;
            }
            for hole in edge_set.ring_offsets[1..].windows(2) {
                let hole_inside = if touch.query_ring_meets(member, hole[0]..hole[1]) {
                    touch
                        .query_ring_touching(member, hole[0]..hole[1])
                        .any(|edge_idx| {
                            candidate_contains(&midpoint(self.region.query_edge(member, edge_idx)))
                        })
                } else {
                    candidate_contains(&edge_set.vertices[hole[0]])
                };
                if hole_inside {
                    return false;
                }
            }
        }
        true
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
    /// The position in the covering of the cell these edges appeared in, used to look up the
    /// query polygon's edges in the same cell.
    pub(crate) covering_index: usize,
    /// Edge indices into the candidate geometry's vertex array.
    pub(crate) edge_indices: Vec<u32>,
}

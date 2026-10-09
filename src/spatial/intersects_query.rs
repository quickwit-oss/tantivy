//! Polygon-intersects-polygon query.
//!
//! Shares infrastructure with contains_query. Differs in verification: any overlap between the
//! query polygon and a candidate is sufficient.

use std::collections::{HashMap, HashSet};

use common::BitSet;

use super::cell_index::GeometryId;
use super::cell_index_reader::CellIndexReader;
use super::contains_query::{midpoint, CandidateInfo, Contact, QueryRegion};
use super::edge_cache::EdgeCache;
use super::geometry_set::GeometrySet;
use super::region_coverer::CovererOptions;
use super::shape_index_region::{contains_point, SegmentIndex};
use super::sphere::Sphere;

/// Prepared intersects query, built once from a query polygon and applied per-segment.
pub struct IntersectsQuery {
    region: QueryRegion,
}

impl IntersectsQuery {
    /// Build the query from a smashed GeometrySet.
    pub fn new(set: GeometrySet, options: CovererOptions) -> Self {
        Self {
            region: QueryRegion::new(set, options),
        }
    }

    /// Search one segment for geometries that intersect the query polygon.
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

    fn verify_candidates(
        &self,
        candidates: HashMap<GeometryId, CandidateInfo>,
        cell_reader: &CellIndexReader,
        edge_cache: &mut EdgeCache<'_, Sphere>,
    ) -> Vec<u32> {
        let mut doc_ids = HashSet::new();

        for (geometry_id, info) in candidates {
            // A document intersects once, however many of its members do.
            let doc_id = edge_cache.get(geometry_id).doc_id();
            if !doc_ids.contains(&doc_id)
                && self.verify_one(geometry_id, &info, cell_reader, edge_cache)
            {
                doc_ids.insert(doc_id);
            }
        }

        doc_ids.into_iter().collect()
    }

    /// The candidate intersects the query geometry if their edges cross, if part of the
    /// candidate's boundary lies inside the query geometry, if part of the query geometry's
    /// boundary lies inside the candidate, or if they share an edge with both interiors on the same
    /// side of it.
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
            Contact::Cross => return true,
            Contact::Touch(touch) => touch,
        };

        let touches_inside = touch
            .touching
            .iter()
            .any(|&edge_idx| self.region.contains(&midpoint(entry.edge(edge_idx))));
        if touches_inside {
            return true;
        }
        let ring_inside = candidate.ring_offsets.windows(2).any(|ring| {
            !touch.candidate_ring_meets(ring[0]..ring[1])
                && self.region.contains(&candidate.vertices[ring[0]])
        });
        if ring_inside {
            return true;
        }
        if !candidate.closed {
            return false;
        }

        let mut segment = SegmentIndex {
            cell_reader,
            edge_cache,
        };
        let mut candidate_contains =
            |point: &[f64; 3]| contains_point(&mut segment, geometry_id, point);

        let query_touches_inside = touch.query_touching.iter().any(|&(member, edge_idx)| {
            candidate_contains(&midpoint(self.region.query_edge(member, edge_idx)))
        });
        if query_touches_inside {
            return true;
        }
        for (member, edge_set) in self.region.members().iter().enumerate() {
            for ring in edge_set.ring_offsets.windows(2) {
                if !touch.query_ring_meets(member as u32, ring[0]..ring[1])
                    && candidate_contains(&edge_set.vertices[ring[0]])
                {
                    return true;
                }
            }
        }
        touch.along.keys().any(|&edge_idx| {
            self.region
                .covers_shared_interior(candidate, edge_idx, &touch)
        })
    }
}

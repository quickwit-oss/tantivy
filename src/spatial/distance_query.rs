//! Distance query: find geometries within a given distance of a point.
//!
//! Builds an S2Cap from a center point and radius, covers it with RegionCoverer, and scans the
//! segment's cell index for candidates. Verification computes the minimum distance from the
//! center to each candidate geometry. For polygons, containment is checked first. If the
//! center is inside the polygon, the distance is zero.

use std::collections::HashSet;

use common::BitSet;

use super::cell_index_reader::CellIndexReader;
use super::edge_reader::EdgeReader;
use super::region::Region;
use super::region_coverer::{CovererOptions, RegionCoverer};
use super::s1chord_angle::S1ChordAngle;
use super::s2cap::S2Cap;
use super::s2cell::S2Cell;
use super::s2cell_id::S2CellId;
use super::s2edge_distances::update_min_distance;
use super::shape_index_region::{contains_point, SegmentIndex};

/// Prepared distance query, built once from a center point and radius and applied per-segment.
pub struct DistanceQuery {
    center: [f64; 3],
    outer_radius: S1ChordAngle,
    inner_radius: S1ChordAngle,
    covering: Vec<S2CellId>,
    interior: Vec<bool>,
}

impl DistanceQuery {
    /// Build a $within query: geometries whose nearest point is within radius of center.
    pub fn within(center: [f64; 3], radius: S1ChordAngle, options: CovererOptions) -> Self {
        let cap = S2Cap::new(center, radius);
        let coverer = RegionCoverer::new(options);
        let covering = coverer.get_covering(&cap);

        let interior: Vec<bool> = covering
            .cell_ids()
            .iter()
            .map(|&id| cap.contains_cell(&S2Cell::new(id)))
            .collect();

        let covering = covering.into_cell_ids();

        Self {
            center,
            outer_radius: radius,
            inner_radius: S1ChordAngle::zero(),
            covering,
            interior,
        }
    }

    /// Build a $between query: geometries whose nearest point is between inner and outer radius.
    pub fn between(
        center: [f64; 3],
        inner_radius: S1ChordAngle,
        outer_radius: S1ChordAngle,
        options: CovererOptions,
    ) -> Self {
        let cap = S2Cap::new(center, outer_radius);
        let coverer = RegionCoverer::new(options);
        let covering = coverer.get_covering(&cap);

        let interior: Vec<bool> = covering
            .cell_ids()
            .iter()
            .map(|&id| cap.contains_cell(&S2Cell::new(id)))
            .collect();

        let covering = covering.into_cell_ids();

        Self {
            center,
            outer_radius,
            inner_radius,
            covering,
            interior,
        }
    }

    /// Search one segment for geometries within the distance bounds.
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
    ) -> HashSet<u32> {
        let mut candidates = HashSet::new();

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

                    if cell_is_interior && self.inner_radius.length2() == 0.0 {
                        // Interior cell with no inner bound. Geometry is within radius.
                    }
                    candidates.insert(clipped.geometry_id);
                }
            }
        }

        candidates
    }

    fn verify_candidates<'a>(
        &self,
        candidates: HashSet<u32>,
        cell_reader: &'a CellIndexReader<'a>,
        edge_reader: &mut EdgeReader<'a>,
    ) -> Vec<u32> {
        let mut doc_ids = Vec::new();

        for geometry_id in candidates {
            if let Some(doc_id) = self.verify_one(geometry_id, cell_reader, edge_reader) {
                doc_ids.push(doc_id);
            }
        }

        doc_ids
    }

    fn verify_one<'a>(
        &self,
        geometry_id: u32,
        cell_reader: &'a CellIndexReader<'a>,
        edge_reader: &mut EdgeReader<'a>,
    ) -> Option<u32> {
        // Check closed flag and doc_id first, then release the borrow.
        let (closed, doc_id) = {
            let (doc_id, edge_set) = edge_reader.get_edge_set(geometry_id);
            (edge_set.closed, doc_id)
        };

        // For closed geometries (polygons), check if the center is inside the candidate.
        // If so, distance is zero.
        if closed {
            let mut seg = SegmentIndex {
                cell_reader,
                edge_reader,
            };
            if contains_point(&mut seg, geometry_id, &self.center) {
                if self.inner_radius.length2() == 0.0 {
                    return Some(doc_id);
                } else {
                    return None;
                }
            }
        }

        // Re-borrow for edge distance computation.
        let (_, edge_set) = edge_reader.get_edge_set(geometry_id);
        let vertices = &edge_set.vertices;

        if vertices.is_empty() {
            return None;
        }

        // Compute the minimum distance from the center to any edge of the candidate.
        let mut min_dist = self.outer_radius.successor();
        let n = vertices.len();

        if n >= 2 {
            let edge_count = n - 1;
            for i in 0..edge_count {
                update_min_distance(&self.center, &vertices[i], &vertices[i + 1], &mut min_dist);
            }
        } else {
            min_dist = S1ChordAngle::from_points(&self.center, &vertices[0]);
        }

        if min_dist > self.outer_radius {
            return None;
        }
        if min_dist < self.inner_radius {
            return None;
        }

        Some(doc_id)
    }
}

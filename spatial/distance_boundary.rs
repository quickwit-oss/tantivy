//! Uniform cover of the query boundary and distance bounds for the flood fill.

use std::collections::BTreeMap;

use super::boundary_tree::{BoundaryNode, BoundaryTree};
use super::covering::split_edges;
use super::s1chord_angle::{S1ChordAngle, RELATIVE_SUM_ERROR};
use super::s2cell_id::S2CellId;
use super::s2edge_distances::get_update_min_distance_max_error;
use super::s2metrics::S2Metrics;
use super::s2padded_cell::S2PaddedCell;
use super::surface::Surface;

pub(super) struct DistanceBoundary {
    pub level: i32,
    pub cells: Vec<S2CellId>,
    pub tree: BoundaryTree,
    max_radius: S1ChordAngle,
    search_distance: S1ChordAngle,
}

impl DistanceBoundary {
    pub fn build<S: Surface>(
        distance: S1ChordAngle,
        seeds: impl IntoIterator<Item = (S2CellId, Vec<u32>)>,
        get_edge: &impl Fn(u32) -> (S::Point, S::Point),
    ) -> Self {
        let level = S2Metrics::get_level_for_min_width(distance.to_radians() / 2.0);
        let mut cells: BTreeMap<S2CellId, Vec<u32>> = BTreeMap::new();
        let mut pending: Vec<_> = seeds.into_iter().collect();
        while let Some((id, edges)) = pending.pop() {
            if edges.is_empty() {
                continue;
            }
            if id.level() >= level {
                cells.entry(id.parent(level)).or_default().extend(edges);
                continue;
            }
            let pcell = S2PaddedCell::<S>::new(id, S::CELL_PADDING);
            let mut children = split_edges(&pcell, &edges, get_edge);
            for pos in 0..4 {
                let (i, j) = pcell.get_child_ij(pos);
                let edges = std::mem::take(&mut children[i][j]);
                if !edges.is_empty() {
                    pending.push((id.child(pos), edges));
                }
            }
        }

        let mut max_radius = S1ChordAngle::zero();
        let nodes = cells
            .iter_mut()
            .map(|(&id, edges)| {
                edges.sort_unstable();
                edges.dedup();
                max_radius = max_radius.max(cell_radius::<S>(id));
                BoundaryNode {
                    point: xyz(&S::cell_center(id)),
                    edge_indices: std::mem::take(edges),
                }
            })
            .collect();
        Self {
            level,
            cells: cells.into_keys().collect(),
            tree: BoundaryTree::build(nodes),
            max_radius,
            // The edge distance calculation can round down. Allow for that error while finding
            // candidates, so they reach the final comparison at the requested distance.
            search_distance: expand(distance, discovery_error(distance)),
        }
    }

    pub fn search_threshold<S: Surface>(&self, cell: S2CellId) -> f64 {
        // The bound is D plus the radii of both cells. Adding squared chord lengths would
        // underestimate it, so add the angles before converting back.
        let sum = add_upper(
            add_upper(self.search_distance, cell_radius::<S>(cell)),
            self.max_radius,
        );
        let threshold = expand(sum, sum.get_s2point_constructor_max_error());
        if threshold == S1ChordAngle::straight() {
            // Rounding can make squared coordinate distances slightly greater than four.
            // A bound covering the whole sphere must include those distances too.
            return f64::INFINITY;
        }
        threshold.length2()
    }
}

fn discovery_error(distance: S1ChordAngle) -> f64 {
    let error = get_update_min_distance_max_error(distance);
    if distance.length2() <= 1.0 {
        // Up to 60 degrees the primitive's error bound is increasing, so its value at D
        // bounds every computed distance that could pass the final <= D comparison.
        return error;
    }
    // The interior-distance bound eventually decreases, then drops to zero at 90 degrees.
    // With a, b in [0, 1] and a^2 = b * (2 - b), its expression is at most
    // (2.5 + 2 * sqrt(3)) + 8.5 + (2 + 2 * sqrt(3) / 3) + 6.5 / 4 < 20, times epsilon,
    // plus an O(epsilon^2) term. The point-distance bound is at most 18 epsilon + 16 epsilon^2.
    // 32 epsilon covers both bounds, including rounding. Keep the smaller bound for distances
    // up to 60 degrees so small distance queries do not search unnecessarily large areas.
    error.max(32.0 * f64::EPSILON)
}

pub(super) fn xyz<P: AsRef<[f64]>>(point: &P) -> [f64; 3] {
    let p = point.as_ref();
    [p[0], p[1], p[2]]
}

/// Enclose the padded cell around the same center used by the boundary tree. These cells
/// are convex and lie in a hemisphere, so a cap containing their corners contains their edges
/// and interior. S2Cell::get_cap_bound uses a different center and cannot supply this radius.
fn cell_radius<S: Surface>(id: S2CellId) -> S1ChordAngle {
    let center = xyz(&S::cell_center(id));
    let cell = S2PaddedCell::<S>::new(id, S::CELL_PADDING);
    let mut radius = S1ChordAngle::zero();
    for k in 0..4 {
        let uv = cell.bound().get_vertex(k);
        let vertex = xyz(&S::face_uv_to_point(id.face(), uv[0], uv[1]));
        let distance = S1ChordAngle::from_points(&center, &vertex);
        radius = radius.max(expand(
            distance,
            distance.get_s2point_constructor_max_error(),
        ));
    }
    radius
}

fn expand(angle: S1ChordAngle, error: f64) -> S1ChordAngle {
    // S1ChordAngle addition requires finite inputs, including at PI.
    S1ChordAngle::from_length2((angle.length2() + error.next_up()).next_up())
}

fn add_upper(a: S1ChordAngle, b: S1ChordAngle) -> S1ChordAngle {
    let sum = a + b;
    expand(sum, RELATIVE_SUM_ERROR * sum.length2())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spatial::sphere::Sphere;

    #[test]
    fn test_boundary_retains_both_sides_of_a_divider() {
        for swap in [false, true] {
            let point = |u, v| {
                if swap {
                    Sphere::face_uv_to_point(0, v, u)
                } else {
                    Sphere::face_uv_to_point(0, u, v)
                }
            };
            let a = point(0.0, -0.5);
            let b = point(0.0, 0.5);
            let boundary = DistanceBoundary::build::<Sphere>(
                S1ChordAngle::from_radians(0.2),
                [(S2CellId::from_face(0), vec![0])],
                &|_| (a, b),
            );
            assert!(boundary.level > 0);
            for i in 0..=16 {
                let v = -0.49 + 0.98 * i as f64 / 16.0;
                for u in [-1e-8, 1e-8] {
                    let id = Sphere::cell_id_from_point(&point(u, v)).parent(boundary.level);
                    assert!(boundary.cells.contains(&id), "missing boundary cell {id:?}");
                }
            }
        }
    }

    #[test]
    fn test_radius_uses_the_flood_center() {
        let id = S2CellId(0x0555350000000000);
        let center = Sphere::cell_center(id);
        let radius = cell_radius::<Sphere>(id);
        let cell = S2PaddedCell::<Sphere>::new(id, Sphere::CELL_PADDING);
        for i in 0..=8 {
            for j in 0..=8 {
                let bound = cell.bound();
                let u = bound[0].lo() + bound[0].length() * (i as f64 / 8.0);
                let v = bound[1].lo() + bound[1].length() * (j as f64 / 8.0);
                let point = Sphere::face_uv_to_point(id.face(), u, v);
                assert!(S1ChordAngle::from_points(&center, &point) <= radius);
            }
        }
    }

    #[test]
    fn test_angle_composition_includes_the_cross_term() {
        let d = S1ChordAngle::from_radians(2000.0 / 6_371_000.0);
        let r = S1ChordAngle::from_radians(100.0 / 6_371_000.0);
        let expected = S1ChordAngle::from_radians(2200.0 / 6_371_000.0);
        assert!(add_upper(add_upper(d, r), r) >= expected);
        assert!(d.length2() + 2.0 * r.length2() < expected.length2());
        assert_eq!(
            add_upper(S1ChordAngle::straight(), r),
            S1ChordAngle::straight()
        );
    }

    #[test]
    fn test_discovery_error_bounds_all_accepted_distances() {
        // In particular, the error near 85 degrees is larger than the endpoint-only error
        // returned at 90 degrees; evaluating the latter alone is not an interval bound.
        for degrees in [60.0_f64, 70.0, 85.0, 90.0, 120.0, 180.0] {
            let limit = S1ChordAngle::from_radians(degrees.to_radians());
            let error = discovery_error(limit);
            for i in 0..=1000 {
                let accepted = S1ChordAngle::from_length2(limit.length2() * i as f64 / 1000.0);
                assert!(get_update_min_distance_max_error(accepted) <= error);
            }
        }
    }
}

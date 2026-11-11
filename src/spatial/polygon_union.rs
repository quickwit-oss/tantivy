//! Exposed boundary of a union of polygon members, including boundaries of holes created by
//! their arrangement. Edges are split at crossings and coincident endpoints before classifying
//! each piece, so another member can cover part or all of a member's boundary.

use std::collections::{HashMap, HashSet};

use num_traits::{Signed, ToPrimitive};

use super::cell_index::CellIndex;
use super::contains_query::{interior_is_left, midpoint, QueryEdgeProvider};
use super::crossings::{robust_cross, S2EdgeCrosser};
use super::exact::{exact_cross, expensive_sign, to_exact};
use super::geometry_set::EdgeSet;
use super::math::{cross, dot, neg, normalize};
use super::s1chord_angle::S1ChordAngle;
use super::s2cap::S2Cap;
use super::s2cell_id::S2CellId;
use super::shape_index_region::{clipped_shape_contains_point, EdgeProvider};

#[derive(Clone, Copy)]
pub(crate) struct PolygonEdge {
    pub(crate) v0: [f64; 3],
    pub(crate) v1: [f64; 3],
    pub(crate) interior_left: bool,
    /// Original endpoints define the great circle even after its cuts have been rounded.
    source: ([f64; 3], [f64; 3]),
    bound: S2Cap,
}

impl PolygonEdge {
    fn new(v0: [f64; 3], v1: [f64; 3], interior_left: bool) -> Self {
        let center = midpoint((v0, v1));
        let radius = S1ChordAngle::from_points(&center, &v0).to_radians();
        Self {
            v0,
            v1,
            interior_left,
            source: (v0, v1),
            // Account for rounding in the midpoint and cap radius when rejecting edge pairs.
            bound: S2Cap::from_center_angle(center, radius + 1e-14),
        }
    }

    pub(crate) fn midpoint(&self) -> [f64; 3] {
        midpoint((self.v0, self.v1))
    }

    /// Position along the minor arc, with endpoints at zero and one.
    fn position(&self, point: &[f64; 3]) -> f64 {
        if *point == self.v0 {
            return 0.0;
        }
        if *point == self.v1 {
            return 1.0;
        }
        let (tangent, length) = self.frame();
        dot(point, &tangent).atan2(dot(point, &self.v0)) / length
    }

    fn frame(&self) -> ([f64; 3], f64) {
        let tangent = normalize(&cross(
            &robust_cross(&self.source.0, &self.source.1),
            &self.v0,
        ));
        let length = dot(&self.v1, &tangent).atan2(dot(&self.v1, &self.v0));
        (tangent, length)
    }

    fn piece(&self, v0: [f64; 3], v1: [f64; 3]) -> Self {
        let mut piece = Self::new(v0, v1, self.interior_left);
        piece.source = self.source;
        piece
    }
}

/// Edges of a member, excluding closure-to-next-ring links and zero-length edges. For an open
/// member the interior side is unused.
pub(crate) fn member_edges(member: &EdgeSet) -> impl Iterator<Item = (u32, PolygonEdge)> + '_ {
    member.ring_offsets.windows(2).flat_map(move |ring| {
        (ring[0]..ring[1].saturating_sub(1))
            .filter(|&i| member.vertices[i] != member.vertices[i + 1])
            .map(move |i| {
                (
                    i as u32,
                    PolygonEdge::new(
                        member.vertices[i],
                        member.vertices[i + 1],
                        interior_is_left(member, i as u32),
                    ),
                )
            })
    })
}

struct Overlap {
    start: f64,
    end: f64,
    member: u32,
    interior_left: bool,
}

struct SplitEdge {
    edge: PolygonEdge,
    cuts: Vec<(f64, [f64; 3])>,
    overlaps: Vec<Overlap>,
}

impl SplitEdge {
    fn new(edge: PolygonEdge) -> Self {
        Self {
            edge,
            cuts: vec![(0.0, edge.v0), (1.0, edge.v1)],
            overlaps: Vec::new(),
        }
    }

    fn add_cut(&mut self, point: [f64; 3]) {
        let position = self.edge.position(&point);
        if position > 0.0 && position < 1.0 {
            self.cuts.push((position, point));
        }
    }

    fn split_at(&mut self, other: PolygonEdge, member: u32) {
        if !self.edge.bound.intersects_cap(&other.bound) {
            return;
        }
        let edge = self.edge;
        // Treat coincident arcs geometrically, before the crossing predicate's symbolic
        // perturbation distinguishes otherwise collinear edges.
        let identical =
            edge.source == other.source || edge.source == (other.source.1, other.source.0);
        let collinear = identical
            || (expensive_sign(&edge.source.0, &edge.source.1, &other.source.0, false) == 0
                && expensive_sign(&edge.source.0, &edge.source.1, &other.source.1, false) == 0);
        if collinear {
            let a = edge.position(&other.v0);
            let mut b = edge.position(&other.v1);
            let same_direction = dot(
                &robust_cross(&edge.source.0, &edge.source.1),
                &robust_cross(&other.source.0, &other.source.1),
            ) > 0.0;
            let period = std::f64::consts::TAU / edge.frame().1;
            // atan2 wraps at pi. Unwrap the other minor arc using its direction before
            // intersecting it with this arc, including its equivalent positions a turn away.
            if same_direction && b < a {
                b += period;
            } else if !same_direction && b > a {
                b -= period;
            }
            for shift in [-period, 0.0, period] {
                let start = (a.min(b) + shift).max(0.0);
                let end = (a.max(b) + shift).min(1.0);
                if start < end {
                    self.add_cut(other.v0);
                    self.add_cut(other.v1);
                    self.overlaps.push(Overlap {
                        start,
                        end,
                        member,
                        interior_left: other.interior_left ^ !same_direction,
                    });
                }
            }
        } else if S2EdgeCrosser::new(&edge.v0, &edge.v1).crossing_sign_two(&other.v0, &other.v1) > 0
        {
            self.add_cut(intersection(edge, other));
        }
    }

    fn pieces(&mut self) -> Vec<(PolygonEdge, f64)> {
        self.cuts.sort_unstable_by(|a, b| a.0.total_cmp(&b.0));
        self.cuts.dedup_by(|a, b| a.0 == b.0);
        self.cuts
            .windows(2)
            .filter(|pair| pair[0].1 != pair[1].1)
            .map(|pair| {
                (
                    self.edge.piece(pair[0].1, pair[1].1),
                    0.5 * (pair[0].0 + pair[1].0),
                )
            })
            .collect()
    }

    fn overlaps_at(&self, position: f64) -> impl Iterator<Item = &Overlap> {
        self.overlaps
            .iter()
            .filter(move |overlap| overlap.start < position && position < overlap.end)
    }
}

/// Intersection of two crossing minor arcs. Exact products avoid moving the cut across a
/// narrow gap when the edges are nearly parallel; the resulting direction is rounded only once.
fn intersection(a: PolygonEdge, b: PolygonEdge) -> [f64; 3] {
    // Reuse an existing vertex at a T-junction. Renormalizing it can otherwise introduce a
    // tiny extra piece between two representations of the same intersection.
    for (point, other) in [
        (a.source.0, b),
        (a.source.1, b),
        (b.source.0, a),
        (b.source.1, a),
    ] {
        if expensive_sign(&other.source.0, &other.source.1, &point, false) == 0
            && dot(&point, &a.midpoint()) > 0.0
            && dot(&point, &b.midpoint()) > 0.0
        {
            return point;
        }
    }
    let ab = exact_cross(&to_exact(&a.source.0), &to_exact(&a.source.1));
    let cd = exact_cross(&to_exact(&b.source.0), &to_exact(&b.source.1));
    let point = exact_cross(&ab, &cd);
    let scale = point.iter().map(Signed::abs).max().unwrap();
    let point = normalize(&point.map(|coordinate| (coordinate / &scale).to_f64().unwrap()));
    let middle = a.midpoint();
    if dot(&point, &middle) < 0.0 {
        neg(point)
    } else {
        point
    }
}

/// Prepare the union's exposed boundary once per query. The query index narrows the edge
/// pairs that can meet, and each pair is split only once even if it occurs in several cells.
pub(crate) fn union_boundary(index: &CellIndex, provider: &QueryEdgeProvider) -> Vec<PolygonEdge> {
    let mut positions = HashMap::new();
    let mut edges = Vec::new();
    for (member, set) in provider.set.members.iter().enumerate() {
        if !set.closed {
            continue;
        }
        for (edge_idx, edge) in member_edges(set) {
            positions.insert((member as u32, edge_idx), edges.len());
            edges.push((member as u32, SplitEdge::new(edge)));
        }
    }
    if edges.is_empty() {
        return Vec::new();
    }

    let mut visited = HashSet::new();
    for cell in &index.cells {
        let shapes: Vec<_> = cell
            .shapes
            .iter()
            .filter(|shape| !shape.edge_indices.is_empty() && provider.is_closed(shape.geometry_id))
            .collect();
        for (i, a) in shapes.iter().enumerate() {
            for b in &shapes[i + 1..] {
                for ai in &a.edge_indices {
                    let Some(&ai) = positions.get(&(a.geometry_id.1, *ai)) else {
                        continue;
                    };
                    for bi in &b.edge_indices {
                        let Some(&bi) = positions.get(&(b.geometry_id.1, *bi)) else {
                            continue;
                        };
                        let (ai, bi) = (ai.min(bi), ai.max(bi));
                        if !visited.insert((ai, bi)) {
                            continue;
                        }
                        let (before, after) = edges.split_at_mut(bi);
                        let (am, a) = &mut before[ai];
                        let (bm, b) = &mut after[0];
                        a.split_at(b.edge, *bm);
                        b.split_at(a.edge, *am);
                    }
                }
            }
        }
    }

    let mut boundary = Vec::new();
    for (member, mut edge) in edges {
        for (piece, position) in edge.pieces() {
            let mut covered = [false; 2];
            covered[usize::from(piece.interior_left)] = true;
            for overlap in edge.overlaps_at(position) {
                covered[usize::from(overlap.interior_left)] = true;
            }
            let point = piece.midpoint();
            if let Some(cell) = index.find_cell(S2CellId::from_point(&point)) {
                for shape in &cell.shapes {
                    let other = shape.geometry_id.1;
                    if other == member
                        || !provider.is_closed(shape.geometry_id)
                        || edge
                            .overlaps_at(position)
                            .any(|overlap| overlap.member == other)
                    {
                        continue;
                    }
                    if clipped_shape_contains_point(cell.cell_id, shape, &point, |edge_idx| {
                        provider.get_edge(shape.geometry_id, edge_idx)
                    }) {
                        covered = [true; 2];
                        break;
                    }
                }
            }
            if covered[0] != covered[1] {
                boundary.push(piece);
            }
        }
    }
    boundary
}

pub(crate) struct EdgePiece {
    pub(crate) edge: PolygonEdge,
    /// The interior sides contributed by edges running along this piece, indexed by leftness.
    pub(crate) shared_sides: [bool; 2],
}

/// Split an edge against another boundary, retaining which sides of coincident pieces it covers.
pub(crate) fn split_edge(edge: PolygonEdge, boundary: &[PolygonEdge]) -> Vec<EdgePiece> {
    let mut split = SplitEdge::new(edge);
    for other in boundary {
        split.split_at(*other, 0);
    }
    split
        .pieces()
        .into_iter()
        .map(|(edge, position)| {
            let mut shared_sides = [false; 2];
            for overlap in split.overlaps_at(position) {
                shared_sides[usize::from(overlap.interior_left)] = true;
            }
            EdgePiece { edge, shared_sides }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spatial::sphere::Sphere;
    use crate::spatial::surface::Surface;

    #[test]
    fn test_coincident_minor_arcs_across_the_angle_wrap() {
        let point = |longitude| Sphere::project(longitude, 0.0);
        let edge = PolygonEdge::new(point(10.0), point(100.0), true);
        for (start, end, left) in [(80.0, 200.0, true), (200.0, 80.0, false)] {
            let other = PolygonEdge::new(point(start), point(end), true);
            let pieces = split_edge(edge, &[other]);
            assert_eq!(pieces.len(), 2);
            assert_eq!(pieces[0].shared_sides, [false; 2]);
            assert!(pieces[1].shared_sides[usize::from(left)]);
            assert!(!pieces[1].shared_sides[usize::from(!left)]);
        }
    }
}

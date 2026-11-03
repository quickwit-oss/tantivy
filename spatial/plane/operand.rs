//! Whole-value planar material over source coordinates.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};

use num_rational::BigRational;
use num_traits::{Signed, Zero};

use super::contact::{cross, difference, enclosing_bound, parameter, point, Point};
use super::edge_index::EdgeIndex;
use crate::spatial::geometry_set::{GeometryKind, GeometrySet};
use crate::spatial::r1interval::R1Interval;
use crate::spatial::r2rect::R2Rect;
use crate::spatial::surface::Surface;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Location {
    Exterior,
    Boundary,
    Interior,
}

pub(super) struct Edge {
    pub a: Point,
    pub b: Point,
    pub bound: R2Rect,
    pub ring: Option<usize>,
    pub source: (usize, usize),
}

pub(super) struct Operand {
    pub bound: R2Rect,
    pub edges: Vec<Edge>,
    pub points: BTreeSet<Point>,
    pub index: EdgeIndex,
    polygons: Vec<Vec<usize>>,
    ring_count: usize,
    endpoints: BTreeMap<Point, bool>,
}

impl Operand {
    pub fn new<S: Surface>(set: &GeometrySet<S>) -> Self {
        assert_eq!(S::DIMENSIONS, 2);
        let mut operand = Self {
            bound: R2Rect::empty(),
            edges: Vec::new(),
            points: BTreeSet::new(),
            index: EdgeIndex::default(),
            polygons: Vec::new(),
            ring_count: 0,
            endpoints: BTreeMap::new(),
        };
        for (member_id, member) in set.members.iter().enumerate() {
            let vertices: Vec<_> = member
                .vertices
                .iter()
                .map(|v| {
                    let coordinates = v.as_ref();
                    [coordinates[0], coordinates[1]]
                })
                .collect();
            let positions: Vec<_> = vertices.iter().copied().map(point).collect();
            for &vertex in &vertices {
                operand.bound = operand.bound.union(&R2Rect::from_point(vertex));
            }
            match member.kind {
                GeometryKind::Point => {
                    operand.points.extend(positions);
                }
                GeometryKind::LineString => {
                    let Some(first) = positions.first() else {
                        continue;
                    };
                    if positions.iter().all(|p| p == first) {
                        operand.points.insert(first.clone());
                        continue;
                    }
                    for endpoint in [first, positions.last().unwrap()] {
                        let odd = operand.endpoints.entry(endpoint.clone()).or_default();
                        *odd = !*odd;
                    }
                    for index in 0..positions.len() - 1 {
                        operand.add_edge(&vertices, &positions, member_id, index, None);
                    }
                }
                GeometryKind::Polygon => {
                    let mut rings = Vec::new();
                    for offsets in member.ring_offsets.windows(2) {
                        let ring = operand.ring_count;
                        operand.ring_count += 1;
                        for index in offsets[0]..offsets[1].saturating_sub(1) {
                            operand.add_edge(&vertices, &positions, member_id, index, Some(ring));
                        }
                        rings.push(ring);
                    }
                    operand.polygons.push(rings);
                }
            }
        }
        operand.index = EdgeIndex::new(&operand.edges);
        operand
    }

    fn add_edge(
        &mut self,
        vertices: &[[f64; 2]],
        positions: &[Point],
        member: usize,
        index: usize,
        ring: Option<usize>,
    ) {
        if positions[index] != positions[index + 1] {
            self.edges.push(Edge {
                a: positions[index].clone(),
                b: positions[index + 1].clone(),
                bound: R2Rect::from_point_pair(vertices[index], vertices[index + 1]),
                ring,
                source: (member, index),
            });
        }
    }

    pub fn area_contains(&self, p: &Point, direction: &Point) -> bool {
        if self.polygons.is_empty() {
            return false;
        }
        let point_bound = enclosing_bound(p);
        let ray_bound = R2Rect::new(
            R1Interval::new(point_bound.x.lo(), f64::INFINITY),
            point_bound.y,
        );
        let mut inside = vec![false; self.ring_count];
        for index in self.index.intersecting(&self.edges, &ray_bound) {
            let edge = &self.edges[index];
            let Some(ring) = edge.ring else {
                continue;
            };
            // The sign at p + epsilon * direction is the first nonzero coefficient.
            let above = |vertex: &Point| {
                if vertex[1] == p[1] {
                    direction[1].is_negative()
                } else {
                    vertex[1] > p[1]
                }
            };
            if above(&edge.a) == above(&edge.b) {
                continue;
            }
            let ab = difference(&edge.b, &edge.a);
            let mut side = cross(&ab, &difference(p, &edge.a));
            if side.is_zero() {
                side = cross(&ab, direction);
            }
            if side.is_positive() == (edge.b[1] > edge.a[1]) {
                inside[ring] = !inside[ring];
            }
        }
        self.polygons.iter().any(|rings| {
            rings.first().is_some_and(|&shell| inside[shell])
                && !rings.iter().skip(1).any(|&ring| inside[ring])
        })
    }

    pub fn locate(&self, p: &Point) -> Location {
        let mut rays = Vec::new();
        let mut on_line = false;
        for index in self.index.intersecting(&self.edges, &enclosing_bound(p)) {
            let edge = &self.edges[index];
            if parameter(&edge.a, &edge.b, p).is_none() {
                continue;
            }
            if edge.ring.is_some() {
                for vertex in [&edge.a, &edge.b] {
                    if vertex != p {
                        rays.push(difference(vertex, p));
                    }
                }
            } else {
                on_line = true;
            }
        }
        let area = if rays.is_empty() {
            if self.area_contains(p, &[BigRational::zero(), BigRational::zero()]) {
                Location::Interior
            } else {
                Location::Exterior
            }
        } else {
            rays.sort_unstable_by(direction_order);
            rays.dedup_by(|a, b| direction_order(a, b) == Ordering::Equal);
            let mut filled = false;
            let mut empty = false;
            for index in 0..rays.len() {
                let direction = sector_direction(&rays[index], &rays[(index + 1) % rays.len()]);
                if self.area_contains(p, &direction) {
                    filled = true;
                } else {
                    empty = true;
                }
            }
            match (filled, empty) {
                (true, false) => Location::Interior,
                (true, true) => Location::Boundary,
                _ => Location::Exterior,
            }
        };
        if area != Location::Exterior {
            return area;
        }
        if on_line {
            return if self.endpoints.get(p).copied().unwrap_or(false) {
                Location::Boundary
            } else {
                Location::Interior
            };
        }
        if self.points.contains(p) {
            Location::Interior
        } else {
            Location::Exterior
        }
    }
}

fn direction_order(a: &Point, b: &Point) -> Ordering {
    let lower = |v: &Point| v[1].is_negative() || (v[1].is_zero() && v[0].is_negative());
    lower(a)
        .cmp(&lower(b))
        .then_with(|| cross(b, a).cmp(&BigRational::zero()))
}

fn sector_direction(a: &Point, b: &Point) -> Point {
    let turn = cross(a, b);
    if turn.is_zero() {
        return [-a[1].clone(), a[0].clone()];
    }
    let a_size = a[0].abs() + a[1].abs();
    let b_size = b[0].abs() + b[1].abs();
    let sum = [
        &a[0] / &a_size + &b[0] / &b_size,
        &a[1] / a_size + &b[1] / b_size,
    ];
    if turn.is_positive() {
        sum
    } else {
        sum.map(|coordinate| -coordinate)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spatial::geometry::Geometry;
    use crate::spatial::geometry_set::to_geometry_set;
    use crate::spatial::plane::Plane;

    fn rectangle(x0: f64, y0: f64, x1: f64, y1: f64) -> Vec<[f64; 2]> {
        vec![[x0, y0], [x1, y0], [x1, y1], [x0, y1], [x0, y0]]
    }

    fn operand(geometry: Geometry<Plane>) -> Operand {
        Operand::new(&to_geometry_set(&geometry, 0))
    }

    #[test]
    fn test_union_seams_and_diagonal_contact() {
        let joined = operand(Geometry::MultiPolygon(vec![
            vec![rectangle(-1.0, -1.0, 0.0, 1.0)],
            vec![rectangle(0.0, -1.0, 1.0, 1.0)],
        ]));
        assert_eq!(joined.locate(&point([0.0, 0.0])), Location::Interior);
        assert_eq!(joined.locate(&point([0.0, 1.0])), Location::Boundary);
        let diagonal = operand(Geometry::MultiPolygon(vec![
            vec![rectangle(-1.0, -1.0, 0.0, 0.0)],
            vec![rectangle(0.0, 0.0, 1.0, 1.0)],
        ]));
        assert_eq!(diagonal.locate(&point([0.0, 0.0])), Location::Boundary);
    }

    #[test]
    fn test_empty_material_and_dimension_precedence() {
        let ring = rectangle(0.0, 0.0, 1.0, 1.0);
        let empty = operand(Geometry::Polygon(vec![ring.clone(), ring.clone()]));
        assert_eq!(empty.locate(&point([0.0, 0.0])), Location::Exterior);
        assert_eq!(empty.locate(&point([0.5, 0.0])), Location::Exterior);
        let mixed = operand(Geometry::GeometryCollection(vec![
            Geometry::Polygon(vec![ring]),
            Geometry::Point([0.0, 0.0]),
            Geometry::LineString(vec![[-1.0, 0.0], [0.5, 0.0]]),
        ]));
        assert_eq!(mixed.locate(&point([0.0, 0.0])), Location::Boundary);
        assert_eq!(mixed.locate(&point([-0.5, 0.0])), Location::Interior);
    }

    #[test]
    fn test_chain_endpoint_multiplicity() {
        let line = vec![[0.0, 0.0], [1.0, 0.0]];
        let single = operand(Geometry::LineString(line.clone()));
        let double = operand(Geometry::MultiLineString(vec![line.clone(), line]));
        assert_eq!(single.locate(&point([0.0, 0.0])), Location::Boundary);
        assert_eq!(double.locate(&point([0.0, 0.0])), Location::Interior);
        let collapsed = operand(Geometry::LineString(vec![[0.0, -0.0]; 3]));
        assert_eq!(collapsed.locate(&point([-0.0, 0.0])), Location::Interior);
    }

    #[test]
    fn test_indexed_area_location_against_exhaustive_rational_ray() {
        // This independent calculation uses ray intersection abscissas instead of
        // orientation signs.
        fn exhaustive(operand: &Operand, p: &Point, direction: &Point) -> Option<bool> {
            let mut inside = vec![false; operand.ring_count];
            for edge in &operand.edges {
                let Some(ring) = edge.ring else {
                    continue;
                };
                let dx = &edge.b[0] - &edge.a[0];
                let dy = &edge.b[1] - &edge.a[1];
                if parameter(&edge.a, &edge.b, p).is_some()
                    && (&dx * &direction[1] - &dy * &direction[0]).is_zero()
                {
                    // Sector and face callers choose a direction off incident traces.
                    return None;
                }
                let above =
                    |y: &BigRational| y > &p[1] || (y == &p[1] && direction[1].is_negative());
                if above(&edge.a[1]) == above(&edge.b[1]) {
                    continue;
                }
                let x = &edge.a[0] + (&p[1] - &edge.a[1]) * &dx / &dy - &p[0];
                let slope = &direction[1] * dx / dy - &direction[0];
                if x.is_positive() || (x.is_zero() && slope.is_positive()) {
                    inside[ring] = !inside[ring];
                }
            }
            Some(operand.polygons.iter().any(|rings| {
                rings.first().is_some_and(|&shell| inside[shell])
                    && !rings.iter().skip(1).any(|&ring| inside[ring])
            }))
        }
        let region = operand(Geometry::MultiPolygon(vec![
            vec![
                rectangle(-2.0, -2.0, 2.0, 2.0),
                rectangle(-1.0, -1.0, 1.0, 1.0),
            ],
            vec![vec![[0.0, 0.0], [1.0, 1.0], [0.0, 1.0], [0.0, 0.0]]],
            vec![vec![[0.0, 1.0], [1.0, -1.0], [1.0, 1.0], [0.0, 1.0]]],
        ]));
        let mut points: Vec<_> = region.edges.iter().map(|edge| edge.a.clone()).collect();
        points.push([
            BigRational::new(1.into(), 3.into()),
            BigRational::new(1.into(), 3.into()),
        ]);
        points.extend((0..128).map(|i| {
            [
                BigRational::new((i % 17 - 8).into(), 3.into()),
                BigRational::new(((i * 31) % 37 - 18).into(), 7.into()),
            ]
        }));
        for p in points {
            for direction in [
                [0.0, 0.0],
                [1.0, 0.0],
                [-1.0, 0.0],
                [0.0, 1.0],
                [0.0, -1.0],
                [1.0, 1.0],
            ] {
                let direction = point(direction);
                if let Some(expected) = exhaustive(&region, &p, &direction) {
                    assert_eq!(
                        region.area_contains(&p, &direction),
                        expected,
                        "{p:?}, {direction:?}"
                    );
                }
            }
        }
    }
}

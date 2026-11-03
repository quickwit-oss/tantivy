//! Relation witnesses on exact nodes, open fragments and adjacent faces.

use std::collections::{BTreeSet, HashMap};

use num_rational::BigRational;
use num_traits::{One, Zero};

use super::contact::{contacts, difference, enclosing_bound, parameter, position, Point};
use super::operand::{Edge, Location, Operand};

#[derive(Clone, Copy, Debug)]
pub(crate) enum Predicate {
    Intersects,
    Contains,
    Covers,
    CoveredBy,
    Within,
}

pub(super) struct PreparedOperand {
    pub operand: Operand,
    cuts: Vec<Vec<BigRational>>,
}

impl PreparedOperand {
    pub fn new(operand: Operand) -> Self {
        let mut cuts = vec![vec![BigRational::zero(), BigRational::one()]; operand.edges.len()];
        let mut order: Vec<_> = (0..operand.edges.len()).collect();
        order.sort_unstable_by(|&a, &b| {
            operand.edges[a]
                .bound
                .x
                .lo()
                .total_cmp(&operand.edges[b].bound.x.lo())
        });
        let mut active: Vec<usize> = Vec::new();
        for index in order {
            let edge = &operand.edges[index];
            active.retain(|&other| operand.edges[other].bound.x.hi() >= edge.bound.x.lo());
            for &other in &active {
                let other_edge = &operand.edges[other];
                debug_assert_ne!(edge.source, other_edge.source);
                for (a, b) in edge_contacts(edge, other_edge) {
                    cuts[index].push(a);
                    cuts[other].push(b);
                }
            }
            active.push(index);
        }
        for point in &operand.points {
            for index in operand
                .index
                .intersecting(&operand.edges, &enclosing_bound(point))
            {
                let edge = &operand.edges[index];
                if let Some(t) = parameter(&edge.a, &edge.b, point) {
                    cuts[index].push(t);
                }
            }
        }
        sort_cuts(&mut cuts);
        Self { operand, cuts }
    }
}

fn edge_contacts(a: &Edge, b: &Edge) -> Vec<(BigRational, BigRational)> {
    if a.bound.intersects(&b.bound) {
        contacts(&a.a, &a.b, &b.a, &b.b)
    } else {
        Vec::new()
    }
}

struct Cuts<'a> {
    original: &'a [Vec<BigRational>],
    changed: HashMap<usize, Vec<BigRational>>,
}

impl<'a> Cuts<'a> {
    fn new(original: &'a [Vec<BigRational>]) -> Self {
        Self {
            original,
            changed: HashMap::new(),
        }
    }

    fn add(&mut self, edge: usize, parameter: BigRational) {
        self.changed
            .entry(edge)
            .or_insert_with(|| self.original[edge].clone())
            .push(parameter);
    }

    fn get(&self, edge: usize) -> &[BigRational] {
        self.changed.get(&edge).unwrap_or(&self.original[edge])
    }

    fn finish(&mut self) {
        for parameters in self.changed.values_mut() {
            parameters.sort_unstable();
            parameters.dedup();
        }
    }
}

fn add_points(operand: &Operand, points: &BTreeSet<Point>, cuts: &mut Cuts<'_>) {
    for point in points {
        for index in operand
            .index
            .intersecting(&operand.edges, &enclosing_bound(point))
        {
            let edge = &operand.edges[index];
            if let Some(t) = parameter(&edge.a, &edge.b, point) {
                cuts.add(index, t);
            }
        }
    }
}

fn sort_cuts(cuts: &mut [Vec<BigRational>]) {
    for parameters in cuts {
        parameters.sort_unstable();
        parameters.dedup();
    }
}

#[derive(Default)]
struct Witnesses {
    left_nonempty: bool,
    right_nonempty: bool,
    intersects: bool,
    interiors: bool,
    left_outside: bool,
    right_outside: bool,
}

impl Witnesses {
    fn observe(&mut self, left: Location, right: Location) {
        let a = left != Location::Exterior;
        let b = right != Location::Exterior;
        self.left_nonempty |= a;
        self.right_nonempty |= b;
        self.intersects |= a && b;
        self.interiors |= left == Location::Interior && right == Location::Interior;
        self.left_outside |= a && !b;
        self.right_outside |= b && !a;
    }

    fn decided(&self, predicate: Predicate) -> Option<bool> {
        match predicate {
            Predicate::Intersects if self.intersects => Some(true),
            Predicate::Contains | Predicate::Covers if self.right_outside => Some(false),
            Predicate::CoveredBy | Predicate::Within if self.left_outside => Some(false),
            _ => None,
        }
    }

    fn result(&self, predicate: Predicate) -> bool {
        match predicate {
            Predicate::Intersects => self.intersects,
            Predicate::Contains => self.right_nonempty && !self.right_outside && self.interiors,
            Predicate::Covers => self.right_nonempty && !self.right_outside,
            Predicate::CoveredBy => self.left_nonempty && !self.left_outside,
            Predicate::Within => self.left_nonempty && !self.left_outside && self.interiors,
        }
    }
}

pub(super) fn evaluate(
    left: &PreparedOperand,
    right: &PreparedOperand,
    predicate: Predicate,
) -> bool {
    let mut left_cuts = Cuts::new(&left.cuts);
    let mut right_cuts = Cuts::new(&right.cuts);
    for (i, a) in left.operand.edges.iter().enumerate() {
        for j in right
            .operand
            .index
            .intersecting(&right.operand.edges, &a.bound)
        {
            let b = &right.operand.edges[j];
            for (t, u) in edge_contacts(a, b) {
                left_cuts.add(i, t);
                right_cuts.add(j, u);
            }
        }
    }
    add_points(&left.operand, &right.operand.points, &mut left_cuts);
    add_points(&right.operand, &left.operand.points, &mut right_cuts);
    left_cuts.finish();
    right_cuts.finish();
    let mut witnesses = Witnesses::default();
    let mut visited = BTreeSet::new();
    for point in left.operand.points.iter().chain(&right.operand.points) {
        if visited.insert(point.clone()) {
            witnesses.observe(left.operand.locate(point), right.operand.locate(point));
            if let Some(result) = witnesses.decided(predicate) {
                return result;
            }
        }
    }
    let sides = match predicate {
        Predicate::Contains | Predicate::Covers => [
            (&right.operand, &right_cuts, None),
            (&left.operand, &left_cuts, Some(&right.operand.bound)),
        ],
        Predicate::CoveredBy | Predicate::Within => [
            (&left.operand, &left_cuts, None),
            (&right.operand, &right_cuts, Some(&left.operand.bound)),
        ],
        Predicate::Intersects => [
            (&left.operand, &left_cuts, Some(&right.operand.bound)),
            (&right.operand, &right_cuts, Some(&left.operand.bound)),
        ],
    };
    for (operand, cuts, bound) in sides {
        // A reverse pocket witness must meet the covered operand's bounds.
        let edges = if let Some(bound) = bound {
            operand.index.intersecting(&operand.edges, bound)
        } else {
            (0..operand.edges.len()).collect()
        };
        for index in edges {
            let edge = &operand.edges[index];
            let parameters = cuts.get(index);
            for parameter in parameters {
                let point = position(&edge.a, &edge.b, parameter);
                if visited.insert(point.clone()) {
                    witnesses.observe(left.operand.locate(&point), right.operand.locate(&point));
                    if let Some(result) = witnesses.decided(predicate) {
                        return result;
                    }
                }
            }
            for interval in parameters.windows(2) {
                let middle = (&interval[0] + &interval[1]) / BigRational::from_integer(2.into());
                let point = position(&edge.a, &edge.b, &middle);
                witnesses.observe(left.operand.locate(&point), right.operand.locate(&point));
                if let Some(result) = witnesses.decided(predicate) {
                    return result;
                }
                if edge.ring.is_some() {
                    // Every bounded face where area membership differs has a ring
                    // fragment on its boundary. Inspect both adjacent open faces.
                    let direction = difference(&edge.b, &edge.a);
                    let normal = [-direction[1].clone(), direction[0].clone()];
                    for direction in [normal.clone(), normal.map(|coordinate| -coordinate)] {
                        let location = |operand: &Operand| {
                            if operand.area_contains(&point, &direction) {
                                Location::Interior
                            } else {
                                Location::Exterior
                            }
                        };
                        witnesses.observe(location(&left.operand), location(&right.operand));
                        if let Some(result) = witnesses.decided(predicate) {
                            return result;
                        }
                    }
                }
            }
        }
    }
    witnesses.result(predicate)
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

    fn prepared(geometry: Geometry<Plane>) -> PreparedOperand {
        PreparedOperand::new(Operand::new(&to_geometry_set(&geometry, 0)))
    }

    #[test]
    fn test_three_bar_hidden_pocket() {
        let bars = prepared(Geometry::MultiPolygon(vec![
            vec![rectangle(0.0, 0.0, 4.0, 1.0)],
            vec![vec![
                [-0.5, 0.0],
                [0.5, 0.0],
                [2.5, 4.0],
                [1.5, 4.0],
                [-0.5, 0.0],
            ]],
            vec![vec![
                [3.5, 0.0],
                [4.5, 0.0],
                [2.5, 4.0],
                [1.5, 4.0],
                [3.5, 0.0],
            ]],
        ]));
        let ring = vec![[0.5, 0.5], [3.5, 0.5], [2.0, 3.5], [0.5, 0.5]];
        let boundary = prepared(Geometry::LineString(ring.clone()));
        let polygon = prepared(Geometry::Polygon(vec![ring]));
        assert!(evaluate(&bars, &boundary, Predicate::Covers));
        assert!(evaluate(&bars, &polygon, Predicate::Intersects));
        assert!(!evaluate(&bars, &polygon, Predicate::Covers));
        assert!(!evaluate(&bars, &polygon, Predicate::Contains));
    }

    #[test]
    fn test_boundary_line_coverage_and_interior() {
        let area = prepared(Geometry::Polygon(vec![rectangle(0.0, 0.0, 1.0, 1.0)]));
        let boundary = prepared(Geometry::LineString(vec![[0.25, 0.0], [0.75, 0.0]]));
        assert!(evaluate(&area, &boundary, Predicate::Covers));
        assert!(!evaluate(&area, &boundary, Predicate::Contains));
        assert!(evaluate(&boundary, &area, Predicate::CoveredBy));
    }

    #[test]
    fn test_point_does_not_cover_a_line_midpoint() {
        let point = prepared(Geometry::Point([0.5, 0.0]));
        let line = prepared(Geometry::LineString(vec![[0.0, 0.0], [1.0, 0.0]]));
        assert!(evaluate(&point, &line, Predicate::Intersects));
        assert!(!evaluate(&point, &line, Predicate::Covers));
        assert!(evaluate(&line, &point, Predicate::Contains));
    }

    #[test]
    fn test_effective_empty_never_matches() {
        let ring = rectangle(0.0, 0.0, 1.0, 1.0);
        let empty = prepared(Geometry::Polygon(vec![ring.clone(), ring.clone()]));
        let area = prepared(Geometry::Polygon(vec![ring]));
        for predicate in [
            Predicate::Intersects,
            Predicate::Covers,
            Predicate::Contains,
            Predicate::CoveredBy,
        ] {
            assert!(!evaluate(&empty, &area, predicate));
            assert!(!evaluate(&area, &empty, predicate));
            assert!(!evaluate(&empty, &empty, predicate));
        }
    }
}

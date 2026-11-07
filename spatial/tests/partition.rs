use crate::spatial::clipped_shape::GeometryId;
use crate::spatial::geometry_set::EdgeSet;
use crate::spatial::plane::Plane;
use crate::spatial::r2rect::R2Rect;
use crate::spatial::s2cell_id::S2CellId;
use crate::spatial::shape_index::ShapeCell;
use num_bigint::BigInt;
use num_rational::BigRational;
use num_traits::{One, Zero};
use std::collections::BTreeMap;

pub(crate) fn rational(value: f64) -> BigRational {
    BigRational::from_float(value).unwrap()
}

pub(crate) fn displacement() -> BigRational {
    // Finite binary64 determinants have magnitude at least 2^-2148 when nonzero.
    // Coordinate differences are below 2^1025, so this displacement cannot reverse
    // a nonzero sign, even for subnormal inputs. Its square breaks the remaining ties.
    BigRational::new(BigInt::one(), BigInt::one() << 4096)
}

pub(crate) fn nominal_bound(id: S2CellId) -> R2Rect {
    let (_, i, j, _) = id.to_face_ij_orientation();
    let width = 1 << (30 - id.level());
    let lo = [i & -width, j & -width];
    let coordinate = |n: i32| n as f64 / (1u64 << 29) as f64 - 1.0;
    R2Rect::from_points(
        [180.0 * coordinate(lo[0]), 90.0 * coordinate(lo[1])],
        [
            180.0 * coordinate(lo[0] + width),
            90.0 * coordinate(lo[1] + width),
        ],
    )
}

pub(crate) fn interval_intersects(a: [f64; 2], b: [f64; 2], rect: &R2Rect) -> bool {
    let a = a.map(rational);
    let b = b.map(rational);
    let mut enter = BigRational::zero();
    let mut leave = BigRational::one();
    for axis in 0..2 {
        let lo = rational(rect[axis].lo());
        let hi = rational(rect[axis].hi());
        let delta = &b[axis] - &a[axis];
        if delta.is_zero() {
            if a[axis] < lo || a[axis] > hi {
                return false;
            }
        } else {
            let first = (&lo - &a[axis]) / &delta;
            let last = (&hi - &a[axis]) / &delta;
            enter = enter.max(first.clone().min(last.clone()));
            leave = leave.min(first.max(last));
            if enter > leave {
                return false;
            }
        }
    }
    true
}

pub(crate) fn shifted_contains(point: [f64; 2], ring: &[[f64; 2]]) -> bool {
    let e = displacement();
    let x = rational(point[0]) + &e;
    let y = rational(point[1]) + &e * &e;
    let mut inside = false;
    for edge in ring.windows(2) {
        let [a, b] = [edge[0].map(rational), edge[1].map(rational)];
        if (a[1] > y) != (b[1] > y) {
            let intersection = &a[0] + (&y - &a[1]) * (&b[0] - &a[0]) / (&b[1] - &a[1]);
            inside ^= intersection > x;
        }
    }
    inside
}

pub(crate) fn supported_edges(member: &EdgeSet<Plane>, bound: &R2Rect) -> Vec<u32> {
    // Open members have one chain. Closed members leave an edge-id gap between rings.
    let mut result = Vec::new();
    for offsets in member.ring_offsets.windows(2) {
        let start = offsets[0];
        let end = offsets[1];
        if end == start + 1 {
            if interval_intersects(member.vertices[start], member.vertices[start], bound) {
                result.push(start as u32);
            }
        } else {
            for i in start..end.saturating_sub(1) {
                if interval_intersects(member.vertices[i], member.vertices[i + 1], bound) {
                    result.push(i as u32);
                }
            }
        }
    }
    result
}

pub(crate) fn member_contains(member: &EdgeSet<Plane>, point: [f64; 2]) -> bool {
    if !member.closed {
        return false;
    }
    let mut rings = member
        .ring_offsets
        .windows(2)
        .map(|offsets| shifted_contains(point, &member.vertices[offsets[0]..offsets[1]]));
    rings.next().unwrap_or(false) && !rings.any(|inside| inside)
}

pub(crate) fn assert_partition(cells: &[ShapeCell], members: &[(GeometryId, &EdgeSet<Plane>)]) {
    for pair in cells.windows(2) {
        assert!(pair[0].cell_id.range_max() < pair[1].cell_id.range_min());
    }
    fn visit(id: S2CellId, cells: &[ShapeCell], members: &[(GeometryId, &EdgeSet<Plane>)]) {
        if !cells.is_empty() && cells[0].cell_id != id {
            assert!(!id.is_leaf());
            let mut start = 0;
            for child in id.children() {
                let count =
                    cells[start..].partition_point(|cell| cell.cell_id <= child.range_max());
                let end = start + count;
                assert!(cells[start..end]
                    .iter()
                    .all(|cell| child.contains(cell.cell_id)));
                visit(child, &cells[start..end], members);
                start = end;
            }
            assert_eq!(start, cells.len());
            return;
        }
        let bound = nominal_bound(id);
        let center = [
            (bound.x.lo() + bound.x.hi()) / 2.0,
            (bound.y.lo() + bound.y.hi()) / 2.0,
        ];
        let mut expected = BTreeMap::new();
        for &(gid, member) in members {
            let edges = supported_edges(member, &bound);
            let inside = member_contains(member, center);
            if inside || !edges.is_empty() {
                expected.insert(gid, (inside, edges));
            }
        }
        let mut actual = BTreeMap::new();
        if let Some(cell) = cells.first() {
            assert_eq!(cells.len(), 1);
            for shape in &cell.shapes {
                let mut edges = shape.edge_indices.clone();
                edges.sort_unstable();
                assert!(!edges.windows(2).any(|pair| pair[0] == pair[1]));
                assert!(actual
                    .insert(shape.geometry_id, (shape.contains_center, edges))
                    .is_none());
            }
        }
        assert_eq!(actual, expected, "cell {id:?}, bounds {bound:?}");
    }
    visit(S2CellId::from_face(0), cells, members);
}

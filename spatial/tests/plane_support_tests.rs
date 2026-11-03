use std::collections::BTreeMap;

use common::CountingWriter;
use num_bigint::BigInt;
use num_rational::BigRational;
use num_traits::{One, Zero};

use crate::spatial::cell_index_reader::CellIndexReader;
use crate::spatial::clip_options::ClipOptions;
use crate::spatial::clipped_shape::{ClippedShape, GeometryId};
use crate::spatial::clipper::Clipper;
use crate::spatial::covering::{
    covering_contains_center, covering_split, split_edges, CoveringCell,
};
use crate::spatial::edge_crosser::EdgeCrosser;
use crate::spatial::geometry::Geometry;
use crate::spatial::geometry_set::{to_geometry_set, EdgeSet};
use crate::spatial::plane::crosser::PlaneEdgeCrosser;
use crate::spatial::plane::math::point_in_polygon;
use crate::spatial::plane::Plane;
use crate::spatial::query_edge_provider::QueryEdgeProvider;
use crate::spatial::r2rect::R2Rect;
use crate::spatial::s2cell::S2Cell;
use crate::spatial::s2cell_id::S2CellId;
use crate::spatial::s2padded_cell::S2PaddedCell;
use crate::spatial::shape_index::ShapeCell;
use crate::spatial::shape_index_region::any_edge_intersects;
use crate::spatial::surface::Surface;

fn rational(value: f64) -> BigRational {
    BigRational::from_float(value).unwrap()
}

fn displacement() -> BigRational {
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

fn supported_edges(member: &EdgeSet<Plane>, bound: &R2Rect) -> Vec<u32> {
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

fn member_contains(member: &EdgeSet<Plane>, point: [f64; 2]) -> bool {
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

#[test]
fn test_plane_build_closed_support() {
    let tiny = f64::from_bits(1);
    let mut shapes = vec![
        Geometry::LineString(vec![[0.0, -0.75], [0.0, 0.75]]),
        Geometry::LineString(vec![[-0.75, 0.0], [0.75, 0.0]]),
        Geometry::LineString(vec![[-0.5, -0.5], [0.0, 0.0]]),
        Geometry::LineString(vec![[0.8, 0.4], [-0.4, -0.8]]),
        Geometry::LineString(vec![[-tiny, -tiny], [tiny, tiny]]),
        Geometry::MultiPoint(vec![[0.0, 0.0], [-tiny, tiny], [1.0, 1.0], [-1.0, -1.0]]),
    ];
    for x in [-0.625, -0.375, 0.375, 0.625] {
        for y in [-0.625, -0.375, 0.375, 0.625] {
            shapes.push(Geometry::Point([x, y]));
        }
    }
    for reverse in [false, true] {
        let mut shapes = shapes.clone();
        if reverse {
            shapes.reverse();
        }
        let set = to_geometry_set(&Geometry::GeometryCollection(shapes), 0);
        let index = Clipper::<Plane>::new(
            ClipOptions::default()
                .with_max_edges_per_cell(1)
                .with_min_short_edge_fraction(0.0),
        )
        .build(std::slice::from_ref(&set));
        assert!(index.cells.iter().any(|cell| cell.cell_id.level() >= 3));
        let members: Vec<_> = set
            .members
            .iter()
            .enumerate()
            .map(|(i, m)| ((0, i as u32), m))
            .collect();
        assert_partition(&index.cells, &members);
        let mut bytes = Vec::new();
        index.write(&mut CountingWriter::wrap(&mut bytes));
        let reader = CellIndexReader::open(&bytes).unwrap();
        let cells: Vec<_> = reader.iter().collect();
        assert_partition(&cells, &members);
    }
}

#[test]
fn test_plane_build_ring_roles() {
    let rectangle = |x0, y0, x1, y1| vec![[x0, y0], [x1, y0], [x1, y1], [x0, y1], [x0, y0]];
    let shell = rectangle(-80.0, -40.0, 80.0, 40.0);
    let mut twice = shell.clone();
    twice.extend_from_slice(&shell[1..]);
    for rings in [
        vec![
            shell.clone(),
            rectangle(-40.0, -20.0, 20.0, 20.0),
            rectangle(-20.0, -10.0, 40.0, 30.0),
        ],
        vec![shell.clone(), rectangle(100.0, -20.0, 140.0, 20.0)],
        vec![shell.clone(), shell.clone()],
        vec![twice],
        vec![Vec::new(), shell],
    ] {
        for reverse in [false, true] {
            let mut rings = rings.clone();
            if reverse {
                for ring in &mut rings {
                    ring.reverse();
                }
            }
            let set = to_geometry_set(&Geometry::<Plane>::Polygon(rings), 0);
            for limit in [1, 100] {
                let index = Clipper::<Plane>::new(
                    ClipOptions::default()
                        .with_max_edges_per_cell(limit)
                        .with_min_short_edge_fraction(0.0),
                )
                .build(std::slice::from_ref(&set));
                assert_partition(&index.cells, &[((0, 0), &set.members[0])]);
                let provider = QueryEdgeProvider { set: set.clone() };
                for point in [[0.0, 0.0], [-30.0, 0.0], [60.0, 0.0], [120.0, 0.0]] {
                    assert_eq!(
                        crate::spatial::shape_index_region::index_contains_point::<Plane, _>(
                            &index,
                            &provider,
                            (0, 0),
                            &point,
                        ),
                        member_contains(&set.members[0], point),
                    );
                }
            }
        }
    }
}

#[test]
fn test_plane_shrink_closed_boundaries() {
    for (point, level) in [
        ([0.0, 0.25], 0),
        ([0.25, 0.375], 2),
        ([1.0, 1.0], 30),
        ([0.3, 0.3], 30),
        ([-1.0, -1.0], 30),
    ] {
        let point = Plane::face_uv_to_point(0, point[0], point[1]);
        let set = to_geometry_set(&Geometry::<Plane>::Point(point), 0);
        let index = Clipper::<Plane>::new(ClipOptions::default()).build(std::slice::from_ref(&set));
        assert_eq!(index.cells.len(), 1);
        assert_eq!(index.cells[0].cell_id.level(), level, "{point:?}");
        assert_partition(&index.cells, &[((0, 0), &set.members[0])]);
    }
}

#[test]
fn test_plane_query_contacts_next_to_dividers() {
    let tiny = f64::from_bits(1);
    let edges = [
        ([0.8, 0.4], [-0.4, -0.8]),
        ([0.0, 0.0], [0.0, 0.0]),
        ([-tiny, -0.25], [-tiny, 0.25]),
        ([tiny, -0.25], [tiny, 0.25]),
        ([-0.25, -0.25], [0.0, 0.0]),
        ([-0.25, -0.25], [0.0, -tiny]),
    ];
    for padding in [0.0, 0.125] {
        let parent = S2PaddedCell::<Plane>::new(S2CellId::from_face(0), padding);
        for (a, b) in edges {
            for swap in [false, true] {
                for sx in [-1.0, 1.0] {
                    for sy in [-1.0, 1.0] {
                        let transform = |mut p: [f64; 2]| {
                            if swap {
                                p.swap(0, 1);
                            }
                            [sx * p[0], sy * p[1]]
                        };
                        let (a, b) = (transform(a), transform(b));
                        for (a, b) in [(a, b), (b, a)] {
                            let children = split_edges(&parent, &[0], &|_| (a, b));
                            for pos in 0..4 {
                                let (i, j) = parent.get_child_ij(pos);
                                let bound = nominal_bound(parent.id().child(pos));
                                assert_eq!(
                                    !children[i][j].is_empty(),
                                    interval_intersects(a, b, &bound),
                                    "{a:?}, {b:?}, {bound:?}"
                                );
                            }
                        }
                    }
                }
            }
        }
    }
}

#[test]
fn test_plane_query_partition_and_region() {
    for mut ring in [
        vec![
            [-0.5, -0.5],
            [0.5, -0.5],
            [0.5, 0.5],
            [-0.5, 0.5],
            [-0.5, -0.5],
        ],
        vec![[0.8, 0.4], [-0.4, -0.8], [0.8, -0.8], [0.8, 0.4]],
    ] {
        for reverse in [false, true] {
            if reverse {
                ring.reverse();
            }
            let set = to_geometry_set(&Geometry::<Plane>::Polygon(vec![ring.clone()]), 0);
            let index = Clipper::<Plane>::new(
                ClipOptions::default()
                    .with_max_edges_per_cell(1)
                    .with_min_short_edge_fraction(0.0),
            )
            .build(std::slice::from_ref(&set));
            assert_partition(&index.cells, &[((0, 0), &set.members[0])]);
            let get_edge = |i: u32| (ring[i as usize], ring[i as usize + 1]);
            let provider = QueryEdgeProvider { set };
            let mut pending = vec![CoveringCell {
                pcell: S2PaddedCell::<Plane>::new(S2CellId::from_face(0), 0.0),
                query_edges: (0..ring.len() as u32 - 1).collect(),
                contains_center: shifted_contains([0.0, 0.0], &ring),
                index_start: 0,
                index_end: 1,
                first_index_level: 0,
            }];
            while let Some(parent) = pending.pop() {
                let mut children = covering_split(&parent, &get_edge, &|_, _, _| unreachable!());
                covering_contains_center(
                    &parent.pcell.get_center(),
                    &mut children,
                    &get_edge,
                    &|point| shifted_contains(*point, &ring),
                );
                for id in parent.pcell.id().children() {
                    let expected = supported_edges(&provider.set.members[0], &nominal_bound(id));
                    let inside = shifted_contains(Plane::cell_center(id), &ring);
                    let shape = ClippedShape {
                        geometry_id: (0, 0),
                        contains_center: false,
                        edge_indices: parent.query_edges.clone(),
                    };
                    assert_eq!(
                        any_edge_intersects::<Plane, _>(&shape, &S2Cell::new(id), &provider),
                        !expected.is_empty()
                    );
                    let child = children.iter().find(|child| child.pcell.id() == id);
                    match child {
                        Some(child) => {
                            assert_eq!(child.query_edges, expected, "{id:?}");
                            assert_eq!(child.contains_center, inside, "{id:?}");
                        }
                        None => {
                            assert!(expected.is_empty());
                            assert!(!inside);
                        }
                    }
                }
                if parent.pcell.level() < 3 {
                    pending.extend(children);
                }
            }
        }
    }
}

fn shifted_crossing(a: [f64; 2], b: [f64; 2], c: [f64; 2], d: [f64; 2]) -> bool {
    let e = displacement();
    let shifted = |p: [f64; 2]| [rational(p[0]) + &e, rational(p[1]) + &e * &e];
    let (a, b, c, d) = (shifted(a), shifted(b), c.map(rational), d.map(rational));
    let orientation = |a: &[BigRational; 2], b: &[BigRational; 2], c: &[BigRational; 2]| {
        (&b[0] - &a[0]) * (&c[1] - &a[1]) - (&b[1] - &a[1]) * (&c[0] - &a[0])
    };
    let straddles = |x: BigRational, y: BigRational| {
        (x < BigRational::zero() && y > BigRational::zero())
            || (x > BigRational::zero() && y < BigRational::zero())
    };
    straddles(orientation(&a, &b, &c), orientation(&a, &b, &d))
        && straddles(orientation(&c, &d, &a), orientation(&c, &d, &b))
}

#[test]
fn test_plane_transport_extreme_signs() {
    for s in [f64::from_bits(1), 2.0f64.powi(-600), 1.0, 2.0f64.powi(1000)] {
        let ring = [[-s, -s], [s, -s], [s, s], [-s, s], [-s, -s]];
        let points = [[0.0, 0.0], [-s, 0.0], [s, 0.0], [0.0, s], [-s, -s]];
        for p in points {
            assert_eq!(
                point_in_polygon(&p, &ring),
                shifted_contains(p, &ring),
                "{p:?}"
            );
            for q in points {
                let mut crosser = PlaneEdgeCrosser::new(&p, &q);
                for edge in ring.windows(2) {
                    assert_eq!(
                        crosser.edge_or_vertex_crossing_two(&edge[0], &edge[1]),
                        shifted_crossing(p, q, edge[0], edge[1]),
                        "{p:?}, {q:?}, {edge:?}"
                    );
                }
            }
        }
        let mut crosser = PlaneEdgeCrosser::new(&[-s, 0.0], &[s, 0.0]);
        assert_eq!(crosser.crossing_sign_two(&[0.0, -s], &[0.0, s]), 1);
        assert_eq!(crosser.crossing_sign_two(&[-s, 0.0], &[0.0, s]), 0);
        assert_eq!(
            crosser.crossing_sign_two(&[-2.0 * s, 0.0], &[2.0 * s, 0.0]),
            -1
        );
    }
}

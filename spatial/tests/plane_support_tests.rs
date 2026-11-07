use common::CountingWriter;
use num_rational::BigRational;
use num_traits::Zero;

use crate::spatial::cell_index_reader::CellIndexReader;
use crate::spatial::clip_options::ClipOptions;
use crate::spatial::clipped_shape::ClippedShape;
use crate::spatial::clipper::Clipper;
use crate::spatial::covering::{
    covering_contains_center, covering_split, split_edges, CoveringCell,
};
use crate::spatial::edge_crosser::EdgeCrosser;
use crate::spatial::geometry::to_geometry_set;
use crate::spatial::geometry::Geometry;
use crate::spatial::plane::crosser::PlaneEdgeCrosser;
use crate::spatial::plane::math::point_in_polygon;
use crate::spatial::plane::Plane;
use crate::spatial::query_edge_provider::QueryEdgeProvider;
use crate::spatial::s2cell::S2Cell;
use crate::spatial::s2cell_id::S2CellId;
use crate::spatial::s2padded_cell::S2PaddedCell;
use crate::spatial::shape_index_region::any_edge_intersects;
use crate::spatial::surface::Surface;

pub(crate) use super::partition::*;

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

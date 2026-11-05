use num_bigint::BigInt;
use num_rational::BigRational;

use crate::spatial::clip_options::ClipOptions;
use crate::spatial::clipper::Clipper;
use crate::spatial::edge_crosser::EdgeCrosser;
use crate::spatial::geometry::Geometry;
use crate::spatial::geometry_set::to_geometry_set;
use crate::spatial::plane::crosser::PlaneEdgeCrosser;
use crate::spatial::plane::math::point_in_polygon;
use crate::spatial::plane::Plane;
use crate::spatial::query_edge_provider::QueryEdgeProvider;
use crate::spatial::shape_index_region::index_contains_point;
use crate::spatial::surface::Surface;

fn parity(a: [f64; 2], b: [f64; 2], ring: &[[f64; 2]]) -> bool {
    let mut crosser = PlaneEdgeCrosser::new(&a, &b);
    ring.windows(2).fold(false, |inside, edge| {
        inside ^ crosser.edge_or_vertex_crossing_two(&edge[0], &edge[1])
    })
}

fn shifted_contains(p: [f64; 2], ring: &[[f64; 2]]) -> bool {
    let rational = |x| BigRational::from_float(x).unwrap();
    // This finite displacement is exact and smaller than every nonincident clearance
    // in the dyadic corpus below. The oracle intersects a ray, without zero-sign rules.
    let e = BigRational::new(BigInt::from(1), BigInt::from(1) << 40);
    let x = rational(p[0]) + &e;
    let y = rational(p[1]) + &e * &e;
    let mut inside = false;
    for edge in ring.windows(2) {
        let [a, b] = [edge[0].map(rational), edge[1].map(rational)];
        if (a[1] > y) != (b[1] > y) {
            let intersection = &a[0] + (&y - &a[1]) * (&b[0] - &a[0]) / (&b[1] - &a[1]);
            if intersection > x {
                inside = !inside;
            }
        }
    }
    inside
}

#[test]
fn test_plane_transport_composition() {
    let rings = [
        vec![[0.0, 0.0], [1.0, 0.0], [1.0, 1.0], [0.0, 1.0]],
        vec![[0.0, 0.0], [1.0, 0.0], [0.0, 1.0]],
        vec![
            [0.0, 0.0],
            [1.0, 0.0],
            [1.0, 0.5],
            [0.5, 0.5],
            [0.5, 1.0],
            [0.0, 1.0],
        ],
    ];
    for base in rings {
        for swap in [false, true] {
            for sx in [-1.0, 1.0] {
                for sy in [-1.0, 1.0] {
                    let transform = |mut p: [f64; 2]| {
                        if swap {
                            p.swap(0, 1);
                        }
                        [sx * p[0], sy * p[1]]
                    };
                    for reverse in [false, true] {
                        let mut ring: Vec<_> = base.iter().copied().map(transform).collect();
                        if reverse {
                            ring.reverse();
                        }
                        ring.push(ring[0]);
                        let mut points = Vec::new();
                        for x in [-0.5, 0.0, 0.5, 1.0, 1.5] {
                            for y in [-0.5, 0.0, 0.5, 1.0, 1.5] {
                                points.push(transform([x, y]));
                            }
                        }
                        let expected: Vec<_> =
                            points.iter().map(|&p| shifted_contains(p, &ring)).collect();
                        for (i, &p) in points.iter().enumerate() {
                            assert_eq!(point_in_polygon(&p, &ring), expected[i], "{p:?}, {ring:?}");
                        }
                        let mut pairs = vec![vec![false; points.len()]; points.len()];
                        for (i, &a) in points.iter().enumerate() {
                            for (j, &b) in points.iter().enumerate() {
                                pairs[i][j] = parity(a, b, &ring);
                                assert_eq!(
                                    pairs[i][j],
                                    expected[i] ^ expected[j],
                                    "{a:?}, {b:?}, {ring:?}"
                                );
                            }
                        }
                        for i in 0..points.len() {
                            for j in 0..points.len() {
                                assert_eq!(pairs[i][j], pairs[j][i]);
                                for k in 0..points.len() {
                                    assert_eq!(pairs[i][j] ^ pairs[j][k], pairs[i][k]);
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

#[test]
fn test_plane_corner_and_collinear_paths() {
    for (ring, route, expected) in [
        (
            vec![
                [0.25, 0.25],
                [0.8, 0.25],
                [0.8, 0.8],
                [0.25, 0.8],
                [0.25, 0.25],
            ],
            vec![[0.0, 0.0], [0.5, 0.5], [0.25, 0.25], [0.375, 0.125]],
            vec![true, false, true],
        ),
        (
            vec![[0.0, 0.0], [1.0, 0.0], [1.0, 1.0], [0.0, 1.0], [0.0, 0.0]],
            vec![[0.5, -1.0], [0.5, 0.0], [2.0, 0.0]],
            vec![true, true],
        ),
    ] {
        let mut composed = false;
        for (leg, expected) in route.windows(2).zip(expected) {
            assert_eq!(parity(leg[0], leg[1], &ring), expected);
            assert_eq!(parity(leg[1], leg[0], &ring), expected);
            composed ^= expected;
        }
        assert!(!composed);
        assert!(!parity(route[0], *route.last().unwrap(), &ring));
    }
}

#[test]
fn test_plane_degenerate_transport_and_edges() {
    let mut stationary = PlaneEdgeCrosser::new(&[0.0, 0.0], &[0.0, 0.0]);
    assert!(!stationary.edge_or_vertex_crossing_two(&[-1.0, 0.0], &[1.0, 0.0]));
    let mut crosser = PlaneEdgeCrosser::new(&[-1.0, 0.0], &[1.0, 0.0]);
    assert!(!crosser.edge_or_vertex_crossing_two(&[0.0, 0.0], &[0.0, 0.0]));
    assert_eq!(crosser.crossing_sign_two(&[1.0, 0.0], &[1.0, 1.0]), 0);
    assert_eq!(crosser.crossing_sign_two(&[-0.5, 0.0], &[0.5, 0.0]), -1);
}

#[test]
fn test_plane_stored_boundary_state() {
    for [x0, y0, x1, y1] in [
        [-0.8, -0.8, 0.0, 0.8],
        [0.0, -0.8, 0.8, 0.8],
        [-0.8, -0.8, 0.8, 0.0],
        [-0.8, 0.0, 0.8, 0.8],
        [-1.0, -1.0, 0.8, 0.8],
    ] {
        for reverse in [false, true] {
            let mut ring = vec![[x0, y0], [x1, y0], [x1, y1], [x0, y1]];
            if reverse {
                ring.reverse();
            }
            ring.push(ring[0]);
            let set = to_geometry_set(&Geometry::<Plane>::Polygon(vec![ring]), 0);
            let index = Clipper::new(ClipOptions::default()).build(std::slice::from_ref(&set));
            assert_eq!(index.cells.len(), 1);
            let cell = &index.cells[0];
            let [u, v] = Plane::cell_center(cell.cell_id);
            assert_eq!(
                cell.shapes[0].contains_center,
                u >= x0 && u < x1 && v >= y0 && v < y1
            );
            let edges = QueryEdgeProvider { set };
            let target = [(x0 + x1) / 2.0, (y0 + y1) / 2.0];
            assert!(index_contains_point::<Plane, _>(
                &index,
                &edges,
                (0, 0),
                &target
            ));
        }
    }
}

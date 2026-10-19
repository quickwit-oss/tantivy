use super::*;
use crate::spatial::plane::Plane;
use crate::spatial::sphere::Sphere;

fn check_contacts<S: Surface>() {
    let parent = S2PaddedCell::<S>::new(S2CellId::from_face(0), S::CELL_PADDING);
    for (a, b) in [
        ([0.0, -0.75], [0.0, 0.75]),
        ([-0.75, 0.0], [0.75, 0.0]),
        ([-0.75, -0.5], [0.0, 0.0]),
    ] {
        for reverse in [false, true] {
            let a = S::face_uv_to_point(0, a[0], a[1]);
            let b = S::face_uv_to_point(0, b[0], b[1]);
            let get_edge = |_| if reverse { (b, a) } else { (a, b) };
            let (a_uv, b_uv) = S::clip_to_face(&a, &b, 0, S::CELL_PADDING).unwrap();
            assert!(a_uv[0] == 0.0 || a_uv[1] == 0.0 || b_uv == [0.0, 0.0]);
            assert!(b_uv[0] == 0.0 || b_uv[1] == 0.0);
            // Each closed child touches the edge, even if only at the common corner.
            let children = split_edges(&parent, &[0], &get_edge);
            for edges in children.iter().flatten() {
                assert_eq!(edges, &[0]);
            }
        }
    }
}

#[test]
fn test_plane_divider_and_corner_contacts() {
    check_contacts::<Plane>();
}

#[test]
fn test_sphere_divider_and_corner_contacts() {
    check_contacts::<Sphere>();
}

fn check_rectangle<S: Surface>(polygon: R2Rect, reverse: bool) {
    let mut uv: Vec<_> = (0..4).map(|k| polygon.get_vertex(k)).collect();
    if reverse {
        uv.reverse();
    }
    uv.push(uv[0]);
    let ring: Vec<_> = uv
        .iter()
        .map(|p| S::face_uv_to_point(0, p[0], p[1]))
        .collect();
    let get_edge = |edge: u32| (ring[edge as usize], ring[edge as usize + 1]);
    let edge_bounds: Vec<_> = ring
        .windows(2)
        .map(|edge| {
            let (u0, v0) = S::point_to_face_uv(0, &edge[0]);
            let (u1, v1) = S::point_to_face_uv(0, &edge[1]);
            R2Rect::from_point_pair([u0, v0], [u1, v1])
        })
        .collect();
    let root = CoveringCell {
        pcell: S2PaddedCell::<S>::new(S2CellId::from_face(0), S::CELL_PADDING),
        query_edges: vec![0, 1, 2, 3],
        contains_center: polygon.interior_contains_point([0.0, 0.0]),
        index_start: 0,
        index_end: 4,
        first_index_level: 5,
    };
    let mut pending = std::collections::VecDeque::from([root]);
    while let Some(mut parent) = pending.pop_front() {
        // Give each child an occupied range so only geometry can prune it.
        parent.index_start = 0;
        parent.index_end = 4;
        let ids: Vec<_> = (0..4).map(|pos| parent.pcell.id().child(pos)).collect();
        let start = |id, _, _| (ids.iter().position(|&child| child == id).unwrap() as u32, 5);
        let mut children = covering_split(&parent, &get_edge, &start);
        covering_contains_center(&parent.pcell.get_center(), &mut children, &get_edge);
        for id in ids {
            let cell = S2PaddedCell::<S>::new(id, 0.0);
            let bound = cell.bound();
            let child = children.iter().find(|c| c.pcell.id() == id);
            let Some(child) = child else {
                let interior_overlap = (0..2).all(|axis| {
                    bound[axis].hi() > polygon[axis].lo() && bound[axis].lo() < polygon[axis].hi()
                });
                assert!(!interior_overlap, "pruned interior at {id:?}, {polygon:?}");
                continue;
            };
            if child.query_edges.is_empty() {
                for edge_bound in &edge_bounds {
                    assert!(!edge_bound.intersects(&bound), "lost edge at {id:?}");
                }
            }
            let (u, v) = S::point_to_face_uv(0, &cell.get_center());
            // Boundary centers carry a crossing convention, not an interior assertion.
            let off_boundary = [u, v].iter().enumerate().all(|(axis, value)| {
                (value - polygon[axis].lo()).abs() > 1e-12
                    && (value - polygon[axis].hi()).abs() > 1e-12
            });
            if off_boundary {
                assert_eq!(
                    child.contains_center,
                    polygon.interior_contains_point([u, v]),
                    "center ({u}, {v}) at {id:?}, {polygon:?}, reverse {reverse}"
                );
            }
        }
        pending.extend(children.into_iter().filter(|c| c.pcell.level() < 5));
    }
}

fn check_rectangles<S: Surface>() {
    let divider = S::st_to_uv(0.625);
    for (lo, hi) in [
        ([divider, -0.2], [0.8, 0.8]),
        ([-0.3, -0.2], [divider, 0.8]),
        ([divider, divider], [0.8, 0.8]),
        ([divider, -0.2], [S::st_to_uv(0.75), 0.8]),
    ] {
        for swap in [false, true] {
            for x_sign in [-1.0, 1.0] {
                for y_sign in [-1.0, 1.0] {
                    let transform = |p: [f64; 2]| {
                        let p = if swap { [p[1], p[0]] } else { p };
                        [x_sign * p[0], y_sign * p[1]]
                    };
                    let bounds = R2Rect::from_point_pair(transform(lo), transform(hi));
                    for reverse in [false, true] {
                        check_rectangle::<S>(bounds, reverse);
                    }
                }
            }
        }
    }
}

#[test]
fn test_plane_divider_containment() {
    check_rectangles::<Plane>();
}

#[test]
fn test_sphere_divider_containment() {
    check_rectangles::<Sphere>();
}

#[test]
fn test_coarse_index_range_reaches_every_child() {
    use common::CountingWriter;

    use crate::spatial::cell_index_reader::CellIndexReader;
    use crate::spatial::shape_index::{ShapeCell, ShapeIndex};

    let id = S2CellId::from_face(0);
    let index = ShapeIndex {
        cells: vec![ShapeCell {
            cell_id: id,
            shapes: vec![],
        }],
    };
    let mut bytes = Vec::new();
    index.write(&mut CountingWriter::wrap(&mut bytes));
    let reader = CellIndexReader::open(&bytes);
    let mut entry = CoveringCell {
        pcell: S2PaddedCell::<Plane>::new(id, 0.0),
        query_edges: vec![],
        contains_center: true,
        index_start: 0,
        index_end: 1,
        first_index_level: 0,
    };
    let get_edge = |_| panic!("interior cell has no edges");
    let start = |id, lo, hi| (reader.start_for_cell(id, lo, hi), 0);
    // Repeat below the stored level to cover an ancestor as well as an equal cell.
    for _ in 0..4 {
        let children = covering_split(&entry, &get_edge, &start);
        assert_eq!(children.len(), 4);
        for child in &children {
            let expected = reader.range_for_cell(child.pcell.id(), 0, 1);
            assert_eq!(expected, (0, 1));
            assert_eq!((child.index_start, child.index_end), expected);
            assert_eq!(child.first_index_level, 0);
        }
        entry = children.into_iter().next().unwrap();
    }
}

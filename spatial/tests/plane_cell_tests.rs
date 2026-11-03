use common::CountingWriter;
use num_bigint::BigInt;
use num_rational::BigRational;
use num_traits::ToPrimitive;
use proptest::prelude::*;

use crate::spatial::cell_index_reader::CellIndexReader;
use crate::spatial::clipped_shape::ClippedShape;
use crate::spatial::plane::Plane;
use crate::spatial::s2cell_id::S2CellId;
use crate::spatial::s2coords::LIMIT_IJ;
use crate::spatial::shape_index::{ShapeCell, ShapeIndex};
use crate::spatial::surface::Surface;

fn grid_index(u: f64, scale: i32) -> i32 {
    let scaled = (BigRational::from_float(u).unwrap() / BigInt::from(scale) + BigInt::from(1))
        * BigInt::from(LIMIT_IJ / 2);
    scaled
        .to_integer()
        .to_i64()
        .unwrap()
        .clamp(0, i64::from(LIMIT_IJ - 1)) as i32
}

fn check_point(point: [f64; 2]) {
    let id = Plane::cell_id_from_point(&point);
    assert!(id.is_leaf());
    let (face, i, j, _) = id.to_face_ij_orientation();
    assert_eq!(
        (face, i, j),
        (0, grid_index(point[0], 180), grid_index(point[1], 90)),
        "{point:?}"
    );
}

#[test]
fn test_cell_id_from_point_boundaries() {
    let mut indices = vec![0, LIMIT_IJ, 0x1555_5555, 0x2aaa_aaaa];
    for level in 1..=S2CellId::MAX_LEVEL {
        let n = 1 << level;
        let stride = LIMIT_IJ / n;
        for k in [1, n / 2, n - 1] {
            indices.push(k * stride);
        }
    }
    indices.sort_unstable();
    indices.dedup();
    for i in indices {
        let boundary = (i as f64) / ((LIMIT_IJ / 2) as f64) - 1.0;
        for (axis, scale) in [(0, 180.0), (1, 90.0)] {
            let divider = boundary * scale;
            for coordinate in [divider.next_down(), divider, divider.next_up()] {
                let mut point = [-0.3, -0.3];
                point[axis] = coordinate;
                check_point(point);
            }
        }
    }
    for exponent in [1, 29, 30, 52, 53, 54, 55, 100, 600, 1022, 1074] {
        let u = 2.0f64.powi(-exponent);
        check_point([-u, u]);
        check_point([u, -u]);
    }
    for point in [
        [0.0, -0.0],
        [-0.0, 0.0],
        [-180.0, 90.0],
        [180.0, -90.0],
        [-200.0, 100.0],
    ] {
        check_point(point);
    }
}

fn unequal_depth_index() -> ShapeIndex {
    let target = S2CellId::from_face_ij(0, LIMIT_IJ / 2 - 1, LIMIT_IJ / 2 - 1);
    let mut pending = vec![S2CellId::from_face(0)];
    let mut cells = Vec::new();
    // Refine only the lower-left side of zero. The other quadrants stay coarse.
    while let Some(id) = pending.pop() {
        if id.contains(target) && id.level() < 8 {
            pending.extend(id.children());
        } else {
            cells.push(ShapeCell::new(id));
        }
    }
    cells.sort_unstable_by_key(|cell| cell.cell_id);
    for (i, cell) in cells.iter_mut().enumerate() {
        cell.add_shape(ClippedShape::new((0, i as u32), true));
    }
    ShapeIndex { cells }
}

fn lookup_points(index: &ShapeIndex) -> Vec<[f64; 2]> {
    let mut points = vec![[-2.0f64.powi(-54), -2.0f64.powi(-54)], [0.0, -0.0]];
    for cell in &index.cells {
        points.push(Plane::cell_center(cell.cell_id));
        let bound = crate::spatial::plane_support_tests::nominal_bound(cell.cell_id);
        for k in 0..4 {
            let [u, v] = bound.get_vertex(k);
            for u in [u.next_down(), u, u.next_up()] {
                for v in [v.next_down(), v, v.next_up()] {
                    if (-180.0..=180.0).contains(&u) && (-90.0..=90.0).contains(&v) {
                        points.push([u, v]);
                    }
                }
            }
        }
    }
    points
}

fn containing_cell(index: &ShapeIndex, point: [f64; 2]) -> &ShapeCell {
    index
        .cells
        .iter()
        .find(|cell| {
            let bound = crate::spatial::plane_support_tests::nominal_bound(cell.cell_id);
            (0..2).all(|axis| {
                point[axis] >= bound[axis].lo()
                    && (point[axis] < bound[axis].hi()
                        || (point[axis] == [180.0, 90.0][axis]
                            && bound[axis].hi() == [180.0, 90.0][axis]))
            })
        })
        .unwrap()
}

#[test]
fn test_cell_id_from_point_in_memory_lookup() {
    let index = unequal_depth_index();
    for point in lookup_points(&index) {
        let expected = containing_cell(&index, point);
        assert_eq!(
            index.find_cell(Plane::cell_id_from_point(&point)),
            Some(expected),
            "{point:?}"
        );
    }
}

#[test]
fn test_cell_id_from_point_serialized_lookup() {
    let index = unequal_depth_index();
    let mut bytes = Vec::new();
    index.write(&mut CountingWriter::wrap(&mut bytes));
    let reader = CellIndexReader::open(&bytes).unwrap();
    for point in lookup_points(&index) {
        let expected = containing_cell(&index, point);
        assert_eq!(
            reader.find(Plane::cell_id_from_point(&point)).as_ref(),
            Some(expected),
            "{point:?}"
        );
    }
}

proptest! {
    #[test]
    fn test_cell_id_from_point_rational_grid(bits in any::<[u64; 2]>()) {
        let point = bits.map(f64::from_bits);
        prop_assume!(
            point[0].is_finite()
                && point[0].abs() <= 180.0
                && point[1].is_finite()
                && point[1].abs() <= 90.0
        );
        check_point(point);
    }
}

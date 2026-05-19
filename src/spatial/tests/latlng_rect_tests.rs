use std::f64::consts::{FRAC_PI_2, FRAC_PI_4, PI};

use super::*;

#[test]
fn test_region_trait_rect_contains_point() {
    // Rectangle around prime meridian and equator
    let rect = S2LatLngRect::new(
        R1Interval::new(-FRAC_PI_4, FRAC_PI_4),
        S1Interval::new(-FRAC_PI_4, FRAC_PI_4),
    );

    // Point on equator at prime meridian
    let p = [1.0, 0.0, 0.0];
    assert!(rect.contains_point(&p));

    // Point at north pole
    let north_pole = [0.0, 0.0, 1.0];
    assert!(!rect.contains_point(&north_pole));
}

#[test]
fn test_region_trait_rect_contains_cell() {
    // Full rectangle
    let full = S2LatLngRect::full();

    // Face 0 cell
    let cell = S2Cell::from_face(0);
    assert!(full.contains_cell(&cell));

    // Empty rectangle contains nothing
    let empty = S2LatLngRect::empty();
    assert!(!empty.contains_cell(&cell));
}

#[test]
fn test_region_trait_rect_may_intersect() {
    // Rectangle in northern hemisphere
    let rect = S2LatLngRect::new(R1Interval::new(0.0, FRAC_PI_2), S1Interval::new(-PI, PI));

    // North pole face (face 2)
    let north_cell = S2Cell::from_face(2);
    assert!(rect.may_intersect(&north_cell));

    // South pole face (face 5)
    let south_cell = S2Cell::from_face(5);
    assert!(!rect.may_intersect(&south_cell));
}

#[test]
fn test_region_trait_rect_get_cap_bound() {
    // Small rectangle at equator
    let rect = S2LatLngRect::new(R1Interval::new(-0.1, 0.1), S1Interval::new(-0.1, 0.1));

    let cap = rect.get_cap_bound();
    assert!(!cap.is_empty());
    assert!(!cap.is_full());

    // Cap should contain all rectangle vertices
    for k in 0..4 {
        let vertex = rect.get_vertex(k);
        let cos_lat = vertex[1].cos();
        let p = [
            cos_lat * vertex[0].cos(),
            cos_lat * vertex[0].sin(),
            vertex[1].sin(),
        ];
        assert!(cap.contains_point(&p));
    }
}

#[test]
fn test_region_trait_rect_get_rect_bound() {
    let rect = S2LatLngRect::new(R1Interval::new(-0.5, 0.5), S1Interval::new(-1.0, 1.0));

    // Rect bound of a rect is itself
    let bound = rect.get_rect_bound();
    assert_eq!(rect, bound);
}

#[test]
fn test_region_trait_rect_get_cell_union_bound() {
    // Small rectangle
    let rect = S2LatLngRect::new(R1Interval::new(0.0, 0.1), S1Interval::new(0.0, 0.1));

    let mut cell_ids = Vec::new();
    rect.get_cell_union_bound(&mut cell_ids);

    // Should return a small number of cells
    assert!(!cell_ids.is_empty());
    assert!(cell_ids.len() <= 6); // At most 6 face cells
}

#[test]
fn test_region_trait_empty_rect() {
    let empty = S2LatLngRect::empty();

    // Empty rect has empty cap bound
    let cap = empty.get_cap_bound();
    assert!(cap.is_empty());

    // Empty rect does not contain any point
    assert!(!empty.contains_point(&[1.0, 0.0, 0.0]));

    // Empty rect does not contain any cell
    let cell = S2Cell::from_face(0);
    assert!(!empty.contains_cell(&cell));

    // Empty rect does not intersect any cell
    assert!(!empty.may_intersect(&cell));
}

#[test]
fn test_region_trait_full_rect() {
    let full = S2LatLngRect::full();

    // Full rect has full cap bound
    let cap = full.get_cap_bound();
    assert!(cap.is_full());

    // Full rect contains every point
    assert!(full.contains_point(&[1.0, 0.0, 0.0]));
    assert!(full.contains_point(&[0.0, 0.0, 1.0]));

    // Full rect contains every cell
    for face in 0..6 {
        let cell = S2Cell::from_face(face);
        assert!(full.contains_cell(&cell));
    }
}

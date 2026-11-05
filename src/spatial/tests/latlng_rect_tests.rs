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

/// A rectangle from "lat:lng" of its low and high corners, in degrees, as S2's tests write it.
fn rect(lo: (f64, f64), hi: (f64, f64)) -> S2LatLngRect {
    S2LatLngRect::new(
        R1Interval::new(lo.0.to_radians(), hi.0.to_radians()),
        S1Interval::new(lo.1.to_radians(), hi.1.to_radians()),
    )
}

fn point((lat, lng): (f64, f64)) -> [f64; 3] {
    let (lat, lng) = (lat.to_radians(), lng.to_radians());
    [lat.cos() * lng.cos(), lat.cos() * lng.sin(), lat.sin()]
}

fn crosses(rect: &S2LatLngRect, a: (f64, f64), b: (f64, f64)) -> bool {
    rect.boundary_intersects(&point(a), &point(b))
}

#[test]
fn test_boundary_intersects_empty_and_full_rectangles() {
    let (a, b) = (point((10.0, 20.0)), point((30.0, 40.0)));
    assert!(!S2LatLngRect::empty().boundary_intersects(&a, &b));
    assert!(!S2LatLngRect::full().boundary_intersects(&a, &b));
}

#[test]
fn test_boundary_intersects_spherical_lune() {
    // This rectangle only has two non-degenerate sides.
    let rect = rect((-90.0, 100.0), (90.0, 120.0));
    assert!(!crosses(&rect, (60.0, 60.0), (90.0, 60.0)));
    assert!(!crosses(&rect, (-60.0, 110.0), (60.0, 110.0)));
    assert!(crosses(&rect, (-60.0, 95.0), (60.0, 110.0)));
    assert!(crosses(&rect, (60.0, 115.0), (80.0, 125.0)));
}

#[test]
fn test_boundary_intersects_hemispheres() {
    // These rectangles have only one non-degenerate side.
    let north = S2LatLngRect::new(R1Interval::new(0.0, FRAC_PI_2), S1Interval::full());
    assert!(!crosses(&north, (60.0, -180.0), (90.0, -180.0)));
    assert!(!crosses(&north, (60.0, -170.0), (60.0, 170.0)));
    assert!(crosses(&north, (-10.0, -180.0), (10.0, -180.0)));
    let south = S2LatLngRect::new(R1Interval::new(-FRAC_PI_2, 0.0), S1Interval::full());
    assert!(!crosses(&south, (-90.0, -180.0), (-60.0, -180.0)));
    assert!(!crosses(&south, (-60.0, -170.0), (-60.0, 170.0)));
    assert!(crosses(&south, (-10.0, -180.0), (10.0, -180.0)));
}

#[test]
fn test_boundary_intersects_rect_crossing_anti_meridian() {
    let rect = rect((20.0, 170.0), (40.0, -170.0));
    assert!(rect.contains_point(&point((30.0, 180.0))));

    // Check that crossings of all four sides are detected.
    assert!(crosses(&rect, (25.0, 160.0), (25.0, 180.0)));
    assert!(crosses(&rect, (25.0, -160.0), (25.0, -180.0)));
    assert!(crosses(&rect, (15.0, 175.0), (30.0, 175.0)));
    assert!(crosses(&rect, (45.0, 175.0), (30.0, 175.0)));

    // Check that the edges on the opposite side of the sphere but at the same latitude do not
    // intersect the rectangle boundary.
    assert!(!crosses(&rect, (25.0, -20.0), (25.0, 0.0)));
    assert!(!crosses(&rect, (25.0, 20.0), (25.0, 0.0)));
    assert!(!crosses(&rect, (15.0, -5.0), (30.0, -5.0)));
    assert!(!crosses(&rect, (45.0, -5.0), (30.0, -5.0)));
}

#[test]
fn test_an_edge_meets_a_parallel_twice_where_it_rises_above_it() {
    let lng = S1Interval::new(0.0, 10f64.to_radians());
    let (a, b) = (point((9.99, 2.0)), point((9.99, 8.0)));
    assert!(S2LatLngRect::intersects_lat_edge(
        &a,
        &b,
        10f64.to_radians(),
        &lng
    ));
    assert!(!S2LatLngRect::intersects_lat_edge(
        &a,
        &b,
        10.1f64.to_radians(),
        &lng
    ));
    // Where the edge meets the parallel, east of the longitudes asked about.
    let west = S1Interval::new(0.0, 1f64.to_radians());
    assert!(!S2LatLngRect::intersects_lat_edge(
        &a,
        &b,
        10f64.to_radians(),
        &west
    ));
}

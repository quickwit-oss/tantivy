use std::f64::consts::{FRAC_PI_2, PI};

use super::*;
use crate::spatial::math::normalize;

const EPS: f64 = 1e-14;

fn get_latlng_point(lat_degrees: f64, lng_degrees: f64) -> [f64; 3] {
    let lat = lat_degrees.to_radians();
    let lng = lng_degrees.to_radians();
    let cos_lat = lat.cos();
    [cos_lat * lng.cos(), cos_lat * lng.sin(), lat.sin()]
}

#[test]
fn test_basic_properties() {
    let empty = S2Cap::empty();
    let full = S2Cap::full();

    assert!(empty.is_empty());
    assert!(!empty.is_full());
    assert!(empty.complement().is_full());

    assert!(full.is_full());
    assert!(!full.is_empty());
    assert!(full.complement().is_empty());

    assert!((full.height() - 2.0).abs() < EPS);
    assert!((full.get_radius_radians() - PI).abs() < 1e-10);
}

#[test]
fn test_singleton_cap() {
    let xaxis = S2Cap::from_point([1.0, 0.0, 0.0]);
    assert!(xaxis.contains_point(&[1.0, 0.0, 0.0]));
    assert!(!xaxis.contains_point(&[1.0, 1e-20, 0.0]));
    assert!(xaxis.get_radius_radians().abs() < EPS);
    assert!(xaxis.height().abs() < EPS);

    // Complement of singleton is full
    let xcomp = xaxis.complement();
    assert!(xcomp.is_full());
    assert!(xcomp.contains_point(&xaxis.center()));
}

#[test]
fn test_hemisphere() {
    let hemi = S2Cap::from_center_height(normalize(&[1.0, 0.0, 1.0]), 1.0);
    assert!((hemi.complement().height() - 1.0).abs() < EPS);
    assert!(hemi.contains_point(&[1.0, 0.0, 0.0]));
    assert!(!hemi.complement().contains_point(&[1.0, 0.0, 0.0]));
}

#[test]
fn test_cap_cap_containment() {
    let empty = S2Cap::empty();
    let full = S2Cap::full();
    let xaxis = S2Cap::from_point([1.0, 0.0, 0.0]);

    assert!(empty.contains_cap(&empty));
    assert!(full.contains_cap(&empty));
    assert!(full.contains_cap(&full));
    assert!(!xaxis.contains_cap(&full));
    assert!(xaxis.contains_cap(&empty));
}

#[test]
fn test_cap_cap_intersection() {
    let empty = S2Cap::empty();
    let full = S2Cap::full();
    let xaxis = S2Cap::from_point([1.0, 0.0, 0.0]);

    assert!(!empty.intersects_cap(&empty));
    assert!(full.intersects_cap(&full));
    assert!(!full.intersects_cap(&empty));
    assert!(full.intersects_cap(&xaxis));
}

#[test]
fn test_get_rect_bound_empty_full() {
    assert!(S2Cap::empty().get_rect_bound().is_empty());
    assert!(S2Cap::full().get_rect_bound().is_full());
}

#[test]
fn test_get_rect_bound_south_pole() {
    let cap = S2Cap::from_center_angle(get_latlng_point(-45.0, 57.0), 50.0_f64.to_radians());
    let rect = cap.get_rect_bound();

    // Should include south pole
    assert!((rect.lat_lo() - (-FRAC_PI_2)).abs() < 1e-10);
    assert!(rect.lng().is_full());
}

#[test]
fn test_get_rect_bound_equator() {
    let cap = S2Cap::from_center_angle(get_latlng_point(0.0, 50.0), 20.0_f64.to_radians());
    let rect = cap.get_rect_bound();

    assert!((rect.lat_lo().to_degrees() - (-20.0)).abs() < 1e-10);
    assert!((rect.lat_hi().to_degrees() - 20.0).abs() < 1e-10);
    assert!((rect.lng_lo().to_degrees() - 30.0).abs() < 1e-10);
    assert!((rect.lng_hi().to_degrees() - 70.0).abs() < 1e-10);
}

#[test]
fn test_region_contains_cell() {
    let full = S2Cap::full();
    let empty = S2Cap::empty();

    let root_cell = S2Cell::from_face(0);
    assert!(full.contains_cell(&root_cell));
    assert!(!empty.contains_cell(&root_cell));
}

#[test]
fn test_region_may_intersect() {
    let full = S2Cap::full();
    let empty = S2Cap::empty();

    let root_cell = S2Cell::from_face(0);
    assert!(full.may_intersect(&root_cell));
    assert!(!empty.may_intersect(&root_cell));
}

#[test]
fn test_region_singleton_may_intersect() {
    // A singleton cap at face center should intersect that face
    for face in 0..6 {
        let center = S2Cell::from_face(face).get_center();
        let singleton = S2Cap::from_point(center);

        let root_cell = S2Cell::from_face(face);
        assert!(singleton.may_intersect(&root_cell));

        // Should not intersect other faces
        for other_face in 0..6 {
            if other_face != face {
                let other_cell = S2Cell::from_face(other_face);
                assert!(!singleton.may_intersect(&other_cell));
            }
        }
    }
}

#[test]
fn test_complement() {
    let small = S2Cap::from_center_angle([1.0, 0.0, 0.0], 0.1);
    let comp = small.complement();

    // Complement should be large (almost full)
    assert!((comp.get_radius_radians() - (PI - 0.1)).abs() < 1e-10);

    // Complement center should be opposite
    assert!((comp.center()[0] - (-1.0)).abs() < EPS);

    // Double complement of empty is empty
    assert!(S2Cap::empty().complement().complement().is_empty());

    // Double complement of full is full
    assert!(S2Cap::full().complement().complement().is_full());
}

#[test]
fn test_add_point() {
    let mut cap = S2Cap::empty();
    cap.add_point(&[1.0, 0.0, 0.0]);
    assert!(!cap.is_empty());
    assert!(cap.contains_point(&[1.0, 0.0, 0.0]));

    cap.add_point(&[0.0, 1.0, 0.0]);
    assert!(cap.contains_point(&[1.0, 0.0, 0.0]));
    assert!(cap.contains_point(&[0.0, 1.0, 0.0]));
}

#[test]
fn test_expanded() {
    let cap = S2Cap::from_point([1.0, 0.0, 0.0]);
    let expanded = cap.expanded(0.1);

    assert!((expanded.get_radius_radians() - 0.1).abs() < 1e-10);
    assert!(expanded.contains_point(&[1.0, 0.0, 0.0]));

    // Expanding empty cap stays empty
    assert!(S2Cap::empty().expanded(0.1).is_empty());
}

#[test]
fn test_cell_intersection_hemisphere() {
    // A hemisphere should contain 3 face cells and may-intersect more
    let hemi = S2Cap::from_center_angle([0.0, 0.0, 1.0], FRAC_PI_2);

    let north_cell = S2Cell::from_face(2); // North pole face
    assert!(hemi.may_intersect(&north_cell));

    let south_cell = S2Cell::from_face(5); // South pole face
    assert!(!hemi.may_intersect(&south_cell));
}

#[test]
fn test_cell_covering() {
    // Small cap should return 3-4 cells
    let small = S2Cap::from_center_angle([1.0, 0.0, 0.0], 0.01);
    let mut cell_ids = Vec::new();
    small.get_cell_union_bound(&mut cell_ids);

    assert!(cell_ids.len() >= 3);
    assert!(cell_ids.len() <= 4);

    // Large cap should return 6 face cells
    let large = S2Cap::from_center_angle([1.0, 0.0, 0.0], 2.0);
    cell_ids.clear();
    large.get_cell_union_bound(&mut cell_ids);
    assert_eq!(cell_ids.len(), 6);
}

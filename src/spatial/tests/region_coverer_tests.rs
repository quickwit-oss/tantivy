use super::super::s2cap::S2Cap;
use super::*;

fn get_latlng_point(lat_degrees: f64, lng_degrees: f64) -> [f64; 3] {
    let lat = lat_degrees.to_radians();
    let lng = lng_degrees.to_radians();
    let cos_lat = lat.cos();
    [cos_lat * lng.cos(), cos_lat * lng.sin(), lat.sin()]
}

#[test]
fn test_covering_respects_max_level() {
    // THIS TEST WAS FAILING - now should pass
    let coverer = RegionCoverer::new(CovererOptions::new().max_level(5).max_cells(100));
    let center = get_latlng_point(0.0, 0.0);
    let cap = S2Cap::from_center_angle(center, 0.001); // Tiny cap

    let covering = coverer.get_covering(&cap);

    // All cells should be at level <= 5
    for id in covering.cell_ids() {
        assert!(
            id.level() <= 5,
            "Cell at level {} exceeds max_level 5",
            id.level()
        );
    }
}

#[test]
fn test_get_initial_candidates_respects_max_level() {
    // Direct test of get_initial_candidates behavior
    let coverer = RegionCoverer::new(CovererOptions::new().max_level(3).max_cells(100));
    let center = get_latlng_point(0.0, 0.0);
    let cap = S2Cap::from_center_angle(center, 0.0001); // Very tiny cap

    let mut cells = Vec::new();
    coverer.get_initial_candidates(&cap, &mut cells);

    // All initial candidates should be at level <= 3
    for id in &cells {
        assert!(
            id.level() <= 3,
            "Initial candidate at level {} exceeds max_level 3",
            id.level()
        );
    }
}

#[test]
fn test_canonicalize_respects_max_level() {
    let coverer = RegionCoverer::new(CovererOptions::new().max_level(5).max_cells(10));

    // Create a covering with cells too deep
    let mut covering = vec![
        S2CellId::from_face(0)
            .child(0)
            .child(0)
            .child(0)
            .child(0)
            .child(0)
            .child(0), // Level 6
        S2CellId::from_face(1)
            .child(0)
            .child(0)
            .child(0)
            .child(0)
            .child(0)
            .child(0)
            .child(0), // Level 7
    ];

    coverer.canonicalize_covering(&mut covering);

    // All cells should now be at level <= 5
    for id in &covering {
        assert!(id.level() <= 5);
    }
    assert!(coverer.is_canonical(&covering));
}

#[test]
fn test_fast_covering_respects_max_level() {
    let coverer = RegionCoverer::new(CovererOptions::new().max_level(4).max_cells(10));
    let center = get_latlng_point(0.0, 0.0);
    let cap = S2Cap::from_center_angle(center, 0.0001); // Very tiny cap

    let mut covering = Vec::new();
    coverer.get_fast_covering(&cap, &mut covering);

    // Fast covering should also respect max_level
    for id in &covering {
        assert!(
            id.level() <= 4,
            "Fast covering cell at level {} exceeds max_level 4",
            id.level()
        );
    }
}

#[test]
fn test_is_canonical_rejects_too_deep_cells() {
    let coverer = RegionCoverer::new(CovererOptions::new().max_level(5));

    // A covering with a cell at level 6 should not be canonical
    let bad_covering = vec![S2CellId::from_face(0)
        .child(0)
        .child(0)
        .child(0)
        .child(0)
        .child(0)
        .child(0)];

    assert!(!coverer.is_canonical(&bad_covering));
}

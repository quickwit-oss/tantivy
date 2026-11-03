use super::*;
#[test]
fn test_s2metrics_get_level_for_min_width() {
    // Very small value -> level 30
    assert_eq!(S2Metrics::get_level_for_min_width(1e-10), 30);

    // Very large value -> level 0 or -1
    assert!(S2Metrics::get_level_for_min_width(2.0) <= 0);

    // Moderate value -> somewhere in between
    let level = S2Metrics::get_level_for_min_width(0.01);
    assert!(level > 0);
    assert!(level < 30);
}

use super::*;

const EPSILON: f64 = 1e-14;

fn approx_eq(a: f64, b: f64) -> bool {
    (a - b).abs() < EPSILON
}

#[test]
fn test_empty_and_full() {
    let empty = S1Interval::empty();
    let full = S1Interval::full();

    assert!(empty.is_empty());
    assert!(!empty.is_full());
    assert!(empty.is_inverted());
    assert!(empty.length() < 0.0);

    assert!(full.is_full());
    assert!(!full.is_empty());
    assert!(!full.is_inverted());
    assert!(approx_eq(full.length(), 2.0 * PI));
}

#[test]
fn test_from_point() {
    let i = S1Interval::from_point(1.0);
    assert!(!i.is_empty());
    assert_eq!(i.lo(), 1.0);
    assert_eq!(i.hi(), 1.0);
    assert_eq!(i.length(), 0.0);

    // -PI should be normalized to PI
    let i2 = S1Interval::from_point(-PI);
    assert_eq!(i2.lo(), PI);
    assert_eq!(i2.hi(), PI);
}

#[test]
fn test_from_point_pair() {
    // Normal interval
    let i1 = S1Interval::from_point_pair(0.0, 1.0);
    assert_eq!(i1.lo(), 0.0);
    assert_eq!(i1.hi(), 1.0);

    // Inverted interval (crossing PI)
    let i2 = S1Interval::from_point_pair(3.0, -3.0);
    assert!(i2.is_inverted());
    assert_eq!(i2.lo(), 3.0);
    assert_eq!(i2.hi(), -3.0);
}

#[test]
fn test_inverted_interval() {
    // An interval from 3 to -3 radians should wrap around
    let i = S1Interval::new(3.0, -3.0);
    assert!(i.is_inverted());
    assert!(!i.is_empty());
    assert!(!i.is_full());

    // Length should be ~0.28 (6.28 - 6.0)
    assert!(approx_eq(i.length(), 2.0 * PI - 6.0));

    // Should contain PI (the wrap point)
    assert!(i.contains_point(PI));
    assert!(i.contains_point(-PI));
    assert!(i.contains_point(3.1));
    assert!(i.contains_point(-3.1));

    // Should not contain points in the gap
    assert!(!i.contains_point(0.0));
    assert!(!i.contains_point(2.0));
}

#[test]
fn test_center() {
    // Normal interval
    let i1 = S1Interval::new(0.0, 2.0);
    assert!(approx_eq(i1.center(), 1.0));

    // Inverted interval spanning PI
    let i2 = S1Interval::new(3.0, -3.0);
    // Center should be at PI (or equivalent)
    let c = i2.center();
    assert!(approx_eq(c.abs(), PI));
}

#[test]
fn test_contains_point() {
    let normal = S1Interval::new(0.0, 2.0);
    assert!(normal.contains_point(0.0));
    assert!(normal.contains_point(1.0));
    assert!(normal.contains_point(2.0));
    assert!(!normal.contains_point(-1.0));
    assert!(!normal.contains_point(3.0));

    // Full contains everything
    assert!(S1Interval::full().contains_point(0.0));
    assert!(S1Interval::full().contains_point(PI));
    assert!(S1Interval::full().contains_point(-PI));

    // Empty contains nothing
    assert!(!S1Interval::empty().contains_point(0.0));
}

#[test]
fn test_add_point() {
    let mut i = S1Interval::empty();
    i.add_point(0.0);
    assert_eq!(i.lo(), 0.0);
    assert_eq!(i.hi(), 0.0);

    i.add_point(1.0);
    assert_eq!(i.lo(), 0.0);
    assert_eq!(i.hi(), 1.0);

    // Adding a point that would create a shorter inverted interval
    i.add_point(-0.5);
    assert_eq!(i.lo(), -0.5);
    assert_eq!(i.hi(), 1.0);
}

#[test]
fn test_add_point_wraparound() {
    // Start near PI, add point near -PI
    let mut i = S1Interval::from_point(3.0);
    i.add_point(-3.0);
    // Should create an inverted interval through PI
    assert!(i.is_inverted());
    assert!(i.contains_point(PI));
}

#[test]
fn test_union() {
    let a = S1Interval::new(0.0, 1.0);
    let b = S1Interval::new(0.5, 2.0);
    let u = a.union(&b);
    assert_eq!(u.lo(), 0.0);
    assert_eq!(u.hi(), 2.0);

    // Union with empty
    assert_eq!(a.union(&S1Interval::empty()), a);
    assert_eq!(S1Interval::empty().union(&a), a);

    // Union that becomes full
    let c = S1Interval::new(-2.0, 2.0);
    let d = S1Interval::new(2.0, -1.0); // inverted
    let u2 = c.union(&d);
    assert!(u2.is_full());
}

#[test]
fn test_intersection() {
    let a = S1Interval::new(0.0, 2.0);
    let b = S1Interval::new(1.0, 3.0);
    let i = a.intersection(&b);
    assert_eq!(i.lo(), 1.0);
    assert_eq!(i.hi(), 2.0);

    // Disjoint intervals
    let c = S1Interval::new(0.0, 1.0);
    let d = S1Interval::new(2.0, 3.0);
    assert!(c.intersection(&d).is_empty());
}

#[test]
fn test_complement() {
    let i = S1Interval::new(0.0, 1.0);
    let c = i.complement();
    assert_eq!(c.lo(), 1.0);
    assert_eq!(c.hi(), 0.0);
    assert!(c.is_inverted());
    assert!(c.contains_point(PI));
    assert!(!c.contains_point(0.5));

    // Singleton complement is full
    let s = S1Interval::from_point(0.0);
    assert!(s.complement().is_full());
}

#[test]
fn test_expanded() {
    let i = S1Interval::new(0.0, 1.0);

    // Expand
    let e = i.expanded(0.5);
    assert!(approx_eq(e.lo(), -0.5));
    assert!(approx_eq(e.hi(), 1.5));

    // Contract
    let c = i.expanded(-0.25);
    assert!(approx_eq(c.lo(), 0.25));
    assert!(approx_eq(c.hi(), 0.75));

    // Contract to empty
    let e2 = i.expanded(-1.0);
    assert!(e2.is_empty());

    // Expand to full
    let i2 = S1Interval::new(-2.0, 2.0);
    let e3 = i2.expanded(2.0);
    assert!(e3.is_full());
}

#[test]
fn test_positive_distance() {
    assert!(approx_eq(positive_distance(0.0, 1.0), 1.0));
    assert!(approx_eq(positive_distance(1.0, 0.0), 2.0 * PI - 1.0));
    assert!(approx_eq(positive_distance(-1.0, 1.0), 2.0));
    assert!(approx_eq(positive_distance(1.0, -1.0), 2.0 * PI - 2.0));
}

#[test]
fn test_project() {
    let i = S1Interval::new(0.0, 1.0);
    assert_eq!(i.project(0.5), 0.5);
    assert_eq!(i.project(0.0), 0.0);
    assert_eq!(i.project(1.0), 1.0);
    // Point outside, closer to lo
    assert_eq!(i.project(-0.1), 0.0);
    // Point outside, closer to hi
    assert_eq!(i.project(1.1), 1.0);
}

#[test]
fn test_approx_equals() {
    let a = S1Interval::new(0.0, 1.0);
    let b = S1Interval::new(0.0 + 1e-16, 1.0 - 1e-16);
    assert!(a.approx_equals(&b, 1e-15));
    assert!(!a.approx_equals(&b, 1e-17));

    // Empty intervals
    let e1 = S1Interval::empty();
    let e2 = S1Interval::new(0.0, 1e-16);
    assert!(e1.approx_equals(&e2, 1e-15));
}

#[test]
fn test_normalization_of_negative_pi() {
    // -PI should become PI except for Full and Empty
    let i = S1Interval::new(-PI, 1.0);
    assert_eq!(i.lo(), PI);
    assert_eq!(i.hi(), 1.0);

    let i2 = S1Interval::new(1.0, -PI);
    assert_eq!(i2.lo(), 1.0);
    assert_eq!(i2.hi(), PI);

    // Full is the exception
    let f = S1Interval::full();
    assert_eq!(f.lo(), -PI);
    assert_eq!(f.hi(), PI);
}

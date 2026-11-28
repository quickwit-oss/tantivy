use num_rational::BigRational;
use num_traits::{One, Signed, ToPrimitive, Zero};
use proptest::prelude::*;

use crate::spatial::plane::math::{orient2d_sign, segment_intersects_rect};
use crate::spatial::r2rect::R2Rect;

fn rational(value: f64) -> BigRational {
    BigRational::from_float(value).unwrap()
}

fn interval_intersects(a: [f64; 2], b: [f64; 2], rect: &R2Rect) -> bool {
    if rect.is_empty() {
        return false;
    }
    let a = a.map(rational);
    let b = b.map(rational);
    let mut enter = BigRational::zero();
    let mut leave = BigRational::one();
    for axis in 0..2 {
        let lo = rational(rect[axis].lo());
        let hi = rational(rect[axis].hi());
        let delta = &b[axis] - &a[axis];
        if delta.is_zero() {
            if a[axis] < lo || a[axis] > hi {
                return false;
            }
        } else {
            let mut first = (lo - &a[axis]) / &delta;
            let mut last = (hi - &a[axis]) / &delta;
            if first > last {
                std::mem::swap(&mut first, &mut last);
            }
            enter = enter.max(first);
            leave = leave.min(last);
            if enter > leave {
                return false;
            }
        }
    }
    true
}

fn rational_orientation(a: [f64; 2], b: [f64; 2], c: [f64; 2]) -> i32 {
    let [a, b, c] = [a, b, c].map(|p| p.map(rational));
    let det = (&a[0] - &c[0]) * (&b[1] - &c[1]) - (&a[1] - &c[1]) * (&b[0] - &c[0]);
    det.is_positive() as i32 - det.is_negative() as i32
}

fn check_orientation(a: [f64; 2], b: [f64; 2], c: [f64; 2]) {
    let expected = rational_orientation(a, b, c);
    for (a, b, c) in [(a, b, c), (b, c, a), (c, a, b)] {
        assert_eq!(orient2d_sign(&a, &b, &c), expected, "{a:?}, {b:?}, {c:?}");
        assert_eq!(orient2d_sign(&b, &a, &c), -expected, "{b:?}, {a:?}, {c:?}");
    }
}

fn check_contact(a: [f64; 2], b: [f64; 2], rect: R2Rect, expected: bool) {
    for swap in [false, true] {
        for sx in [-1.0, 1.0] {
            for sy in [-1.0, 1.0] {
                let transform = |mut p: [f64; 2]| {
                    if swap {
                        p.swap(0, 1);
                    }
                    [sx * p[0], sy * p[1]]
                };
                let [a, b] = [a, b].map(transform);
                let rect = if rect.is_empty() {
                    rect
                } else {
                    R2Rect::from_point_pair(transform(rect.lo()), transform(rect.hi()))
                };
                assert_eq!(interval_intersects(a, b, &rect), expected);
                for (a, b) in [(a, b), (b, a)] {
                    assert_eq!(
                        segment_intersects_rect(&a, &b, &rect),
                        expected,
                        "{a:?}, {b:?}, {rect:?}"
                    );
                }
            }
        }
    }
}

#[test]
fn test_segment_intersects_rect_contacts() {
    let rect = R2Rect::from_points([0.0, 0.0], [1.0, 1.0]);
    for (a, b, expected) in [
        ([0.5, 0.5], [0.5, 0.5], true),
        ([0.0, 0.0], [-0.0, 0.0], true),
        ([2.0, 0.0], [2.0, 0.0], false),
        ([-1.0, -1.0], [0.0, 0.0], true),
        ([-1.0, 1.0], [1.0, -1.0], true),
        ([-1.0, 0.5], [2.0, 0.5], true),
        ([0.0, -1.0], [0.0, 2.0], true),
        ([-1.0, 1.0], [2.0, 1.0], true),
        ([-2.0, 0.0], [-1.0, 0.0], false),
        ([-1.0, 0.5], [0.5, -1.0], false),
        ([0.25, 0.25], [0.75, 0.75], true),
    ] {
        check_contact(a, b, rect, expected);
    }
    check_contact(
        [0.8, 0.4],
        [-0.4, -0.8],
        R2Rect::from_points([-1.0, 0.0], [0.0, 1.0]),
        false,
    );
    check_contact([0.0, -1.0], [0.0, 1.0], R2Rect::empty(), false);
}

#[test]
fn test_segment_intersects_rect_degenerate_rectangles() {
    let point = R2Rect::from_point([0.0, 0.0]);
    check_contact([-1.0, -1.0], [1.0, 1.0], point, true);
    check_contact([-1.0, 0.5], [0.5, -1.0], point, false);
    check_contact([0.0, 0.0], [0.0, 0.0], point, true);
    let line = R2Rect::from_points([0.0, 0.0], [0.0, 1.0]);
    check_contact([-1.0, 0.5], [1.0, 0.5], line, true);
    check_contact([0.0, -1.0], [0.0, 2.0], line, true);
    check_contact([-1.0, 0.5], [0.5, -1.0], line, false);
}

#[test]
fn test_segment_intersects_rect_adjacent_floats() {
    let rect = R2Rect::from_points([0.0, 0.0], [1.0, 1.0]);
    let below = f64::from_bits(1.0f64.to_bits() - 1);
    let above = f64::from_bits(1.0f64.to_bits() + 1);
    for (y, expected) in [(-above, false), (-1.0, true), (-below, true)] {
        check_contact([-1.0, 1.0], [1.0, y], rect, expected);
    }
    for (y, expected) in [(below, true), (1.0, true), (above, false)] {
        check_contact([-1.0, y], [2.0, y], rect, expected);
    }
    let tiny = f64::from_bits(1);
    for (x, expected) in [(-tiny, false), (0.0, true), (tiny, true)] {
        check_contact([x, -1.0], [x, 2.0], rect, expected);
    }
}

#[test]
fn test_segment_intersects_rect_extreme_exponents() {
    let rect = R2Rect::from_points([0.0, 0.0], [1.0, 1.0]);
    for exponent in [-500, -600, -1000] {
        let t = 2.0f64.powi(exponent);
        check_contact([t, -t], [-t, t / 2.0], rect, false);
        check_contact([t, -t], [-t, t], rect, true);
    }
    let tiny = f64::from_bits(1);
    check_contact([0.5, -0.5], [-4.0 * tiny, 3.0 * tiny], rect, false);
    check_contact([2.0 * tiny, -2.0 * tiny], [-2.0 * tiny, tiny], rect, false);
    let large = f64::MAX;
    check_contact([large, -large], [-large, large / 2.0], rect, false);
    check_contact([large, -large], [-large, large], rect, true);
}

#[test]
fn test_segment_intersects_rect_grid() {
    let points: Vec<_> = [-1.0, -0.5, 0.0, 0.5, 1.0]
        .into_iter()
        .flat_map(|x| [-1.0, -0.5, 0.0, 0.5, 1.0].map(|y| [x, y]))
        .collect();
    for x in [-1.0, 0.0] {
        for y in [-1.0, 0.0] {
            let rect = R2Rect::from_points([x, y], [x + 1.0, y + 1.0]);
            for &a in &points {
                for &b in &points {
                    assert_eq!(
                        segment_intersects_rect(&a, &b, &rect),
                        interval_intersects(a, b, &rect),
                        "{a:?}, {b:?}, {rect:?}"
                    );
                }
            }
        }
    }
}

#[test]
fn test_orient2d_sign_exponent_range() {
    for t in [
        f64::from_bits(1),
        f64::MIN_POSITIVE,
        2.0f64.powi(-600),
        2.0f64.powi(-201),
        2.0f64.powi(-200),
        1.0,
        2.0f64.powi(200),
        2.0f64.powi(201),
        2.0f64.powi(700),
        f64::MAX,
    ] {
        for (a, b, c, expected) in [
            ([0.0, 0.0], [t, 0.0], [0.0, t], 1),
            ([t, t], [-t, t], [t, -t], 1),
            ([-t, -t], [0.0, 0.0], [t, t], 0),
            ([t, 0.0], [t, 0.0], [0.0, t], 0),
        ] {
            assert_eq!(rational_orientation(a, b, c), expected);
            assert_eq!(orient2d_sign(&a, &b, &c), expected);
            assert_eq!(orient2d_sign(&b, &a, &c), -expected);
            assert_eq!(orient2d_sign(&b, &c, &a), expected);
        }
    }
    assert_eq!(orient2d_sign(&[0.0; 2], &[-0.0; 2], &[0.0; 2]), 0);
    let next = f64::from_bits(1.0f64.to_bits() + 1);
    assert_eq!(orient2d_sign(&[1.0, 1.0], &[next, 1.0], &[1.0, next]), 1);
    let tiny = f64::from_bits(1);
    let a = [0.5, -0.5];
    let b = [-4.0 * tiny, 3.0 * tiny];
    assert_eq!(orient2d_sign(&a, &b, &[0.0, 0.0]), -1);
    assert_eq!(orient2d_sign(&b, &a, &[0.0, 0.0]), 1);
    assert_eq!(orient2d_sign(&[tiny, tiny], &[1.0, 1.0], &[-1.0, -1.0]), 0);
}

#[test]
fn test_orient2d_sign_near_collinear() {
    for exponent in [-900, -201, -200, -1, 0, 199, 200, 201, 900] {
        let scale = 2.0f64.powi(exponent);
        for (a, b) in [
            ([0.0, 0.0], [1.0, 1.0]),
            ([-2.0, 1.0], [1.0, -1.0]),
            ([1.0, 1.0], [3.0, 2.0]),
            ([2.0, -1.0], [-1.0, 2.0]),
        ] {
            let a = a.map(|value| value * scale);
            let b = b.map(|value| value * scale);
            let ar = a.map(rational);
            let br = b.map(rational);
            for (numerator, denominator) in [(-1, 2), (1, 3), (2, 3), (3, 2), (2, 1)] {
                let t = BigRational::new(numerator.into(), denominator.into());
                let c: [f64; 2] = std::array::from_fn(|axis| {
                    (&ar[axis] + &t * (&br[axis] - &ar[axis])).to_f64().unwrap()
                });
                for x in [c[0].next_down(), c[0], c[0].next_up()] {
                    for y in [c[1].next_down(), c[1], c[1].next_up()] {
                        check_orientation(a, b, [x, y]);
                    }
                }
            }
        }
    }
}

#[test]
fn test_orient2d_sign_cancellation() {
    let e = f64::EPSILON;
    let b = [1.0, 1.0 + e];
    let c = [1.0 + e, 1.0 + 2.0 * e];
    // The rounded products cancel, but the exact determinant is -e squared.
    assert_eq!(b[0] * c[1] - b[1] * c[0], 0.0);
    assert_eq!(rational_orientation([0.0; 2], b, c), -1);
    for (x_exponent, y_exponent) in [
        (-900, -900),
        (-201, -201),
        (-200, -200),
        (0, 0),
        (200, 200),
        (201, 201),
        (900, 900),
        (-200, 200),
        (-200, 201),
        (-900, -500),
        (-900, -499),
        (500, 900),
        (499, 900),
    ] {
        let x = 2.0f64.powi(x_exponent);
        let y = 2.0f64.powi(y_exponent);
        let b = [x, y * (1.0 + e)];
        let c = [x * (1.0 + e), y * (1.0 + 2.0 * e)];
        assert_eq!(rational_orientation([0.0; 2], b, c), -1);
        for cx in [c[0].next_down(), c[0], c[0].next_up()] {
            for cy in [c[1].next_down(), c[1], c[1].next_up()] {
                check_orientation([0.0; 2], b, [cx, cy]);
            }
        }
    }
}

proptest! {
    #[test]
    fn test_orient2d_sign_oracle(bits in any::<[u64; 6]>()) {
        let values = bits.map(f64::from_bits);
        prop_assume!(values.iter().all(|value| value.is_finite()));
        let a = [values[0], values[1]];
        let b = [values[2], values[3]];
        let c = [values[4], values[5]];
        prop_assert_eq!(orient2d_sign(&a, &b, &c), rational_orientation(a, b, c));
    }

    #[test]
    fn test_segment_intersects_rect_oracle(bits in any::<[u64; 8]>()) {
        let values = bits.map(f64::from_bits);
        prop_assume!(values.iter().all(|value| value.is_finite()));
        let a = [values[0], values[1]];
        let b = [values[2], values[3]];
        let rect = R2Rect::from_point_pair([values[4], values[5]], [values[6], values[7]]);
        prop_assert_eq!(segment_intersects_rect(&a, &b, &rect), interval_intersects(a, b, &rect));
    }
}

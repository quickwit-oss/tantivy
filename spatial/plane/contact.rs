//! Exact positions and parameters on original planar edges.

use num_rational::BigRational;
use num_traits::{One, ToPrimitive, Zero};

use crate::spatial::r1interval::R1Interval;
use crate::spatial::r2rect::R2Rect;

pub(super) type Point = [BigRational; 2];

pub(super) fn point(source: [f64; 2]) -> Point {
    source.map(|coordinate| BigRational::from_float(coordinate).expect("finite Plane coordinate"))
}

pub(super) fn enclosing_bound(p: &Point) -> R2Rect {
    let interval = |value: &BigRational| {
        let estimate = value.to_f64().expect("finite Plane position");
        let mut lo = estimate;
        let mut hi = estimate;
        // The exact comparisons certify the bracket, including underflow to zero.
        while BigRational::from_float(lo).unwrap() > *value {
            lo = lo.next_down();
        }
        while BigRational::from_float(hi).unwrap() < *value {
            hi = hi.next_up();
        }
        R1Interval::new(lo, hi)
    };
    R2Rect::new(interval(&p[0]), interval(&p[1]))
}

pub(super) fn difference(a: &Point, b: &Point) -> Point {
    [&a[0] - &b[0], &a[1] - &b[1]]
}

pub(super) fn cross(a: &Point, b: &Point) -> BigRational {
    &a[0] * &b[1] - &a[1] * &b[0]
}

pub(super) fn position(a: &Point, b: &Point, parameter: &BigRational) -> Point {
    [
        &a[0] + parameter * (&b[0] - &a[0]),
        &a[1] + parameter * (&b[1] - &a[1]),
    ]
}

pub(super) fn parameter(a: &Point, b: &Point, p: &Point) -> Option<BigRational> {
    if (0..2).any(|axis| {
        (p[axis] < a[axis] && p[axis] < b[axis]) || (p[axis] > a[axis] && p[axis] > b[axis])
    }) {
        return None;
    }
    let direction = difference(b, a);
    let offset = difference(p, a);
    if !cross(&direction, &offset).is_zero() {
        return None;
    }
    let axis = if !direction[0].is_zero() { 0 } else { 1 };
    if direction[axis].is_zero() {
        return (a == p).then(BigRational::zero);
    }
    let t = &offset[axis] / &direction[axis];
    (t >= BigRational::zero() && t <= BigRational::one()).then_some(t)
}

/// Collinear contacts return the endpoints of the common interval.
pub(super) fn contacts(
    a: &Point,
    b: &Point,
    c: &Point,
    d: &Point,
) -> Vec<(BigRational, BigRational)> {
    let ab = difference(b, a);
    let cd = difference(d, c);
    let ac = difference(c, a);
    let denominator = cross(&ab, &cd);
    if !denominator.is_zero() {
        let t = cross(&ac, &cd) / &denominator;
        let u = cross(&ac, &ab) / denominator;
        return if t >= BigRational::zero()
            && t <= BigRational::one()
            && u >= BigRational::zero()
            && u <= BigRational::one()
        {
            vec![(t, u)]
        } else {
            Vec::new()
        };
    }
    let mut result = Vec::new();
    for p in [a, b, c, d] {
        if let (Some(t), Some(u)) = (parameter(a, b, p), parameter(c, d, p)) {
            result.push((t, u));
        }
    }
    result.sort_unstable();
    result.dedup();
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rational_lookup_bounds_include_subnormal_and_nonrepresentable_nodes() {
        let tiny = BigRational::from_float(f64::from_bits(1)).unwrap();
        for value in [
            BigRational::new(1.into(), 3.into()),
            tiny / BigRational::from_integer(3.into()),
            BigRational::from_float(90.0f64.next_down()).unwrap(),
        ] {
            for value in [value.clone(), -value] {
                let bound = enclosing_bound(&[value.clone(), value.clone()]);
                let lo = BigRational::from_float(bound.x.lo()).unwrap();
                let hi = BigRational::from_float(bound.x.hi()).unwrap();
                assert!(lo <= value && value <= hi);
                assert!(bound.x.lo() == bound.x.hi() || bound.x.lo().next_up() == bound.x.hi());
            }
        }
    }

    #[test]
    fn test_nonrepresentable_simultaneous_event() {
        let a = point([0.0, 0.0]);
        let b = point([1.0, 1.0]);
        let c = point([0.0, 1.0]);
        let d = point([1.0, -1.0]);
        let e = point([1.0, 0.0]);
        let f = point([-1.0, 1.0]);
        let third = BigRational::new(1.into(), 3.into());
        let expected = [third.clone(), third.clone()];
        assert_eq!(
            contacts(&a, &b, &c, &d),
            vec![(third.clone(), third.clone())]
        );
        for (p, q, r, s) in [(&a, &b, &c, &d), (&c, &d, &e, &f), (&f, &e, &b, &a)] {
            let events = contacts(p, q, r, s);
            assert_eq!(events.len(), 1);
            assert_eq!(position(p, q, &events[0].0), expected);
            assert_eq!(position(r, s, &events[0].1), expected);
        }
    }

    #[test]
    fn test_contact_intervals_and_points() {
        let a = point([0.0, 0.0]);
        let b = point([4.0, 4.0]);
        let c = point([3.0, 3.0]);
        let d = point([1.0, 1.0]);
        let quarter = BigRational::new(1.into(), 4.into());
        assert_eq!(
            contacts(&a, &b, &c, &d),
            vec![
                (quarter.clone(), BigRational::one()),
                (
                    quarter * BigRational::from_integer(3.into()),
                    BigRational::zero()
                ),
            ]
        );
        assert_eq!(
            contacts(&a, &a, &a, &b),
            vec![(BigRational::zero(), BigRational::zero())]
        );
        assert!(contacts(&a, &a, &c, &d).is_empty());
    }

    #[test]
    fn test_event_order_survives_extreme_exponents() {
        let a = point([0.0, 0.0]);
        let b = point([1.0, 0.0]);
        let mut parameters = Vec::new();
        for x in [
            f64::from_bits(1),
            2.0f64.powi(-600),
            0.5,
            1.0f64.next_down(),
        ] {
            let c = point([x, -1.0]);
            let d = point([x, 1.0]);
            let event = contacts(&a, &b, &c, &d).pop().unwrap();
            assert_eq!(event.0, BigRational::from_float(x).unwrap());
            assert_eq!(position(&a, &b, &event.0), point([x, 0.0]));
            parameters.push(event.0);
        }
        assert!(parameters.windows(2).all(|pair| pair[0] < pair[1]));
    }
}

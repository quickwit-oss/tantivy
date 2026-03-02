//! An S2LatLngRect represents a closed latitude-longitude rectangle. It is capable of
//! representing the empty and full rectangles as well as single points. Note that the
//! latitude-longitude space is considered to have a *cylindrical* topology rather than a spherical
//! one, i.e. the poles have multiple lat/lng representations. An S2LatLngRect may be defined so
//! that includes some representations of a pole but not others. Use the PolarClosure() method if
//! you want to expand a rectangle so that it contains all possible representations of any
//! contained poles.
//!
//! Ported from s2/s2latlng_rect.h (Eric Veach, Google).

use std::f64::consts::{FRAC_PI_2, PI};

use super::r1interval::R1Interval;
use super::region::Region;
use super::s1chord_angle::S1ChordAngle;
use super::s1interval::S1Interval;
use super::s2cap::S2Cap;
use super::s2cell::S2Cell;
use super::s2cell_id::S2CellId;

/// A closed latitude-longitude rectangle.
#[derive(Clone, Copy, Debug)]
pub struct S2LatLngRect {
    lat: R1Interval,
    lng: S1Interval,
}

impl S2LatLngRect {
    /// Construct a rectangle from latitude and longitude intervals.  The two intervals must
    /// either be both empty or both non-empty, and the latitude interval must not extend outside
    /// [-90, +90] degrees.  Note that both intervals (and hence the rectangle) are closed.
    pub fn new(lat: R1Interval, lng: S1Interval) -> Self {
        let result = Self { lat, lng };
        debug_assert!(result.is_valid());
        result
    }

    /// Construct a rectangle from minimum and maximum latitudes and longitudes.  If lo.lng() >
    /// hi.lng(), the rectangle spans the 180 degree longitude line.  Both points must be
    /// normalized, with lo.lat() <= hi.lat().
    pub fn from_lo_hi(lo: &[f64; 2], hi: &[f64; 2]) -> Self {
        Self::new(R1Interval::new(lo[1], hi[1]), S1Interval::new(lo[0], hi[0]))
    }

    /// Returns the empty rectangle (contains no points).
    pub fn empty() -> Self {
        Self {
            lat: R1Interval::empty(),
            lng: S1Interval::empty(),
        }
    }

    /// Returns the full rectangle (contains all points).
    pub fn full() -> Self {
        Self {
            lat: Self::full_lat(),
            lng: S1Interval::full(),
        }
    }

    /// The full allowable range of latitudes.
    pub fn full_lat() -> R1Interval {
        R1Interval::new(-FRAC_PI_2, FRAC_PI_2)
    }

    /// Construct a rectangle containing a single (normalized) point.
    pub fn from_point(p: &[f64; 2]) -> Self {
        Self {
            lat: R1Interval::from_point(p[1]),
            lng: S1Interval::from_point(p[0]),
        }
    }

    /// Construct the minimal bounding rectangle containing the two given normalized points.
    /// This is equivalent to starting with an empty rectangle and calling AddPoint() twice.
    /// Note that it is different than the S2LatLngRect(lo, hi) constructor, where the first
    /// point is always used as the lower-left corner of the resulting rectangle.
    pub fn from_point_pair(p1: &[f64; 2], p2: &[f64; 2]) -> Self {
        Self {
            lat: R1Interval::from_point_pair(p1[1], p2[1]),
            lng: S1Interval::from_point_pair(p1[0], p2[0]),
        }
    }

    /// Construct a rectangle of the given size centered around the given point.  The latitude
    /// interval of the result is clamped to [-90,90] degrees, and the longitude interval of the
    /// result is Full() if and only if the longitude size is 360 degrees or more.
    pub fn from_center_size(center: &[f64; 2], size: &[f64; 2]) -> Self {
        Self::from_point(center).expanded(size[1] * 0.5, size[0] * 0.5)
    }

    /// The latitude interval.
    pub fn lat(&self) -> &R1Interval {
        &self.lat
    }

    /// The longitude interval.
    pub fn lng(&self) -> &S1Interval {
        &self.lng
    }

    /// The low latitude.
    pub fn lat_lo(&self) -> f64 {
        self.lat.lo()
    }

    /// The high latitude.
    pub fn lat_hi(&self) -> f64 {
        self.lat.hi()
    }

    /// The low longitude.
    pub fn lng_lo(&self) -> f64 {
        self.lng.lo()
    }

    /// The high longitude.
    pub fn lng_hi(&self) -> f64 {
        self.lng.hi()
    }

    /// The low corner as [lng, lat].
    pub fn lo(&self) -> [f64; 2] {
        [self.lng.lo(), self.lat.lo()]
    }

    /// The high corner as [lng, lat].
    pub fn hi(&self) -> [f64; 2] {
        [self.lng.hi(), self.lat.hi()]
    }

    /// Returns true if the rectangle is valid, which essentially just means that the latitude
    /// bounds do not exceed Pi/2 in absolute value and the longitude bounds do not exceed Pi in
    /// absolute value.  Also, if either the latitude or longitude bound is empty then both must
    /// be.
    pub fn is_valid(&self) -> bool {
        self.lat.lo().abs() <= FRAC_PI_2
            && self.lat.hi().abs() <= FRAC_PI_2
            && self.lng.is_valid()
            && self.lat.is_empty() == self.lng.is_empty()
    }

    /// Returns true if the rectangle is empty, i.e. it contains no points at all.
    pub fn is_empty(&self) -> bool {
        self.lat.is_empty()
    }

    /// Returns true if the rectangle is full, i.e. it contains all points.
    pub fn is_full(&self) -> bool {
        self.lat == Self::full_lat() && self.lng.is_full()
    }

    /// Returns true if the rectangle is a point, i.e. lo() == hi()
    pub fn is_point(&self) -> bool {
        self.lat.lo() == self.lat.hi() && self.lng.lo() == self.lng.hi()
    }

    /// Returns true if lng_.lo() > lng_.hi(), i.e. the rectangle crosses the 180 degree
    /// longitude line.
    pub fn is_inverted(&self) -> bool {
        self.lng.is_inverted()
    }

    /// Returns the k-th vertex of the rectangle (k = 0,1,2,3) in CCW order (lower left, lower
    /// right, upper right, upper left).  For convenience, the argument is reduced modulo 4 to
    /// the range [0..3].
    pub fn get_vertex(&self, k: usize) -> [f64; 2] {
        let i = (k >> 1) & 1;
        let lat = if i == 0 { self.lat.lo() } else { self.lat.hi() };
        let j = i ^ (k & 1);
        let lng = if j == 0 { self.lng.lo() } else { self.lng.hi() };
        [lng, lat]
    }

    /// Returns the center of the rectangle in latitude-longitude space (in general this is not
    /// the center of the region on the sphere).
    pub fn get_center(&self) -> [f64; 2] {
        [self.lng.center(), self.lat.center()]
    }

    /// Returns the width and height of this rectangle in latitude-longitude space.  Empty
    /// rectangles have a negative width and height.
    pub fn get_size(&self) -> [f64; 2] {
        [self.lng.length(), self.lat.length()]
    }

    /// Returns the surface area of this rectangle on the unit sphere.
    pub fn area(&self) -> f64 {
        if self.is_empty() {
            0.0
        } else {
            self.lng.length() * (self.lat_hi().sin() - self.lat_lo().sin())
        }
    }

    /// More efficient version of Contains() that accepts a lat/lng rather than an S2Point.
    pub fn contains_latlng(&self, ll: &[f64; 2]) -> bool {
        self.lat.contains_point(ll[1]) && self.lng.contains_point(ll[0])
    }

    /// Returns true if and only if the given point is contained by the region.
    pub fn contains_point(&self, p: &[f64; 3]) -> bool {
        let ll = [
            p[1].atan2(p[0]),
            p[2].atan2((p[0] * p[0] + p[1] * p[1]).sqrt()),
        ];
        self.contains_latlng(&ll)
    }

    /// Returns true if and only if the given point is contained in the interior of the region
    /// (i.e. the region excluding its boundary).
    pub fn interior_contains_latlng(&self, ll: &[f64; 2]) -> bool {
        self.lat.interior_contains_point(ll[1]) && self.lng.interior_contains_point(ll[0])
    }

    /// Returns true if and only if the rectangle contains the given other rectangle.
    pub fn contains(&self, other: &S2LatLngRect) -> bool {
        self.lat.contains(&other.lat) && self.lng.contains(&other.lng)
    }

    /// Returns true if and only if the interior of this rectangle contains all points of the
    /// given other rectangle (including its boundary).
    pub fn interior_contains(&self, other: &S2LatLngRect) -> bool {
        self.lat.interior_contains(&other.lat) && self.lng.interior_contains(&other.lng)
    }

    /// Returns true if this rectangle and the given other rectangle have any points in common.
    pub fn intersects(&self, other: &S2LatLngRect) -> bool {
        self.lat.intersects(&other.lat) && self.lng.intersects(&other.lng)
    }

    /// Returns true if and only if the interior of this rectangle intersects any point
    /// (including the boundary) of the given other rectangle.
    pub fn interior_intersects(&self, other: &S2LatLngRect) -> bool {
        self.lat.interior_intersects(&other.lat) && self.lng.interior_intersects(&other.lng)
    }

    /// Increase the size of the bounding rectangle to include the given point.  The rectangle
    /// is expanded by the minimum amount possible.
    pub fn add_point_3d(&mut self, p: &[f64; 3]) {
        let ll = [
            p[1].atan2(p[0]),
            p[2].atan2((p[0] * p[0] + p[1] * p[1]).sqrt()),
        ];
        self.add_point(&ll);
    }

    /// Increase the size of the bounding rectangle to include the given point.
    pub fn add_point(&mut self, ll: &[f64; 2]) {
        self.lat.add_point(ll[1]);
        self.lng.add_point(ll[0]);
    }

    /// Returns a rectangle that has been expanded by the given margin on each side in the
    /// latitude direction, and by the given margin on each side in the longitude direction.
    /// If either margin is negative, then shrinks the rectangle on the corresponding sides
    /// instead.  The resulting rectangle may be empty.
    ///
    /// If either the latitude or longitude interval becomes empty after expansion by a negative
    /// margin, the result is empty.
    pub fn expanded(&self, lat_margin: f64, lng_margin: f64) -> S2LatLngRect {
        let lat = self.lat.expanded(lat_margin);
        let lng = self.lng.expanded(lng_margin);
        if lat.is_empty() || lng.is_empty() {
            return S2LatLngRect::empty();
        }
        S2LatLngRect::new(lat.intersection(&Self::full_lat()), lng)
    }

    /// If the rectangle does not include either pole, returns it unmodified.  Otherwise expands
    /// the longitude range to Full() so that the rectangle contains all possible representations
    /// of the contained pole(s).
    pub fn polar_closure(&self) -> S2LatLngRect {
        if self.lat.lo() == -FRAC_PI_2 || self.lat.hi() == FRAC_PI_2 {
            S2LatLngRect::new(self.lat, S1Interval::full())
        } else {
            *self
        }
    }

    /// Returns the smallest rectangle containing the union of this rectangle and the given
    /// rectangle.
    pub fn union(&self, other: &S2LatLngRect) -> S2LatLngRect {
        S2LatLngRect::new(self.lat.union(&other.lat), self.lng.union(&other.lng))
    }

    /// Returns the smallest rectangle containing the intersection of this rectangle and the
    /// given rectangle.  Note that the region of intersection may consist of two disjoint
    /// rectangles, in which case a single rectangle spanning both of them is returned.
    pub fn intersection(&self, other: &S2LatLngRect) -> S2LatLngRect {
        let lat = self.lat.intersection(&other.lat);
        let lng = self.lng.intersection(&other.lng);
        if lat.is_empty() || lng.is_empty() {
            S2LatLngRect::empty()
        } else {
            S2LatLngRect::new(lat, lng)
        }
    }

    /// Returns true if the latitude and longitude intervals of the two rectangles are the same
    /// up to the given tolerance.
    pub fn approx_equals(&self, other: &S2LatLngRect, max_error: f64) -> bool {
        self.lat.approx_equals(&other.lat, max_error)
            && self.lng.approx_equals(&other.lng, max_error)
    }

    /// Returns a bounding spherical cap for this rectangle.  We consider two possible bounding
    /// caps, one whose axis passes through the center of the rectangle and one whose axis is the
    /// north or south pole, and return the smaller of the two.
    pub fn get_cap_bound_impl(&self) -> S2Cap {
        if self.is_empty() {
            return S2Cap::empty();
        }

        let (pole_z, pole_angle) = if self.lat.lo() + self.lat.hi() < 0.0 {
            (-1.0, FRAC_PI_2 + self.lat.hi())
        } else {
            (1.0, FRAC_PI_2 - self.lat.lo())
        };

        let pole_cap = S2Cap::new(
            [0.0, 0.0, pole_z],
            S1ChordAngle::from_radians((1.0 + 2.0 * f64::EPSILON) * pole_angle),
        );

        if self.lng.length() < 2.0 * PI {
            let center = self.get_center();
            let cos_lat = center[1].cos();
            let center_point = [
                cos_lat * center[0].cos(),
                cos_lat * center[0].sin(),
                center[1].sin(),
            ];

            let mut mid_cap = S2Cap::from_point(center_point);
            for k in 0..4 {
                let vertex = self.get_vertex(k);
                let cos_lat_v = vertex[1].cos();
                let vertex_point = [
                    cos_lat_v * vertex[0].cos(),
                    cos_lat_v * vertex[0].sin(),
                    vertex[1].sin(),
                ];
                mid_cap.add_point(&vertex_point);
            }

            // C++ compares height(), which is 0.5 * length2().  The factor
            // cancels in the comparison so length2() is equivalent.
            if mid_cap.radius().length2() < pole_cap.radius().length2() {
                return mid_cap;
            }
        }

        pole_cap
    }
}

impl Region for S2LatLngRect {
    fn get_cap_bound(&self) -> S2Cap {
        self.get_cap_bound_impl()
    }

    fn get_rect_bound(&self) -> S2LatLngRect {
        *self
    }

    fn get_cell_union_bound(&self, cell_ids: &mut Vec<S2CellId>) {
        self.get_cap_bound().get_cell_union_bound(cell_ids);
    }

    /// Returns true if this rectangle contains the given cell.  This is an exact test assuming
    /// that S2Cell::GetRectBound() returns tight bounds.
    fn contains_cell(&self, cell: &S2Cell) -> bool {
        self.contains(&cell.get_rect_bound())
    }

    /// This test is cheap but is NOT exact.  Use Intersects() if you want a more accurate and
    /// more expensive test.  Note that when this method is used by an S2RegionCoverer, the
    /// accuracy isn't all that important since if a cell may intersect the region then it is
    /// subdivided, and the accuracy of this method goes up as the cells get smaller.
    fn may_intersect(&self, cell: &S2Cell) -> bool {
        self.intersects(&cell.get_rect_bound())
    }

    /// The point 'p' does not need to be normalized.
    fn contains_point(&self, p: &[f64; 3]) -> bool {
        self.contains_point(p)
    }
}

impl PartialEq for S2LatLngRect {
    fn eq(&self, other: &Self) -> bool {
        self.lat == other.lat && self.lng == other.lng
    }
}

impl std::fmt::Display for S2LatLngRect {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[Lo{:?}, Hi{:?}]", self.lo(), self.hi())
    }
}

#[cfg(test)]
#[path = "tests/latlng_rect_tests.rs"]
mod tests;

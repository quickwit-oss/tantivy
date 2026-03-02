//! S2Cap represents a disc-shaped region defined by a center and radius. Technically this shape is
//! called a "spherical cap" (rather than disc) because it is not planar; the cap represents a
//! portion of the sphere that has been cut off by a plane.  The boundary of the cap is the circle
//! defined by the intersection of the sphere and the plane.  For containment purposes, the cap is
//! a closed set, i.e. it contains its boundary.
//!
//! The radius of the cap is measured along the surface of the sphere (rather than the
//! straight-line distance through the interior).  Thus a cap of radius Pi/2 is a hemisphere, and a
//! cap of radius Pi covers the entire sphere.
//!
//! Ported from s2/s2cap.h (Eric Veach, Google).

use std::f64::consts::{FRAC_PI_2, PI};

use super::latlng_rect::S2LatLngRect;
use super::math::{cross, dot, neg, norm2, remainder};
use super::r1interval::R1Interval;
use super::region::Region;
use super::s1chord_angle::{S1ChordAngle, RELATIVE_SUM_ERROR};
use super::s1interval::S1Interval;
use super::s2cell::S2Cell;
use super::s2cell_id::S2CellId;
use super::s2metrics::S2Metrics;

/// S2Cap represents a disc-shaped region defined by a center and radius.
#[derive(Clone, Copy, Debug)]
pub struct S2Cap {
    center: [f64; 3],
    radius: S1ChordAngle,
}

impl S2Cap {
    /// Return an empty cap, i.e. a cap that contains no points.
    pub fn empty() -> Self {
        Self {
            center: [1.0, 0.0, 0.0],
            radius: S1ChordAngle::negative(),
        }
    }

    /// Return a full cap, i.e. a cap that contains all points.
    pub fn full() -> Self {
        Self {
            center: [1.0, 0.0, 0.0],
            radius: S1ChordAngle::straight(),
        }
    }

    /// Constructs a cap where the angle is expressed as an S1ChordAngle.  A negative radius yields
    /// an empty cap; a radius of 180 degrees or more yields a full cap.  "center" should be unit
    /// length.
    pub fn new(center: [f64; 3], radius: S1ChordAngle) -> Self {
        Self { center, radius }
    }

    /// Constructs a cap with the given center and radius in radians.
    pub fn from_center_angle(center: [f64; 3], radius_radians: f64) -> Self {
        Self {
            center,
            radius: S1ChordAngle::from_radians(radius_radians.min(PI)),
        }
    }

    /// Convenience function that creates a cap containing a single point.
    pub fn from_point(center: [f64; 3]) -> Self {
        Self {
            center,
            radius: S1ChordAngle::zero(),
        }
    }

    /// Returns a cap with the given center and height (see comments above).  A negative height
    /// yields an empty cap; a height of 2 or more yields a full cap.  "center" should be unit
    /// length.
    pub fn from_center_height(center: [f64; 3], height: f64) -> Self {
        Self {
            center,
            radius: S1ChordAngle::from_length2(2.0 * height),
        }
    }

    /// Return a cap with the given center and surface area.  Note that the area can also be
    /// interpreted as the solid angle subtended by the cap (because the sphere has unit radius). A
    /// negative area yields an empty cap; an area of 4*Pi or more yields a full cap.
    pub fn from_center_area(center: [f64; 3], area: f64) -> Self {
        Self {
            center,
            radius: S1ChordAngle::from_length2(area / PI),
        }
    }

    /// The center of the cap.
    #[inline]
    pub fn center(&self) -> [f64; 3] {
        self.center
    }

    /// The radius of the cap as an S1ChordAngle.
    #[inline]
    pub fn radius(&self) -> S1ChordAngle {
        self.radius
    }

    /// Returns the height of the cap, i.e. the distance from the center point to the cutoff plane.
    #[inline]
    pub fn height(&self) -> f64 {
        0.5 * self.radius.length2()
    }

    /// Return the cap radius as an S1Angle.  (Note that the cap angle is stored internally as an
    /// S1ChordAngle, so this method requires a trigonometric operation and may yield a slightly
    /// different result than the value passed to the (S2Point, S1Angle) constructor.)
    pub fn get_radius_radians(&self) -> f64 {
        self.radius.to_radians()
    }

    /// Return the area of the cap.
    pub fn get_area(&self) -> f64 {
        2.0 * PI * self.height().max(0.0)
    }

    /// Return the true centroid of the cap multiplied by its surface area (see s2centroids.h for
    /// details on centroids).  The result lies on the ray from the origin through the cap's
    /// center, but it is not unit length.  Note that if you just want the "surface centroid", i.e.
    /// the normalized result, then it is much simpler just to call center().
    ///
    /// The reason for multiplying the result by the cap area is to make it easier to compute the
    /// centroid of more complicated shapes.  The centroid of a union of disjoint regions can be
    /// computed simply by adding their GetCentroid() results.  Caveat: for caps that contain a
    /// single point (i.e., zero radius), this method always returns the origin (0, 0, 0).  This is
    /// because shapes with no area don't affect the centroid of a union whose total area is
    /// positive.
    pub fn get_centroid(&self) -> [f64; 3] {
        if self.is_empty() {
            return [0.0, 0.0, 0.0];
        }
        // From symmetry, the centroid of the cap must be somewhere on the line from the origin to
        // the center of the cap on the surface of the sphere. When a sphere is divided into slices
        // of constant thickness by a set of parallel planes, all slices have the same surface
        // area. This implies that the radial component of the centroid is simply the midpoint of
        // the range of radial distances spanned by the cap. That is easily computed from the cap
        // height.
        let r = 1.0 - 0.5 * self.height();
        let scale = r * self.get_area();
        [
            scale * self.center[0],
            scale * self.center[1],
            scale * self.center[2],
        ]
    }

    /// We allow negative heights (to represent empty caps) but heights are normalized so that they
    /// do not exceed 2.
    pub fn is_valid(&self) -> bool {
        let norm2 = self.center[0] * self.center[0]
            + self.center[1] * self.center[1]
            + self.center[2] * self.center[2];
        (norm2 - 1.0).abs() <= 5.0 * f64::EPSILON && self.radius.length2() <= 4.0
    }

    /// Return true if the cap is empty, i.e. it contains no points.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.radius.is_negative()
    }

    /// Return true if the cap is full, i.e. it contains all points.
    #[inline]
    pub fn is_full(&self) -> bool {
        self.radius.length2() == 4.0
    }

    /// Return the complement of the interior of the cap.  A cap and its complement have the same
    /// boundary but do not share any interior points.  The complement operator is not a bijection
    /// because the complement of a singleton cap (containing a single point) is the same as the
    /// complement of an empty cap.
    pub fn complement(&self) -> Self {
        // The complement of a full cap is an empty cap, not a singleton. Also make sure that the
        // complement of an empty cap is full.
        if self.is_full() {
            return Self::empty();
        }
        if self.is_empty() {
            return Self::full();
        }
        Self {
            center: neg(self.center),
            radius: S1ChordAngle::from_length2(4.0 - self.radius.length2()),
        }
    }

    /// Return true if and only if this cap contains the given other cap (in a set containment
    /// sense, e.g. every cap contains the empty cap).
    pub fn contains_cap(&self, other: &S2Cap) -> bool {
        if self.is_full() || other.is_empty() {
            return true;
        }
        self.radius >= S1ChordAngle::from_points(&self.center, &other.center) + other.radius
    }

    /// Return true if and only if this cap intersects the given other cap, i.e. whether they have
    /// any points in common.
    pub fn intersects_cap(&self, other: &S2Cap) -> bool {
        if self.is_empty() || other.is_empty() {
            return false;
        }
        self.radius + other.radius >= S1ChordAngle::from_points(&self.center, &other.center)
    }

    /// Return true if and only if the interior of this cap intersects the given other cap. (This
    /// relationship is not symmetric, since only the interior of this cap is used.)
    pub fn interior_intersects_cap(&self, other: &S2Cap) -> bool {
        // Make sure this cap has an interior and the other cap is non-empty.
        if self.radius.length2() <= 0.0 || other.is_empty() {
            return false;
        }
        self.radius + other.radius > S1ChordAngle::from_points(&self.center, &other.center)
    }

    /// The point "p" should be a unit-length vector.
    #[inline]
    pub fn contains_point(&self, p: &[f64; 3]) -> bool {
        S1ChordAngle::from_points(&self.center, p) <= self.radius
    }

    /// Return true if and only if the given point is contained in the interior of the cap (i.e.
    /// the cap excluding its boundary).  "p" should be a unit-length vector.
    pub fn interior_contains_point(&self, p: &[f64; 3]) -> bool {
        self.is_full() || S1ChordAngle::from_points(&self.center, p) < self.radius
    }

    /// Increase the cap height if necessary to include the given point.  If the cap is empty then
    /// the center is set to the given point, but otherwise the center is not changed. "p" should
    /// be a unit-length vector.
    pub fn add_point(&mut self, p: &[f64; 3]) {
        if self.is_empty() {
            self.center = *p;
            self.radius = S1ChordAngle::zero();
        } else {
            let dist = S1ChordAngle::from_points(&self.center, p);
            if dist > self.radius {
                self.radius = dist;
            }
        }
    }

    /// Increase the cap height if necessary to include "other".  If the current cap is empty it is
    /// set to the given other cap.
    pub fn add_cap(&mut self, other: &S2Cap) {
        if self.is_empty() {
            *self = *other;
        } else if !other.is_empty() {
            // We round up the distance to ensure that the cap is actually contained.
            let dist = S1ChordAngle::from_points(&self.center, &other.center) + other.radius;
            let error = (2.0 * f64::EPSILON + RELATIVE_SUM_ERROR) * dist.length2();
            let dist_with_error = dist.plus_error(error);
            if dist_with_error > self.radius {
                self.radius = dist_with_error;
            }
        }
    }

    /// Return a cap that contains all points within a given distance of this cap.  Note that any
    /// expansion of the empty cap is still empty.
    pub fn expanded(&self, distance_radians: f64) -> Self {
        debug_assert!(distance_radians >= 0.0);
        if self.is_empty() {
            return Self::empty();
        }
        Self {
            center: self.center,
            radius: self.radius + S1ChordAngle::from_radians(distance_radians),
        }
    }

    // Return true if this cap intersects any point of 'cell' excluding its vertices (which are
    // assumed to already have been checked).
    fn intersects_cell(&self, cell: &S2Cell, vertices: &[[f64; 3]; 4]) -> bool {
        // If the cap is a hemisphere or larger, the cell and the complement of the cap are both
        // convex.  Therefore since no vertex of the cell is contained, no other interior point of
        // the cell is contained either.
        if self.radius >= S1ChordAngle::right() {
            return false;
        }

        // We need to check for empty caps due to the center check just below.
        if self.is_empty() {
            return false;
        }

        // Optimization: return true if the cell contains the cap center.  (This allows half of the
        // edge checks below to be skipped.)
        if cell.contains_point(&self.center) {
            return true;
        }

        // At this point we know that the cell does not contain the cap center, and the cap does
        // not contain any cell vertex.  The only way that they can intersect is if the cap
        // intersects the interior of some edge.
        let sin2_angle = self.radius.sin2();
        for k in 0..4 {
            let edge = cell.get_edge_raw(k);
            let dotted = dot(&self.center, &edge);
            if dotted > 0.0 {
                // The center is in the interior half-space defined by the edge.  We don't need to
                // consider these edges, since if the cap intersects this edge then it also
                // intersects the edge on the opposite side of the cell (because we know the center
                // is not contained with the cell).
                continue;
            }
            // The Norm2() factor is necessary because "edge" is not normalized.
            let edge_norm2 = norm2(&edge);
            if dotted * dotted > sin2_angle * edge_norm2 {
                return false;
            }
            // Otherwise, the great circle containing this edge intersects the interior of the cap.
            // We just need to check whether the point of closest approach occurs between the two
            // edge endpoints.
            let dir = cross(&edge, &self.center);
            if dot(&dir, &vertices[k]) < 0.0 && dot(&dir, &vertices[(k + 1) & 3]) > 0.0 {
                return true;
            }
        }
        false
    }

    /// Computes a covering of the S2Cap.  In general the covering consists of at most 4 cells
    /// except for very large caps, which may need up to 6 cells. The output is not sorted.
    pub fn get_cell_union_bound(&self, cell_ids: &mut Vec<S2CellId>) {
        cell_ids.clear();

        // Find the maximum level such that the cap contains at most one cell vertex and such that
        // S2CellId::AppendVertexNeighbors() can be called.
        let level = S2Metrics::get_level_for_min_width(self.get_radius_radians()) - 1;

        // If level < 0, then more than three face cells are required.
        if level < 0 {
            for face in 0..6 {
                cell_ids.push(S2CellId::from_face(face));
            }
        } else {
            // The covering consists of the 4 cells at the given level that share the cell vertex
            // that is closest to the cap center.
            let center_cell = S2CellId::from_point(&self.center);
            center_cell.append_vertex_neighbors(level, cell_ids);
        }
    }

    /// Computes the bounding latitude-longitude rectangle for the cap.
    pub fn get_rect_bound_impl(&self) -> S2LatLngRect {
        if self.is_empty() {
            return S2LatLngRect::empty();
        }

        // Convert the center to a (lat, lng) pair, and compute the cap angle.
        let center_lat = self.center[2].asin();
        let center_lng = self.center[1].atan2(self.center[0]);
        let cap_angle = self.get_radius_radians();

        let mut all_longitudes = false;
        let mut lat_lo;
        let mut lat_hi;
        let mut lng_lo = -PI;
        let mut lng_hi = PI;

        // Check whether cap includes the south pole.
        lat_lo = center_lat - cap_angle;
        if lat_lo <= -FRAC_PI_2 {
            lat_lo = -FRAC_PI_2;
            all_longitudes = true;
        }

        // Check whether cap includes the north pole.
        lat_hi = center_lat + cap_angle;
        if lat_hi >= FRAC_PI_2 {
            lat_hi = FRAC_PI_2;
            all_longitudes = true;
        }

        if !all_longitudes {
            // Compute the range of longitudes covered by the cap.  We use the law of sines for
            // spherical triangles.  Consider the triangle ABC where A is the north pole, B is the
            // center of the cap, and C is the point of tangency between the cap boundary and a
            // line of longitude.  Then C is a right angle, and letting a,b,c denote the sides
            // opposite A,B,C, we have sin(a)/sin(A) = sin(c)/sin(C), or sin(A) = sin(a)/sin(c).
            // Here "a" is the cap angle, and "c" is the colatitude (90 degrees minus the
            // latitude).  This formula also works for negative latitudes.
            //
            // The formula for sin(a) follows from the relationship h = 1 - cos(a).
            let sin_a = self.radius.sin();
            let sin_c = center_lat.cos();
            if sin_a <= sin_c {
                let angle_a = (sin_a / sin_c).asin();
                lng_lo = remainder(center_lng - angle_a, 2.0 * PI);
                lng_hi = remainder(center_lng + angle_a, 2.0 * PI);
            }
        }

        S2LatLngRect::new(
            R1Interval::new(lat_lo, lat_hi),
            S1Interval::new(lng_lo, lng_hi),
        )
    }
}

impl Region for S2Cap {
    fn get_cap_bound(&self) -> S2Cap {
        *self
    }

    fn get_rect_bound(&self) -> S2LatLngRect {
        self.get_rect_bound_impl()
    }

    fn get_cell_union_bound(&self, cell_ids: &mut Vec<S2CellId>) {
        self.get_cell_union_bound(cell_ids);
    }

    // If the cap does not contain all cell vertices, return false. We check the vertices before
    // taking the Complement() because we can't accurately represent the complement of a very small
    // cap (a height of 2-epsilon is rounded off to 2).
    fn contains_cell(&self, cell: &S2Cell) -> bool {
        let mut vertices = [[0.0; 3]; 4];
        for k in 0..4 {
            vertices[k] = cell.get_vertex(k);
            if !self.contains_point(&vertices[k]) {
                return false;
            }
        }
        // Otherwise, return true if the complement of the cap does not intersect the cell.  (This
        // test is slightly conservative, because technically we want
        // Complement().InteriorIntersects() here.)
        !self.complement().intersects_cell(cell, &vertices)
    }

    fn may_intersect(&self, cell: &S2Cell) -> bool {
        // If the cap contains any cell vertex, return true.
        let mut vertices = [[0.0; 3]; 4];
        for k in 0..4 {
            vertices[k] = cell.get_vertex(k);
            if self.contains_point(&vertices[k]) {
                return true;
            }
        }
        self.intersects_cell(cell, &vertices)
    }

    fn contains_point(&self, p: &[f64; 3]) -> bool {
        self.contains_point(p)
    }
}

impl PartialEq for S2Cap {
    fn eq(&self, other: &Self) -> bool {
        (self.center == other.center && self.radius == other.radius)
            || (self.is_empty() && other.is_empty())
            || (self.is_full() && other.is_full())
    }
}

impl Eq for S2Cap {}

impl std::fmt::Display for S2Cap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "[Center=({:.6}, {:.6}, {:.6}), Radius={:.6}deg]",
            self.center[0],
            self.center[1],
            self.center[2],
            self.radius.to_degrees()
        )
    }
}

#[cfg(test)]
#[path = "tests/s2cap_tests.rs"]
mod tests;

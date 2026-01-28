//! Axis-aligned rectangles in the (x,y) plane.
//!
//! R2Rect represents closed, bounded rectangles using intervals for each axis.
//! Used for UV-space bounds during cell operations and edge clipping.
//!
//! R2Rect in r2rect.h and r2rect.cc.

use super::r1interval::R1Interval;

/// A closed axis-aligned rectangle defined by intervals on the x and y axes.
///
/// Empty rectangles have empty intervals on both axes. Use `is_empty()` to test, since empty
/// rectangles have multiple representations.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct R2Rect {
    /// The x-axis interval.
    pub x: R1Interval,
    /// The y-axis interval.
    pub y: R1Interval,
}

impl R2Rect {
    /// Construct a rectangle from the given intervals in x and y.  The two intervals must either
    /// be both empty or both non-empty.
    #[inline]
    pub fn new(x: R1Interval, y: R1Interval) -> Self {
        debug_assert!(x.is_empty() == y.is_empty());
        Self { x, y }
    }

    /// Construct a rectangle from the given lower-left and upper-right points.
    #[inline]
    pub fn from_points(lo: [f64; 2], hi: [f64; 2]) -> Self {
        let x = R1Interval::new(lo[0], hi[0]);
        let y = R1Interval::new(lo[1], hi[1]);
        debug_assert!(x.is_empty() == y.is_empty());
        Self { x, y }
    }

    /// The canonical empty rectangle.  Use is_empty() to test for empty rectangles, since they
    /// have more than one representation.
    #[inline]
    pub fn empty() -> Self {
        Self {
            x: R1Interval::empty(),
            y: R1Interval::empty(),
        }
    }

    /// Convenience method to construct a rectangle containing a single point.
    #[inline]
    pub fn from_point(p: [f64; 2]) -> Self {
        Self::from_points(p, p)
    }

    /// Convenience method to construct the minimal bounding rectangle containing the two given
    /// points.  This is equivalent to starting with an empty rectangle and calling AddPoint()
    /// twice.  Note that it is different than the R2Rect(lo, hi) constructor, where the first
    /// point is always used as the lower-left corner of the resulting rectangle.
    #[inline]
    pub fn from_point_pair(p1: [f64; 2], p2: [f64; 2]) -> Self {
        Self {
            x: R1Interval::from_point_pair(p1[0], p2[0]),
            y: R1Interval::from_point_pair(p1[1], p2[1]),
        }
    }

    /// Accessor.
    #[inline]
    pub fn lo(&self) -> [f64; 2] {
        [self.x.lo(), self.y.lo()]
    }

    /// Accessor.
    #[inline]
    pub fn hi(&self) -> [f64; 2] {
        [self.x.hi(), self.y.hi()]
    }

    /// Return true if the rectangle is empty, i.e. it contains no points at all.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.x.is_empty()
    }

    /// Return the vertex in direction "i" along the x-axis (0=left, 1=right) and direction "j"
    /// along the y-axis (0=down, 1=up).  Equivalently, return the vertex constructed by selecting
    /// endpoint "i" of the x-interval (0=lo, 1=hi) and vertex "j" of the y-interval.
    #[inline]
    pub fn get_vertex(&self, k: usize) -> [f64; 2] {
        let j = (k >> 1) & 1;
        self.get_vertex_ij(j ^ (k & 1), j)
    }

    /// Return the vertex in direction "i" along the x-axis (0=left, 1=right) and direction "j"
    /// along the y-axis (0=down, 1=up).  Equivalently, return the vertex constructed by selecting
    /// endpoint "i" of the x-interval (0=lo, 1=hi) and vertex "j" of the y-interval.
    #[inline]
    pub fn get_vertex_ij(&self, i: usize, j: usize) -> [f64; 2] {
        [
            if i == 0 { self.x.lo() } else { self.x.hi() },
            if j == 0 { self.y.lo() } else { self.y.hi() },
        ]
    }

    /// Return the center of the rectangle in (x,y)-space.
    #[inline]
    pub fn get_center(&self) -> [f64; 2] {
        [self.x.center(), self.y.center()]
    }

    /// Return the width and height of this rectangle in (x,y)-space.  Empty rectangles have a
    /// negative width and height.
    #[inline]
    pub fn get_size(&self) -> [f64; 2] {
        [self.x.length(), self.y.length()]
    }

    /// Return true if the rectangle contains the given point.  Note that rectangles are closed
    /// regions, i.e. they contain their boundary.
    #[inline]
    pub fn contains_point(&self, p: [f64; 2]) -> bool {
        self.x.contains_point(p[0]) && self.y.contains_point(p[1])
    }

    /// Return true if and only if the given point is contained in the interior of the region (i.e.
    /// the region excluding its boundary).
    #[inline]
    pub fn interior_contains_point(&self, p: [f64; 2]) -> bool {
        self.x.interior_contains_point(p[0]) && self.y.interior_contains_point(p[1])
    }

    /// Return true if and only if the rectangle contains the given other rectangle.
    #[inline]
    pub fn contains(&self, other: &R2Rect) -> bool {
        self.x.contains(&other.x) && self.y.contains(&other.y)
    }

    /// Return true if this rectangle and the given other rectangle have any points in common.
    #[inline]
    pub fn intersects(&self, other: &R2Rect) -> bool {
        self.x.intersects(&other.x) && self.y.intersects(&other.y)
    }

    /// Return a rectangle that has been expanded on each side in the x-direction by margin.x(),
    /// and on each side in the y-direction by margin.y().  If either margin is negative, then
    /// shrink the interval on the corresponding sides instead.  The resulting rectangle may be
    /// empty.  Any expansion of an empty rectangle remains empty.
    #[inline]
    pub fn expanded(&self, margin: f64) -> Self {
        let x = self.x.expanded(margin);
        let y = self.y.expanded(margin);
        if x.is_empty() || y.is_empty() {
            return Self::empty();
        }
        Self { x, y }
    }

    /// Returns the smallest rectangle containing both this and the given rectangle.
    #[inline]
    pub fn expanded_xy(&self, margin: [f64; 2]) -> Self {
        let x = self.x.expanded(margin[0]);
        let y = self.y.expanded(margin[1]);
        if x.is_empty() || y.is_empty() {
            return Self::empty();
        }
        Self { x, y }
    }

    /// Return the smallest rectangle containing the union of this rectangle and the given
    /// rectangle.
    #[inline]
    pub fn union(&self, other: &R2Rect) -> Self {
        Self {
            x: self.x.union(&other.x),
            y: self.y.union(&other.y),
        }
    }

    /// Return the smallest rectangle containing the intersection of this rectangle and the given
    /// rectangle.
    #[inline]
    pub fn intersection(&self, other: &R2Rect) -> Self {
        Self {
            x: self.x.intersection(&other.x),
            y: self.y.intersection(&other.y),
        }
    }

    /// Expand the rectangle to include the given point.  The rectangle is expanded by the minimum
    /// amount possible.
    #[inline]
    pub fn add_point(&mut self, p: [f64; 2]) {
        self.x.add_point(p[0]);
        self.y.add_point(p[1]);
    }

    /// Expand the rectangle to include the given other rectangle.  This is the same as replacing
    /// the rectangle by the union of the two rectangles, but is somewhat more efficient.
    #[inline]
    pub fn add_rect(&mut self, other: &R2Rect) {
        self.x = self.x.union(&other.x);
        self.y = self.y.union(&other.y);
    }

    /// Return the closest point in the rectangle to the given point "p". The rectangle must be
    /// non-empty.
    #[inline]
    pub fn project(&self, p: [f64; 2]) -> [f64; 2] {
        [self.x.project(p[0]), self.y.project(p[1])]
    }
}

impl std::ops::Index<usize> for R2Rect {
    type Output = R1Interval;
    #[inline]
    fn index(&self, i: usize) -> &R1Interval {
        match i {
            0 => &self.x,
            1 => &self.y,
            _ => panic!("R2Rect index out of bounds"),
        }
    }
}

impl Default for R2Rect {
    /// The default constructor creates an empty R2Rect.
    fn default() -> Self {
        Self::empty()
    }
}

//! Stateful edge crosser for planar geometry.
//!
//! Same interface as S2EdgeCrosser but uses orient2d for crossing decisions.

use super::math::orient2d;
use crate::spatial::edge_crosser::EdgeCrosser;

/// Stateful edge crosser for the plane. Holds a reference edge AB and tests successive edges
/// against it.
pub struct PlaneEdgeCrosser {
    a: [f64; 2],
    b: [f64; 2],
}

impl PlaneEdgeCrosser {
    /// Create a crosser for the fixed edge AB.
    pub fn new(a: &[f64; 2], b: &[f64; 2]) -> Self {
        PlaneEdgeCrosser { a: *a, b: *b }
    }

    /// Returns +1 if AB and CD cross in their interiors, 0 if they share a vertex, -1 otherwise.
    pub fn crossing_sign_two(&mut self, c: &[f64; 2], d: &[f64; 2]) -> i32 {
        if self.a == *c || self.a == *d || self.b == *c || self.b == *d {
            return 0;
        }

        let d1 = orient2d(&self.a, &self.b, c);
        let d2 = orient2d(&self.a, &self.b, d);
        let d3 = orient2d(c, d, &self.a);
        let d4 = orient2d(c, d, &self.b);

        if ((d1 > 0.0 && d2 < 0.0) || (d1 < 0.0 && d2 > 0.0))
            && ((d3 > 0.0 && d4 < 0.0) || (d3 < 0.0 && d4 > 0.0))
        {
            return 1;
        }

        -1
    }
}

impl EdgeCrosser for PlaneEdgeCrosser {
    type Point = [f64; 2];

    fn new(a: &[f64; 2], b: &[f64; 2]) -> Self {
        PlaneEdgeCrosser::new(a, b)
    }

    fn crossing_sign_two(&mut self, c: &[f64; 2], d: &[f64; 2]) -> i32 {
        self.crossing_sign_two(c, d)
    }

    /// Transport containment with A and B displaced by (e, e^2), for positive symbolic e.
    /// Only zero orientations change. Each endpoint then has the same boundary meaning
    /// on arrival and departure, matching point_in_polygon.
    fn edge_or_vertex_crossing_two(&mut self, c: &[f64; 2], d: &[f64; 2]) -> bool {
        if self.a == self.b || c == d {
            return false;
        }

        // Translating AB adds (B.y - A.y)*e - (B.x - A.x)*e^2 to each orientation.
        let ab_positive =
            self.b[1] > self.a[1] || (self.b[1] == self.a[1] && self.a[0] > self.b[0]);
        let hc = orient2d(&self.a, &self.b, c);
        let hd = orient2d(&self.a, &self.b, d);
        let c_positive = hc > 0.0 || (hc == 0.0 && ab_positive);
        let d_positive = hd > 0.0 || (hd == 0.0 && ab_positive);
        if c_positive == d_positive {
            return false;
        }

        // Translating a point relative to CD adds -(D.y - C.y)*e + (D.x - C.x)*e^2.
        let cd_positive = c[1] > d[1] || (c[1] == d[1] && d[0] > c[0]);
        let ha = orient2d(c, d, &self.a);
        let hb = orient2d(c, d, &self.b);
        let a_positive = ha > 0.0 || (ha == 0.0 && cd_positive);
        let b_positive = hb > 0.0 || (hb == 0.0 && cd_positive);
        a_positive != b_positive
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn count_crossings(tracker_a: &[f64; 2], tracker_b: &[f64; 2], ring: &[[f64; 2]]) -> usize {
        let mut crosser = PlaneEdgeCrosser::new(tracker_a, tracker_b);
        let mut count = 0;
        for i in 0..ring.len() - 1 {
            if crosser.edge_or_vertex_crossing_two(&ring[i], &ring[i + 1]) {
                count += 1;
            }
        }
        count
    }

    #[test]
    fn test_through_vertex_produces_even_crossings() {
        let ring = [[0.0, 0.0], [1.0, 0.0], [1.0, 1.0], [0.0, 1.0], [0.0, 0.0]];
        let a = [0.0, -1.0];
        let b = [2.0, 1.0];
        assert_eq!(count_crossings(&a, &b, &ring) % 2, 0);
    }

    #[test]
    fn test_through_vertex_parity() {
        let ring = [[0.0, 0.0], [1.0, 0.0], [1.0, 1.0], [0.0, 1.0], [0.0, 0.0]];

        // Through vertex (0,0).
        assert_eq!(count_crossings(&[-1.0, -1.0], &[2.0, 2.0], &ring) % 2, 0);
        // The displaced lower-left vertex starts inside.
        assert_eq!(count_crossings(&[0.0, 0.0], &[2.0, 2.0], &ring) % 2, 1);
        // Through vertex (1,0).
        assert_eq!(count_crossings(&[0.5, -1.0], &[1.5, 1.0], &ring) % 2, 0);
        // Through vertex (0,1).
        assert_eq!(count_crossings(&[-0.5, 0.0], &[0.5, 2.0], &ring) % 2, 0);
    }

    #[test]
    fn test_touch_from_above_preserves_parity() {
        // Tracker passes through a vertex where the polygon touches from above
        // but does not cross (V-shape dipping down to the line).
        // Triangle above the x-axis touching at (0.5, 0).
        let ring = [[0.0, 1.0], [0.5, 0.0], [1.0, 1.0], [0.0, 1.0]];
        let a = [-1.0, 0.0];
        let b = [2.0, 0.0];
        assert_eq!(count_crossings(&a, &b, &ring) % 2, 0);
    }

    #[test]
    fn test_touch_from_below_preserves_parity() {
        // Triangle below the x-axis touching at (0.5, 0).
        let ring = [[0.0, -1.0], [0.5, 0.0], [1.0, -1.0], [0.0, -1.0]];
        let a = [-1.0, 0.0];
        let b = [2.0, 0.0];
        assert_eq!(count_crossings(&a, &b, &ring) % 2, 0);
    }

    #[test]
    fn test_interior_crossing_still_works() {
        // No vertex on the line. Standard interior crossing.
        let ring = [[0.0, 0.0], [1.0, 0.0], [1.0, 1.0], [0.0, 1.0], [0.0, 0.0]];
        let a = [0.5, -1.0];
        let b = [0.5, 2.0];
        assert_eq!(count_crossings(&a, &b, &ring), 2);
    }

    #[test]
    fn test_no_crossing_when_disjoint() {
        let ring = [[0.0, 0.0], [1.0, 0.0], [1.0, 1.0], [0.0, 1.0], [0.0, 0.0]];
        let a = [2.0, 0.0];
        let b = [3.0, 1.0];
        assert_eq!(count_crossings(&a, &b, &ring), 0);
    }

    #[test]
    fn test_collinear_overlap_preserves_parity() {
        // Tracker segment lies on the same line as a polygon edge.
        let ring = [[0.0, 0.0], [2.0, 0.0], [2.0, 1.0], [0.0, 1.0], [0.0, 0.0]];
        let a = [-1.0, 0.0];
        let b = [3.0, 0.0];
        assert_eq!(count_crossings(&a, &b, &ring) % 2, 0);
    }
}

#[cfg(test)]
#[path = "../tests/plane_transport_tests.rs"]
mod transport_tests;

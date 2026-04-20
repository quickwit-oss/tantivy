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
        let d1 = orient2d(&self.a, &self.b, c);
        let d2 = orient2d(&self.a, &self.b, d);
        let d3 = orient2d(c, d, &self.a);
        let d4 = orient2d(c, d, &self.b);

        if ((d1 > 0.0 && d2 < 0.0) || (d1 < 0.0 && d2 > 0.0))
            && ((d3 > 0.0 && d4 < 0.0) || (d3 < 0.0 && d4 > 0.0))
        {
            return 1;
        }

        if (d1 == 0.0 && on_segment(&self.a, &self.b, c))
            || (d2 == 0.0 && on_segment(&self.a, &self.b, d))
            || (d3 == 0.0 && on_segment(c, d, &self.a))
            || (d4 == 0.0 && on_segment(c, d, &self.b))
        {
            return 0;
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
}

fn on_segment(a: &[f64; 2], b: &[f64; 2], p: &[f64; 2]) -> bool {
    p[0] >= a[0].min(b[0])
        && p[0] <= a[0].max(b[0])
        && p[1] >= a[1].min(b[1])
        && p[1] <= a[1].max(b[1])
}

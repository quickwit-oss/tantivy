//! Edge crossing trait for surface-generic code.

/// Stateful edge crosser. Holds a reference edge AB and tests candidate edges against it.
pub trait EdgeCrosser: Sized {
    /// The point type for this crosser.
    type Point;

    /// Create a crosser for the fixed edge AB.
    fn new(a: &Self::Point, b: &Self::Point) -> Self;

    /// Returns +1 if AB and CD cross in their interiors, 0 if they share a vertex, -1 otherwise.
    fn crossing_sign_two(&mut self, c: &Self::Point, d: &Self::Point) -> i32;

    /// Returns true if AB crosses CD in interior or at a vertex.
    fn edge_or_vertex_crossing_two(&mut self, c: &Self::Point, d: &Self::Point) -> bool {
        self.crossing_sign_two(c, d) >= 0
    }
}

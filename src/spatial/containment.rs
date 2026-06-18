//! Loop containment and intersection predicates.
use super::crossings::{ordered_ccw, ref_dir, S2EdgeCrosser};
use super::sphere::ORIGIN;

/// A version of contains that does not use the shape index.
pub fn brute_force_contains(p: &[f64; 3], vertices: &[[f64; 3]], origin_inside: bool) -> bool {
    // Handle degenerate cases
    let n = vertices.len();
    if n < 3 {
        return origin_inside;
    }

    let origin = ORIGIN;
    let mut crosser = S2EdgeCrosser::new(&origin, p);
    crosser.restart_at(&vertices[0]);

    let mut inside = origin_inside;

    // Test each edge of the loop
    for i in 1..=n {
        let vertex = &vertices[i % n];
        inside ^= crosser.edge_or_vertex_crossing(vertex);
    }

    inside
}

/// Computes whether the reference origin is inside a loop. Compares angle containment at vertex 1
/// with brute force result to determine the answer.
pub fn compute_origin_inside(vertices: &[[f64; 3]]) -> bool {
    let n = vertices.len();
    if n < 3 {
        return false;
    }

    // Check if vertex(1) is inside the angle formed by edges 0->1 and 1->2
    // This is true by CCW convention for non-degenerate loops
    let v1_inside = angle_contains_vertex(&vertices[0], &vertices[1], &vertices[2]);

    // Compute containment of v1 assuming origin_inside is false
    let contains_v1 = brute_force_contains(&vertices[1], vertices, false);

    // If the results disagree, Origin must be inside
    v1_inside != contains_v1
}

fn angle_contains_vertex(a: &[f64; 3], b: &[f64; 3], c: &[f64; 3]) -> bool {
    // B is inside angle ABC if C is to the left of ray AB
    // This is equivalent to Sign(A, B, C) > 0
    !ordered_ccw(&ref_dir(b), c, a, b)
}

/// Returns true if wedge A contains wedge B. Both wedges share the common vertex ab1.
pub fn wedge_contains(
    a0: &[f64; 3],
    ab1: &[f64; 3],
    a2: &[f64; 3],
    b0: &[f64; 3],
    b2: &[f64; 3],
) -> bool {
    // For A to contain B, the CCW edge order around ab1 must be a2 b2 b0 a0
    ordered_ccw(a2, b2, b0, ab1) && ordered_ccw(b0, a0, a2, ab1)
}

/// Returns true if wedge A intersects wedge B. Both wedges share the common vertex ab1.
pub fn wedge_intersects(
    a0: &[f64; 3],
    ab1: &[f64; 3],
    a2: &[f64; 3],
    b0: &[f64; 3],
    b2: &[f64; 3],
) -> bool {
    // A and B are disjoint if CCW order is a0 b2 b0 a2
    !(ordered_ccw(a0, b2, b0, ab1) && ordered_ccw(b0, a2, a0, ab1))
}

/// Returns true if loop A contains loop B.
pub fn loop_contains(
    a_vertices: &[[f64; 3]],
    a_origin_inside: bool,
    b_vertices: &[[f64; 3]],
    b_origin_inside: bool,
) -> bool {
    let n_a = a_vertices.len();
    let n_b = b_vertices.len();

    // Handle degenerate cases
    if n_a < 3 || n_b < 3 {
        // Empty loop contains nothing, full loop contains everything
        return a_origin_inside || !b_origin_inside;
    }

    let mut found_shared_vertex = false;

    // Check all edge pairs
    for i in 0..n_a {
        let a0 = &a_vertices[i];
        let a1 = &a_vertices[(i + 1) % n_a];

        let mut crosser = S2EdgeCrosser::new(a0, a1);

        for j in 0..n_b {
            let b0 = &b_vertices[j];
            let b1 = &b_vertices[(j + 1) % n_b];

            let crossing = crosser.crossing_sign_two(b0, b1);

            if crossing > 0 {
                // Interior crossing: A does not contain B
                return false;
            }

            if crossing == 0 {
                // Shared vertex: check wedge containment
                // Find which vertex is shared
                if a1 == b1 {
                    found_shared_vertex = true;
                    let a_prev = a0;
                    let a_next = &a_vertices[(i + 2) % n_a];
                    let b_prev = b0;
                    let b_next = &b_vertices[(j + 2) % n_b];

                    // A must contain B at this vertex
                    if !wedge_contains(a_prev, a1, a_next, b_prev, b_next) {
                        return false;
                    }
                }
            }
        }
    }

    // If we found shared vertices and all wedges passed, A contains B
    if found_shared_vertex {
        return true;
    }

    // No shared vertices: check point containment
    // A must contain a vertex of B
    if !brute_force_contains(&b_vertices[0], a_vertices, a_origin_inside) {
        return false;
    }

    // B must not contain a vertex of A (handles sphere-covering case)
    if brute_force_contains(&a_vertices[0], b_vertices, b_origin_inside) {
        return false;
    }

    true
}

/// Returns true if loop A intersects loop B.
pub fn loop_intersects(
    a_vertices: &[[f64; 3]],
    a_origin_inside: bool,
    b_vertices: &[[f64; 3]],
    b_origin_inside: bool,
) -> bool {
    let n_a = a_vertices.len();
    let n_b = b_vertices.len();

    // Handle degenerate cases
    if n_a < 3 || n_b < 3 {
        // Empty loop intersects nothing, full loop intersects everything non-empty
        return a_origin_inside && b_origin_inside;
    }

    let mut found_shared_vertex = false;

    // Check all edge pairs
    for i in 0..n_a {
        let a0 = &a_vertices[i];
        let a1 = &a_vertices[(i + 1) % n_a];

        let mut crosser = S2EdgeCrosser::new(a0, a1);

        for j in 0..n_b {
            let b0 = &b_vertices[j];
            let b1 = &b_vertices[(j + 1) % n_b];

            let crossing = crosser.crossing_sign_two(b0, b1);

            if crossing > 0 {
                // Interior crossing: loops intersect
                return true;
            }

            if crossing == 0 {
                // Shared vertex: check wedge intersection
                if a1 == b1 {
                    found_shared_vertex = true;
                    let a_prev = a0;
                    let a_next = &a_vertices[(i + 2) % n_a];
                    let b_prev = b0;
                    let b_next = &b_vertices[(j + 2) % n_b];

                    if wedge_intersects(a_prev, a1, a_next, b_prev, b_next) {
                        return true;
                    }
                }
            }
        }
    }

    // If we found shared vertices but no wedge intersection, loops don't intersect
    if found_shared_vertex {
        return false;
    }

    // No crossings or shared vertices: check containment
    // If A contains B or B contains A, they intersect
    if brute_force_contains(&b_vertices[0], a_vertices, a_origin_inside) {
        return true;
    }
    if brute_force_contains(&a_vertices[0], b_vertices, b_origin_inside) {
        return true;
    }

    false
}

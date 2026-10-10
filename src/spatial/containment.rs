//! Loop containment and intersection predicates.
use super::crossings::{ordered_ccw, ref_dir, robust_cross, sign, S2EdgeCrosser};
use super::exact::expensive_sign;
use super::math::{cross, dot, norm2};
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

    // Check if vertex(1) is inside the angle formed by edges 0->1 and 1->2. This is true by CCW
    // convention for non-degenerate loops. A repeated vertex makes the angle degenerate, and S2
    // takes it to contain nothing.
    let v1_inside = vertices[0] != vertices[1]
        && vertices[2] != vertices[1]
        && angle_contains_vertex(&vertices[0], &vertices[1], &vertices[2]);

    // Compute containment of v1 assuming origin_inside is false
    let contains_v1 = brute_force_contains(&vertices[1], vertices, false);

    // If the results disagree, Origin must be inside
    v1_inside != contains_v1
}

/// Removes the degeneracies of a ring: repeated vertices, and spikes, where an edge doubles back
/// along the one before it. Returns the loop without a closing vertex, or an empty loop when
/// nothing but degeneracies remain, and the spikes it removed, as (base, tip) edges. Port of
/// PruneDegeneracies from s2loop_measures.cc, which drops the spikes.
pub fn prune_degeneracies(ring: &[[f64; 3]]) -> (Vec<[f64; 3]>, Vec<[[f64; 3]; 2]>) {
    let mut vertices: Vec<[f64; 3]> = Vec::with_capacity(ring.len());
    let mut spikes = Vec::new();
    for v in ring {
        if vertices.last() == Some(v) {
            continue;
        }
        if vertices.len() >= 2 && vertices[vertices.len() - 2] == *v {
            // Remove the "B" of a spike "ABA".
            let tip = vertices.pop().unwrap();
            spikes.push([*v, tip]);
        } else {
            vertices.push(*v);
        }
    }
    if vertices.len() < 3 {
        return (Vec::new(), spikes);
    }
    if vertices[0] == vertices[vertices.len() - 1] {
        vertices.pop();
    }
    // Remove the spikes across the start of the loop: it begins with "BA" and ends with "A".
    let n = vertices.len();
    let mut k = 0;
    while 2 * k + 3 <= n && vertices[k + 1] == vertices[n - 1 - k] {
        spikes.push([vertices[k + 1], vertices[k]]);
        k += 1;
    }
    if n - 2 * k < 3 {
        return (Vec::new(), spikes);
    }
    vertices.truncate(n - k);
    vertices.drain(..k);
    (vertices, spikes)
}

/// Whether every vertex of a loop lies exactly on one great circle, so that the loop bounds no
/// area however many distinct vertices it has. Exact, as S2's predicates are.
pub fn on_one_great_circle(vertices: &[[f64; 3]]) -> bool {
    let (a, b) = (&vertices[0], &vertices[1]);
    vertices[2..]
        .iter()
        .all(|v| v == a || v == b || expensive_sign(a, b, v, false) == 0)
}

/// The geodesic curvature of a loop without degeneracies or a closing vertex: the sum of its
/// turn angles, which is 2π minus its area. It is positive when the loop turns left around a
/// region smaller than a hemisphere. Port of S2::GetCurvature.
pub fn curvature(vertices: &[[f64; 3]]) -> f64 {
    // Kahan summation, as S2's, so that the error stays within curvature_max_error.
    let n = vertices.len();
    let (mut sum, mut compensation) = (0.0f64, 0.0f64);
    for i in 0..n {
        let angle = turn_angle(
            &vertices[(i + n - 1) % n],
            &vertices[i],
            &vertices[(i + 1) % n],
        ) + compensation;
        let old_sum = sum;
        sum += angle;
        compensation = (old_sum - sum) + angle;
    }
    sum + compensation
}

/// The maximum error in the curvature of a loop of `n` vertices. Port of
/// S2::GetCurvatureMaxError.
pub fn curvature_max_error(n: usize) -> f64 {
    11.25 * f64::EPSILON * n as f64
}

/// The exterior angle at vertex B of the path ABC: positive for a left turn, negative for a right
/// one. Port of S2::TurnAngle.
fn turn_angle(a: &[f64; 3], b: &[f64; 3], c: &[f64; 3]) -> f64 {
    let x = robust_cross(a, b);
    let y = robust_cross(b, c);
    let angle = norm2(&cross(&x, &y)).sqrt().atan2(dot(&x, &y));
    if sign(a, b, c) > 0 {
        angle
    } else {
        -angle
    }
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

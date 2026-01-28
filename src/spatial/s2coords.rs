//! S2 coordinate system transformations.
//!
//! Provides conversions between the various S2 coordinate systems:
//! (face, i, j), (face, s, t), (face, u, v), and (x, y, z).
//!
//! s2coords.h, s2coords.cc and s2coords_internal.h.

/// Maximum cell level (30 levels of subdivision).
pub const MAX_CELL_LEVEL: i32 = 30;

/// Maximum index of valid leaf cell coordinates.
pub const LIMIT_IJ: i32 = 1 << MAX_CELL_LEVEL;

/// Maximum value of si/ti coordinates.
pub const MAX_SI_TI: u32 = 1 << (MAX_CELL_LEVEL + 1);

/// Number of cube faces.
pub const NUM_FACES: i32 = 6;

/// Convert an s- or t-value to the corresponding u- or v-value. This is a non-linear
/// transformation from [0,1] to [-1,1] that attempts to make the cell sizes more uniform.
#[inline]
pub fn st_to_uv(s: f64) -> f64 {
    if s >= 0.5 {
        (1.0 / 3.0) * (4.0 * s * s - 1.0)
    } else {
        (1.0 / 3.0) * (1.0 - 4.0 * (1.0 - s) * (1.0 - s))
    }
}

/// The inverse of the st_to_uv transformation.  Note that it is not always true that
/// uv_to_st(st_to_uv(x)) == x due to numerical errors.
#[inline]
pub fn uv_to_st(u: f64) -> f64 {
    if u >= 0.0 {
        0.5 * (1.0 + 3.0 * u).sqrt()
    } else {
        1.0 - 0.5 * (1.0 - 3.0 * u).sqrt()
    }
}

/// Convert the i- or j-index of a leaf cell to the minimum corresponding s- or t-value contained
/// by that cell.  The argument must be in the range [0..2**30], i.e. up to one position beyond the
/// normal range of valid leaf cell indices.
#[inline]
pub fn ij_to_st_min(i: i32) -> f64 {
    debug_assert!((0..=LIMIT_IJ).contains(&i));
    (1.0 / LIMIT_IJ as f64) * i as f64
}

/// Return the i- or j-index of the leaf cell containing the given s- or t-value.  If the argument
/// is outside the range spanned by valid leaf cell indices, return the index of the closest valid
/// leaf cell (i.e., return values are clamped to the range of valid leaf cell indices).
#[inline]
pub fn st_to_ij(s: f64) -> i32 {
    let ij = (LIMIT_IJ as f64 * s) as i32;
    ij.clamp(0, LIMIT_IJ - 1)
}

/// Convert si- or ti-coordinate to s- or t-value.
#[inline]
pub fn si_ti_to_st(si: u32) -> f64 {
    debug_assert!(si <= MAX_SI_TI);
    (1.0 / MAX_SI_TI as f64) * si as f64
}

/// Convert (face, u, v) coordinates to a direction vector (not necessarily unit length).
#[inline]
pub fn face_uv_to_xyz(face: i32, u: f64, v: f64) -> [f64; 3] {
    match face {
        0 => [1.0, u, v],
        1 => [-u, 1.0, v],
        2 => [-u, -v, 1.0],
        3 => [-1.0, -v, -u],
        4 => [v, -1.0, -u],
        _ => [v, u, -1.0],
    }
}

/// Return the face containing the given direction vector.  (For points on the boundary between
/// faces, the result is arbitrary but repeatable.)
#[inline]
pub fn get_face(p: &[f64; 3]) -> i32 {
    let ax = p[0].abs();
    let ay = p[1].abs();
    let az = p[2].abs();

    // Strict > mirrors LargestAbsComponent() in vector.h.
    let face = if ax > ay {
        if ax > az {
            0
        } else {
            2
        }
    } else if ay > az {
        1
    } else {
        2
    };

    if p[face as usize] < 0.0 {
        face + 3
    } else {
        face
    }
}

/// Convert a direction vector (not necessarily unit length) to (face, u, v) coordinates.
#[inline]
pub fn xyz_to_face_uv(p: &[f64; 3]) -> (i32, f64, f64) {
    let face = get_face(p);
    let (u, v) = valid_face_xyz_to_uv(face, p);
    (face, u, v)
}

/// Given a *valid* face for the given point p (meaning that dot product of p with the face normal
/// is positive), return the corresponding u and v values (which may lie outside the range [-1,1]).
#[inline]
pub fn valid_face_xyz_to_uv(face: i32, p: &[f64; 3]) -> (f64, f64) {
    match face {
        0 => (p[1] / p[0], p[2] / p[0]),
        1 => (-p[0] / p[1], p[2] / p[1]),
        2 => (-p[0] / p[2], -p[1] / p[2]),
        3 => (p[2] / p[0], p[1] / p[0]),
        4 => (p[2] / p[1], -p[0] / p[1]),
        _ => (-p[1] / p[2], -p[0] / p[2]),
    }
}

/// Convert (face, si, ti) to XYZ direction vector.
#[inline]
pub fn face_si_ti_to_xyz(face: i32, si: u32, ti: u32) -> [f64; 3] {
    let u = st_to_uv(si_ti_to_st(si));
    let v = st_to_uv(si_ti_to_st(ti));
    face_uv_to_xyz(face, u, v)
}

/// IJ-to-position mapping (inverse of pos_to_ij).
pub const IJ_TO_POS: [[i32; 4]; 4] = [[0, 1, 3, 2], [0, 3, 1, 2], [2, 3, 1, 0], [2, 1, 3, 0]];

/// Converts (i,j) child indices to traversal position.
#[inline]
pub fn ij_to_pos(orientation: i32, i: i32, j: i32) -> i32 {
    IJ_TO_POS[orientation as usize][((i << 1) + j) as usize]
}

/// Face axis definitions for the S2 cube projection.
const FACE_U_AXES: [[f64; 3]; 6] = [
    [0.0, 1.0, 0.0],
    [-1.0, 0.0, 0.0],
    [-1.0, 0.0, 0.0],
    [0.0, 0.0, -1.0],
    [0.0, 0.0, -1.0],
    [0.0, 1.0, 0.0],
];

const FACE_V_AXES: [[f64; 3]; 6] = [
    [0.0, 0.0, 1.0],
    [0.0, 0.0, 1.0],
    [0.0, -1.0, 0.0],
    [0.0, -1.0, 0.0],
    [1.0, 0.0, 0.0],
    [1.0, 0.0, 0.0],
];

/// Together with INVERT_MASK, defines a cell orientation. If true, the canonical traversal order
/// is flipped around the diagonal (i.e. i and j are swapped with each other).
pub const SWAP_MASK: i32 = 0x01;

/// Together with SWAP_MASK, defines a cell orientation. If true, the traversal order is rotated by
/// 180 degrees (i.e. the bits of i and j are inverted, or equivalently, the axis directions are
/// reversed).
pub const INVERT_MASK: i32 = 0x02;

/// Position-to-IJ mapping for each orientation.
pub const POS_TO_IJ: [[i32; 4]; 4] = [
    [0, 1, 3, 2], // canonical order
    [0, 2, 3, 1], // axes swapped
    [3, 2, 0, 1], // bits inverted
    [3, 1, 0, 2], // swapped & inverted
];

/// Orientation modifier for each child position.
pub const POS_TO_ORIENTATION: [i32; 4] = [SWAP_MASK, 0, 0, INVERT_MASK | SWAP_MASK];

/// Returns the (i,j) indices for child at position `pos` with given orientation.
#[inline]
pub fn pos_to_ij(orientation: i32, pos: i32) -> i32 {
    POS_TO_IJ[orientation as usize][pos as usize]
}

/// Returns the orientation delta for child at position `pos`.
#[inline]
pub fn pos_to_orientation(pos: i32) -> i32 {
    POS_TO_ORIENTATION[pos as usize]
}

/// Return the right-handed normal (not necessarily unit length) for an edge in the direction of
/// the positive v-axis at the given u-value on the given face.  (This vector is perpendicular to
/// the plane through the sphere origin that contains the given edge.)
#[inline]
pub fn get_u_norm(face: i32, u: f64) -> [f64; 3] {
    match face {
        0 => [u, -1.0, 0.0],
        1 => [1.0, u, 0.0],
        2 => [1.0, 0.0, u],
        3 => [-u, 0.0, 1.0],
        4 => [0.0, -u, 1.0],
        _ => [0.0, -1.0, -u],
    }
}

/// Return the right-handed normal (not necessarily unit length) for an edge in the direction of
/// the positive u-axis at the given v-value on the given face.
#[inline]
pub fn get_v_norm(face: i32, v: f64) -> [f64; 3] {
    match face {
        0 => [-v, 0.0, 1.0],
        1 => [0.0, -v, 1.0],
        2 => [0.0, -1.0, -v],
        3 => [v, -1.0, 0.0],
        4 => [1.0, v, 0.0],
        _ => [1.0, 0.0, v],
    }
}

/// If the dot product of p with the given face normal is positive, set the corresponding u and v
/// values (which may lie outside the range [-1,1]) and return true.  Otherwise return false.
#[inline]
pub fn face_xyz_to_uv(face: i32, p: &[f64; 3]) -> Option<[f64; 2]> {
    match face {
        0 => {
            if p[0] > 0.0 {
                Some([p[1] / p[0], p[2] / p[0]])
            } else {
                None
            }
        }
        1 => {
            if p[1] > 0.0 {
                Some([-p[0] / p[1], p[2] / p[1]])
            } else {
                None
            }
        }
        2 => {
            if p[2] > 0.0 {
                Some([-p[0] / p[2], -p[1] / p[2]])
            } else {
                None
            }
        }
        3 => {
            if p[0] < 0.0 {
                Some([p[2] / p[0], p[1] / p[0]])
            } else {
                None
            }
        }
        4 => {
            if p[1] < 0.0 {
                Some([p[2] / p[1], -p[0] / p[1]])
            } else {
                None
            }
        }
        _ => {
            if p[2] < 0.0 {
                Some([-p[1] / p[2], -p[0] / p[2]])
            } else {
                None
            }
        }
    }
}

/// Transform the given point P to the (u,v,w) coordinate frame of the given face (where the w-axis
/// represents the face normal).
#[inline]
pub fn face_xyz_to_uvw(face: i32, p: &[f64; 3]) -> [f64; 3] {
    match face {
        0 => [p[1], p[2], p[0]],
        1 => [-p[0], p[2], p[1]],
        2 => [-p[0], -p[1], p[2]],
        3 => [-p[2], -p[1], -p[0]],
        4 => [-p[2], p[0], -p[1]],
        _ => [p[1], p[0], -p[2]],
    }
}

/// Return the u-axis for the given face.
#[inline]
pub fn get_u_axis(face: i32) -> [f64; 3] {
    FACE_U_AXES[face as usize]
}

/// Return the v-axis for the given face.
#[inline]
pub fn get_v_axis(face: i32) -> [f64; 3] {
    FACE_V_AXES[face as usize]
}

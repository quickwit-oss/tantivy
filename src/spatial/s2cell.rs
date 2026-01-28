//! S2Cell: An S2Region object representing a cell.
//!
//! Unlike S2CellId, S2Cell supports efficient containment and intersection tests by caching the
//! cell's (u,v) bounds and providing vertex/edge access.
//!
//! S2Cell from s2cell.h and s2cell.cc.
use std::f64::consts::PI;

use super::math::{ldexp, neg, normalize};
use super::r1interval::R1Interval;
use super::r2rect::R2Rect;
use super::s2cell_id::S2CellId;
use super::s2coords::{
    self, face_uv_to_xyz, get_u_norm, get_v_norm, ij_to_st_min, st_to_uv, MAX_CELL_LEVEL,
};

/// An S2Cell represents a cell in the S2 cell decomposition.
#[derive(Clone, Copy, Debug)]
pub struct S2Cell {
    id: S2CellId,
    face: u8,
    level: u8,
    orientation: u8,
    uv: R2Rect,
}

impl S2Cell {
    /// Creates an S2Cell from an S2CellId.
    pub fn new(id: S2CellId) -> Self {
        let (face, i, j, orientation) = id.to_face_ij_orientation();
        let level = id.level();
        let uv = ij_level_to_bound_uv(i, j, level);
        Self {
            id,
            face: face as u8,
            level: level as u8,
            orientation: orientation as u8,
            uv,
        }
    }

    /// Creates an S2Cell for the given cube face (level 0).
    pub fn from_face(face: i32) -> Self {
        Self::new(S2CellId::from_face(face))
    }

    /// Creates an S2Cell containing the given point.
    pub fn from_point(p: &[f64; 3]) -> Self {
        Self::new(S2CellId::from_point(p))
    }

    /// Returns the cell ID.
    #[inline]
    pub fn id(&self) -> S2CellId {
        self.id
    }

    /// Returns the cube face (0-5).
    #[inline]
    pub fn face(&self) -> i32 {
        self.face as i32
    }

    /// Returns the subdivision level (0-30).
    #[inline]
    pub fn level(&self) -> i32 {
        self.level as i32
    }

    /// Returns the Hilbert curve orientation.
    #[inline]
    pub fn orientation(&self) -> i32 {
        self.orientation as i32
    }

    /// Returns true if this is a leaf cell.
    #[inline]
    pub fn is_leaf(&self) -> bool {
        self.level as i32 == MAX_CELL_LEVEL
    }

    /// Returns the bounds in (u,v)-space.
    #[inline]
    pub fn get_bound_uv(&self) -> R2Rect {
        self.uv
    }

    /// Returns the size of the cell in (i,j) coordinates.
    #[inline]
    pub fn get_size_ij(&self) -> i32 {
        S2CellId::size_ij_for_level(self.level as i32)
    }

    /// Returns the size of the cell in (s,t) coordinates.
    #[inline]
    pub fn get_size_st(&self) -> f64 {
        ij_to_st_min(self.get_size_ij())
    }

    /// Returns the k-th vertex (k = 0,1,2,3) in CCW order.
    /// Lower left, lower right, upper right, upper left in UV plane.
    #[inline]
    pub fn get_vertex(&self, k: usize) -> [f64; 3] {
        normalize(&self.get_vertex_raw(k))
    }

    /// Returns the k-th vertex without normalization.
    #[inline]
    pub fn get_vertex_raw(&self, k: usize) -> [f64; 3] {
        let v = self.uv.get_vertex(k % 4);
        face_uv_to_xyz(self.face as i32, v[0], v[1])
    }

    /// Returns the center of the cell.
    #[inline]
    pub fn get_center(&self) -> [f64; 3] {
        normalize(&self.get_center_raw())
    }

    /// Returns the center without normalization.
    #[inline]
    pub fn get_center_raw(&self) -> [f64; 3] {
        self.id.to_point_raw()
    }

    /// Returns the inward-facing normal of edge k (k = 0,1,2,3).
    /// Edge k connects vertex k to vertex k+1.
    #[inline]
    pub fn get_edge(&self, k: usize) -> [f64; 3] {
        normalize(&self.get_edge_raw(k))
    }

    /// Returns the edge normal without normalization.
    #[inline]
    pub fn get_edge_raw(&self, k: usize) -> [f64; 3] {
        match k & 3 {
            0 => get_v_norm(self.face as i32, self.uv[1].lo()),
            1 => get_u_norm(self.face as i32, self.uv[0].hi()),
            2 => neg(get_v_norm(self.face as i32, self.uv[1].hi())),
            _ => neg(get_u_norm(self.face as i32, self.uv[0].lo())),
        }
    }

    /// Returns the UV coordinate of edge k (the constant coord along that edge).
    pub fn get_uv_coord_of_edge(&self, k: usize) -> f64 {
        match k & 3 {
            0 => self.uv[1].lo(), // Bottom: v = min
            1 => self.uv[0].hi(), // Right: u = max
            2 => self.uv[1].hi(), // Top: v = max
            _ => self.uv[0].lo(), // Left: u = min
        }
    }

    /// Subdivides this cell into its four children. Returns None if this is a leaf cell.
    pub fn subdivide(&self) -> Option<[S2Cell; 4]> {
        if self.is_leaf() {
            return None;
        }

        // Compute the cell midpoint in uv-space.
        let uv_mid = self.id.get_center_uv();
        let child_ids = self.id.children();

        // Create children using the same optimization as S2.
        let mut children = [S2Cell::new(child_ids[0]); 4];

        for (pos, child_id) in child_ids.iter().enumerate() {
            let ij = s2coords::pos_to_ij(self.orientation as i32, pos as i32);
            let i = (ij >> 1) as usize;
            let j = (ij & 1) as usize;

            children[pos] = S2Cell {
                id: *child_id,
                face: self.face,
                level: self.level + 1,
                orientation: (self.orientation as i32 ^ s2coords::pos_to_orientation(pos as i32))
                    as u8,
                uv: R2Rect::new(
                    R1Interval::new(
                        if i == 0 { self.uv[0].lo() } else { uv_mid[0] },
                        if i == 0 { uv_mid[0] } else { self.uv[0].hi() },
                    ),
                    R1Interval::new(
                        if j == 0 { self.uv[1].lo() } else { uv_mid[1] },
                        if j == 0 { uv_mid[1] } else { self.uv[1].hi() },
                    ),
                ),
            };
        }

        Some(children)
    }

    /// Returns true if this cell contains the given point. Note that S2Cells are closed regions
    /// (include their boundary).
    pub fn contains_point(&self, p: &[f64; 3]) -> bool {
        // Project point onto this face and check UV containment.
        if let Some(uv) = s2coords::face_xyz_to_uv(self.face as i32, p) {
            // Expand slightly to account for numerical error.
            self.uv.expanded(f64::EPSILON).contains_point(uv)
        } else {
            false
        }
    }

    /// Returns true if this cell contains the given cell.
    pub fn contains_cell(&self, other: &S2Cell) -> bool {
        self.id.contains(other.id)
    }

    /// Returns true if this cell may intersect the given cell.
    pub fn may_intersect(&self, other: &S2Cell) -> bool {
        self.id.intersects(other.id)
    }

    /// Returns the average area of cells at the given level.
    #[inline]
    pub fn average_area(level: i32) -> f64 {
        const AVG_AREA_BASE: f64 = 4.0 * PI / 6.0; // ~2.094
        ldexp(AVG_AREA_BASE, -2 * level)
    }
}

/// Computes the (u,v) bounding rectangle for a cell at level `level` whose (i,j) coordinates are
/// `i` and `j`.
fn ij_level_to_bound_uv(i: i32, j: i32, level: i32) -> R2Rect {
    let cell_size = S2CellId::size_ij_for_level(level);

    // Round down i and j to the nearest multiple of cell_size.
    let i_lo = i & -cell_size;
    let j_lo = j & -cell_size;
    let i_hi = i_lo + cell_size;
    let j_hi = j_lo + cell_size;

    R2Rect::new(
        R1Interval::new(st_to_uv(ij_to_st_min(i_lo)), st_to_uv(ij_to_st_min(i_hi))),
        R1Interval::new(st_to_uv(ij_to_st_min(j_lo)), st_to_uv(ij_to_st_min(j_hi))),
    )
}

impl PartialEq for S2Cell {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for S2Cell {}

impl PartialOrd for S2Cell {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for S2Cell {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.id.cmp(&other.id)
    }
}

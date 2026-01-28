//! S2PaddedCell represents an S2Cell whose (u,v)-range has been expanded on all sides by a given
//! amount of "padding". Unlike S2Cell, its methods and representation are optimized for clipping
//! edges against S2Cell boundaries to determine which cells are intersected by a given set of
//! edges.
use super::math::normalize;
use super::r1interval::R1Interval;
use super::r2rect::R2Rect;
use super::s2cell_id::S2CellId;
use super::s2coords::{
    self, face_si_ti_to_xyz, pos_to_ij, pos_to_orientation, si_ti_to_st, st_to_uv, INVERT_MASK,
    MAX_CELL_LEVEL, SWAP_MASK,
};

/// An S2Cell whose (u,v)-range has been expanded on all sides by a given amount of "padding".
///
/// Unlike S2Cell, its methods and representation are optimized for clipping edges against S2Cell
/// boundaries to determine which cells are intersected by a given set of edges.
#[derive(Clone, Debug)]
pub struct S2PaddedCell {
    id: S2CellId,
    padding: f64,
    bound: R2Rect,
    middle: R2Rect,
    ij_lo: [i32; 2],
    orientation: i32,
    level: i32,
}

fn middle(ij_lo: [i32; 2], level: i32, padding: f64) -> R2Rect {
    let ij_size = S2CellId::size_ij_for_level(level);
    let u = st_to_uv(si_ti_to_st((2 * ij_lo[0] + ij_size) as u32));
    let v = st_to_uv(si_ti_to_st((2 * ij_lo[1] + ij_size) as u32));
    R2Rect::new(
        R1Interval::new(u - padding, u + padding),
        R1Interval::new(v - padding, v + padding),
    )
}

impl S2PaddedCell {
    /// Construct an S2PaddedCell for the given cell id and padding.
    pub fn new(id: S2CellId, padding: f64) -> Self {
        let (bound, ij_lo, orientation, level) = if id.is_face() {
            let limit = 1.0 + padding;
            let bound = R2Rect::new(
                R1Interval::new(-limit, limit),
                R1Interval::new(-limit, limit),
            );
            (bound, [0, 0], id.face() & 1, 0)
        } else {
            let (_, i, j, orientation) = id.to_face_ij_orientation();
            let level = id.level();
            let ij_size = S2CellId::size_ij_for_level(level);
            let ij_lo = [i & -ij_size, j & -ij_size];
            let uv_bound = ij_level_to_bound_uv(i, j, level);
            let bound = uv_bound.expanded(padding);
            (bound, ij_lo, orientation, level)
        };
        Self {
            id,
            padding,
            bound,
            middle: middle(ij_lo, level, padding),
            ij_lo,
            orientation,
            level,
        }
    }

    /// Construct the child of "parent" with the given (i,j) index.
    ///
    /// The four child cells have indices of (0,0), (0,1), (1,0), (1,1), where the i and j indices
    /// correspond to increasing u- and v-values respectively.
    pub fn from_parent(parent: &S2PaddedCell, i: usize, j: usize) -> Self {
        let pos = s2coords::ij_to_pos(parent.orientation, i as i32, j as i32);
        let id = parent.id.child(pos);
        let level = parent.level + 1;
        let ij_size = S2CellId::size_ij_for_level(level);

        let ij_lo = [
            parent.ij_lo[0] + i as i32 * ij_size,
            parent.ij_lo[1] + j as i32 * ij_size,
        ];

        let orientation = parent.orientation ^ pos_to_orientation(pos);

        // Compute child bound from parent bound and middle.
        let parent_middle = parent.middle();
        let bound = R2Rect::new(
            R1Interval::new(
                if i == 0 {
                    parent.bound[0].lo()
                } else {
                    parent_middle[0].lo()
                },
                if i == 0 {
                    parent_middle[0].hi()
                } else {
                    parent.bound[0].hi()
                },
            ),
            R1Interval::new(
                if j == 0 {
                    parent.bound[1].lo()
                } else {
                    parent_middle[1].lo()
                },
                if j == 0 {
                    parent_middle[1].hi()
                } else {
                    parent.bound[1].hi()
                },
            ),
        );
        Self {
            id,
            padding: parent.padding,
            bound,
            middle: middle(ij_lo, level, parent.padding),
            ij_lo,
            orientation,
            level,
        }
    }

    /// Returns the cell ID.
    #[inline]
    pub fn id(&self) -> S2CellId {
        self.id
    }

    /// Returns the level.
    #[inline]
    pub fn level(&self) -> i32 {
        self.level
    }

    /// Returns the "middle" rectangle that belongs to all four children.
    pub fn middle(&self) -> R2Rect {
        self.middle
    }

    /// Returns the (i,j) coordinates for the child at the given traversal position.
    #[inline]
    pub fn get_child_ij(&self, pos: i32) -> (usize, usize) {
        let ij = pos_to_ij(self.orientation, pos);
        ((ij >> 1) as usize, (ij & 1) as usize)
    }

    /// Returns the center of the cell.
    pub fn get_center(&self) -> [f64; 3] {
        let ij_size = S2CellId::size_ij_for_level(self.level);
        let si = (2 * self.ij_lo[0] + ij_size) as u32;
        let ti = (2 * self.ij_lo[1] + ij_size) as u32;
        normalize(&face_si_ti_to_xyz(self.id.face(), si, ti))
    }

    /// Returns the vertex where the Hilbert curve enters this cell.
    pub fn get_entry_vertex(&self) -> [f64; 3] {
        let mut i = self.ij_lo[0] as u32;
        let mut j = self.ij_lo[1] as u32;

        if self.orientation & INVERT_MASK != 0 {
            let ij_size = S2CellId::size_ij_for_level(self.level) as u32;
            i += ij_size;
            j += ij_size;
        }

        normalize(&face_si_ti_to_xyz(self.id.face(), 2 * i, 2 * j))
    }

    /// Returns the vertex where the Hilbert curve exits this cell.
    pub fn get_exit_vertex(&self) -> [f64; 3] {
        let mut i = self.ij_lo[0] as u32;
        let mut j = self.ij_lo[1] as u32;
        let ij_size = S2CellId::size_ij_for_level(self.level) as u32;

        if self.orientation == 0 || self.orientation == SWAP_MASK | INVERT_MASK {
            i += ij_size;
        } else {
            j += ij_size;
        }

        normalize(&face_si_ti_to_xyz(self.id.face(), 2 * i, 2 * j))
    }

    /// Returns the smallest cell that contains all descendants whose bounds
    /// intersect the given rectangle.
    ///
    /// REQUIRES: self.bound.intersects(rect)
    pub fn shrink_to_fit(&self, rect: &R2Rect) -> S2CellId {
        debug_assert!(self.bound.intersects(rect));

        // Quick rejection: if rect contains the center, no shrinking is possible.
        let ij_size = S2CellId::size_ij_for_level(self.level);
        if self.level == 0 {
            if rect[0].contains_point(0.0) || rect[1].contains_point(0.0) {
                return self.id;
            }
        } else {
            let u = st_to_uv(si_ti_to_st((2 * self.ij_lo[0] + ij_size) as u32));
            let v = st_to_uv(si_ti_to_st((2 * self.ij_lo[1] + ij_size) as u32));
            if rect[0].contains_point(u) || rect[1].contains_point(v) {
                return self.id;
            }
        }

        // Expand rect by padding to find the range of i,j coordinates.
        let padded = rect.expanded(self.padding + 1.5 * f64::EPSILON);

        let mut ij_min = [0i32; 2];
        let mut ij_xor = [0i32; 2];

        for d in 0..2 {
            ij_min[d] = self.ij_lo[d].max(s2coords::st_to_ij(s2coords::uv_to_st(padded[d].lo())));
            let ij_max = (self.ij_lo[d] + ij_size - 1)
                .min(s2coords::st_to_ij(s2coords::uv_to_st(padded[d].hi())));
            ij_xor[d] = ij_min[d] ^ ij_max;
        }

        // Find the highest differing bit position.
        let level_msb = ((ij_xor[0] | ij_xor[1]) << 1) + 1;
        let level = MAX_CELL_LEVEL - (31 - level_msb.leading_zeros() as i32);

        if level <= self.level {
            return self.id;
        }

        S2CellId::from_face_ij(self.id.face(), ij_min[0], ij_min[1]).parent(level)
    }
}

/// Helper to compute (u,v) bounds from (i,j) coordinates and level.
fn ij_level_to_bound_uv(i: i32, j: i32, level: i32) -> R2Rect {
    let cell_size = S2CellId::size_ij_for_level(level);
    let i_lo = i & -cell_size;
    let j_lo = j & -cell_size;
    let i_hi = i_lo + cell_size;
    let j_hi = j_lo + cell_size;

    R2Rect::new(
        R1Interval::new(
            st_to_uv(s2coords::ij_to_st_min(i_lo)),
            st_to_uv(s2coords::ij_to_st_min(i_hi)),
        ),
        R1Interval::new(
            st_to_uv(s2coords::ij_to_st_min(j_lo)),
            st_to_uv(s2coords::ij_to_st_min(j_hi)),
        ),
    )
}

//! An S2CellId is a 64-bit unsigned integer that uniquely identifies a cell in the S2 cell
//! decomposition.  It has the following format:
//!
//! ```text
//!  id = [face][face_pos]
//!
//!  face:     a 3-bit number (range 0..5) encoding the cube face.
//!
//!  face_pos: a 61-bit number encoding the position of the center of this
//!            cell along the Hilbert curve over this face (see the Wiki
//!            pages for details).
//! ```
//!
//! Sequentially increasing cell ids follow a continuous space-filling curve over the entire
//! sphere.  They have the following properties:
//!
//! - The id of a cell at level k consists of a 3-bit face number followed by k bit pairs that
//!   recursively select one of the four children of each cell.  The next bit is always 1, and all
//!   other bits are 0. Therefore, the level of a cell is determined by the position of its
//!   lowest-numbered bit that is turned on (for a cell at level k, this position is 2 * (kMaxLevel
//!   - k).)
//!
//! - The id of a parent cell is at the midpoint of the range of ids spanned by its children (or by
//!   its descendants at any level).
//!
//! Leaf cells are often used to represent points on the unit sphere, and this class provides
//! methods for converting directly between these two representations.  For cells that represent 2D
//! regions rather than discrete point, it is better to use the S2Cell class.
//!
//! All methods require `is_valid()` to be true unless otherwise specified (although not all
//! methods enforce this).
//!
//! This class is intended to be copied by value as desired.  It uses the default copy constructor
//! and assignment operator.
//!
//! s2cell_id.h and s2cell_id.cc
use super::math::normalize;
use super::s2coords::{
    face_si_ti_to_xyz, face_uv_to_xyz, si_ti_to_st, st_to_ij, st_to_uv, uv_to_st, xyz_to_face_uv,
    MAX_CELL_LEVEL, NUM_FACES, POS_TO_IJ, POS_TO_ORIENTATION,
};

/// Number of bits used to encode the face.
const FACE_BITS: i32 = 3;

/// Number of bits used for the position along the Hilbert curve.
const POS_BITS: i32 = 2 * MAX_CELL_LEVEL + 1; // 61

/// Maximum cell size (number of leaf cells along each axis of a face).
const MAX_SIZE: i32 = 1 << MAX_CELL_LEVEL;

/// Hilbert curve orientation flags.
const SWAP_MASK: i32 = 0x01;
const INVERT_MASK: i32 = 0x02;

/// Number of bits to process at once in lookup tables.
const LOOKUP_BITS: usize = 4;

/// Lookup tables for Hilbert curve encoding/decoding.
/// Generated at compile time.
struct LookupTables {
    /// (ij, orientation) -> (pos, new_orientation)
    pos: [u16; 1 << (2 * LOOKUP_BITS + 2)],
    /// (pos, orientation) -> (ij, new_orientation)
    ij: [u16; 1 << (2 * LOOKUP_BITS + 2)],
}

/// Generate lookup tables at compile time.
const fn make_lookup_tables() -> LookupTables {
    let mut tables = LookupTables {
        pos: [0; 1 << (2 * LOOKUP_BITS + 2)],
        ij: [0; 1 << (2 * LOOKUP_BITS + 2)],
    };

    // Initialize for all 4 orientations
    tables = init_lookup_cell(0, 0, 0, 0, 0, 0, tables);
    tables = init_lookup_cell(0, 0, 0, SWAP_MASK, 0, SWAP_MASK, tables);
    tables = init_lookup_cell(0, 0, 0, INVERT_MASK, 0, INVERT_MASK, tables);
    tables = init_lookup_cell(
        0,
        0,
        0,
        SWAP_MASK | INVERT_MASK,
        0,
        SWAP_MASK | INVERT_MASK,
        tables,
    );

    tables
}

const fn init_lookup_cell(
    level: usize,
    i: usize,
    j: usize,
    orig_orientation: i32,
    pos: usize,
    orientation: i32,
    mut tables: LookupTables,
) -> LookupTables {
    if level == LOOKUP_BITS {
        let ij = (i << LOOKUP_BITS) + j;
        tables.pos[(ij << 2) + orig_orientation as usize] =
            ((pos << 2) + orientation as usize) as u16;
        tables.ij[(pos << 2) + orig_orientation as usize] =
            ((ij << 2) + orientation as usize) as u16;
    } else {
        let next_level = level + 1;
        let i2 = i << 1;
        let j2 = j << 1;
        let pos2 = pos << 2;

        // Process all 4 child positions
        let r = POS_TO_IJ[orientation as usize];

        tables = init_lookup_cell(
            next_level,
            i2 + ((r[0] >> 1) as usize),
            j2 + ((r[0] & 1) as usize),
            orig_orientation,
            pos2,
            orientation ^ POS_TO_ORIENTATION[0],
            tables,
        );
        tables = init_lookup_cell(
            next_level,
            i2 + ((r[1] >> 1) as usize),
            j2 + ((r[1] & 1) as usize),
            orig_orientation,
            pos2 + 1,
            orientation ^ POS_TO_ORIENTATION[1],
            tables,
        );
        tables = init_lookup_cell(
            next_level,
            i2 + ((r[2] >> 1) as usize),
            j2 + ((r[2] & 1) as usize),
            orig_orientation,
            pos2 + 2,
            orientation ^ POS_TO_ORIENTATION[2],
            tables,
        );
        tables = init_lookup_cell(
            next_level,
            i2 + ((r[3] >> 1) as usize),
            j2 + ((r[3] & 1) as usize),
            orig_orientation,
            pos2 + 3,
            orientation ^ POS_TO_ORIENTATION[3],
            tables,
        );
    }
    tables
}

/// The precomputed Hilbert curve lookup tables.
static LOOKUP: LookupTables = make_lookup_tables();

/// A 64-bit cell identifier.
///
/// S2CellId uniquely identifies a cell in the S2 cell decomposition. The cell ID encodes the face
/// (0-5), position along the Hilbert curve, and subdivision level (0-30).
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct S2CellId(pub u64);

impl S2CellId {
    /// The maximum level (30) for S2CellIds.
    pub const MAX_LEVEL: i32 = MAX_CELL_LEVEL;

    /// Creates an S2CellId from a raw 64-bit value.
    #[inline]
    pub const fn new(id: u64) -> Self {
        Self(id)
    }

    /// Returns the raw 64-bit value.
    #[inline]
    pub const fn id(&self) -> u64 {
        self.0
    }

    /// Returns an invalid cell ID.
    #[inline]
    pub const fn none() -> Self {
        Self(0)
    }

    /// Returns a sentinel cell ID larger than all valid cell IDs.
    #[inline]
    pub const fn sentinel() -> Self {
        Self(!0u64)
    }

    /// Creates a cell ID for the given face (level 0).
    #[inline]
    pub fn from_face(face: i32) -> Self {
        assert!(face < NUM_FACES);
        Self(((face as u64) << POS_BITS as u32) + Self::lsb_for_level(0))
    }

    /// Creates a cell ID from face, position, and level.
    #[inline]
    pub fn from_face_pos_level(face: i32, pos: u64, level: i32) -> Self {
        assert!(face < NUM_FACES);
        assert!(level <= MAX_CELL_LEVEL);
        let cell = Self(((face as u64) << POS_BITS as u32) + (pos | 1));
        cell.parent(level)
    }

    /// Creates a leaf cell from (face, i, j) coordinates.
    pub fn from_face_ij(face: i32, i: i32, j: i32) -> Self {
        assert!(face < NUM_FACES);
        assert!(i >= 0 && i < MAX_SIZE);
        assert!(j >= 0 && j < MAX_SIZE);

        let mut n = (face as u64) << (POS_BITS as u32 - 1);
        let mut bits = (face & SWAP_MASK) as u64;

        // Process 4 bits at a time using lookup tables
        for k in (0..8).rev() {
            let mask = (1 << LOOKUP_BITS) - 1;
            bits += (((i >> (k * LOOKUP_BITS)) & mask) as u64) << (LOOKUP_BITS + 2);
            bits += (((j >> (k * LOOKUP_BITS)) & mask) as u64) << 2;
            bits = LOOKUP.pos[bits as usize] as u64;
            n |= (bits >> 2) << (k * 2 * LOOKUP_BITS);
            bits &= (SWAP_MASK | INVERT_MASK) as u64;
        }

        Self(n * 2 + 1)
    }

    /// Creates a leaf cell containing the given point.
    pub fn from_point(p: &[f64; 3]) -> Self {
        let (face, u, v) = xyz_to_face_uv(p);
        let i = st_to_ij(uv_to_st(u));
        let j = st_to_ij(uv_to_st(v));
        Self::from_face_ij(face, i, j)
    }

    /// Returns true if this is a valid cell ID.
    #[inline]
    pub fn is_valid(&self) -> bool {
        self.face() < NUM_FACES && (self.lsb() & 0x1555555555555555u64) != 0
    }

    /// Returns the face (0-5) this cell belongs to.
    #[inline]
    pub fn face(&self) -> i32 {
        (self.0 >> POS_BITS as u32) as i32
    }

    /// Returns the position along the Hilbert curve.
    #[inline]
    pub fn pos(&self) -> u64 {
        self.0 & ((!0u64) >> FACE_BITS as u32)
    }

    /// Returns the subdivision level (0-30).
    #[inline]
    pub fn level(&self) -> i32 {
        assert!(self.0 != 0);
        MAX_CELL_LEVEL - (self.0.trailing_zeros() as i32 >> 1)
    }

    /// Returns the lowest set bit (determines level).
    #[inline]
    pub fn lsb(&self) -> u64 {
        self.0 & (!self.0).wrapping_add(1)
    }

    /// Returns the LSB for cells at the given level.
    #[inline]
    pub const fn lsb_for_level(level: i32) -> u64 {
        1u64 << (2 * (MAX_CELL_LEVEL - level) as u32)
    }

    /// Returns true if this is a leaf cell (level 30).
    #[inline]
    pub fn is_leaf(&self) -> bool {
        (self.0 & 1) != 0
    }

    /// Returns true if this is a face cell (level 0).
    #[inline]
    pub fn is_face(&self) -> bool {
        (self.0 & (Self::lsb_for_level(0) - 1)) == 0
    }

    /// Returns the size of this cell in (i,j) units.
    #[inline]
    pub fn size_ij(&self) -> i32 {
        Self::size_ij_for_level(self.level())
    }

    /// Returns the size of cells at the given level.
    #[inline]
    pub const fn size_ij_for_level(level: i32) -> i32 {
        1 << (MAX_CELL_LEVEL - level)
    }

    /// Returns the child position (0-3) within the parent.
    #[inline]
    pub fn child_position(&self) -> u8 {
        self.child_position_at_level(self.level())
    }

    /// Returns the child position at the given level.
    #[inline]
    pub fn child_position_at_level(&self, level: i32) -> u8 {
        assert!(level >= 1 && level <= self.level());
        ((self.0 >> (2 * (MAX_CELL_LEVEL - level) as u32 + 1)) & 3) as u8
    }

    /// Returns the minimum cell ID contained by this cell.
    #[inline]
    pub fn range_min(&self) -> Self {
        Self(self.0 - (self.lsb() - 1))
    }

    /// Returns the maximum cell ID contained by this cell.
    #[inline]
    pub fn range_max(&self) -> Self {
        Self(self.0 + (self.lsb() - 1))
    }

    /// Returns true if this cell contains the other cell.
    #[inline]
    pub fn contains(&self, other: Self) -> bool {
        assert!(self.is_valid());
        assert!(other.is_valid());
        other >= self.range_min() && other <= self.range_max()
    }

    /// Returns true if this cell intersects the other cell.
    #[inline]
    pub fn intersects(&self, other: Self) -> bool {
        assert!(self.is_valid());
        assert!(other.is_valid());
        other.range_min() <= self.range_max() && other.range_max() >= self.range_min()
    }

    /// Returns the parent cell at the given level.
    #[inline]
    pub fn parent(&self, level: i32) -> Self {
        assert!(self.is_valid());
        assert!(level <= self.level());
        let new_lsb = Self::lsb_for_level(level);
        Self((self.0 & (!new_lsb + 1)) | new_lsb)
    }

    /// Returns the immediate parent cell.
    #[inline]
    pub fn immediate_parent(&self) -> Self {
        assert!(self.is_valid());
        assert!(!self.is_face());
        let new_lsb = self.lsb() << 2;
        Self((self.0 & (!new_lsb + 1)) | new_lsb)
    }

    /// Returns the child at the given position (0-3).
    #[inline]
    pub fn child(&self, position: i32) -> Self {
        assert!(self.is_valid());
        assert!(!self.is_leaf());
        assert!(position < 4);
        let new_lsb = self.lsb() >> 2;
        let delta = (2 * position as i64 - 3) * new_lsb as i64;
        Self((self.0 as i64 + delta) as u64)
    }

    /// Returns the four children of this cell.
    #[inline]
    pub fn children(&self) -> [Self; 4] {
        assert!(self.is_valid());
        assert!(!self.is_leaf());
        [self.child(0), self.child(1), self.child(2), self.child(3)]
    }

    /// Returns the next cell at the same level along the Hilbert curve.
    #[inline]
    pub fn next(&self) -> Self {
        Self(self.0 + (self.lsb() << 1))
    }

    /// Returns the previous cell at the same level along the Hilbert curve.
    #[inline]
    pub fn prev(&self) -> Self {
        Self(self.0 - (self.lsb() << 1))
    }

    /// Returns the first cell at this level.
    pub fn begin(level: i32) -> Self {
        Self::from_face(0).child_begin_at_level(level)
    }

    /// Returns one past the last cell at this level.
    pub fn end(level: i32) -> Self {
        Self::from_face(5).child_end_at_level(level)
    }

    /// Returns the first child of this cell (at level + 1).
    pub fn child_begin(&self) -> Self {
        assert!(self.is_valid());
        assert!(!self.is_leaf());
        let old_lsb = self.lsb();
        Self(self.0 - old_lsb + (old_lsb >> 2))
    }

    /// Returns one past the last child of this cell (at level + 1).
    pub fn child_end(&self) -> Self {
        assert!(self.is_valid());
        assert!(!self.is_leaf());
        let old_lsb = self.lsb();
        Self(self.0 + old_lsb + (old_lsb >> 2))
    }

    /// Returns the first descendant at the given level.
    pub fn child_begin_at_level(&self, level: i32) -> Self {
        assert!(self.is_valid());
        assert!(level >= self.level());
        assert!(level <= MAX_CELL_LEVEL);
        Self(self.0 - self.lsb() + Self::lsb_for_level(level))
    }

    /// Returns one past the last descendant at the given level.
    pub fn child_end_at_level(&self, level: i32) -> Self {
        assert!(self.is_valid());
        assert!(level >= self.level());
        assert!(level <= MAX_CELL_LEVEL);
        Self(self.0 + self.lsb() + Self::lsb_for_level(level))
    }

    /// Converts to (face, i, j) coordinates with optional orientation.
    pub fn to_face_ij_orientation(&self) -> (i32, i32, i32, i32) {
        let face = self.face();
        let mut bits = (face & SWAP_MASK) as u64;
        let mut i = 0i32;
        let mut j = 0i32;

        for k in (0..8).rev() {
            let nbits = if k == 7 {
                MAX_CELL_LEVEL as usize - 7 * LOOKUP_BITS as usize
            } else {
                LOOKUP_BITS as usize
            };
            bits += ((self.0 >> (k * 2 * LOOKUP_BITS + 1)) & ((1 << (2 * nbits)) - 1)) << 2;
            bits = LOOKUP.ij[bits as usize] as u64;
            i += ((bits >> (LOOKUP_BITS + 2)) as i32) << (k * LOOKUP_BITS);
            j += (((bits >> 2) & ((1 << LOOKUP_BITS) - 1)) as i32) << (k * LOOKUP_BITS);
            bits &= (SWAP_MASK | INVERT_MASK) as u64;
        }

        // Compute orientation based on LSB pattern
        let orientation = if self.lsb() & 0x1111111111111110u64 != 0 {
            (bits ^ SWAP_MASK as u64) as i32
        } else {
            bits as i32
        };

        (face, i, j, orientation)
    }

    /// Returns the (face, si, ti) coordinates of the cell center.
    pub fn get_center_si_ti(&self) -> (i32, u32, u32) {
        let (face, i, j, _) = self.to_face_ij_orientation();

        let delta = if self.is_leaf() {
            1
        } else if (i ^ ((self.0 >> 2) as i32)) & 1 != 0 {
            2
        } else {
            0
        };

        let si = (2 * i + delta) as u32;
        let ti = (2 * j + delta) as u32;

        (face, si, ti)
    }

    /// Returns the center of this cell as a point.
    pub fn to_point(&self) -> [f64; 3] {
        normalize(&self.to_point_raw())
    }

    /// Returns the center of this cell as an unnormalized point.
    pub fn to_point_raw(&self) -> [f64; 3] {
        let (face, si, ti) = self.get_center_si_ti();
        face_si_ti_to_xyz(face, si, ti)
    }

    /// Returns a debug string representation "f/dd...d".
    pub fn to_debug_string(&self) -> String {
        if !self.is_valid() {
            return format!("Invalid: {:016x}", self.0);
        }
        let mut out = format!("{}/", self.face());
        for level in 1..=self.level() {
            out.push(char::from_digit(self.child_position_at_level(level) as u32, 10).unwrap());
        }
        out
    }

    /// Returns a hex token representation.
    pub fn to_token(&self) -> String {
        if self.0 == 0 {
            return "X".to_string();
        }
        let num_zero_digits = self.0.trailing_zeros() / 4;
        let id_shifted = self.0 >> (4 * num_zero_digits);
        let width = 16 - num_zero_digits as usize;
        format!("{:0>width$x}", id_shifted, width = width)
    }

    /// Creates a cell ID from a hex token.
    pub fn from_token(token: &str) -> Self {
        if token.len() > 16 {
            return Self::none();
        }
        if token == "X" {
            return Self(0);
        }

        let mut id = 0u64;
        for (i, c) in token.chars().enumerate() {
            let d = match c {
                '0'..='9' => c as u64 - '0' as u64,
                'a'..='f' => c as u64 - 'a' as u64 + 10,
                'A'..='F' => c as u64 - 'A' as u64 + 10,
                _ => return Self::none(),
            };
            id |= d << (60 - i * 4);
        }
        Self(id)
    }
    /// Returns the center of this cell in (u,v) coordinates.
    pub fn get_center_uv(&self) -> [f64; 2] {
        let (_, si, ti) = self.get_center_si_ti();
        [st_to_uv(si_ti_to_st(si)), st_to_uv(si_ti_to_st(ti))]
    }
    /// Appends the cells at `level` that share the closest vertex to this cell.
    ///
    /// Normally returns 4 cells, but returns only 3 if the vertex is one of the 8 cube vertices
    /// (where only 3 faces meet).
    ///
    /// REQUIRES: level < self.level() (so we can determine which vertex is closest)
    pub fn append_vertex_neighbors(&self, level: i32, output: &mut Vec<S2CellId>) {
        // "level" must be strictly less than this cell's level so that we can
        // determine which vertex this cell is closest to.
        assert!(level < self.level());

        let (face, i, j, _) = self.to_face_ij_orientation();

        // Determine the i- and j-offsets to the closest neighboring cell in each direction. This
        // involves looking at the next bit of "i" and "j" to determine which quadrant of
        // self.parent(level) this cell lies in.
        let halfsize = Self::size_ij_for_level(level + 1);
        let size = halfsize << 1;

        let (ioffset, isame) = if (i & halfsize) != 0 {
            // Cell is in the right half, so nearest vertex is to the right
            (size, (i + size) < MAX_SIZE)
        } else {
            // Cell is in the left half, so nearest vertex is to the left
            (-size, (i - size) >= 0)
        };

        let (joffset, jsame) = if (j & halfsize) != 0 {
            (size, (j + size) < MAX_SIZE)
        } else {
            (-size, (j - size) >= 0)
        };

        // Add the parent cell (always present)
        output.push(self.parent(level));

        // Add the neighbor in the i-direction
        output.push(Self::from_face_ij_same(face, i + ioffset, j, isame).parent(level));

        // Add the neighbor in the j-direction
        output.push(Self::from_face_ij_same(face, i, j + joffset, jsame).parent(level));

        // Add the diagonal neighbor if both i- and j-neighbors are on the same face. If both are
        // on different faces, this vertex is one of the 8 cube vertices and only has 3 neighbors
        // (not 4).
        if isame || jsame {
            output.push(
                Self::from_face_ij_same(face, i + ioffset, j + joffset, isame && jsame)
                    .parent(level),
            );
        }
    }

    /// Helper: creates a leaf cell from (face, i, j), wrapping to adjacent face if needed when
    /// `same_face` is false.
    fn from_face_ij_same(face: i32, i: i32, j: i32, same_face: bool) -> Self {
        if same_face {
            Self::from_face_ij(face, i, j)
        } else {
            Self::from_face_ij_wrap(face, i, j)
        }
    }

    /// Creates a leaf cell from (face, i, j), wrapping to the appropriate
    /// adjacent face if (i, j) is outside [0, MAX_SIZE).
    fn from_face_ij_wrap(face: i32, i: i32, j: i32) -> Self {
        // Clamp i and j to just beyond the valid range to prevent overflow.
        let i = i.clamp(-1, MAX_SIZE);
        let j = j.clamp(-1, MAX_SIZE);

        // Convert to (u, v) using linear projection and clamp to barely outside the face to avoid
        // numerical issues during reprojection.
        const SCALE: f64 = 1.0 / MAX_SIZE as f64;
        const LIMIT: f64 = 1.0 + f64::EPSILON;

        // The arithmetic below is designed to avoid 32-bit integer overflows.
        let u = (SCALE * (2.0 * (i - MAX_SIZE / 2) as f64 + 1.0)).clamp(-LIMIT, LIMIT);
        let v = (SCALE * (2.0 * (j - MAX_SIZE / 2) as f64 + 1.0)).clamp(-LIMIT, LIMIT);

        // Project onto sphere and find the containing face.
        let p = face_uv_to_xyz(face, u, v);
        let (new_face, new_u, new_v) = xyz_to_face_uv(&p);

        // Convert back to (i, j) on the new face using linear projection inverse.
        let new_i = st_to_ij(0.5 * (new_u + 1.0));
        let new_j = st_to_ij(0.5 * (new_v + 1.0));

        Self::from_face_ij(new_face, new_i, new_j)
    }

    /// Returns the largest cell whose range_min is <= self and range_max limit.
    ///
    /// Used to iterate through a range of leaf cells efficiently:
    /// ```ignore
    /// let mut id = start.maximum_tile(limit);
    /// while id != limit {
    ///     // process id
    ///     id = id.next().maximum_tile(limit);
    /// }
    /// ```
    pub fn maximum_tile(&self, limit: S2CellId) -> S2CellId {
        let mut id = *self;
        let start = id.range_min();

        // If we're already at or past the limit, return limit.
        if start >= limit.range_min() {
            return limit;
        }

        if id.range_max() >= limit {
            // The cell is too large.  Shrink it.  Note that when generating coverings of S2CellId
            // ranges, this loop usually executes only once.  Also because id.range_min() <
            // limit.range_min(), we will always exit the loop by the time we reach a leaf cell.
            loop {
                id = id.child(0);
                if id.range_max() < limit {
                    break;
                }
            }
            return id;
        }

        // The cell may be too small.  Grow it if necessary.  Note that generally this loop only
        // iterates once.
        while !id.is_face() {
            let parent = id.immediate_parent();
            if parent.range_min() != start || parent.range_max() >= limit {
                break;
            }
            id = parent;
        }
        id
    }

    /// Returns the level of the lowest common ancestor of this cell and other, or -1 if they are
    /// on different faces.
    pub fn common_ancestor_level(&self, other: S2CellId) -> i8 {
        // The max() below is necessary for the case where one S2CellId is a descendant of the
        // other.
        let bits = (self.0 ^ other.0).max(self.lsb().max(other.lsb()));

        // Compute the position of the most significant bit, and then map the bit position as
        // follows:
        // {0} -> 30, {1,2} -> 29, {3,4} -> 28, ... , {59,60} -> 0, {61,62,63} -> -1.
        let bit_width = 64 - bits.leading_zeros();
        ((61i32 - bit_width as i32).max(-1) >> 1) as i8
    }
}

impl std::fmt::Display for S2CellId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_debug_string())
    }
}

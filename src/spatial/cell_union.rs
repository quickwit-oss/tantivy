//! CellUnion: A normalized, sorted collection of non-overlapping S2CellIds.
//!
//! A CellUnion represents a region on the sphere as a collection of S2CellIds at various levels.
//! The collection is:
//! - Sorted in increasing order by cell ID
//! - Non-overlapping (no cell contains another)
//! - Normalized (four sibling cells are replaced by their parent)
//!
//! From s2cell_union.h and s2cell_union.cc

use super::latlng_rect::S2LatLngRect;
use super::math::normalize;
use super::region::Region;
use super::s2cap::S2Cap;
use super::s2cell::S2Cell;
use super::s2cell_id::S2CellId;

/// A normalized collection of S2CellIds representing a region.
///
/// The cell IDs are stored sorted and non-overlapping. In a normalized CellUnion, groups of 4
/// sibling cells are replaced by their parent cell.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CellUnion {
    cell_ids: Vec<S2CellId>,
}

impl CellUnion {
    /// Creates an empty CellUnion.
    pub fn new() -> Self {
        Self {
            cell_ids: Vec::new(),
        }
    }

    /// Creates a CellUnion from a vector of S2CellIds, then normalizes.
    ///
    /// The input cells can be in any order and may overlap. The constructor sorts them, removes
    /// duplicates and contained cells, and merges sibling groups.
    pub fn from_cell_ids(mut cell_ids: Vec<S2CellId>) -> Self {
        Self::normalize_vec(&mut cell_ids);
        Self { cell_ids }
    }

    /// Creates a CellUnion from pre-normalized cell IDs (no normalization).
    ///
    /// REQUIRES: cell_ids satisfies is_normalized() requirements.
    pub fn from_normalized(cell_ids: Vec<S2CellId>) -> Self {
        assert!(Self::check_normalized(&cell_ids));
        Self { cell_ids }
    }

    /// Creates a CellUnion from sorted, non-overlapping cell IDs.
    ///
    /// Unlike from_normalized, this does not require sibling merging.
    /// The cells must be valid, sorted, and non-overlapping.
    pub fn from_verbatim(cell_ids: Vec<S2CellId>) -> Self {
        assert!(Self::check_valid(&cell_ids));
        Self { cell_ids }
    }

    /// Creates a CellUnion covering the entire sphere (all 6 face cells).
    pub fn whole_sphere() -> Self {
        Self {
            cell_ids: vec![
                S2CellId::from_face(0),
                S2CellId::from_face(1),
                S2CellId::from_face(2),
                S2CellId::from_face(3),
                S2CellId::from_face(4),
                S2CellId::from_face(5),
            ],
        }
    }

    /// Creates a CellUnion covering leaf cells from min_id to max_id inclusive.
    ///
    /// Uses the maximum_tile algorithm to find the largest cells that fit
    ///
    /// REQUIRES: min_id.is_leaf(), max_id.is_leaf(), min_id <= max_id
    pub fn from_min_max(min_id: S2CellId, max_id: S2CellId) -> Self {
        assert!(min_id.is_leaf());
        assert!(max_id.is_leaf());
        assert!(min_id <= max_id);
        Self::from_begin_end(min_id, max_id.next())
    }

    /// Creates a CellUnion covering leaf cells from begin (inclusive) to end (exclusive).
    ///
    /// Like Python ranges or STL iterator ranges. Returns empty if begin == end.
    ///
    /// REQUIRES: begin.is_leaf(), end.is_leaf(), begin <= end
    pub fn from_begin_end(begin: S2CellId, end: S2CellId) -> Self {
        assert!(begin.is_leaf() || begin == S2CellId::end(S2CellId::MAX_LEVEL));
        assert!(end.is_leaf() || end == S2CellId::end(S2CellId::MAX_LEVEL));
        assert!(begin <= end);

        // We repeatedly add the largest cell we can.
        let mut cell_ids = Vec::new();
        let mut id = begin.maximum_tile(end);
        while id != end {
            cell_ids.push(id);
            id = id.next().maximum_tile(end);
        }
        // The output is already normalized.
        Self { cell_ids }
    }

    /// Returns the number of cells.
    #[inline]
    pub fn len(&self) -> usize {
        self.cell_ids.len()
    }

    /// Returns true if empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.cell_ids.is_empty()
    }

    /// Returns the cell at the given index.
    #[inline]
    pub fn cell_id(&self, i: usize) -> S2CellId {
        self.cell_ids[i]
    }

    /// Returns the underlying cell ID vector.
    #[inline]
    pub fn cell_ids(&self) -> &[S2CellId] {
        &self.cell_ids
    }

    /// Consumes self and returns the cell ID vector.
    pub fn into_cell_ids(self) -> Vec<S2CellId> {
        self.cell_ids
    }

    /// Clears the CellUnion.
    pub fn clear(&mut self) {
        self.cell_ids.clear();
        self.cell_ids.shrink_to_fit();
    }

    /// Returns true if the cell union is valid.
    ///
    /// Valid means: cell IDs are valid, non-overlapping, and sorted.
    pub fn is_valid(&self) -> bool {
        Self::check_valid(&self.cell_ids)
    }

    /// Returns true if the cell union is normalized.
    ///
    /// Normalized means: valid and no four cells have a common parent.
    pub fn is_normalized(&self) -> bool {
        Self::check_normalized(&self.cell_ids)
    }

    /// Checks validity of a cell ID vector.
    fn check_valid(cell_ids: &[S2CellId]) -> bool {
        if !cell_ids.is_empty() && !cell_ids[0].is_valid() {
            return false;
        }
        for i in 1..cell_ids.len() {
            if !cell_ids[i].is_valid() {
                return false;
            }
            // Check non-overlapping: previous range_max < current range_min
            if cell_ids[i - 1].range_max() >= cell_ids[i].range_min() {
                return false;
            }
        }
        true
    }

    /// Checks normalization of a cell ID vector.
    fn check_normalized(cell_ids: &[S2CellId]) -> bool {
        if !cell_ids.is_empty() && !cell_ids[0].is_valid() {
            return false;
        }
        for i in 1..cell_ids.len() {
            if !cell_ids[i].is_valid() {
                return false;
            }
            if cell_ids[i - 1].range_max() >= cell_ids[i].range_min() {
                return false;
            }
            // Check no 4 siblings exist
            if i >= 3
                && are_siblings(
                    cell_ids[i - 3],
                    cell_ids[i - 2],
                    cell_ids[i - 1],
                    cell_ids[i],
                )
            {
                return false;
            }
        }
        true
    }

    /// Normalizes the cell union in place.
    ///
    /// Sorts the cells, removes contained cells, and merges sibling groups.
    pub fn normalize(&mut self) {
        Self::normalize_vec(&mut self.cell_ids);
    }

    /// Normalizes a vector of cell IDs in place.
    pub fn normalize_vec(ids: &mut Vec<S2CellId>) {
        // Sort cells in increasing order
        ids.sort();

        let mut out = 0usize;
        for i in 0..ids.len() {
            let mut id = ids[i];
            assert!(id.is_valid());

            // Check whether this cell is contained by the previous output cell.
            if out > 0 && ids[out - 1].contains(id) {
                continue;
            }

            // Discard any previous cells contained by this cell.
            while out > 0 && id.contains(ids[out - 1]) {
                out -= 1;
            }

            // Check whether the last 3 elements plus "id" can be collapse into
            // a single parent cell.
            while out >= 3 && are_siblings(ids[out - 3], ids[out - 2], ids[out - 1], id) {
                // Replace four children by their parent cell.
                id = id.immediate_parent();
                out -= 3;
            }
            ids[out] = id;
            out += 1;
        }

        // Resize to actual output size
        ids.truncate(out);
    }

    /// Denormalizes to satisfy min_level and level_mod constraints.
    ///
    /// Replaces cells whose level is less than min_level or where
    /// (level - min_level) is not a multiple of level_mod with their children.
    ///
    /// This allows coverings generated with min_level/level_mod constraints
    /// to be stored normalized and then restored to the original form.
    pub fn denormalize(&self, min_level: i32, level_mod: i32) -> Vec<S2CellId> {
        assert!(min_level <= S2CellId::MAX_LEVEL);
        assert!((1..=3).contains(&level_mod));

        let mut output = Vec::with_capacity(self.cell_ids.len());

        for &id in &self.cell_ids {
            let level = id.level();
            let mut new_level = level.max(min_level);

            if level_mod > 1 {
                // Round up so that (new_level - min_level) is a multiple of level_mod.
                // (Note that MAX_LEVEL is a multiple of 1, 2, and 3.)
                let remainder = (S2CellId::MAX_LEVEL - (new_level - min_level)) % level_mod;
                new_level += remainder;
                new_level = new_level.min(S2CellId::MAX_LEVEL);
            }

            if new_level == level {
                output.push(id);
            } else {
                // Expand to children at new_level
                let end = id.child_end_at_level(new_level);
                let mut child = id.child_begin_at_level(new_level);
                while child != end {
                    output.push(child);
                    child = child.next();
                }
            }
        }
        output
    }

    /// Returns true if the cell union contains the given cell ID.
    ///
    /// Uses binary search with the EntirelyPrecedes comparator.
    pub fn contains(&self, id: S2CellId) -> bool {
        assert!(id.is_valid());

        // Binary search for the first cell that might contain id
        // (i.e., the first cell that does not entirely precede id).
        let i = self.lower_bound(id);
        i < self.cell_ids.len() && self.cell_ids[i].contains(id)
    }

    /// Returns true if the cell union intersects the given cell ID.
    ///
    /// Uses binary search with the EntirelyPrecedes comparator.
    pub fn intersects(&self, id: S2CellId) -> bool {
        assert!(id.is_valid());

        // Binary search for the first cell that might intersect id.
        let i = self.lower_bound(id);
        i < self.cell_ids.len() && self.cell_ids[i].intersects(id)
    }

    /// Returns true if the cell union contains the given point.
    ///
    /// The point does not need to be normalized.
    pub fn contains_point(&self, p: &[f64; 3]) -> bool {
        self.contains(S2CellId::from_point(p))
    }

    /// Binary search for the first cell that does not entirely precede target.
    ///
    /// Uses the EntirelyPrecedes comparator: a cell entirely precedes another
    /// if its range_max < the other's range_min.
    fn lower_bound(&self, target: S2CellId) -> usize {
        self.cell_ids
            .partition_point(|&id| entirely_precedes(id, target))
    }

    /// Shrinks the capacity if there is excess allocation.
    ///
    /// Useful when holding many CellUnions in memory.
    pub fn pack(&mut self, excess: usize) {
        if self.cell_ids.capacity() - self.cell_ids.len() > excess {
            self.cell_ids.shrink_to_fit();
        }
    }
}

/// Returns true if cell `a` entirely precedes cell `b` on the Hilbert curve.
///
/// This means a.range_max() < b.range_min(), i.e., no overlap is possible
///
/// Note: This is not a total ordering. For sorted, disjoint cell IDs, it can
/// be used with partition_point to find the first cell that might intersect.
#[inline]
fn entirely_precedes(a: S2CellId, b: S2CellId) -> bool {
    a.range_max() < b.range_min()
}

/// Returns true if the four cells are siblings (have a common parent).
///
/// Uses the XOR trick: a ^ b ^ c == d is a necessary (but not sufficient)
/// condition. The mask test provides the exact check.
///
/// REQUIRES: The four cells are distinct.
fn are_siblings(a: S2CellId, b: S2CellId, c: S2CellId, d: S2CellId) -> bool {
    // A necessary (but not sufficient) condition is that the XOR of the
    // four cells must be zero. This is also very fast to test.
    if (a.id() ^ b.id() ^ c.id()) != d.id() {
        return false;
    }

    // Now we do a slightly more expensive but exact test. First, compute
    // mask that blocks out the two bits that encode the child position of
    // "d" with respect to its parent, then check that the other three
    // children all agree with "mask".
    let lsb = d.lsb();
    let mask = lsb << 1;
    let mask = !(mask + (mask << 1));
    let id_masked = d.id() & mask;

    (a.id() & mask) == id_masked
        && (b.id() & mask) == id_masked
        && (c.id() & mask) == id_masked
        && !d.is_face() // Face cells have no parent
}

impl std::ops::Index<usize> for CellUnion {
    type Output = S2CellId;

    fn index(&self, index: usize) -> &Self::Output {
        &self.cell_ids[index]
    }
}

impl IntoIterator for CellUnion {
    type Item = S2CellId;
    type IntoIter = std::vec::IntoIter<S2CellId>;

    fn into_iter(self) -> Self::IntoIter {
        self.cell_ids.into_iter()
    }
}

impl<'a> IntoIterator for &'a CellUnion {
    type Item = &'a S2CellId;
    type IntoIter = std::slice::Iter<'a, S2CellId>;

    fn into_iter(self) -> Self::IntoIter {
        self.cell_ids.iter()
    }
}

impl FromIterator<S2CellId> for CellUnion {
    fn from_iter<I: IntoIterator<Item = S2CellId>>(iter: I) -> Self {
        Self::from_cell_ids(iter.into_iter().collect())
    }
}

impl std::fmt::Display for CellUnion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        const MAX_DISPLAY: usize = 20;
        write!(f, "CellUnion(size={}, cells=[", self.len())?;
        for (i, id) in self.cell_ids.iter().take(MAX_DISPLAY).enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{}", id.to_token())?;
        }
        if self.len() > MAX_DISPLAY {
            write!(f, ", ...]")?;
        } else {
            write!(f, "]")?;
        }
        write!(f, ")")
    }
}

impl Region for CellUnion {
    /// Returns a bounding spherical cap for this cell union.
    ///
    /// Computes an area-weighted centroid of the cells, then expands a cap
    /// from that center to include all cells' bounding caps.
    ///
    /// Note: This won't produce the bounding cap of minimal area, but
    /// it should be close enough for covering purposes.
    fn get_cap_bound(&self) -> S2Cap {
        if self.cell_ids.is_empty() {
            return S2Cap::empty();
        }

        // Compute the approximate centroid of the region, weighted by cell area.
        let mut centroid = [0.0, 0.0, 0.0];
        for &id in &self.cell_ids {
            let area = S2Cell::average_area(id.level());
            let p = id.to_point();
            centroid[0] += area * p[0];
            centroid[1] += area * p[1];
            centroid[2] += area * p[2];
        }

        // Handle the case where centroid is zero (can happen if cells are
        // symmetrically distributed around the origin).
        let centroid = if centroid[0] == 0.0 && centroid[1] == 0.0 && centroid[2] == 0.0 {
            [1.0, 0.0, 0.0]
        } else {
            normalize(&centroid)
        };

        // Use the centroid as the cap axis, and expand the cap angle so that it
        // contains the bounding caps of all the individual cells. Note that it is
        // *not* sufficient to just bound all the cell vertices because the bounding
        // cap may be concave (i.e. cover more than one hemisphere).
        let mut cap = S2Cap::from_point(centroid);
        for &id in &self.cell_ids {
            let cell = S2Cell::new(id);
            cap.add_cap(&cell.get_cap_bound());
        }
        cap
    }

    /// Returns a bounding latitude-longitude rectangle for this cell union.
    ///
    /// Computes the union of all cells' rect bounds.
    fn get_rect_bound(&self) -> S2LatLngRect {
        let mut bound = S2LatLngRect::empty();
        for &id in &self.cell_ids {
            let cell = S2Cell::new(id);
            bound = bound.union(&cell.get_rect_bound());
        }
        bound
    }

    /// Returns a small cell covering for this cell union.
    ///
    /// Delegates to get_cap_bound().get_cell_union_bound() rather than
    /// returning the cells directly, because get_cell_union_bound should
    /// return a small covering.
    fn get_cell_union_bound(&self, cell_ids: &mut Vec<S2CellId>) {
        self.get_cap_bound().get_cell_union_bound(cell_ids);
    }

    /// Returns true if the cell union completely contains the given cell.
    fn contains_cell(&self, cell: &S2Cell) -> bool {
        self.contains(cell.id())
    }

    /// Returns true if the cell union may intersect the given cell.
    ///
    /// Because Intersects is exact, false means definitely no intersection.
    fn may_intersect(&self, cell: &S2Cell) -> bool {
        self.intersects(cell.id())
    }

    /// Returns true if the cell union contains the given point.
    ///
    /// The point does not need to be normalized.
    fn contains_point(&self, p: &[f64; 3]) -> bool {
        self.contains_point(p)
    }
}

#[cfg(test)]
#[path = "tests/cell_union_tests.rs"]
mod tests;

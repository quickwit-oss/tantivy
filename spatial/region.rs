//! An S2Region represents a two-dimensional region over the unit sphere.  It is an abstract
//! interface with various concrete subtypes.
//!
//! The main purpose of this interface is to allow complex regions to be approximated as simpler
//! regions.  So rather than having a wide variety of virtual methods that are implemented by all
//! subtypes, the interface is restricted to methods that are useful for computing approximations.
//!
//! Ported from s2/s2region.h (Eric Veach, Google).

use super::latlng_rect::S2LatLngRect;
use super::s2cap::S2Cap;
use super::s2cell::S2Cell;
use super::s2cell_id::S2CellId;

/// A two-dimensional region over the unit sphere.
pub trait Region {
    /// Returns a bounding spherical cap that contains the region.  The bound may not be tight.
    fn get_cap_bound(&self) -> S2Cap;

    /// Returns a bounding latitude-longitude rectangle that contains the region.  The bound may
    /// not be tight.
    fn get_rect_bound(&self) -> S2LatLngRect;

    /// Returns a small collection of S2CellIds whose union covers the region.  The cells are not
    /// sorted, may have redundancies (such as cells that contain other cells), and may cover much
    /// more area than necessary.
    ///
    /// Implementations should attempt to return a small covering (ideally 4 cells or fewer) that
    /// covers the region and can be computed quickly.  The result is used by S2RegionCoverer as
    /// a starting point for further refinement.
    fn get_cell_union_bound(&self, cell_ids: &mut Vec<S2CellId>) {
        self.get_cap_bound().get_cell_union_bound(cell_ids);
    }

    /// Returns true if the region completely contains the given cell, otherwise either the region
    /// does not contain the cell or the containment relationship could not be determined.
    fn contains_cell(&self, cell: &S2Cell) -> bool;

    /// If this method returns false, the region does not intersect the given cell.  Otherwise,
    /// either the region intersects the cell, or the intersection relationship could not be
    /// determined.
    fn may_intersect(&self, cell: &S2Cell) -> bool;

    /// Returns true if and only if the given point is contained by the region.  The point 'p' is
    /// generally required to be unit length, although some subtypes may relax this restriction.
    fn contains_point(&self, p: &[f64; 3]) -> bool;
}

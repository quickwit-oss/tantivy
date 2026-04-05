//! Options controlling how the clipper subdivides cells.

/// Default maximum number of edges per cell (not counting "long" edges). Reasonable values range
/// from 10 to about 50 or so.
pub const DEFAULT_MAX_EDGES_PER_CELL: usize = 10;

/// Options that affect construction of the ShapeIndex.
#[derive(Clone, Debug)]
pub struct ClipOptions {
    /// Maximum edges per cell before subdivision.
    pub max_edges_per_cell: usize,
    /// Minimum fraction of 'short' edges required for subdivision. If this parameter is non-zero
    /// then the total index size and construction time are guaranteed to be linear in the number
    /// of input edges. Default: 0.2 (from FLAGS_s2shape_index_min_short_edge_fraction)
    pub min_short_edge_fraction: f64,
}

impl Default for ClipOptions {
    fn default() -> Self {
        Self {
            max_edges_per_cell: DEFAULT_MAX_EDGES_PER_CELL,
            min_short_edge_fraction: 0.2,
        }
    }
}

//! RegionCoverer: Approximates regions as cell unions.
//!
//! Produces coverings (cells that cover the region) using a priority-queue based refinement
//! algorithm. The output satisfies configurable constraints on cell count, levels, and level
//! spacing.

use std::cmp::Reverse;
use std::collections::BinaryHeap;

use super::cell_union::CellUnion;
use super::region::Region;
use super::s2cell::S2Cell;
use super::s2cell_id::S2CellId;

/// Options for controlling the covering algorithm.
#[derive(Clone, Debug)]
pub struct CovererOptions {
    /// Maximum number of cells in the output.
    ///
    /// Note: Up to 6 cells may be returned if the region intersects all faces. Up to 3 cells may
    /// be returned for tiny regions at cube face intersections.
    ///
    /// Default: 8
    max_cells: i32,

    /// Minimum cell level to use.
    ///
    /// Takes priority over max_cells: cells below this level are never used even if this causes
    /// more cells to be returned.
    ///
    /// Default: 0
    min_level: i32,

    /// Maximum cell level to use.
    ///
    /// Default: 30 (leaf level)
    max_level: i32,

    /// Level spacing constraint.
    ///
    /// Only cells where (level - min_level) % level_mod == 0 are used.
    /// - level_mod = 1: branching factor 4 (default)
    /// - level_mod = 2: branching factor 16
    /// - level_mod = 3: branching factor 64
    ///
    /// Default: 1
    level_mod: i32,
}

impl Default for CovererOptions {
    fn default() -> Self {
        Self {
            max_cells: 8,
            min_level: 0,
            max_level: S2CellId::MAX_LEVEL,
            level_mod: 1,
        }
    }
}

impl CovererOptions {
    /// Creates options with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the maximum number of cells.
    pub fn max_cells(mut self, max_cells: i32) -> Self {
        self.max_cells = max_cells;
        self
    }

    /// Sets the minimum cell level.
    pub fn min_level(mut self, min_level: i32) -> Self {
        debug_assert!(min_level <= S2CellId::MAX_LEVEL);
        self.min_level = min_level.min(S2CellId::MAX_LEVEL);
        self
    }

    /// Sets the maximum cell level.
    pub fn max_level(mut self, max_level: i32) -> Self {
        debug_assert!(max_level <= S2CellId::MAX_LEVEL);
        self.max_level = max_level.min(S2CellId::MAX_LEVEL);
        self
    }

    /// Sets both min and max level to the same value.
    pub fn fixed_level(mut self, level: i32) -> Self {
        self.min_level = level.min(S2CellId::MAX_LEVEL);
        self.max_level = level.min(S2CellId::MAX_LEVEL);
        self
    }

    /// Sets the level spacing constraint.
    pub fn level_mod(mut self, level_mod: i32) -> Self {
        debug_assert!((1..=3).contains(&level_mod));
        self.level_mod = level_mod.clamp(1, 3);
        self
    }

    /// Returns the maximum level that will actually be used.
    ///
    /// This is the largest level <= max_level where (level - min_level) % level_mod == 0.
    pub fn true_max_level(&self) -> i32 {
        if self.level_mod == 1 {
            return self.max_level;
        }
        self.max_level - (self.max_level - self.min_level) % self.level_mod
    }

    /// Returns the number of children per candidate (4^level_mod).
    #[inline]
    fn max_children(&self) -> usize {
        1 << self.max_children_shift()
    }

    /// Returns log base 2 of max_children.
    #[inline]
    fn max_children_shift(&self) -> usize {
        2 * self.level_mod as usize
    }
}

/// A candidate cell being considered for the covering.
struct Candidate {
    cell: S2Cell,
    is_terminal: bool,
    num_children: usize,
    children: Vec<Candidate>,
}

impl Candidate {
    /// Creates a new candidate.
    fn new(cell: S2Cell, is_terminal: bool, max_children: usize) -> Self {
        Self {
            cell,
            is_terminal,
            num_children: 0,
            children: if is_terminal {
                Vec::new()
            } else {
                Vec::with_capacity(max_children)
            },
        }
    }
}

/// Priority queue entry.
#[derive(Eq, PartialEq)]
struct QueueEntry {
    priority: i32,
    candidate_idx: usize,
}

impl Ord for QueueEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.priority
            .cmp(&other.priority)
            .then_with(|| self.candidate_idx.cmp(&other.candidate_idx))
    }
}

impl PartialOrd for QueueEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// Generates cell coverings for arbitrary regions.
///
/// The covering algorithm uses a priority queue to iteratively refine cells. It starts with
/// initial candidates from the region's cell union bound, then repeatedly expands the largest
/// cells until max_cells is reached or no further refinement is possible.
pub struct RegionCoverer {
    options: CovererOptions,
}

impl RegionCoverer {
    /// Creates a new RegionCoverer with the given options.
    pub fn new(options: CovererOptions) -> Self {
        debug_assert!(options.min_level <= options.max_level);
        Self { options }
    }

    /// Creates a new RegionCoverer with default options.
    pub fn with_defaults() -> Self {
        Self::new(CovererOptions::default())
    }

    /// Returns a reference to the options.
    pub fn options(&self) -> &CovererOptions {
        &self.options
    }

    /// Returns a mutable reference to the options.
    pub fn options_mut(&mut self) -> &mut CovererOptions {
        &mut self.options
    }

    /// Returns a CellUnion that covers the given region.
    ///
    /// The covering satisfies all constraints in options(). If min_level > 0 or level_mod > 1, the
    /// result may not be normalized (siblings may exist).
    pub fn get_covering<R: Region>(&self, region: &R) -> CellUnion {
        let mut cell_ids = Vec::new();
        self.get_covering_cells(region, &mut cell_ids);
        CellUnion::from_verbatim(cell_ids)
    }

    /// Like get_covering but writes directly to a vector.
    pub fn get_covering_cells<R: Region>(&self, region: &R, covering: &mut Vec<S2CellId>) {
        covering.clear();
        self.get_covering_internal(region, covering);
    }

    /// Returns a fast, loose covering of the region.
    ///
    /// Much faster than get_covering but less tight. Uses the region's cell union bound and
    /// canonicalizes it.
    pub fn get_fast_covering<R: Region>(&self, region: &R, covering: &mut Vec<S2CellId>) {
        covering.clear();
        region.get_cell_union_bound(covering);
        self.canonicalize_covering(covering);
    }

    /// Internal covering algorithm.
    fn get_covering_internal<R: Region>(&self, region: &R, result: &mut Vec<S2CellId>) {
        debug_assert!(self.options.min_level <= self.options.max_level);

        let mut pq: BinaryHeap<Reverse<QueueEntry>> = BinaryHeap::new();
        let mut candidates: Vec<Option<Candidate>> = Vec::new();

        // Get initial candidates from the region's cell union bound.
        let mut initial_cells = Vec::new();
        self.get_initial_candidates(region, &mut initial_cells);

        for cell_id in initial_cells {
            if let Some(candidate) = self.new_candidate(region, &S2Cell::new(cell_id)) {
                self.add_candidate(region, candidate, result, &mut pq, &mut candidates);
            }
        }

        while let Some(Reverse(entry)) = pq.pop() {
            let Some(mut candidate) = candidates[entry.candidate_idx].take() else {
                continue;
            };

            // For exterior coverings, the decision logic is:
            // - If below min_level: must expand
            // - If only 1 child: no harm in expanding
            // - If adding children wouldn't exceed max_cells: expand
            // - Otherwise: mark as terminal and add
            if candidate.cell.level() < self.options.min_level
                || candidate.num_children == 1
                || (result.len() + pq.len() + candidate.num_children
                    <= self.options.max_cells as usize)
            {
                // Expand this candidate into its children.
                for child in candidate.children.drain(..) {
                    self.add_candidate(region, child, result, &mut pq, &mut candidates);
                }
            } else {
                // Mark as terminal and process.
                candidate.is_terminal = true;
                self.add_candidate(region, candidate, result, &mut pq, &mut candidates);
            }
        }

        // Rather than just returning the raw list of cell ids, we construct a cell union and then
        // denormalize it. This has the effect of replacing four child cells with their parent
        // whenever this does not violate the covering parameters.
        CellUnion::normalize_vec(result);
        if self.options.min_level > 0 || self.options.level_mod > 1 {
            let normalized = std::mem::take(result);
            let cu = CellUnion::from_verbatim(normalized);
            *result = cu.denormalize(self.options.min_level, self.options.level_mod);
        }
        debug_assert!(self.is_canonical(result));
    }

    /// Computes a set of initial candidates that cover the given region.
    fn get_initial_candidates<R: Region>(&self, region: &R, cells: &mut Vec<S2CellId>) {
        // Optimization: start with a small (usually 4 cell) covering of the
        // region's bounding cap.
        let tmp_options = CovererOptions {
            max_cells: self.options.max_cells.min(4),
            min_level: 0,
            max_level: self.options.max_level,
            level_mod: 1,
        };
        let tmp_coverer = RegionCoverer::new(tmp_options);
        tmp_coverer.get_fast_covering(region, cells);

        // Adjust cell levels for level_mod constraint.
        self.adjust_cell_levels(cells);
    }

    /// Processes a candidate by either adding it to the result_ vector or expanding its children
    /// and inserting it into the priority queue. Passing an argument of nullptr does nothing.
    fn add_candidate<R: Region>(
        &self,
        region: &R,
        mut candidate: Candidate,
        result: &mut Vec<S2CellId>,
        pq: &mut BinaryHeap<Reverse<QueueEntry>>,
        candidates: &mut Vec<Option<Candidate>>,
    ) {
        // If terminal, add to result immediately.
        if candidate.is_terminal {
            result.push(candidate.cell.id());
            return;
        }
        debug_assert_eq!(0, candidate.num_children);

        // Expand one level at a time until we hit min_level() to ensure that we don't skip over
        // it.
        let num_levels = if candidate.cell.level() < self.options.min_level {
            1
        } else {
            self.options.level_mod as usize
        };
        let num_terminals = self.expand_children(region, &mut candidate, num_levels);

        if candidate.num_children == 0 {
            // No children intersect the region.
            return;
        }

        // Optimization: add the parent cell rather than all of its children.
        if num_terminals == self.options.max_children()
            && candidate.cell.level() >= self.options.min_level
        {
            candidate.is_terminal = true;
            self.add_candidate(region, candidate, result, pq, candidates);
            return;
        }

        // Add candidate to priority queue.
        self.add_candidate_to_queue(candidate, num_terminals, pq, candidates);
    }

    /// Adds a candidate to the priority queue.
    fn add_candidate_to_queue(
        &self,
        candidate: Candidate,
        num_terminals: usize,
        pq: &mut BinaryHeap<Reverse<QueueEntry>>,
        candidates: &mut Vec<Option<Candidate>>,
    ) {
        // We negate the priority so that smaller absolute priorities are returned first. The
        // heuristic is designed to refine the largest cells first, since those are where we have
        // the largest potential gain.
        let priority = self.compute_priority(&candidate, num_terminals);
        let candidate_idx = candidates.len();
        candidates.push(Some(candidate));
        pq.push(Reverse(QueueEntry {
            priority,
            candidate_idx,
        }));
    }

    /// Creates a new candidate for the given cell, or None if it doesn't intersect.
    ///
    /// A candidate is marked terminal if:
    /// - The region fully contains the cell, OR
    /// - The cell is at max_level (cannot subdivide further)
    fn new_candidate<R: Region>(&self, region: &R, cell: &S2Cell) -> Option<Candidate> {
        if !region.may_intersect(cell) {
            return None;
        }

        let mut is_terminal = false;
        let level = cell.level();

        if level >= self.options.min_level {
            // For exterior coverings (not interior):
            if level + self.options.level_mod > self.options.max_level || region.contains_cell(cell)
            {
                is_terminal = true;
            }
        }

        let max_children = if is_terminal {
            0
        } else {
            self.options.max_children()
        };

        Some(Candidate::new(*cell, is_terminal, max_children))
    }

    /// Expands children of a candidate by the given number of levels.
    ///
    /// Returns the number of terminal children.
    fn expand_children<R: Region>(
        &self,
        region: &R,
        candidate: &mut Candidate,
        num_levels: usize,
    ) -> usize {
        let cell = candidate.cell;
        self.expand_children_recursive(region, candidate, &cell, num_levels)
    }

    /// Recursive helper for expand_children.
    fn expand_children_recursive<R: Region>(
        &self,
        region: &R,
        candidate: &mut Candidate,
        cell: &S2Cell,
        num_levels: usize,
    ) -> usize {
        let num_levels = num_levels - 1;

        let Some(children) = cell.subdivide() else {
            return 0;
        };

        let mut num_terminals = 0;

        for child_cell in children {
            if num_levels > 0 {
                if region.may_intersect(&child_cell) {
                    num_terminals +=
                        self.expand_children_recursive(region, candidate, &child_cell, num_levels);
                }
                continue;
            }

            if let Some(child) = self.new_candidate(region, &child_cell) {
                if child.is_terminal {
                    num_terminals += 1;
                }
                candidate.children.push(child);
                candidate.num_children += 1;
            }
        }

        num_terminals
    }

    // We negate the priority so that smaller absolute priorities are returned first.  The
    // heuristic is designed to refine the largest cells first, since those are where we have the
    // largest potential gain.  Among cells of the same size, we prefer the cells with the fewest
    // children. Finally, among cells with equal numbers of children we prefer those with the
    // smallest number of children that cannot be refined further.
    fn compute_priority(&self, candidate: &Candidate, num_terminals: usize) -> i32 {
        let shift = self.options.max_children_shift() as i32;
        -(((((candidate.cell.level()) << shift) + candidate.num_children as i32) << shift)
            + num_terminals as i32)
    }

    /// If level > min_level(), then reduces "level" if necessary so that it also satisfies
    /// level_mod(). Levels smaller than min_level() are not affected (since cells at these levels
    /// are eventually expanded).
    #[inline]
    fn adjust_level(&self, level: i32) -> i32 {
        if self.options.level_mod > 1 && level > self.options.min_level {
            level - (level - self.options.min_level) % self.options.level_mod
        } else {
            level
        }
    }

    /// Ensures that all cells with level > min_level() also satisfy level_mod(), by replacing them
    /// with an ancestor if necessary. Cell levels smaller than min_level() are not modified (see
    /// AdjustLevel). The output is then normalized to ensure that no redundant cells are present.
    fn adjust_cell_levels(&self, cells: &mut Vec<S2CellId>) {
        debug_assert!(cells.windows(2).all(|w| w[0] <= w[1])); // is_sorted

        if self.options.level_mod == 1 {
            return;
        }

        let mut out = 0;
        for i in 0..cells.len() {
            let mut id = cells[i];
            let level = id.level();
            let new_level = self.adjust_level(level);

            if new_level != level {
                id = id.parent(new_level);
            }

            // Skip if contained by previous output.
            if out > 0 && cells[out - 1].contains(id) {
                continue;
            }

            // Remove any previous cells contained by this one.
            while out > 0 && id.contains(cells[out - 1]) {
                out -= 1;
            }

            cells[out] = id;
            out += 1;
        }

        cells.truncate(out);
    }

    /// Canonicalizes a covering to satisfy all constraints.
    ///
    /// This ensures cells are sorted, non-overlapping, and satisfy min_level, max_level, and
    /// level_mod.
    pub fn canonicalize_covering(&self, covering: &mut Vec<S2CellId>) {
        debug_assert!(self.options.min_level <= self.options.max_level);

        // If any cells are too small, or don't satisfy level_mod(), then replace them with
        // ancestors.
        if self.options.max_level < S2CellId::MAX_LEVEL || self.options.level_mod > 1 {
            for id in covering.iter_mut() {
                let level = id.level();
                let new_level = self.adjust_level(level.min(self.options.max_level));
                if new_level != level {
                    *id = id.parent(new_level);
                }
            }
        }

        // Sort the cells and simplify them.
        CellUnion::normalize_vec(covering);

        // Make sure that the covering satisfies min_level() and level_mod(), possibly at the
        // expense of satisfying max_cells().
        if self.options.min_level > 0 || self.options.level_mod > 1 {
            let normalized = std::mem::take(covering);
            let cu = CellUnion::from_verbatim(normalized);
            *covering = cu.denormalize(self.options.min_level, self.options.level_mod);
        }

        // If there are too many cells and the covering is very large, use the S2RegionCoverer to
        // compute a new covering. (This avoids possible O(n^2) behavior of the simpler algorithm
        // below.)
        let excess = covering.len() as i64 - self.options.max_cells as i64;
        if excess <= 0 || self.is_canonical(covering) {
            return;
        }

        if excess * covering.len() as i64 > 10000 {
            // For very large coverings, recompute using the full algorithm.
            let cell_union = CellUnion::from_verbatim(std::mem::take(covering));
            self.get_covering_cells(&cell_union, covering);
        } else {
            // Repeatedly replace two adjacent cells by their lowest common ancestor.
            self.reduce_to_max_cells_with_parent_check(covering);
        }
        debug_assert!(self.is_canonical(covering));
    }

    /// Reduces the covering to at most max_cells by merging adjacent cells.
    ///
    /// After merging, checks if all children of the parent are present and recursively replaces
    /// them with the parent.
    fn reduce_to_max_cells_with_parent_check(&self, covering: &mut Vec<S2CellId>) {
        while covering.len() > self.options.max_cells as usize {
            // Find the pair of adjacent cells with the highest common ancestor
            // at or above min_level.
            let mut best_index: i32 = -1;
            let mut best_level: i32 = -1;

            for i in 0..covering.len() - 1 {
                let level = covering[i].common_ancestor_level(covering[i + 1]) as i32;
                let level = self.adjust_level(level);
                if level > best_level {
                    best_level = level;
                    best_index = i as i32;
                }
            }

            if best_level < self.options.min_level {
                break;
            }

            // Replace all cells contained by the new ancestor cell.
            let mut id = covering[best_index as usize].parent(best_level);
            self.replace_cells_with_ancestor(covering, id);

            // Now repeatedly check whether all children of the parent cell are present, in which
            // case we can replace those cells with their parent.
            while best_level > self.options.min_level {
                best_level -= self.options.level_mod;
                id = id.parent(best_level);
                if !self.contains_all_children(covering, id) {
                    break;
                }
                self.replace_cells_with_ancestor(covering, id);
            }
        }
    }

    /// Returns true if "covering" contains all children of "id" at level
    /// (id.level() + options_.level_mod()).
    fn contains_all_children(&self, covering: &[S2CellId], id: S2CellId) -> bool {
        let level = id.level() + self.options.level_mod;
        let mut it = covering.partition_point(|&c| c < id.range_min());
        let mut child = id.child_begin_at_level(level);
        let end = id.child_end_at_level(level);

        while child != end {
            if it >= covering.len() || covering[it] != child {
                return false;
            }
            it += 1;
            child = child.next();
        }
        true
    }

    /// Replaces all descendants of an ancestor in the covering with the ancestor.
    fn replace_cells_with_ancestor(&self, covering: &mut Vec<S2CellId>, id: S2CellId) {
        let begin = covering.partition_point(|&c| c < id.range_min());
        let end = covering.partition_point(|&c| c <= id.range_max());

        debug_assert!(begin != end);
        covering[begin] = id;
        if begin + 1 < end {
            covering.drain(begin + 1..end);
        }
    }

    /// Returns true if the covering is canonical.
    pub fn is_canonical(&self, covering: &[S2CellId]) -> bool {
        debug_assert!(self.options.min_level <= self.options.max_level);

        let min_level = self.options.min_level;
        let max_level = self.options.true_max_level();
        let level_mod = self.options.level_mod;
        let too_many_cells = covering.len() > self.options.max_cells as usize;
        let mut same_parent_count = 1;
        let mut prev_id: Option<S2CellId> = None;

        for &id in covering {
            if !id.is_valid() {
                return false;
            }

            // Check that the S2CellId level is acceptable.
            let level = id.level();
            if level < min_level || level > max_level {
                return false;
            }
            if level_mod > 1 && (level - min_level) % level_mod != 0 {
                return false;
            }

            if let Some(prev) = prev_id {
                // Check that cells are sorted and non-overlapping.
                if prev.range_max() >= id.range_min() {
                    return false;
                }

                // If there are too many cells, check that no pair of adjacent cells could be
                // replaced by an ancestor.
                if too_many_cells && id.common_ancestor_level(prev) as i32 >= min_level {
                    return false;
                }

                // Check that there are no sequences of (4 ** level_mod) cells that all have the
                // same parent (considering only multiples of "level_mod").
                let plevel = level - level_mod;
                if plevel < min_level
                    || level != prev.level()
                    || id.parent(plevel) != prev.parent(plevel)
                {
                    same_parent_count = 1;
                } else {
                    same_parent_count += 1;
                    if same_parent_count == self.options.max_children() {
                        return false;
                    }
                }
            }

            prev_id = Some(id);
        }

        true
    }
}

impl Default for RegionCoverer {
    fn default() -> Self {
        Self::with_defaults()
    }
}

#[cfg(test)]
#[path = "tests/region_coverer_tests.rs"]
mod tests;

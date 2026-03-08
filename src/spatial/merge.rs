//! Cell index merge pipeline.
//!
//! Merges N source cell indexes into a single stream of IndexCells. The
//! pipeline is pull-based: the consumer calls `next()` and the request
//! cascades through the stages internally. Bounded memory throughout.
//!
//! Stages (as methods, not separate types, to avoid ownership tangles):
//!   next_interleaved() -> next_split() -> next() [Iterator impl]
//!
//! The merge-siblings and page stages are not yet implemented. When they
//! are, they slot in as additional methods in the cascade.

use std::collections::VecDeque;

use common::ReadOnlyBitSet;

use super::cell_index::{get_edge_max_level, BuildOptions, ClippedShape, IndexCell, CELL_PADDING};
use super::cell_index_reader::CellIndexIter;
use super::crossings::S2EdgeCrosser;
use super::edge_reader::EdgeReader;
use super::r2rect::R2Rect;
use super::s2cell_id::S2CellId;
use super::s2coords::valid_face_xyz_to_uv;
use super::s2padded_cell::S2PaddedCell;

// ---------------------------------------------------------------------------
// Geometry ID mapping
// ---------------------------------------------------------------------------

/// Maps between old per-segment geometry IDs and new merged geometry IDs.
///
/// Source segments' geometry spaces are concatenated into a flat address
/// space. Segment 0 occupies [0, n0), segment 1 occupies [n0, n0+n1), etc.
/// A flat position uniquely identifies a geometry across all source segments.
///
/// The new-to-old vec grows as geometries are first encountered during the
/// Hilbert-order walk. At one geometry per document with 10M documents,
/// this is 40 MB.
pub struct GeometryMap {
    /// Cumulative geometry counts. `segment_offsets[i]` is the start of
    /// segment i's range in the flat space.
    segment_offsets: Vec<u32>,
    /// Indexed by flat old position. `Some(new_id)` if the geometry has been
    /// encountered and assigned a new ID, `None` otherwise.
    old_to_new: Vec<Option<u32>>,
    /// Indexed by new geometry ID. Value is the flat old position.
    new_to_old: Vec<u32>,
    /// Next new geometry ID to assign.
    next_id: u32,
}

impl GeometryMap {
    /// Creates a geometry map from per-segment geometry counts.
    pub fn new(segment_geometry_counts: &[u32]) -> Self {
        let mut offsets = Vec::with_capacity(segment_geometry_counts.len() + 1);
        let mut total = 0u32;
        for &count in segment_geometry_counts {
            offsets.push(total);
            total += count;
        }
        GeometryMap {
            segment_offsets: offsets,
            old_to_new: vec![None; total as usize],
            new_to_old: Vec::new(),
            next_id: 0,
        }
    }

    /// Flat position for a geometry from the given segment.
    fn flat_pos(&self, segment: usize, old_geometry_id: u32) -> u32 {
        self.segment_offsets[segment] + old_geometry_id
    }

    /// Recover (segment index, old geometry ID) from a flat position.
    fn unflatten(&self, flat: u32) -> (usize, u32) {
        let seg = match self.segment_offsets.binary_search(&flat) {
            Ok(i) => i,
            Err(i) => i - 1,
        };
        (seg, flat - self.segment_offsets[seg])
    }

    /// Look up or assign a new geometry ID for the given source geometry.
    /// Returns (new_id, true) on first encounter, (new_id, false) if already
    /// assigned. The `true` case is the signal for the edge writer to write
    /// this geometry — it won't be encountered again as "new."
    pub fn get_or_assign(&mut self, segment: usize, old_geometry_id: u32) -> (u32, bool) {
        let flat = self.flat_pos(segment, old_geometry_id);
        if let Some(new_id) = self.old_to_new[flat as usize] {
            (new_id, false)
        } else {
            let new_id = self.next_id;
            self.next_id += 1;
            self.old_to_new[flat as usize] = Some(new_id);
            self.new_to_old.push(flat);
            (new_id, true)
        }
    }

    /// Given a new geometry ID, return the source segment and old geometry ID.
    /// Used by the split stage to read vertices from the right source reader.
    pub fn source(&self, new_geometry_id: u32) -> (usize, u32) {
        let flat = self.new_to_old[new_geometry_id as usize];
        self.unflatten(flat)
    }
}

// ---------------------------------------------------------------------------
// Coarse cell distribution
// ---------------------------------------------------------------------------

/// A single edge held from a coarse cell for distribution into fine cells.
struct HeldEdge {
    v0: [f64; 3],
    v1: [f64; 3],
    a_uv: [f64; 2],
    b_uv: [f64; 2],
    edge_index: u16,
}

/// A geometry from a coarse cell, held for distribution.
struct HeldShape {
    new_geometry_id: u32,
    /// Whether this geometry contains the coarse cell's center.
    contains_center: bool,
    edges: Vec<HeldEdge>,
    /// UV bounding box of all edges, for early rejection against fine cells.
    _uv_bound: R2Rect,
}

/// A coarse cell whose edges are being distributed into the fine stream.
/// Lives in the `held` vec until the merge walks past its range.
struct HeldCoarse {
    cell_id: S2CellId,
    /// 3D center of the coarse cell, for contains_center ray-casting.
    center_3d: [f64; 3],
    shapes: Vec<HeldShape>,
}

impl HeldCoarse {
    /// True if `other` is a descendant of (or equal to) this cell.
    fn contains_cell(&self, other: S2CellId) -> bool {
        other.range_min() >= self.cell_id.range_min()
            && other.range_max() <= self.cell_id.range_max()
    }

    /// Distribute held shapes into a fine cell. Returns the shapes that
    /// intersect the fine cell or contain its center.
    fn distribute(&self, _fine_cell_id: S2CellId, fine_pcell: &S2PaddedCell) -> Vec<ClippedShape> {
        // S2PaddedCell::bound() needs to exist and return R2Rect for the
        // cell's UV bounds. If the API doesn't have it, compute from the
        // cell's ij-coordinates. See s2padded_cell.rs.
        let fine_bound = fine_pcell.bound();
        let fine_center_3d = fine_pcell.get_center();

        let mut result = Vec::new();

        for shape in &self.shapes {
            // Test each edge for intersection with the fine cell's UV bounds.
            let mut intersecting = Vec::new();
            for edge in &shape.edges {
                let edge_bound = R2Rect::from_point_pair(edge.a_uv, edge.b_uv);
                // Bounding box intersection is conservative — it may include
                // edges that don't actually cross the cell. This matches the
                // build path's behavior. Extra edges are harmless for
                // crossing tests; they just won't produce any crossings.
                if edge_bound.intersects(&fine_bound) {
                    intersecting.push(edge.edge_index);
                }
            }

            // Compute contains_center by ray-casting from the coarse cell's
            // center to the fine cell's center, counting crossings of ALL
            // edges in this shape. Not just the ones near the fine cell —
            // an edge far from the fine cell can still cross the ray between
            // the two centers.
            let contains = self.compute_contains_center(shape, &fine_center_3d);

            if !intersecting.is_empty() || contains {
                let mut clipped = ClippedShape::new(shape.new_geometry_id, contains);
                clipped.edge_indices = intersecting;
                result.push(clipped);
            }
        }

        result
    }

    /// Ray-cast from the coarse cell's center to the given point, counting
    /// crossings of the shape's edges. XOR with the shape's contains_center
    /// flag at the coarse center.
    fn compute_contains_center(&self, shape: &HeldShape, target: &[f64; 3]) -> bool {
        let mut crosser = S2EdgeCrosser::new(&self.center_3d, target);
        let mut crossings = 0u32;
        for edge in &shape.edges {
            if crosser.edge_or_vertex_crossing_two(&edge.v0, &edge.v1) {
                crossings += 1;
            }
        }
        shape.contains_center ^ (crossings % 2 != 0)
    }
}

// ---------------------------------------------------------------------------
// The merge pipeline
// ---------------------------------------------------------------------------

/// Cell index merge pipeline. Implements `Iterator<Item = IndexCell>`.
///
/// Construct with source cell index iterators, source edge readers, and
/// per-segment geometry counts. Pull cells with `next()`.
pub struct CellIndexMerge<'a> {
    /// Peekable iterators over each source segment's cell index.
    sources: Vec<PeekableCell<'a>>,
    /// Edge readers for each source segment. Mmap'd, read-only.
    edge_readers: Vec<EdgeReader<'a>>,
    /// Tracks old-to-new and new-to-old geometry ID mappings.
    geo_map: GeometryMap,
    /// Per-segment alive bitsets from Tantivy's delete-and-merge. `None`
    /// means all documents in that segment are alive. `Some(bitset)` means
    /// check `bitset.contains(doc_id)` before processing a geometry.
    alive_bitsets: &'a [Option<ReadOnlyBitSet>],
    /// Coarse cells currently being distributed into the fine stream.
    held: Vec<HeldCoarse>,
    /// Buffered cells from a split operation, waiting to be yielded.
    split_buffer: VecDeque<IndexCell>,
    /// Level-indexed sibling merge vector. Slot i holds pending siblings
    /// at level i. Face-level cells (level 0) have no parent to merge
    /// into and go straight to output.
    sibling_slots: Vec<Option<SiblingSlot>>,
    /// Cells flushed by the sibling stage, waiting to be yielded.
    sibling_buffer: VecDeque<IndexCell>,
    /// Face of the last cell seen by the sibling stage. Face transitions
    /// drain all slots because siblings never cross faces.
    last_face: Option<i32>,
    max_edges: usize,
    /// Minimum fraction of short edges required for subdivision. Matches
    /// the build path's `min_short_edge_fraction` so the merge produces
    /// the same cell structure as a fresh build.
    min_short_edge_fraction: f64,
}

// The caller owns the CellIndexReaders and passes their iterators in.
// CellIndexMerge can't own both readers and iterators because
// CellIndexIter borrows CellIndexReader (self-referential). This is
// the standard Rust pattern: the caller holds the readers, the merge
// holds the iterators.
struct PeekableCell<'a> {
    iter: CellIndexIter<'a>,
    peeked: Option<IndexCell>,
}

impl<'a> PeekableCell<'a> {
    fn new(iter: CellIndexIter<'a>) -> Self {
        PeekableCell { iter, peeked: None }
    }

    fn peek(&mut self) -> Option<&IndexCell> {
        if self.peeked.is_none() {
            self.peeked = self.iter.next();
        }
        self.peeked.as_ref()
    }

    fn next(&mut self) -> Option<IndexCell> {
        if let Some(cell) = self.peeked.take() {
            Some(cell)
        } else {
            self.iter.next()
        }
    }
}

/// A buffer slot for one level of the sibling merge vector.
struct SiblingSlot {
    /// Buffered cells (up to three, waiting for a fourth sibling).
    cells: Vec<IndexCell>,
    /// Running edge count across all buffered cells.
    edge_count: usize,
    /// Common parent of the buffered cells.
    parent_id: S2CellId,
}

/// Per-geometry state accumulated during sibling collapse.
struct CollapseEntry {
    geometry_id: u32,
    /// contains_center flag at the anchor child's center.
    anchor_flag: bool,
    /// 3D center of the anchor child (first child encountered).
    anchor_center: [f64; 3],
    /// Union of edge indices from all sibling cells.
    edge_indices: Vec<u16>,
}

impl<'a> CellIndexMerge<'a> {
    /// Creates a merge iterator over multiple segment cell indexes.
    pub fn new(
        iters: Vec<CellIndexIter<'a>>,
        edge_readers: Vec<EdgeReader<'a>>,
        segment_geometry_counts: &[u32],
        alive_bitsets: &'a [Option<ReadOnlyBitSet>],
    ) -> Self {
        let options = BuildOptions::default();
        CellIndexMerge {
            sources: iters.into_iter().map(PeekableCell::new).collect(),
            edge_readers,
            geo_map: GeometryMap::new(segment_geometry_counts),
            alive_bitsets,
            held: Vec::new(),
            split_buffer: VecDeque::new(),
            sibling_slots: (0..=S2CellId::MAX_LEVEL as usize).map(|_| None).collect(),
            sibling_buffer: VecDeque::new(),
            last_face: None,
            max_edges: options.max_edges_per_cell,
            min_short_edge_fraction: options.min_short_edge_fraction,
        }
    }

    /// Look up the source segment and old geometry ID for a merged geometry.
    pub fn geo_source(&self, new_geometry_id: u32) -> (usize, u32) {
        self.geo_map.source(new_geometry_id)
    }

    /// Read vertices for a geometry from its source segment's edge reader.
    pub fn read_vertices(&mut self, new_geometry_id: u32) -> (u32, Vec<[f64; 3]>) {
        let (seg, old_id) = self.geo_map.source(new_geometry_id);
        let geo_set = self.edge_readers[seg].get(old_id);
        let member_idx = (old_id - geo_set.geometry_id) as usize;
        (geo_set.doc_id, geo_set.vertices[member_idx].clone())
    }

    /// Read the full geometry set for a given new geometry ID. Returns the
    /// source segment, the member offset within the set, the doc_id, and
    /// all members' vertices.
    pub fn read_set(&mut self, new_geometry_id: u32) -> (usize, u32, u32, Vec<Vec<[f64; 3]>>) {
        let (seg, old_id) = self.geo_map.source(new_geometry_id);
        let geo_set = self.edge_readers[seg].get(old_id);
        let member_offset = old_id - geo_set.geometry_id;
        let doc_id = geo_set.doc_id;
        let vertices = geo_set.vertices.clone();
        (seg, member_offset, doc_id, vertices)
    }

    // -------------------------------------------------------------------
    // Stage 1: Interleave with coarse-into-fine distribution
    // -------------------------------------------------------------------

    fn next_interleaved(&mut self) -> Option<IndexCell> {
        loop {
            // Find the minimum range_min among all source fronts.
            let mut min_rm: Option<S2CellId> = None;
            for source in &mut self.sources {
                if let Some(cell) = source.peek() {
                    let rm = cell.cell_id.range_min();
                    if min_rm.is_none() || rm < min_rm.unwrap() {
                        min_rm = Some(rm);
                    }
                }
            }
            let min_rm = min_rm?;

            // Release held coarse cells whose range we've passed.
            self.held.retain(|h| h.cell_id.range_max() >= min_rm);

            // Collect indices of sources whose front cell starts at min_rm.
            let mut at_position: Vec<usize> = Vec::new();
            for (i, source) in self.sources.iter_mut().enumerate() {
                if let Some(cell) = source.peek() {
                    if cell.cell_id.range_min() == min_rm {
                        at_position.push(i);
                    }
                }
            }

            // Find the finest level among cells at this position.
            let max_level = at_position
                .iter()
                .map(|&i| self.sources[i].peeked.as_ref().unwrap().cell_id.level())
                .max()
                .unwrap();

            // Separate into fine and coarse.
            let mut fine_indices: Vec<usize> = Vec::new();
            let mut coarse_indices: Vec<usize> = Vec::new();
            for &i in &at_position {
                let level = self.sources[i].peeked.as_ref().unwrap().cell_id.level();
                if level == max_level {
                    fine_indices.push(i);
                } else {
                    coarse_indices.push(i);
                }
            }

            // Hold coarse cells for distribution. Read their edge vertices
            // now so we can distribute into fine cells as they stream past.
            for &i in &coarse_indices {
                let cell = self.sources[i].next().unwrap();
                let held = self.make_held_coarse(i, &cell);
                self.held.push(held);
            }

            // Combine fine cells. Multiple sources at the same level with
            // the same cell_id means the same cell from different segments.
            let first = fine_indices[0];
            let mut combined = self.sources[first].next().unwrap();
            self.remap_shapes(first, &mut combined);

            for &i in &fine_indices[1..] {
                let mut cell = self.sources[i].next().unwrap();
                self.remap_shapes(i, &mut cell);
                combined.shapes.extend(cell.shapes);
            }

            // Distribute held coarse edges into this fine cell.
            for held in &self.held {
                if held.contains_cell(combined.cell_id) {
                    let pcell = S2PaddedCell::new(combined.cell_id, CELL_PADDING);
                    let extra = held.distribute(combined.cell_id, &pcell);
                    combined.shapes.extend(extra);
                }
            }

            // Deletion filtering can remove all shapes from a cell.
            // Skip empty cells rather than yielding them downstream.
            if combined.shapes.is_empty() {
                continue;
            }

            return Some(combined);
        }
    }

    /// Remap geometry IDs in a cell's shapes from source-segment IDs to
    /// new merged IDs. Shapes for deleted documents are dropped — the
    /// edge reader gives us the doc_id and the alive bitset says whether
    /// the document survived Tantivy's delete-and-merge.
    fn remap_shapes(&mut self, segment: usize, cell: &mut IndexCell) {
        // Drop shapes whose documents were deleted. The alive bitset
        // for the segment is None when all documents are alive.
        if let Some(bitset) = &self.alive_bitsets[segment] {
            let reader = &mut self.edge_readers[segment];
            cell.shapes.retain(|shape| {
                let doc_id = reader.get(shape.geometry_id).doc_id;
                bitset.contains(doc_id)
            });
        }

        // Remap surviving shapes to new geometry IDs. When a geometry is
        // first encountered, assign IDs for all members of its set so they
        // get consecutive new IDs. The edge writer needs consecutive IDs
        // to write the set with correct set indicators and one doc_id footer.
        for shape in &mut cell.shapes {
            let (new_id, first_encounter) = self.geo_map.get_or_assign(segment, shape.geometry_id);
            if first_encounter {
                let geo_set = self.edge_readers[segment].get(shape.geometry_id);
                let set_size = geo_set.vertices.len() as u32;
                for i in 1..set_size {
                    let sibling_old_id = geo_set.geometry_id + i;
                    self.geo_map.get_or_assign(segment, sibling_old_id);
                }
            }
            shape.geometry_id = new_id;
        }
    }

    /// Build a HeldCoarse from a coarse cell. Reads edge vertices from the
    /// source segment's edge reader, projects to UV, and stores for
    /// distribution.
    fn make_held_coarse(&mut self, segment: usize, cell: &IndexCell) -> HeldCoarse {
        let face = cell.cell_id.face();
        let pcell = S2PaddedCell::new(cell.cell_id, CELL_PADDING);
        let center_3d = pcell.get_center();

        let mut shapes = Vec::new();

        for clipped in &cell.shapes {
            // Skip shapes for deleted documents.
            let doc_id = self.edge_readers[segment].get(clipped.geometry_id).doc_id;
            if let Some(bitset) = &self.alive_bitsets[segment] {
                if !bitset.contains(doc_id) {
                    continue;
                }
            }

            let (new_id, _) = self.geo_map.get_or_assign(segment, clipped.geometry_id);

            // Read vertices from the source edge reader. Second cache
            // lookup is a hit — we just called get() for the doc_id check.
            let geo_set = self.edge_readers[segment].get(clipped.geometry_id);
            let member_idx = (clipped.geometry_id - geo_set.geometry_id) as usize;
            // EdgeReader::get returns a borrow tied to the reader's LRU
            // cache. Clone the vertices so they outlive the borrow. A
            // coarse cell has at most 10 edges, so the clone is small.
            let vertices = geo_set.vertices[member_idx].clone();

            let mut edges = Vec::new();
            let mut uv_bound = R2Rect::empty();

            for &edge_idx in &clipped.edge_indices {
                let v0 = vertices[edge_idx as usize];
                let v1 = vertices[(edge_idx as usize) + 1];

                let (a_u, a_v) = valid_face_xyz_to_uv(face, &v0);
                let (b_u, b_v) = valid_face_xyz_to_uv(face, &v1);
                let a_uv = [a_u, a_v];
                let b_uv = [b_u, b_v];

                let edge_bound = R2Rect::from_point_pair(a_uv, b_uv);
                uv_bound = uv_bound.union(&edge_bound);

                edges.push(HeldEdge {
                    v0,
                    v1,
                    a_uv,
                    b_uv,
                    edge_index: edge_idx,
                });
            }

            shapes.push(HeldShape {
                new_geometry_id: new_id,
                contains_center: clipped.contains_center,
                edges,
                _uv_bound: uv_bound,
            });
        }

        HeldCoarse {
            cell_id: cell.cell_id,
            center_3d,
            shapes,
        }
    }

    // -------------------------------------------------------------------
    // Stage 2: Split
    // -------------------------------------------------------------------

    fn next_split(&mut self) -> Option<IndexCell> {
        // Drain the buffer first.
        if let Some(cell) = self.split_buffer.pop_front() {
            return Some(cell);
        }

        let cell = self.next_interleaved()?;

        let total_edges: usize = cell.shapes.iter().map(|s| s.num_edges()).sum();
        if total_edges <= self.max_edges {
            return Some(cell);
        }

        // Cell is over the limit. Subdivide and buffer the results.
        self.subdivide_cell(&cell);
        self.split_buffer.pop_front()
    }

    /// Subdivide a cell that exceeds the edge limit. Reads vertices from
    /// the source edge readers, projects to UV, and recursively splits
    /// using the same clip_to_children logic as the build path. Results
    /// go into split_buffer in cell_id sorted order.
    fn subdivide_cell(&mut self, cell: &IndexCell) {
        let face = cell.cell_id.face();
        let pcell = S2PaddedCell::new(cell.cell_id, CELL_PADDING);

        // Collect all edges with their 3D and UV representations.
        let mut merge_edges: Vec<MergeEdge> = Vec::new();

        for shape in &cell.shapes {
            let (seg, old_id) = self.geo_map.source(shape.geometry_id);
            let geo_set = self.edge_readers[seg].get(old_id);
            let member_idx = (old_id - geo_set.geometry_id) as usize;
            let vertices = geo_set.vertices[member_idx].clone();

            for &edge_idx in &shape.edge_indices {
                let v0 = vertices[edge_idx as usize];
                let v1 = vertices[(edge_idx as usize) + 1];
                let (a_u, a_v) = valid_face_xyz_to_uv(face, &v0);
                let (b_u, b_v) = valid_face_xyz_to_uv(face, &v1);

                // Recompute max_level the same way the build path does in
                // get_edge_max_level. Long edges (those longer than the cell
                // at a given level) don't count toward the subdivision
                // threshold — subdividing won't help because the edge
                // appears in every child.
                let max_level = get_edge_max_level(&v0, &v1);

                merge_edges.push(MergeEdge {
                    geometry_id: shape.geometry_id,
                    edge_index: edge_idx,
                    max_level,
                    v0,
                    v1,
                    a_uv: [a_u, a_v],
                    b_uv: [b_u, b_v],
                });
            }
        }

        // Build initial clipped-edge bounds for subdivision.
        let clipped: Vec<MergeClippedEdge> = merge_edges
            .iter()
            .enumerate()
            .map(|(i, e)| MergeClippedEdge {
                edge_index: i,
                bound: R2Rect::from_point_pair(e.a_uv, e.b_uv),
            })
            .collect();

        // Collect parent contains_center flags for the ray-cast base.
        let parent_contains: Vec<(u32, bool)> = cell
            .shapes
            .iter()
            .map(|s| (s.geometry_id, s.contains_center))
            .collect();

        // Recurse. At the top level, parent_clipped == clipped because we
        // have all the cell's edges.
        let parent_center = pcell.get_center();
        self.subdivide_recursive(
            &pcell,
            &parent_center,
            &merge_edges,
            &clipped,
            &clipped,
            &parent_contains,
        );
    }

    /// Recursive subdivision. Mirrors `update_edges` in cell_index.rs but
    /// uses local contains_center computation instead of InteriorTracker.
    ///
    /// The contains_center ray-cast at each level uses ALL edges from
    /// the parent level, not just those clipped into a particular child.
    /// An edge in the parent but outside a child can still cross the ray
    /// between the parent center and child center.
    fn subdivide_recursive(
        &mut self,
        pcell: &S2PaddedCell,
        parent_center: &[f64; 3],
        all_edges: &[MergeEdge],
        clipped: &[MergeClippedEdge],
        parent_clipped: &[MergeClippedEdge],
        parent_contains: &[(u32, bool)],
    ) {
        // Stop subdivision when:
        //  - we've reached MAX_LEVEL (no children to create), or
        //  - the count of "short" edges is within the threshold
        //
        // The threshold formula matches the build path exactly:
        //   max(max_edges, min_short_edge_fraction * (edges + shapes))
        // This guarantees linear index size. The shapes term counts
        // geometries tracked in parent_contains, mirroring the build
        // path's tracker.shape_ids().len().
        let should_subdivide = pcell.level() < S2CellId::MAX_LEVEL && {
            let max_short_edges = self.max_edges.max(
                (self.min_short_edge_fraction * (clipped.len() + parent_contains.len()) as f64)
                    as usize,
            );
            let short_count = clipped
                .iter()
                .filter(|ce| pcell.level() < all_edges[ce.edge_index].max_level)
                .count();
            short_count > max_short_edges
        };

        if !should_subdivide {
            // Leaf cell. Compute contains_center and emit.
            let cell_center = pcell.get_center();
            let mut index_cell = IndexCell::new(pcell.id());

            // Group edges by geometry_id. Use a map to handle
            // non-contiguous geometry_ids in the clipped list.
            let mut edge_map: Vec<(u32, Vec<u16>)> = Vec::new();
            for ce in clipped {
                let me = &all_edges[ce.edge_index];
                if let Some((_, edges)) =
                    edge_map.iter_mut().find(|(gid, _)| *gid == me.geometry_id)
                {
                    edges.push(me.edge_index);
                } else {
                    edge_map.push((me.geometry_id, vec![me.edge_index]));
                }
            }

            // Emit shapes that have edges in this cell.
            let mut emitted_gids: Vec<u32> = Vec::new();
            for (geometry_id, edge_indices) in &edge_map {
                let parent_flag = parent_contains
                    .iter()
                    .find(|(gid, _)| gid == geometry_id)
                    .map(|(_, c)| *c)
                    .unwrap_or(false);

                // Ray-cast uses ALL parent edges for this geometry, not
                // just the child's clipped subset.
                let contains = compute_contains_center_local(
                    parent_center,
                    &cell_center,
                    parent_flag,
                    all_edges,
                    parent_clipped,
                    *geometry_id,
                );

                let mut shape = ClippedShape::new(*geometry_id, contains);
                shape.edge_indices = edge_indices.clone();
                index_cell.add_shape(shape);
                emitted_gids.push(*geometry_id);
            }

            // Emit interior-only shapes: geometries that contain this
            // cell's center but have no edges here. The build path keeps
            // these (the "dominated by geometry with no edges" case).
            for &(gid, parent_flag) in parent_contains {
                if emitted_gids.contains(&gid) {
                    continue;
                }
                let contains = compute_contains_center_local(
                    parent_center,
                    &cell_center,
                    parent_flag,
                    all_edges,
                    parent_clipped,
                    gid,
                );
                if contains {
                    index_cell.add_shape(ClippedShape::new(gid, true));
                }
            }

            if !index_cell.is_empty() {
                self.split_buffer.push_back(index_cell);
            }
            return;
        }

        // Subdivide into four children.
        let middle = pcell.middle();
        let mut child_edges: [[Vec<MergeClippedEdge>; 2]; 2] = Default::default();

        for ce in clipped {
            let me = &all_edges[ce.edge_index];
            clip_to_children_merge(ce, me, &middle, &mut child_edges);
        }

        // Compute contains_center at this level for propagation.
        // Uses all parent edges, not just the child's subset.
        let this_center = pcell.get_center();
        let this_contains: Vec<(u32, bool)> = parent_contains
            .iter()
            .map(|&(gid, parent_flag)| {
                let contains = compute_contains_center_local(
                    parent_center,
                    &this_center,
                    parent_flag,
                    all_edges,
                    parent_clipped,
                    gid,
                );
                (gid, contains)
            })
            .collect();

        for pos in 0..4i32 {
            let (i, j) = pcell.get_child_ij(pos);
            let child_pcell = S2PaddedCell::from_parent(pcell, i, j);

            if child_edges[i][j].is_empty() {
                // Empty child — no edges clipped into it. It still
                // matters if a geometry's interior covers it entirely.
                // Ray-cast from this cell's center to the child's center
                // for each tracked geometry. An edge in a sibling child
                // can cross this ray, making the child interior-only
                // even when this cell's center is outside the geometry.
                let child_center = child_pcell.get_center();
                let has_containing = this_contains.iter().any(|&(gid, this_flag)| {
                    compute_contains_center_local(
                        &this_center,
                        &child_center,
                        this_flag,
                        all_edges,
                        clipped,
                        gid,
                    )
                });
                if !has_containing {
                    continue;
                }
            }

            self.subdivide_recursive(
                &child_pcell,
                &this_center,
                all_edges,
                &child_edges[i][j],
                clipped,
                &this_contains,
            );
        }
    }

    // -------------------------------------------------------------------
    // Stage 3: Merge siblings
    // -------------------------------------------------------------------

    fn next_sibling_merged(&mut self) -> Option<IndexCell> {
        if let Some(cell) = self.sibling_buffer.pop_front() {
            return Some(cell);
        }

        loop {
            let cell = match self.next_split() {
                Some(c) => c,
                None => {
                    self.flush_all_sibling_slots();
                    return self.sibling_buffer.pop_front();
                }
            };

            let level = cell.cell_id.level();
            let face = cell.cell_id.face();

            if let Some(last) = self.last_face {
                if last != face {
                    self.flush_all_sibling_slots();
                }
            }
            self.last_face = Some(face);

            self.flush_sibling_slots_deeper_than(level);

            if level == 0 {
                self.sibling_buffer.push_back(cell);
                return self.sibling_buffer.pop_front();
            }

            self.ingest_sibling_cell(cell);

            if let Some(result) = self.sibling_buffer.pop_front() {
                return Some(result);
            }
        }
    }

    /// Buffer a cell into its level's sibling slot, flushing the
    /// existing slot first if the cell is not a sibling.
    fn ingest_sibling_cell(&mut self, cell: IndexCell) {
        let level = cell.cell_id.level() as usize;
        let parent_id = cell.cell_id.immediate_parent();
        let edge_count: usize = cell.shapes.iter().map(|s| s.num_edges()).sum();

        debug_assert!(
            !self.sibling_slots.iter().any(|s| {
                s.as_ref().map_or(false, |slot| {
                    slot.cells.iter().any(|b| cell.cell_id.contains(b.cell_id))
                })
            }),
            "incoming cell contains a buffered cell — upstream ordering bug"
        );

        let is_sibling = self.sibling_slots[level]
            .as_ref()
            .map_or(false, |slot| slot.parent_id == parent_id);

        if is_sibling {
            let slot = self.sibling_slots[level].as_mut().unwrap();

            debug_assert!(
                !slot
                    .cells
                    .iter()
                    .any(|c| { c.cell_id.child_position() == cell.cell_id.child_position() }),
                "duplicate child position in sibling slot"
            );
            debug_assert!(
                slot.cells.last().map_or(true, |c| c.cell_id < cell.cell_id),
                "sibling cells arriving out of Hilbert order"
            );

            slot.cells.push(cell);
            slot.edge_count += edge_count;

            if slot.cells.len() == 4 {
                let slot = self.sibling_slots[level].take().unwrap();
                self.complete_sibling_group(slot);
            }
        } else {
            self.flush_sibling_slot(level);
            self.sibling_slots[level] = Some(SiblingSlot {
                cells: vec![cell],
                edge_count,
                parent_id,
            });
        }
    }

    /// Four siblings arrived. Merge into parent if under threshold,
    /// otherwise flush all four.
    fn complete_sibling_group(&mut self, slot: SiblingSlot) {
        if slot.edge_count <= self.merge_threshold(&slot) {
            let parent_cell = self.collapse_siblings(slot);
            self.push_merged_parent(parent_cell);
        } else {
            for c in slot.cells {
                self.sibling_buffer.push_back(c);
            }
        }
    }

    /// Push a collapsed parent cell into the sibling vector at its
    /// level. This is how recursive merging works — a merged level 25
    /// parent waits for three more level 25 siblings, which may
    /// themselves have merged up from level 26.
    fn push_merged_parent(&mut self, cell: IndexCell) {
        if cell.cell_id.level() == 0 {
            self.sibling_buffer.push_back(cell);
            return;
        }
        self.ingest_sibling_cell(cell);
    }

    /// Merge threshold matching the build and split paths. Conservative:
    /// uses total edge count as proxy for short edges since we don't
    /// read vertices for max_level here.
    fn merge_threshold(&self, slot: &SiblingSlot) -> usize {
        let mut unique_gids: Vec<u32> = Vec::new();
        for cell in &slot.cells {
            for shape in &cell.shapes {
                if !unique_gids.contains(&shape.geometry_id) {
                    unique_gids.push(shape.geometry_id);
                }
            }
        }
        self.max_edges.max(
            (self.min_short_edge_fraction * (slot.edge_count + unique_gids.len()) as f64) as usize,
        )
    }

    /// Collapse four sibling cells into their parent. Unions edge
    /// indices per geometry, deduplicates, and computes contains_center
    /// at the parent center by ray-casting with the full sibling edge
    /// set — not just the anchor child's edges.
    fn collapse_siblings(&mut self, slot: SiblingSlot) -> IndexCell {
        let parent_id = slot.parent_id;
        let parent_pcell = S2PaddedCell::new(parent_id, CELL_PADDING);
        let parent_center = parent_pcell.get_center();

        let mut entries: Vec<CollapseEntry> = Vec::new();

        for cell in &slot.cells {
            let child_pcell = S2PaddedCell::new(cell.cell_id, CELL_PADDING);
            let child_center = child_pcell.get_center();

            for shape in &cell.shapes {
                if let Some(entry) = entries
                    .iter_mut()
                    .find(|e| e.geometry_id == shape.geometry_id)
                {
                    entry.edge_indices.extend_from_slice(&shape.edge_indices);
                } else {
                    entries.push(CollapseEntry {
                        geometry_id: shape.geometry_id,
                        anchor_flag: shape.contains_center,
                        anchor_center: child_center,
                        edge_indices: shape.edge_indices.clone(),
                    });
                }
            }
        }

        for entry in &mut entries {
            entry.edge_indices.sort_unstable();
            entry.edge_indices.dedup();
        }

        let mut parent_cell = IndexCell::new(parent_id);

        for entry in &entries {
            let (seg, old_id) = self.geo_map.source(entry.geometry_id);
            let geo_set = self.edge_readers[seg].get(old_id);
            let member_idx = (old_id - geo_set.geometry_id) as usize;
            let vertices = geo_set.vertices[member_idx].clone();

            let mut crosser = S2EdgeCrosser::new(&entry.anchor_center, &parent_center);
            let mut crossings = 0u32;
            for &edge_idx in &entry.edge_indices {
                let v0 = vertices[edge_idx as usize];
                let v1 = vertices[(edge_idx as usize) + 1];
                if crosser.edge_or_vertex_crossing_two(&v0, &v1) {
                    crossings += 1;
                }
            }
            let contains = entry.anchor_flag ^ (crossings % 2 != 0);

            let mut shape = ClippedShape::new(entry.geometry_id, contains);
            shape.edge_indices = entry.edge_indices.clone();
            parent_cell.add_shape(shape);
        }

        parent_cell
    }

    /// Flush a single sibling slot, emitting its cells to the buffer.
    fn flush_sibling_slot(&mut self, level: usize) {
        if let Some(slot) = self.sibling_slots[level].take() {
            for cell in slot.cells {
                self.sibling_buffer.push_back(cell);
            }
        }
    }

    /// Flush all sibling slots deeper than the given level, shallowest
    /// first to preserve Hilbert order.
    fn flush_sibling_slots_deeper_than(&mut self, level: i32) {
        for l in (level as usize + 1)..self.sibling_slots.len() {
            self.flush_sibling_slot(l);
        }
    }

    /// Flush all sibling slots, shallowest first.
    fn flush_all_sibling_slots(&mut self) {
        for l in 0..self.sibling_slots.len() {
            self.flush_sibling_slot(l);
        }
    }
}

impl<'a> Iterator for CellIndexMerge<'a> {
    type Item = IndexCell;

    fn next(&mut self) -> Option<IndexCell> {
        self.next_sibling_merged()
    }
}

// ---------------------------------------------------------------------------
// Merge-specific edge types
// ---------------------------------------------------------------------------

/// An edge with both 3D and UV representations, used during subdivision.
/// Carries max_level from the build path's long-edge classification so
/// the split stage knows when to stop subdividing. Without this, long
/// edges that span many levels would recurse to MAX_LEVEL.
struct MergeEdge {
    geometry_id: u32,
    edge_index: u16,
    max_level: i32,
    v0: [f64; 3],
    v1: [f64; 3],
    a_uv: [f64; 2],
    b_uv: [f64; 2],
}

/// A clipped edge during subdivision — just an index into the MergeEdge vec
/// plus a UV bounding box that gets narrowed as we descend.
#[derive(Clone)]
struct MergeClippedEdge {
    /// Index into the MergeEdge vec.
    edge_index: usize,
    /// Bounding box of the clipped portion in UV space.
    bound: R2Rect,
}

// ---------------------------------------------------------------------------
// Standalone subdivision helpers
// ---------------------------------------------------------------------------

/// Distribute a clipped edge to the four children of a cell.
/// Same math as clip_to_children in cell_index.rs — the clipping logic
/// should eventually be extracted into shared free functions taking
/// (a_uv, b_uv, bound) tuples rather than duplicated here.
fn clip_to_children_merge(
    ce: &MergeClippedEdge,
    me: &MergeEdge,
    middle: &R2Rect,
    child_edges: &mut [[Vec<MergeClippedEdge>; 2]; 2],
) {
    let u_mid_lo = middle[0].lo();
    let u_mid_hi = middle[0].hi();
    let v_mid_lo = middle[1].lo();
    let v_mid_hi = middle[1].hi();

    if ce.bound[0].hi() <= u_mid_lo {
        clip_v_axis_merge(ce, me, v_mid_lo, v_mid_hi, &mut child_edges[0]);
    } else if ce.bound[0].lo() >= u_mid_hi {
        clip_v_axis_merge(ce, me, v_mid_lo, v_mid_hi, &mut child_edges[1]);
    } else if ce.bound[1].hi() <= v_mid_lo {
        child_edges[0][0].push(clip_u_bound_merge(ce, me, u_mid_hi, true));
        child_edges[1][0].push(clip_u_bound_merge(ce, me, u_mid_lo, false));
    } else if ce.bound[1].lo() >= v_mid_hi {
        child_edges[0][1].push(clip_u_bound_merge(ce, me, u_mid_hi, true));
        child_edges[1][1].push(clip_u_bound_merge(ce, me, u_mid_lo, false));
    } else {
        let left = clip_u_bound_merge(ce, me, u_mid_hi, true);
        clip_v_axis_merge(&left, me, v_mid_lo, v_mid_hi, &mut child_edges[0]);
        let right = clip_u_bound_merge(ce, me, u_mid_lo, false);
        clip_v_axis_merge(&right, me, v_mid_lo, v_mid_hi, &mut child_edges[1]);
    }
}

fn clip_v_axis_merge(
    ce: &MergeClippedEdge,
    me: &MergeEdge,
    v_mid_lo: f64,
    v_mid_hi: f64,
    child_edges: &mut [Vec<MergeClippedEdge>; 2],
) {
    if ce.bound[1].hi() <= v_mid_lo {
        child_edges[0].push(ce.clone());
    } else if ce.bound[1].lo() >= v_mid_hi {
        child_edges[1].push(ce.clone());
    } else {
        child_edges[0].push(clip_v_bound_merge(ce, me, v_mid_hi, true));
        child_edges[1].push(clip_v_bound_merge(ce, me, v_mid_lo, false));
    }
}

/// Clip the u-axis bound. Adapted from IndexBuilder::clip_u_bound.
fn clip_u_bound_merge(
    ce: &MergeClippedEdge,
    me: &MergeEdge,
    u: f64,
    clip_hi: bool,
) -> MergeClippedEdge {
    use super::r1interval::R1Interval;

    if clip_hi {
        if ce.bound[0].hi() <= u {
            return ce.clone();
        }
    } else if ce.bound[0].lo() >= u {
        return ce.clone();
    }

    let v = interpolate_double(u, me.a_uv[0], me.b_uv[0], me.a_uv[1], me.b_uv[1]);
    let v_clamped = ce.bound[1].clamp(v);

    let slopes_same_sign = (me.a_uv[0] > me.b_uv[0]) == (me.a_uv[1] > me.b_uv[1]);
    let clip_v_hi = if clip_hi {
        slopes_same_sign
    } else {
        !slopes_same_sign
    };

    let new_bound = if clip_hi {
        R2Rect::new(
            R1Interval::new(ce.bound[0].lo(), u),
            if clip_v_hi {
                R1Interval::new(ce.bound[1].lo(), v_clamped)
            } else {
                R1Interval::new(v_clamped, ce.bound[1].hi())
            },
        )
    } else {
        R2Rect::new(
            R1Interval::new(u, ce.bound[0].hi()),
            if clip_v_hi {
                R1Interval::new(ce.bound[1].lo(), v_clamped)
            } else {
                R1Interval::new(v_clamped, ce.bound[1].hi())
            },
        )
    };

    MergeClippedEdge {
        edge_index: ce.edge_index,
        bound: new_bound,
    }
}

/// Clip the v-axis bound. Adapted from IndexBuilder::clip_v_bound.
fn clip_v_bound_merge(
    ce: &MergeClippedEdge,
    me: &MergeEdge,
    v: f64,
    clip_hi: bool,
) -> MergeClippedEdge {
    use super::r1interval::R1Interval;

    if clip_hi {
        if ce.bound[1].hi() <= v {
            return ce.clone();
        }
    } else if ce.bound[1].lo() >= v {
        return ce.clone();
    }

    let u = interpolate_double(v, me.a_uv[1], me.b_uv[1], me.a_uv[0], me.b_uv[0]);
    let u_clamped = ce.bound[0].clamp(u);

    let slopes_same_sign = (me.a_uv[0] > me.b_uv[0]) == (me.a_uv[1] > me.b_uv[1]);
    let clip_u_hi = if clip_hi {
        slopes_same_sign
    } else {
        !slopes_same_sign
    };

    let new_bound = if clip_hi {
        R2Rect::new(
            if clip_u_hi {
                R1Interval::new(ce.bound[0].lo(), u_clamped)
            } else {
                R1Interval::new(u_clamped, ce.bound[0].hi())
            },
            R1Interval::new(ce.bound[1].lo(), v),
        )
    } else {
        R2Rect::new(
            if clip_u_hi {
                R1Interval::new(ce.bound[0].lo(), u_clamped)
            } else {
                R1Interval::new(u_clamped, ce.bound[0].hi())
            },
            R1Interval::new(v, ce.bound[1].hi()),
        )
    };

    MergeClippedEdge {
        edge_index: ce.edge_index,
        bound: new_bound,
    }
}

/// Interpolate a value along a line segment. Same as cell_index.rs;
/// extract to a shared location.
#[inline]
fn interpolate_double(x: f64, x0: f64, x1: f64, y0: f64, y1: f64) -> f64 {
    if x0 == x1 {
        return y0;
    }
    if (x0 - x).abs() <= (x1 - x).abs() {
        y0 + (y1 - y0) * ((x - x0) / (x1 - x0))
    } else {
        y1 + (y0 - y1) * ((x - x1) / (x0 - x1))
    }
}

/// Compute contains_center at a target point by ray-casting from a known
/// point with a known flag. Counts crossings of edges belonging to the
/// given geometry_id.
fn compute_contains_center_local(
    from: &[f64; 3],
    to: &[f64; 3],
    from_flag: bool,
    all_edges: &[MergeEdge],
    clipped: &[MergeClippedEdge],
    geometry_id: u32,
) -> bool {
    let mut crosser = S2EdgeCrosser::new(from, to);
    let mut crossings = 0u32;
    for ce in clipped {
        let me = &all_edges[ce.edge_index];
        if me.geometry_id == geometry_id {
            if crosser.edge_or_vertex_crossing_two(&me.v0, &me.v1) {
                crossings += 1;
            }
        }
    }
    from_flag ^ (crossings % 2 != 0)
}

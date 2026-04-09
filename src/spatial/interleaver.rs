//! Heap-based merge of N source cell indexes. Produces cells in Hilbert order. Subdivides coarse
//! cells on demand when they overlap finer cells or exceed the edge limit.

use std::cmp::Ordering;
use std::collections::BinaryHeap;

use common::ReadOnlyBitSet;

use crate::spatial::clip_options::ClipOptions;
use crate::spatial::clipped_shape::{ClippedShape, GeometryId};
use crate::spatial::clipper::{get_edge_max_level, CELL_PADDING};
use crate::spatial::r1interval::R1Interval;
use crate::spatial::shape_index::ShapeCell;

use super::cell_index_reader::CellIndexIter;
use super::edge_cache::EdgeCache;
use super::r2rect::R2Rect;
use super::s2cell_id::S2CellId;
use super::s2edge_clipping::{clip_edge_bound, clip_to_padded_face};
use super::s2padded_cell::S2PaddedCell;
use super::sphere::Sphere;

struct SpongeEdge {
    geometry_id: GeometryId,
    edge_index: u32,
    max_level: i32,
    v0: [f64; 3],
    v1: [f64; 3],
    a_uv: [f64; 2],
    b_uv: [f64; 2],
    bound: R2Rect,
}

// Anchors represent the presence of a geometry in a particular cell so that anchors are unique by
// geometry id and cell id. Anchors do not need de-duplication.
#[derive(Clone)]
struct SpongeShape {
    geometry_id: GeometryId,
    cell_id: S2CellId,
    center: [f64; 3],
    contains_center: bool,
}

// Exploded view of an ShapeCell.
//
// Edges are de-duplicated upon insert so we can maintain an accurate count of edges and short
// edges in the pending cell for our threshold checks. We split based on short edge count.
// De-duplication is necessary because, of course, the same (geometry_id, edge_index) edge can
// appear in multiple cells.
//
// Anchors represent the presence of a geometry in a particular cell so that anchors are unique by
// geometry id and cell id. Anchors do not need de-duplication.
struct SpongeCell {
    cell_id: S2CellId,
    edges: Vec<SpongeEdge>,
    anchors: Vec<SpongeShape>,
    edge_count: usize,
    short_edge_count: usize,
    cell_bound: R2Rect,
    split_count: u32,
}

impl SpongeCell {
    fn new(cell_id: S2CellId, cell_bound: R2Rect) -> Self {
        SpongeCell {
            cell_id,
            edges: Vec::new(),
            anchors: Vec::new(),
            edge_count: 0,
            short_edge_count: 0,
            cell_bound,
            split_count: 0,
        }
    }

    // Both paths clip edge UV endpoints to this sponge's cell bound using clip_edge. This produces
    // endpoints within the sponge's region, consistent with the build path's ClipToPaddedFace. The
    // clipped endpoints are stable through subsequent splits. Only the bound narrows at each split
    // level.
    fn absorb_index_cell_edges(&mut self, cell: &ShapeCell, edge_cache: &EdgeCache<'_, Sphere>) {
        let face = cell.cell_id.face();
        for shape in &cell.shapes {
            if shape.edge_indices.is_empty() {
                continue;
            }
            let cache_entry = edge_cache.get(shape.geometry_id);
            for &edge_index in &shape.edge_indices {
                let key = (shape.geometry_id, edge_index);
                match self
                    .edges
                    .binary_search_by_key(&key, |e| (e.geometry_id, e.edge_index))
                {
                    Ok(_) => {}
                    Err(pos) => {
                        let (v0, v1) = cache_entry.edge(edge_index);
                        let (a_uv, b_uv) = match clip_to_padded_face(
                            &v0, &v1, face, CELL_PADDING,
                        ) {
                            Some(ab) => ab,
                            None => continue,
                        };
                        let mut bound = R2Rect::from_point_pair(a_uv, b_uv);
                        if !clip_edge_bound(a_uv, b_uv, &self.cell_bound, &mut bound) {
                            continue;
                        }
                        let max_level = get_edge_max_level(&v0, &v1);
                        self.edge_count += 1;
                        if self.cell_id.level() < max_level {
                            self.short_edge_count += 1;
                        }
                        self.edges.insert(
                            pos,
                            SpongeEdge {
                                geometry_id: shape.geometry_id,
                                edge_index,
                                max_level,
                                v0,
                                v1,
                                a_uv,
                                b_uv,
                                bound,
                            },
                        );
                    }
                }
            }
        }
    }

    fn absorb_sponge_cell_edges(&mut self, merged: &SpongeCell) {
        // The a_uv/b_uv on incoming edges are face-level. Clip the bound to this sponge.
        for other_edge in &merged.edges {
            let key = (other_edge.geometry_id, other_edge.edge_index);
            match self
                .edges
                .binary_search_by_key(&key, |e| (e.geometry_id, e.edge_index))
            {
                Ok(_) => {}
                Err(pos) => {
                    let mut bound = R2Rect::from_point_pair(other_edge.a_uv, other_edge.b_uv);
                    assert!(clip_edge_bound(
                        other_edge.a_uv, other_edge.b_uv, &self.cell_bound, &mut bound,
                    ));
                    self.edge_count += 1;
                    if self.cell_id.level() < other_edge.max_level {
                        self.short_edge_count += 1;
                    }
                    self.edges.insert(
                        pos,
                        SpongeEdge {
                            geometry_id: other_edge.geometry_id,
                            edge_index: other_edge.edge_index,
                            max_level: other_edge.max_level,
                            v0: other_edge.v0,
                            v1: other_edge.v1,
                            a_uv: other_edge.a_uv,
                            b_uv: other_edge.b_uv,
                            bound,
                        },
                    );
                }
            }
        }
    }

    // The same center that you would use if you were searching. We going to do an ordinary search
    // edge crossing to move a child geometry into a parent.
    fn absorb_index_cell_anchors(&mut self, cell: &ShapeCell) {
        let mut cell_center: Option<[f64; 3]> = None;
        for shape in &cell.shapes {
            let center = *cell_center
                .get_or_insert_with(|| S2PaddedCell::new(cell.cell_id, CELL_PADDING).get_center());
            let anchor = SpongeShape {
                geometry_id: shape.geometry_id,
                cell_id: cell.cell_id,
                center,
                contains_center: shape.contains_center,
            };
            self.anchors.push(anchor);
        }
    }

    fn absorb_sponge_cell_anchors(&mut self, cell: &SpongeCell) {
        for a in &cell.anchors {
            self.anchors.push(SpongeShape {
                geometry_id: a.geometry_id,
                cell_id: a.cell_id,
                center: a.center,
                contains_center: a.contains_center,
            });
        }
    }
}

enum HeapEntry {
    Source { cell: ShapeCell, source: usize },
    Sponge(SpongeCell),
}

impl HeapEntry {
    fn cell_id(&self) -> S2CellId {
        match self {
            HeapEntry::Source { cell, .. } => cell.cell_id,
            HeapEntry::Sponge(ref merged) => merged.cell_id,
        }
    }

    fn range_min(&self) -> S2CellId {
        self.cell_id().range_min()
    }

    fn level(&self) -> i32 {
        self.cell_id().level()
    }
}

impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.range_min() == other.range_min() && self.level() == other.level()
    }
}

impl Eq for HeapEntry {}

impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .range_min()
            .cmp(&self.range_min())
            .then(other.level().cmp(&self.level()))
    }
}

/// Heap-based merge of N source cell indexes. Produces cells in Hilbert order.
pub struct Interleaver<'a> {
    sources: Vec<CellIndexIter<'a>>,
    edge_cache: &'a EdgeCache<'a, Sphere>,
    alive_bitsets: &'a [Option<ReadOnlyBitSet>],
    heap: BinaryHeap<HeapEntry>,
    max_edges: usize,
    min_short_edge_fraction: f64,
    cells_from_source: u64,
    absorptions: u64,
    splits: u64,
    cells_emitted: u64,
    max_sponge_cells: u64,
    crossings: u64,
    crossers: u64,
    segment_names: Vec<String>,
}

impl<'a> Interleaver<'a> {
    /// Creates a heap merge from source iterators and a shared edge cache.
    pub fn new(
        iters: Vec<CellIndexIter<'a>>,
        edge_cache: &'a EdgeCache<'a, Sphere>,
        alive_bitsets: &'a [Option<ReadOnlyBitSet>],
        segment_names: Vec<String>,
    ) -> Self {
        let options = ClipOptions::default();
        let mut merge = Interleaver {
            sources: iters,
            edge_cache,
            alive_bitsets,
            heap: BinaryHeap::new(),
            max_edges: options.max_edges_per_cell,
            min_short_edge_fraction: options.min_short_edge_fraction,
            cells_from_source: 0,
            absorptions: 0,
            splits: 0,
            cells_emitted: 0,
            max_sponge_cells: 0,
            crossings: 0,
            crossers: 0,
            segment_names,
        };
        let n = merge.sources.len();
        for i in 0..n {
            merge.push_next_from_source(i);
        }
        merge
    }

    /// Log interleave metrics to stderr.
    pub fn report(&self) {
        eprintln!(
            "interleave: {} from source, {} absorbed, {} splits, {} emitted, {} crossings, {} \
             crossers, {} max sponge",
            self.cells_from_source,
            self.absorptions,
            self.splits,
            self.cells_emitted,
            self.crossings,
            self.crossers,
            self.max_sponge_cells,
        );
    }

    fn push_next_from_source(&mut self, source_index: usize) {
        let segment = source_index as u16;
        while let Some(mut cell) = self.sources[source_index].next() {
            if let Some(bitset) = &self.alive_bitsets[source_index] {
                cell.shapes.retain(|shape| {
                    let id = (segment, shape.geometry_id.1);
                    let doc_id = self.edge_cache.doc_id_for(id);
                    bitset.contains(doc_id)
                });
            }
            for shape in &mut cell.shapes {
                shape.geometry_id.0 = segment;
            }
            if !cell.shapes.is_empty() {
                self.cells_from_source += 1;
                self.heap.push(HeapEntry::Source { cell, source: source_index });
                return;
            }
        }
    }
}

impl<'a> Iterator for Interleaver<'a> {
    type Item = ShapeCell;

    // Curious as to the performance penalty of explode and push for the sponge and sponged cells
    // since it would simplify this code. One function to explode, one to merge. Would accept some
    // slow down for that simplification.
    fn next(&mut self) -> Option<ShapeCell> {
        loop {
            let sponge = self.heap.pop()?;
            if let HeapEntry::Source { source, .. } = &sponge {
                self.push_next_from_source(*source);
            }
            if let Some(entry) = self.heap.pop() {
                if let HeapEntry::Source { source, .. } = &entry {
                    self.push_next_from_source(*source);
                }
                if sponge.cell_id().contains(entry.cell_id()) || sponge.cell_id() == entry.cell_id()
                {
                    self.absorptions += 1;
                    // First absorption: Source becomes Sponge.
                    let mut merged = match sponge {
                        HeapEntry::Source { cell, .. } => {
                            let cell_bound = S2PaddedCell::new(cell.cell_id, CELL_PADDING).bound();
                            let mut m = SpongeCell::new(cell.cell_id, cell_bound);
                            m.absorb_index_cell_anchors(&cell);
                            m.absorb_index_cell_edges(&cell, self.edge_cache);
                            m
                        }
                        HeapEntry::Sponge(m) => m,
                    };
                    // Absorb the entry.
                    match &entry {
                        HeapEntry::Sponge(ref entry_merged) => {
                            merged.absorb_sponge_cell_edges(entry_merged);
                            merged.absorb_sponge_cell_anchors(entry_merged);
                        }
                        HeapEntry::Source { cell, .. } => {
                            merged.absorb_index_cell_anchors(cell);
                            merged.absorb_index_cell_edges(cell, self.edge_cache);
                        }
                    }
                    if merged.edge_count <= self.max_edges {
                        self.heap.push(HeapEntry::Sponge(merged));
                    } else {
                        let short_edges = merged.short_edge_count;
                        let geometry_count = merged.anchors.len();
                        let threshold = self.max_edges.max(
                            (self.min_short_edge_fraction * (short_edges + geometry_count) as f64)
                                as usize,
                        );
                        if short_edges > threshold {
                            self.splits += 1;
                            for entry in
                                coarse_split(merged, &mut self.crossings, &mut self.crossers, &self.segment_names)
                            {
                                self.heap.push(entry);
                            }
                        } else {
                            self.heap.push(HeapEntry::Sponge(merged));
                        }
                    }
                } else {
                    self.cells_emitted += 1;
                    self.heap.push(entry);
                    return flatten(sponge);
                }
            } else {
                self.cells_emitted += 1;
                return flatten(sponge);
            }
        }
    }
}

fn flatten(sponge: HeapEntry) -> Option<ShapeCell> {
    let mut merged = match sponge {
        HeapEntry::Source { cell, .. } => return Some(cell),
        HeapEntry::Sponge(merged) => merged,
    };

    let cell_id = merged.cell_id;
    let sponge_center = S2PaddedCell::new(cell_id, CELL_PADDING).get_center();
    let mut result = ShapeCell::new(cell_id);
    merged.anchors.sort_unstable_by_key(|a| a.geometry_id);

    let mut edge_index = 0;
    let mut anchor_index = 0;
    while anchor_index < merged.anchors.len() {
        let anchor = &merged.anchors[anchor_index];
        let geometry_id = anchor.geometry_id;
        let mut contains_center = anchor.contains_center;
        let mut edge_indices: Vec<u32> = Vec::new();

        // Advance past edges with geometry_id less than ours.
        while edge_index < merged.edges.len() && merged.edges[edge_index].geometry_id < geometry_id
        {
            edge_index += 1;
        }

        // Collect edges matching our geometry_id.
        if edge_index < merged.edges.len() && merged.edges[edge_index].geometry_id == geometry_id {
            let mut crosser = super::crossings::S2EdgeCrosser::new(&anchor.center, &sponge_center);
            while edge_index < merged.edges.len()
                && merged.edges[edge_index].geometry_id == geometry_id
            {
                let edge = &merged.edges[edge_index];
                edge_indices.push(edge.edge_index);
                if crosser.edge_or_vertex_crossing_two(&edge.v0, &edge.v1) {
                    contains_center = !contains_center;
                }
                edge_index += 1;
            }
        }

        if contains_center || !edge_indices.is_empty() {
            let mut shape = ClippedShape::new(geometry_id, contains_center);
            shape.edge_indices = edge_indices;
            result.add_shape(shape);
        }

        // Skip past remaining anchors with the same geometry_id.
        anchor_index += 1;
        while anchor_index < merged.anchors.len()
            && merged.anchors[anchor_index].geometry_id == geometry_id
        {
            anchor_index += 1;
        }
    }

    if edge_index < merged.edges.len() {
        let orphans: Vec<_> = merged.edges[edge_index..]
            .iter()
            .map(|e| (e.geometry_id, e.edge_index, e.max_level))
            .collect();
        let anchor_gids: Vec<_> = merged
            .anchors
            .iter()
            .map(|a| (a.geometry_id, a.cell_id.0, a.cell_id.level()))
            .collect();
        panic!(
            "flatten: {} orphan edges at cell {} level {}\n  orphans: {:?}\n  anchors: {:?}\n  \
             total_edges: {} total_anchors: {}",
            orphans.len(),
            cell_id.0,
            cell_id.level(),
            orphans,
            &anchor_gids[..anchor_gids.len().min(30)],
            merged.edges.len(),
            merged.anchors.len(),
        );
    }

    Some(result)
}

// Flat index into the geometries map for clipping. Each entry pairs a geometry_id with a
// SpongeEdge reference so the clipping and crossing test code can work with a flat array indexed
// by position.
struct FlatEdge<'a> {
    geometry_id: GeometryId,
    edge: &'a SpongeEdge,
    a_uv: [f64; 2],
    b_uv: [f64; 2],
}

#[derive(Clone)]
struct MergeClippedEdge {
    edge_index: usize,
    bound: R2Rect,
}

fn clip_to_children(
    clipped_edge: &MergeClippedEdge,
    flat_edge: &FlatEdge,
    middle: &R2Rect,
    child_edges: &mut [[Vec<MergeClippedEdge>; 2]; 2],
) {
    let u_mid_lo = middle[0].lo();
    let u_mid_hi = middle[0].hi();
    let v_mid_lo = middle[1].lo();
    let v_mid_hi = middle[1].hi();

    if clipped_edge.bound[0].hi() <= u_mid_lo {
        clip_v_axis(
            clipped_edge,
            flat_edge,
            v_mid_lo,
            v_mid_hi,
            &mut child_edges[0],
        );
    } else if clipped_edge.bound[0].lo() >= u_mid_hi {
        clip_v_axis(
            clipped_edge,
            flat_edge,
            v_mid_lo,
            v_mid_hi,
            &mut child_edges[1],
        );
    } else if clipped_edge.bound[1].hi() <= v_mid_lo {
        child_edges[0][0].push(clip_u_bound(clipped_edge, flat_edge, u_mid_hi, true));
        child_edges[1][0].push(clip_u_bound(clipped_edge, flat_edge, u_mid_lo, false));
    } else if clipped_edge.bound[1].lo() >= v_mid_hi {
        child_edges[0][1].push(clip_u_bound(clipped_edge, flat_edge, u_mid_hi, true));
        child_edges[1][1].push(clip_u_bound(clipped_edge, flat_edge, u_mid_lo, false));
    } else {
        let left = clip_u_bound(clipped_edge, flat_edge, u_mid_hi, true);
        if !left.bound.is_empty() {
            let lower = clip_v_bound(&left, flat_edge, v_mid_hi, true);
            if !lower.bound.is_empty() {
                child_edges[0][0].push(lower);
            }
            let upper = clip_v_bound(&left, flat_edge, v_mid_lo, false);
            if !upper.bound.is_empty() {
                child_edges[0][1].push(upper);
            }
        }
        let right = clip_u_bound(clipped_edge, flat_edge, u_mid_lo, false);
        if !right.bound.is_empty() {
            let lower = clip_v_bound(&right, flat_edge, v_mid_hi, true);
            if !lower.bound.is_empty() {
                child_edges[1][0].push(lower);
            }
            let upper = clip_v_bound(&right, flat_edge, v_mid_lo, false);
            if !upper.bound.is_empty() {
                child_edges[1][1].push(upper);
            }
        }
    }
}

fn clip_v_axis(
    clipped_edge: &MergeClippedEdge,
    flat_edge: &FlatEdge,
    v_mid_lo: f64,
    v_mid_hi: f64,
    children: &mut [Vec<MergeClippedEdge>; 2],
) {
    if clipped_edge.bound[1].hi() <= v_mid_lo {
        children[0].push(clipped_edge.clone());
    } else if clipped_edge.bound[1].lo() >= v_mid_hi {
        children[1].push(clipped_edge.clone());
    } else {
        let lower = clip_v_bound(clipped_edge, flat_edge, v_mid_hi, true);
        if !lower.bound.is_empty() {
            children[0].push(lower);
        }
        let upper = clip_v_bound(clipped_edge, flat_edge, v_mid_lo, false);
        if !upper.bound.is_empty() {
            children[1].push(upper);
        }
    }
}

fn clip_u_bound(
    clipped_edge: &MergeClippedEdge,
    flat_edge: &FlatEdge,
    u: f64,
    clip_hi: bool,
) -> MergeClippedEdge {
    if clip_hi {
        if clipped_edge.bound[0].hi() <= u {
            return clipped_edge.clone();
        }
    } else if clipped_edge.bound[0].lo() >= u {
        return clipped_edge.clone();
    }

    let a = flat_edge.a_uv;
    let b = flat_edge.b_uv;
    let v = interpolate_double(u, a[0], b[0], a[1], b[1]);
    let v_clamped = clipped_edge.bound[1].clamp(v);

    let slopes_same_sign = (a[0] > b[0]) == (a[1] > b[1]);
    let clip_v_hi = if clip_hi {
        slopes_same_sign
    } else {
        !slopes_same_sign
    };

    let u_interval = if clip_hi {
        super::r1interval::R1Interval::new(clipped_edge.bound[0].lo(), u)
    } else {
        super::r1interval::R1Interval::new(u, clipped_edge.bound[0].hi())
    };
    let v_interval = if clip_v_hi {
        super::r1interval::R1Interval::new(clipped_edge.bound[1].lo(), v_clamped)
    } else {
        super::r1interval::R1Interval::new(v_clamped, clipped_edge.bound[1].hi())
    };
    if u_interval.is_empty() || v_interval.is_empty() {
        return MergeClippedEdge {
            edge_index: clipped_edge.edge_index,
            bound: R2Rect::empty(),
        };
    }

    MergeClippedEdge {
        edge_index: clipped_edge.edge_index,
        bound: R2Rect::new(u_interval, v_interval),
    }
}

fn clip_v_bound(
    clipped_edge: &MergeClippedEdge,
    flat_edge: &FlatEdge,
    v: f64,
    clip_hi: bool,
) -> MergeClippedEdge {
    if clip_hi {
        if clipped_edge.bound[1].hi() <= v {
            return clipped_edge.clone();
        }
    } else if clipped_edge.bound[1].lo() >= v {
        return clipped_edge.clone();
    }

    let a = flat_edge.a_uv;
    let b = flat_edge.b_uv;
    let u = interpolate_double(v, a[1], b[1], a[0], b[0]);
    let u_clamped = clipped_edge.bound[0].clamp(u);

    let slopes_same_sign = (a[0] > b[0]) == (a[1] > b[1]);
    let clip_u_hi = if clip_hi {
        slopes_same_sign
    } else {
        !slopes_same_sign
    };

    let u_interval = if clip_u_hi {
        R1Interval::new(clipped_edge.bound[0].lo(), u_clamped)
    } else {
        R1Interval::new(u_clamped, clipped_edge.bound[0].hi())
    };
    let v_interval = if clip_hi {
        R1Interval::new(clipped_edge.bound[1].lo(), v)
    } else {
        R1Interval::new(v, clipped_edge.bound[1].hi())
    };
    if u_interval.is_empty() || v_interval.is_empty() {
        return MergeClippedEdge {
            edge_index: clipped_edge.edge_index,
            bound: R2Rect::empty(),
        };
    }

    MergeClippedEdge {
        edge_index: clipped_edge.edge_index,
        bound: R2Rect::new(u_interval, v_interval),
    }
}

fn interpolate_double(x: f64, x0: f64, x1: f64, y0: f64, y1: f64) -> f64 {
    if x0 == x1 {
        assert!(x == x0 && y0 == y1);
        return y0;
    }
    if (x0 - x).abs() <= (x1 - x).abs() {
        y0 + (y1 - y0) * ((x - x0) / (x1 - x0))
    } else {
        y1 + (y0 - y1) * ((x - x1) / (x0 - x1))
    }
}

fn coarse_split(mut merged: SpongeCell, crossings: &mut u64, crossers: &mut u64, segment_names: &[String]) -> Vec<HeapEntry> {
    let cell_id = merged.cell_id;
    let parent_split_count = merged.split_count;
    merged.anchors.sort_unstable_by_key(|a| a.geometry_id);
    let anchors = &merged.anchors;

    let sponge_level = cell_id.level();
    let child_cell_ids = cell_id.children();

    // SpongeShape at the sponge level need crossing tests because their edges are about to be
    // distributed to children. Finer anchors are distributed to the child that contains them.
    let mut sponge_anchors: Vec<SpongeShape> = Vec::new();
    let mut child_anchors: [Vec<SpongeShape>; 4] = Default::default();
    for anchor in anchors {
        if anchor.cell_id.level() == sponge_level {
            sponge_anchors.push(anchor.clone());
        } else {
            for (idx, child_cell_id) in child_cell_ids.iter().enumerate() {
                if child_cell_id.contains(anchor.cell_id) || *child_cell_id == anchor.cell_id {
                    child_anchors[idx].push(anchor.clone());
                    break;
                }
            }
        }
    }
    let padded_cell = S2PaddedCell::new(cell_id, CELL_PADDING);
    let parent_center = padded_cell.get_center();

    let mut flat_edges: Vec<FlatEdge> = Vec::new();
    let mut clipped: Vec<MergeClippedEdge> = Vec::new();
    for edge in &merged.edges {
        let geometry_id = edge.geometry_id;
        let index = flat_edges.len();
        flat_edges.push(FlatEdge {
            geometry_id,
            edge,
            a_uv: edge.a_uv,
            b_uv: edge.b_uv,
        });
        clipped.push(MergeClippedEdge {
            edge_index: index,
            bound: edge.bound,
        });
    }

    // Tighten each edge's bound to this sponge's padded cell before clipping to children. The
    // bound may have come from a coarser level.
    let sponge_bound = padded_cell.bound();
    for (ce, fe) in clipped.iter_mut().zip(flat_edges.iter()) {
        if !clip_edge_bound(fe.a_uv, fe.b_uv, &sponge_bound, &mut ce.bound) {
            ce.bound = R2Rect::empty();
        }
    }

    let middle = padded_cell.middle();
    let mut child_edges: [[Vec<MergeClippedEdge>; 2]; 2] = Default::default();
    for clipped_edge in &clipped {
        if clipped_edge.bound.is_empty() {
            continue;
        }
        let flat_edge = &flat_edges[clipped_edge.edge_index];
        clip_to_children(clipped_edge, flat_edge, &middle, &mut child_edges);
    }

    let mut entries = Vec::new();

    for pos in 0..4i32 {
        let (i, j) = padded_cell.get_child_ij(pos);
        let child_padded_cell = S2PaddedCell::from_parent(&padded_cell, i, j);
        let child_center = child_padded_cell.get_center();
        let child_cell_id = child_padded_cell.id();
        let mut child_merged = SpongeCell::new(child_cell_id, child_padded_cell.bound());

        let mut sponge_edges: Vec<usize> = Vec::new();

        child_edges[i][j].sort_unstable_by(|a, b| {
            let a_gid = flat_edges[a.edge_index].geometry_id;
            let b_gid = flat_edges[b.edge_index].geometry_id;
            b_gid.cmp(&a_gid)
        });

        child_anchors[pos as usize].sort_unstable_by_key(|a| a.geometry_id);
        let mut seen_geometry_id: Option<GeometryId> = None;
        for anchor in &child_anchors[pos as usize] {
            if seen_geometry_id == Some(anchor.geometry_id) {
                continue;
            }
            seen_geometry_id = Some(anchor.geometry_id);
            while let Some(last) = child_edges[i][j].last() {
                let flat_edge = &flat_edges[last.edge_index];
                if flat_edge.geometry_id < anchor.geometry_id {
                    sponge_edges.push(last.edge_index);
                    child_edges[i][j].pop();
                } else {
                    break;
                }
            }
            let contains_center = anchor.contains_center;
            let mut edge_indices: Vec<u32> = Vec::new();
            let child_level = sponge_level + 1;
            while let Some(last) = child_edges[i][j].last() {
                let flat_edge = &flat_edges[last.edge_index];
                if flat_edge.geometry_id != anchor.geometry_id {
                    break;
                }
                let clipped_bound = last.bound;
                edge_indices.push(flat_edge.edge.edge_index);
                child_merged.edge_count += 1;
                if child_level < flat_edge.edge.max_level {
                    child_merged.short_edge_count += 1;
                }
                child_merged.edges.push(SpongeEdge {
                    geometry_id: anchor.geometry_id,
                    edge_index: flat_edge.edge.edge_index,
                    max_level: flat_edge.edge.max_level,
                    v0: flat_edge.edge.v0,
                    v1: flat_edge.edge.v1,
                    a_uv: flat_edge.a_uv,
                    b_uv: flat_edge.b_uv,
                    bound: clipped_bound,
                });
                child_edges[i][j].pop();
            }
            if !contains_center && edge_indices.is_empty() {
                eprintln!("ABEND geometry {:?} anchor_cell {} anchor_level {} sponge {} sponge_level {} child {} split_count {} segments {:?}",
                    anchor.geometry_id, anchor.cell_id.0, anchor.cell_id.level(),
                    cell_id.0, sponge_level, child_cell_id.0, parent_split_count, segment_names);
                let child_bound = child_padded_cell.bound();
                eprintln!("  child_bound u=[{},{}] v=[{},{}]",
                    child_bound[0].lo(), child_bound[0].hi(), child_bound[1].lo(), child_bound[1].hi());
                eprintln!("  sponge_bound u=[{},{}] v=[{},{}]",
                    sponge_bound[0].lo(), sponge_bound[0].hi(), sponge_bound[1].lo(), sponge_bound[1].hi());
                eprintln!("  middle u=[{},{}] v=[{},{}]",
                    middle[0].lo(), middle[0].hi(), middle[1].lo(), middle[1].hi());
                let all_edges_for_geo: Vec<_> = flat_edges.iter().enumerate()
                    .filter(|(_, fe)| fe.geometry_id == anchor.geometry_id)
                    .collect();
                eprintln!("  {} edges for this geometry in flat_edges:", all_edges_for_geo.len());
                for (idx, fe) in &all_edges_for_geo {
                    let ce = &clipped[*idx];
                    eprintln!("    flat[{}] edge_index={} a_uv={:?} b_uv={:?} bound=[{},{}]x[{},{}] empty={}",
                        idx, fe.edge.edge_index, fe.a_uv, fe.b_uv,
                        ce.bound[0].lo(), ce.bound[0].hi(), ce.bound[1].lo(), ce.bound[1].hi(),
                        ce.bound.is_empty());
                }
                let edges_in_child: Vec<_> = child_edges[i][j].iter()
                    .filter(|ce| flat_edges[ce.edge_index].geometry_id == anchor.geometry_id)
                    .collect();
                eprintln!("  {} edges for this geometry in child[{}][{}]:", edges_in_child.len(), i, j);
                for ce in &edges_in_child {
                    let fe = &flat_edges[ce.edge_index];
                    eprintln!("    edge_index={} bound=[{},{}]x[{},{}]",
                        fe.edge.edge_index, ce.bound[0].lo(), ce.bound[0].hi(),
                        ce.bound[1].lo(), ce.bound[1].hi());
                }
                let sponge_edges_for_geo: Vec<_> = sponge_edges.iter()
                    .filter(|&&idx| flat_edges[idx].geometry_id == anchor.geometry_id)
                    .collect();
                eprintln!("  {} sponge_edges for this geometry", sponge_edges_for_geo.len());
                panic!("coarse_split: anchor without edges");
            }
        }

        // Remaining child_edges are sponge-level edges not claimed by child anchors.
        while let Some(last) = child_edges[i][j].last() {
            sponge_edges.push(last.edge_index);
            child_edges[i][j].pop();
        }
        sponge_edges.sort_unstable_by_key(|&idx| flat_edges[idx].geometry_id);

        let mut sponge_edge_index = 0;
        for anchor in &sponge_anchors {
            if seen_geometry_id == Some(anchor.geometry_id) {
                continue;
            }
            seen_geometry_id = Some(anchor.geometry_id);
            let mut contains_center = anchor.contains_center;
            let mut edge_indices: Vec<u32> = Vec::new();

            while sponge_edge_index < sponge_edges.len()
                && flat_edges[sponge_edges[sponge_edge_index]].geometry_id < anchor.geometry_id
            {
                sponge_edge_index += 1;
            }

            let child_level = sponge_level + 1;
            if sponge_edge_index < sponge_edges.len()
                && flat_edges[sponge_edges[sponge_edge_index]].geometry_id == anchor.geometry_id
            {
                *crossers += 1;
                let mut crosser =
                    super::crossings::S2EdgeCrosser::new(&parent_center, &child_center);
                while sponge_edge_index < sponge_edges.len()
                    && flat_edges[sponge_edges[sponge_edge_index]].geometry_id == anchor.geometry_id
                {
                    let flat_edge = &flat_edges[sponge_edges[sponge_edge_index]];
                    edge_indices.push(flat_edge.edge.edge_index);
                    *crossings += 1;
                    if crosser.edge_or_vertex_crossing_two(&flat_edge.edge.v0, &flat_edge.edge.v1) {
                        contains_center = !contains_center;
                    }
                    child_merged.edge_count += 1;
                    if child_level < flat_edge.edge.max_level {
                        child_merged.short_edge_count += 1;
                    }
                    let mut child_bound = R2Rect::from_point_pair(flat_edge.a_uv, flat_edge.b_uv);
                    if clip_edge_bound(
                        flat_edge.a_uv,
                        flat_edge.b_uv,
                        &child_padded_cell.bound(),
                        &mut child_bound,
                    ) {
                        child_merged.edges.push(SpongeEdge {
                            geometry_id: anchor.geometry_id,
                            edge_index: flat_edge.edge.edge_index,
                            max_level: flat_edge.edge.max_level,
                            v0: flat_edge.edge.v0,
                            v1: flat_edge.edge.v1,
                            a_uv: flat_edge.a_uv,
                            b_uv: flat_edge.b_uv,
                            bound: child_bound,
                        });
                    }
                    sponge_edge_index += 1;
                }
            }
            if contains_center || !edge_indices.is_empty() {
                child_anchors[pos as usize].push(SpongeShape {
                    geometry_id: anchor.geometry_id,
                    cell_id: child_cell_id,
                    center: child_center,
                    contains_center,
                });
            }
        }
        // Orphan edges: sponge-level edges for geometries whose anchors haven't arrived yet. The
        // sponge split before absorbing all overlapping cells. The remaining cells will arrive
        // later in Hilbert order and bring the anchors. Add the edges to the child's merged set so
        // they're ready.
        let child_level = sponge_level + 1;
        for &idx in &sponge_edges[sponge_edge_index..] {
            let flat_edge = &flat_edges[idx];
            let mut child_bound = R2Rect::from_point_pair(flat_edge.a_uv, flat_edge.b_uv);
            if !clip_edge_bound(
                flat_edge.a_uv,
                flat_edge.b_uv,
                &child_padded_cell.bound(),
                &mut child_bound,
            ) {
                continue;
            }
            child_merged.edge_count += 1;
            if child_level < flat_edge.edge.max_level {
                child_merged.short_edge_count += 1;
            }
            child_merged.edges.push(SpongeEdge {
                geometry_id: flat_edge.geometry_id,
                edge_index: flat_edge.edge.edge_index,
                max_level: flat_edge.edge.max_level,
                v0: flat_edge.edge.v0,
                v1: flat_edge.edge.v1,
                a_uv: flat_edge.a_uv,
                b_uv: flat_edge.b_uv,
                bound: child_bound,
            });
        }

        if child_merged.edge_count > 0 || !child_anchors[pos as usize].is_empty() {
            child_merged
                .edges
                .sort_unstable_by_key(|e| (e.geometry_id, e.edge_index));
            child_merged.anchors = std::mem::take(&mut child_anchors[pos as usize]);
            child_merged.anchors.sort_unstable_by_key(|a| a.geometry_id);
            child_merged.split_count = parent_split_count + 1;
            entries.push(HeapEntry::Sponge(child_merged));
        }
    }

    entries
}

//! Heap-based merge of N source cell indexes. Produces cells in Hilbert order. Subdivides coarse
//! cells on demand when they overlap finer cells or exceed the edge limit.

use std::cmp::Ordering;
use std::collections::BinaryHeap;

use common::ReadOnlyBitSet;
use rustc_hash::{FxHashMap, FxHashSet};

use super::cell_index::{
    clip_to_children, get_edge_max_level, BuildOptions, ClippedShape, GeometryId, IndexCell,
    CELL_PADDING,
};
use super::cell_index_reader::CellIndexIter;
use super::edge_cache::EdgeCache;
use super::r2rect::R2Rect;
use super::s2cell_id::S2CellId;
use super::s2edge_clipping::{clip_edge_bound, clip_to_padded_face};
use super::s2padded_cell::S2PaddedCell;
use super::sphere::Sphere;

struct CoarseEdge {
    edge_index: u32,
    max_level: i32,
    v0: [f64; 3],
    v1: [f64; 3],
}

struct CoarseGeometry {
    edges: Vec<CoarseEdge>,
    /// Whether the geometry has an interior. Only then can it contain a cell's center.
    closed: bool,
}

struct CoarseIndexCell {
    cell: IndexCell,
    geometries: FxHashMap<GeometryId, CoarseGeometry>,
    geometry_ids: Vec<GeometryId>,
}

struct HeapEntry {
    cell_id: S2CellId,
    source: Option<usize>,
    cells: Vec<CoarseIndexCell>,
    geometry_ids: FxHashSet<GeometryId>,
}

impl CoarseIndexCell {
    pub fn new(cell: IndexCell, edge_cache: &EdgeCache<'_, Sphere>) -> Self {
        let mut geometries = FxHashMap::default();
        let mut geometry_ids = Vec::with_capacity(cell.shapes.len());
        for shape in &cell.shapes {
            geometry_ids.push(shape.geometry_id);

            if shape.edge_indices.is_empty() {
                continue;
            }

            let entry = edge_cache.get(shape.geometry_id);

            let level_geometry =
                geometries
                    .entry(shape.geometry_id)
                    .or_insert_with(|| CoarseGeometry {
                        edges: Vec::new(),
                        closed: entry.edge_set().closed,
                    });

            for &edge_index in &shape.edge_indices {
                let (v0, v1) = entry.edge(edge_index);
                let max_level = get_edge_max_level(&v0, &v1);
                level_geometry.edges.push(CoarseEdge {
                    edge_index,
                    max_level,
                    v0,
                    v1,
                });
            }
        }
        CoarseIndexCell {
            cell,
            geometries,
            geometry_ids,
        }
    }
}

impl HeapEntry {
    fn range_min(&self) -> S2CellId {
        self.cell_id.range_min()
    }

    fn level(&self) -> i32 {
        self.cell_id.level()
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
pub struct HeapInterleave<'a> {
    sources: Vec<CellIndexIter<'a>>,
    edge_cache: &'a EdgeCache<'a, Sphere>,
    edge_counter: FxHashSet<(GeometryId, u32)>,
    alive_bitsets: &'a [Option<ReadOnlyBitSet>],
    heap: BinaryHeap<HeapEntry>,
    max_edges: usize,
    min_short_edge_fraction: f64,
    cells_from_source: u64,
    absorptions: u64,
    splits: u64,
    cells_emitted: u64,
    short_edge_checks: u64,
    max_sponge_cells: u64,
    crossing_checks: u64,
}

impl<'a> HeapInterleave<'a> {
    /// Creates a heap merge from source iterators and a shared edge cache.
    pub fn new(
        iters: Vec<CellIndexIter<'a>>,
        edge_cache: &'a EdgeCache<'a, Sphere>,
        alive_bitsets: &'a [Option<ReadOnlyBitSet>],
    ) -> Self {
        let options = BuildOptions::default();
        let mut merge = HeapInterleave {
            sources: iters,
            edge_cache,
            edge_counter: FxHashSet::default(),
            alive_bitsets,
            heap: BinaryHeap::new(),
            max_edges: options.max_edges_per_cell,
            min_short_edge_fraction: options.min_short_edge_fraction,
            cells_from_source: 0,
            absorptions: 0,
            splits: 0,
            cells_emitted: 0,
            short_edge_checks: 0,
            max_sponge_cells: 0,
            crossing_checks: 0,
        };
        let n = merge.sources.len();
        for i in 0..n {
            merge.push_next_from_source(i);
        }
        merge
    }

    /// Log interleave metrics at debug level.
    pub fn report(&self) {
        debug!(
            "interleave: {} from source, {} absorbed, {} splits, {} emitted, {} edge checks, {} \
             crossing checks, {} max sponge",
            self.cells_from_source,
            self.absorptions,
            self.splits,
            self.cells_emitted,
            self.short_edge_checks,
            self.crossing_checks,
            self.max_sponge_cells,
        );
    }

    fn push_next_from_source(&mut self, source_idx: usize) {
        let segment = source_idx as u16;
        while let Some(mut cell) = self.sources[source_idx].next() {
            if let Some(bitset) = &self.alive_bitsets[source_idx] {
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
                let mut geometries = FxHashSet::default();
                geometries.extend(cell.shapes.iter().map(|s| s.geometry_id));
                self.heap.push(HeapEntry {
                    cell_id: cell.cell_id,
                    source: Some(source_idx),
                    cells: vec![CoarseIndexCell::new(cell, self.edge_cache)],
                    geometry_ids: geometries,
                });
                return;
            }
        }
    }

    fn coarse_split(&mut self, sponge: HeapEntry) {
        self.splits += 1;
        let mut cells = sponge.cells;
        let head = cells.remove(0);
        let children = subdivide_cell(&head, &mut self.crossing_checks);

        // Build four HeapEntries from the subdivided children.
        let mut entries: Vec<HeapEntry> = children
            .into_iter()
            .map(|child_cell| {
                let child_cell_id = child_cell.cell_id;
                let mut geometry_ids = FxHashSet::default();
                geometry_ids.extend(child_cell.shapes.iter().map(|s| s.geometry_id));
                HeapEntry {
                    cell_id: child_cell_id,
                    source: None,
                    cells: vec![CoarseIndexCell::new(child_cell, self.edge_cache)],
                    geometry_ids,
                }
            })
            .collect();

        // Distribute remaining coarse cells to their parent child. If a coarse cell is at the
        // same level as the children it can't be assigned as a descendant. Push it onto the heap
        // as its own entry.
        for coarse_cell in cells {
            let coarse_cell_id = coarse_cell.cell.cell_id;
            if let Some(parent_entry) = entries
                .iter_mut()
                .find(|e| e.cell_id.contains(coarse_cell_id))
            {
                parent_entry.geometry_ids.extend(&coarse_cell.geometry_ids);
                parent_entry.cells.push(coarse_cell);
            } else {
                let mut geometries = FxHashSet::default();
                geometries.extend(&coarse_cell.geometry_ids);
                self.heap.push(HeapEntry {
                    cell_id: coarse_cell_id,
                    source: None,
                    cells: vec![coarse_cell],
                    geometry_ids: geometries,
                });
            }
        }

        // Push non-empty entries onto the heap.
        for entry in entries {
            let has_geometries = entry.cells.iter().any(|c| !c.geometry_ids.is_empty());
            if has_geometries {
                self.heap.push(entry);
            }
        }
    }
}

impl<'a> Iterator for HeapInterleave<'a> {
    type Item = IndexCell;

    fn next(&mut self) -> Option<IndexCell> {
        loop {
            let mut sponge = self.heap.pop()?;
            if let Some(source) = sponge.source {
                sponge.source = None;
                self.push_next_from_source(source);
            }
            if let Some(mut entry) = self.heap.pop() {
                if let Some(source) = entry.source {
                    entry.source = None;
                    self.push_next_from_source(source);
                }
                if sponge.cell_id.contains(entry.cell_id) || sponge.cell_id == entry.cell_id {
                    self.absorptions += 1;
                    sponge.geometry_ids.extend(&entry.geometry_ids);
                    sponge.cells.extend(entry.cells);
                    let sponge_size = sponge.cells.len() as u64;
                    if sponge_size > self.max_sponge_cells {
                        self.max_sponge_cells = sponge_size;
                    }
                    let mut edges = std::mem::take(&mut self.edge_counter);
                    edges.clear();
                    self.short_edge_checks +=
                        count_short_edges(&sponge.cells, sponge.cell_id.level(), &mut edges);
                    let threshold = self.max_edges.max(
                        (self.min_short_edge_fraction
                            * (edges.len() + sponge.geometry_ids.len()) as f64)
                            as usize,
                    );
                    if edges.len() > threshold {
                        self.coarse_split(sponge);
                    } else {
                        self.heap.push(sponge);
                    }
                    self.edge_counter = edges;
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

/// A geometry's edges across the cells of a sponge, with the contains_center flag and the center
/// of the first cell it appears in: the anchor of the ray-cast to the sponge center.
struct EdgeUnion {
    /// The edges as (edge index, v0, v1).
    edges: Vec<(u32, [f64; 3], [f64; 3])>,
    /// Whether the geometry has an interior. A geometry with no edges in any cell is there only
    /// because it contains a cell's center, so it does.
    closed: bool,
    anchor_flag: bool,
    anchor_center: [f64; 3],
}

fn flatten(sponge: HeapEntry) -> Option<IndexCell> {
    if sponge.cells.len() == 1 {
        return Some(sponge.cells.into_iter().next().unwrap().cell);
    }

    let sponge_pcell = S2PaddedCell::new(sponge.cell_id, CELL_PADDING);
    let sponge_center = sponge_pcell.get_center();

    // Build per-geometry union of edges with anchor info for the ray-cast.
    let mut edge_unions: FxHashMap<GeometryId, EdgeUnion> = FxHashMap::default();

    for coarse_cell in &sponge.cells {
        let cell_center = S2PaddedCell::new(coarse_cell.cell.cell_id, CELL_PADDING).get_center();

        for shape in &coarse_cell.cell.shapes {
            let union_entry = edge_unions
                .entry(shape.geometry_id)
                .or_insert_with(|| EdgeUnion {
                    edges: Vec::new(),
                    closed: true,
                    anchor_flag: shape.contains_center,
                    anchor_center: cell_center,
                });
            if let Some(geo) = coarse_cell.geometries.get(&shape.geometry_id) {
                union_entry.closed = geo.closed;
                for &edge_idx in &shape.edge_indices {
                    if let Some(ce) = geo.edges.iter().find(|e| e.edge_index == edge_idx) {
                        union_entry.edges.push((edge_idx, ce.v0, ce.v1));
                    }
                }
            }
        }
    }

    for union in edge_unions.values_mut() {
        union.edges.sort_unstable_by_key(|(idx, _, _)| *idx);
        union.edges.dedup_by_key(|(idx, _, _)| *idx);
    }

    let mut result = IndexCell::new(sponge.cell_id);

    for (geometry_id, union) in &edge_unions {
        if union.edges.is_empty() {
            result.add_shape(ClippedShape::new(*geometry_id, union.anchor_flag));
            continue;
        }

        // Only a geometry with an interior can contain the center. For a line, the parity of
        // its crossings means nothing.
        let mut contains = false;
        if union.closed {
            let mut crosser =
                super::crossings::S2EdgeCrosser::new(&union.anchor_center, &sponge_center);
            let mut crossings = 0u32;
            for (_, v0, v1) in &union.edges {
                if crosser.edge_or_vertex_crossing_two(v0, v1) {
                    crossings += 1;
                }
            }
            contains = union.anchor_flag ^ !crossings.is_multiple_of(2);
        }

        let mut shape = ClippedShape::new(*geometry_id, contains);
        shape.edge_indices = union.edges.iter().map(|(idx, _, _)| *idx).collect();
        result.add_shape(shape);
    }

    Some(result)
}

fn count_short_edges(
    cells: &[CoarseIndexCell],
    level: i32,
    counter: &mut FxHashSet<(GeometryId, u32)>,
) -> u64 {
    let mut checked = 0u64;
    for cell in cells {
        for (&gid, geo) in &cell.geometries {
            for edge in &geo.edges {
                checked += 1;
                if level < edge.max_level {
                    counter.insert((gid, edge.edge_index));
                }
            }
        }
    }
    checked
}

struct MergeEdge {
    geometry_id: GeometryId,
    edge_index: u32,
    v0: [f64; 3],
    v1: [f64; 3],
    a_uv: [f64; 2],
    b_uv: [f64; 2],
}

#[derive(Clone)]
struct MergeClippedEdge {
    edge_index: usize,
    bound: R2Rect,
}

fn compute_contains_center(
    from: &[f64; 3],
    to: &[f64; 3],
    from_flag: bool,
    all_edges: &[MergeEdge],
    clipped: &[MergeClippedEdge],
    geometry_id: GeometryId,
    crossing_checks: &mut u64,
) -> bool {
    use super::crossings::S2EdgeCrosser;
    let mut crosser = S2EdgeCrosser::new(from, to);
    let mut crossings = 0u32;
    for ce in clipped {
        let me = &all_edges[ce.edge_index];
        if me.geometry_id == geometry_id {
            *crossing_checks += 1;
            if crosser.edge_or_vertex_crossing_two(&me.v0, &me.v1) {
                crossings += 1;
            }
        }
    }
    from_flag ^ !crossings.is_multiple_of(2)
}

fn subdivide_cell(coarse_cell: &CoarseIndexCell, crossing_checks: &mut u64) -> Vec<IndexCell> {
    let cell = &coarse_cell.cell;
    let face = cell.cell_id.face();
    let pcell = S2PaddedCell::new(cell.cell_id, CELL_PADDING);
    let parent_center = pcell.get_center();

    let mut merge_edges: Vec<MergeEdge> = Vec::new();
    let mut clipped: Vec<MergeClippedEdge> = Vec::new();

    // The cell holds the part of each edge on its face, clipped as the builder clips it. An edge
    // that misses the padded cell crosses none of its children, nor any path inside it.
    for (&gid, geo) in &coarse_cell.geometries {
        for edge in &geo.edges {
            let Some((a_uv, b_uv)) = clip_to_padded_face(&edge.v0, &edge.v1, face, CELL_PADDING)
            else {
                continue;
            };
            // The bound of the part of the edge in the padded cell, as tight as the builder's.
            let mut bound = R2Rect::from_point_pair(a_uv, b_uv);
            if !clip_edge_bound(a_uv, b_uv, &pcell.bound(), &mut bound) {
                continue;
            }
            clipped.push(MergeClippedEdge {
                edge_index: merge_edges.len(),
                bound,
            });
            merge_edges.push(MergeEdge {
                geometry_id: gid,
                edge_index: edge.edge_index,
                v0: edge.v0,
                v1: edge.v1,
                a_uv,
                b_uv,
            });
        }
    }

    // Only a geometry with an interior can contain a child's center. One with no edges in the
    // cell is there only because it contains the cell's center, so it has one.
    let parent_contains: Vec<(GeometryId, bool)> = cell
        .shapes
        .iter()
        .filter(|s| {
            coarse_cell
                .geometries
                .get(&s.geometry_id)
                .is_none_or(|geo| geo.closed)
        })
        .map(|s| (s.geometry_id, s.contains_center))
        .collect();

    let middle = pcell.middle();
    let mut child_edges: [[Vec<MergeClippedEdge>; 2]; 2] = Default::default();
    for ce in &clipped {
        let me = &merge_edges[ce.edge_index];
        clip_to_children(&ce.bound, me.a_uv, me.b_uv, &middle, |i, j, bound| {
            child_edges[i][j].push(MergeClippedEdge {
                edge_index: ce.edge_index,
                bound,
            });
        });
    }

    let mut children = Vec::new();

    for pos in 0..4i32 {
        let (i, j) = pcell.get_child_ij(pos);
        let child_pcell = S2PaddedCell::from_parent(&pcell, i, j);
        let child_center = child_pcell.get_center();
        let mut index_cell = IndexCell::new(child_pcell.id());

        if child_edges[i][j].is_empty() {
            for &(gid, parent_flag) in &parent_contains {
                let contains = compute_contains_center(
                    &parent_center,
                    &child_center,
                    parent_flag,
                    &merge_edges,
                    &clipped,
                    gid,
                    crossing_checks,
                );
                if contains {
                    index_cell.add_shape(ClippedShape::new(gid, true));
                }
            }
        } else {
            let mut edge_map: Vec<(GeometryId, Vec<u32>)> = Vec::new();
            for ce in &child_edges[i][j] {
                let me = &merge_edges[ce.edge_index];
                if let Some((_, edges)) =
                    edge_map.iter_mut().find(|(gid, _)| *gid == me.geometry_id)
                {
                    edges.push(me.edge_index);
                } else {
                    edge_map.push((me.geometry_id, vec![me.edge_index]));
                }
            }

            let mut emitted_gids: Vec<GeometryId> = Vec::new();
            for (geometry_id, edge_indices) in &edge_map {
                let parent_flag = parent_contains
                    .iter()
                    .find(|(gid, _)| gid == geometry_id)
                    .map(|(_, c)| *c);

                // A geometry missing from parent_contains has no interior.
                let contains = parent_flag.is_some_and(|parent_flag| {
                    compute_contains_center(
                        &parent_center,
                        &child_center,
                        parent_flag,
                        &merge_edges,
                        &clipped,
                        *geometry_id,
                        crossing_checks,
                    )
                });

                let mut shape = ClippedShape::new(*geometry_id, contains);
                shape.edge_indices = edge_indices.clone();
                index_cell.add_shape(shape);
                emitted_gids.push(*geometry_id);
            }

            for &(gid, parent_flag) in &parent_contains {
                if emitted_gids.contains(&gid) {
                    continue;
                }
                let contains = compute_contains_center(
                    &parent_center,
                    &child_center,
                    parent_flag,
                    &merge_edges,
                    &clipped,
                    gid,
                    crossing_checks,
                );
                if contains {
                    index_cell.add_shape(ClippedShape::new(gid, true));
                }
            }
        }

        if !index_cell.is_empty() {
            children.push(index_cell);
        }
    }

    children
}

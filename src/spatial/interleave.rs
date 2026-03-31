//! Heap-based merge of N source cell indexes. Produces cells in Hilbert order. Subdivides coarse
//! cells on demand when they overlap finer cells or exceed the edge limit.

use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::BinaryHeap;

use common::ReadOnlyBitSet;
use rustc_hash::{FxHashMap, FxHashSet};

use super::cell_index::{
    get_edge_max_level, BuildOptions, ClippedShape, GeometryId, IndexCell, CELL_PADDING,
};
use super::cell_index_reader::CellIndexIter;
use super::edge_cache::EdgeCache;
use super::r2rect::R2Rect;
use super::s2cell_id::S2CellId;
use super::s2coords::valid_face_xyz_to_uv;
use super::s2padded_cell::S2PaddedCell;
use super::sphere::Sphere;

struct CoarseEdge {
    edge_index: u16,
    max_level: i32,
}

struct CoarseGeometry {
    edges: Vec<CoarseEdge>,
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
    geometries: FxHashSet<GeometryId>,
}

impl CoarseIndexCell {
    pub fn new(cell: IndexCell, edge_cache: &RefCell<EdgeCache<'_, Sphere>>) -> Self {
        let mut geometries = FxHashMap::default();
        let mut geometry_ids = Vec::with_capacity(cell.shapes.len());
        for shape in &cell.shapes {
            geometry_ids.push(shape.geometry_id);

            if shape.edge_indices.is_empty() {
                continue;
            }

            let mut cache = edge_cache.borrow_mut();
            let (head, geo_set) = cache.get_geometry_set(shape.geometry_id);
            let member_idx = (shape.geometry_id.1 - head) as usize;
            let vertices = &geo_set.members[member_idx].vertices;

            let level_geometry = geometries
                .entry(shape.geometry_id)
                .or_insert_with(|| CoarseGeometry { edges: Vec::new() });

            for &edge_index in &shape.edge_indices {
                let v0 = vertices[edge_index as usize];
                let v1 = if (edge_index as usize) + 1 < vertices.len() {
                    vertices[(edge_index as usize) + 1]
                } else {
                    v0
                };
                let max_level = get_edge_max_level(&v0, &v1);
                level_geometry.edges.push(CoarseEdge {
                    edge_index,
                    max_level,
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
    edge_cache: &'a RefCell<EdgeCache<'a, Sphere>>,
    edge_counter: FxHashSet<(GeometryId, u16)>,
    alive_bitsets: &'a [Option<ReadOnlyBitSet>],
    heap: BinaryHeap<HeapEntry>,
    max_edges: usize,
    min_short_edge_fraction: f64,
}

impl<'a> HeapInterleave<'a> {
    /// Creates a heap merge from source iterators and a shared edge cache.
    pub fn new(
        iters: Vec<CellIndexIter<'a>>,
        edge_cache: &'a RefCell<EdgeCache<'a, Sphere>>,
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
        };
        let n = merge.sources.len();
        for i in 0..n {
            merge.push_next_from_source(i);
        }
        merge
    }

    fn push_next_from_source(&mut self, source_idx: usize) {
        let segment = source_idx as u16;
        while let Some(mut cell) = self.sources[source_idx].next() {
            if let Some(bitset) = &self.alive_bitsets[source_idx] {
                cell.shapes.retain(|shape| {
                    let id = (segment, shape.geometry_id.1);
                    let doc_id = self.edge_cache.borrow().doc_id_for(id);
                    bitset.contains(doc_id)
                });
            }
            for shape in &mut cell.shapes {
                shape.geometry_id.0 = segment;
            }
            if !cell.shapes.is_empty() {
                let mut geometries = FxHashSet::default();
                geometries.extend(cell.shapes.iter().map(|s| s.geometry_id));
                self.heap.push(HeapEntry {
                    cell_id: cell.cell_id,
                    source: Some(source_idx),
                    cells: vec![CoarseIndexCell::new(cell, self.edge_cache)],
                    geometries,
                });
                return;
            }
        }
    }

    fn coarse_split(&mut self, sponge: HeapEntry) {
        let mut cells = sponge.cells;
        let head = cells.remove(0);
        let children = subdivide_cell(&head.cell, self.edge_cache);

        // Build four HeapEntries from the subdivided children.
        let mut entries: Vec<HeapEntry> = children
            .into_iter()
            .map(|child_cell| {
                let child_cell_id = child_cell.cell_id;
                let mut geometries = FxHashSet::default();
                geometries.extend(child_cell.shapes.iter().map(|s| s.geometry_id));
                HeapEntry {
                    cell_id: child_cell_id,
                    source: None,
                    cells: vec![CoarseIndexCell::new(child_cell, self.edge_cache)],
                    geometries,
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
                parent_entry.geometries.extend(&coarse_cell.geometry_ids);
                parent_entry.cells.push(coarse_cell);
            } else {
                let mut geometries = FxHashSet::default();
                geometries.extend(&coarse_cell.geometry_ids);
                self.heap.push(HeapEntry {
                    cell_id: coarse_cell_id,
                    source: None,
                    cells: vec![coarse_cell],
                    geometries,
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
                    sponge.geometries.extend(&entry.geometries);
                    sponge.cells.extend(entry.cells);
                    let mut edges = std::mem::take(&mut self.edge_counter);
                    edges.clear();
                    count_short_edges(&sponge.cells, sponge.cell_id.level(), &mut edges);
                    let threshold = self.max_edges.max(
                        (self.min_short_edge_fraction
                            * (edges.len() + sponge.geometries.len()) as f64)
                            as usize,
                    );
                    if edges.len() > threshold {
                        self.coarse_split(sponge);
                    } else {
                        self.heap.push(sponge);
                    }
                    self.edge_counter = edges;
                } else {
                    self.heap.push(entry);
                    return flatten(sponge, &self.edge_cache);
                }
            } else {
                return flatten(sponge, &self.edge_cache);
            }
        }
    }
}

fn flatten(sponge: HeapEntry, edge_cache: &RefCell<EdgeCache<'_, Sphere>>) -> Option<IndexCell> {
    if sponge.cells.len() == 1 {
        return Some(sponge.cells.into_iter().next().unwrap().cell);
    }

    let sponge_pcell = S2PaddedCell::new(sponge.cell_id, CELL_PADDING);
    let sponge_center = sponge_pcell.get_center();

    // Build per-geometry union of edge indices with anchor info for the ray-cast.
    let mut edge_unions: FxHashMap<GeometryId, (Vec<u16>, bool, [f64; 3])> = FxHashMap::default();

    for coarse_cell in &sponge.cells {
        let cell_center = S2PaddedCell::new(coarse_cell.cell.cell_id, CELL_PADDING).get_center();

        for shape in &coarse_cell.cell.shapes {
            let entry = edge_unions
                .entry(shape.geometry_id)
                .or_insert_with(|| (Vec::new(), shape.contains_center, cell_center));
            entry.0.extend_from_slice(&shape.edge_indices);
        }
    }

    for (edges, _, _) in edge_unions.values_mut() {
        edges.sort_unstable();
        edges.dedup();
    }

    let mut result = IndexCell::new(sponge.cell_id);

    for (geometry_id, (edge_indices, anchor_flag, anchor_center)) in &edge_unions {
        if edge_indices.is_empty() {
            result.add_shape(ClippedShape::new(*geometry_id, *anchor_flag));
            continue;
        }

        let vertices = {
            let mut cache = edge_cache.borrow_mut();
            let (head, geo_set) = cache.get_geometry_set(*geometry_id);
            let member_idx = (geometry_id.1 - head) as usize;
            geo_set.members[member_idx].vertices.clone()
        };

        let mut crosser = super::crossings::S2EdgeCrosser::new(anchor_center, &sponge_center);
        let mut crossings = 0u32;
        for &edge_idx in edge_indices {
            let v0 = vertices[edge_idx as usize];
            let v1 = if (edge_idx as usize) + 1 < vertices.len() {
                vertices[(edge_idx as usize) + 1]
            } else {
                v0
            };
            if crosser.edge_or_vertex_crossing_two(&v0, &v1) {
                crossings += 1;
            }
        }
        let contains = anchor_flag ^ (crossings % 2 != 0);

        let mut shape = ClippedShape::new(*geometry_id, contains);
        shape.edge_indices = edge_indices.clone();
        result.add_shape(shape);
    }

    Some(result)
}

fn count_short_edges(
    cells: &[CoarseIndexCell],
    level: i32,
    counter: &mut FxHashSet<(GeometryId, u16)>,
) {
    for cell in cells {
        for (&gid, geo) in &cell.geometries {
            for edge in &geo.edges {
                if level < edge.max_level {
                    counter.insert((gid, edge.edge_index));
                }
            }
        }
    }
}

struct MergeEdge {
    geometry_id: GeometryId,
    edge_index: u16,
    #[allow(unused)]
    max_level: i32,
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
) -> bool {
    use super::crossings::S2EdgeCrosser;
    let mut crosser = S2EdgeCrosser::new(from, to);
    let mut crossings = 0u32;
    for ce in clipped {
        let me = &all_edges[ce.edge_index];
        if me.geometry_id == geometry_id && crosser.edge_or_vertex_crossing_two(&me.v0, &me.v1) {
            crossings += 1;
        }
    }
    from_flag ^ (crossings % 2 != 0)
}

fn clip_to_children(
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
        clip_v_axis(ce, me, v_mid_lo, v_mid_hi, &mut child_edges[0]);
    } else if ce.bound[0].lo() >= u_mid_hi {
        clip_v_axis(ce, me, v_mid_lo, v_mid_hi, &mut child_edges[1]);
    } else if ce.bound[1].hi() <= v_mid_lo {
        child_edges[0][0].push(clip_u_bound(ce, me, u_mid_hi, true));
        child_edges[1][0].push(clip_u_bound(ce, me, u_mid_lo, false));
    } else if ce.bound[1].lo() >= v_mid_hi {
        child_edges[0][1].push(clip_u_bound(ce, me, u_mid_hi, true));
        child_edges[1][1].push(clip_u_bound(ce, me, u_mid_lo, false));
    } else {
        let left = clip_u_bound(ce, me, u_mid_hi, true);
        let right = clip_u_bound(ce, me, u_mid_lo, false);
        child_edges[0][0].push(clip_v_bound(&left, me, v_mid_hi, true));
        child_edges[0][1].push(clip_v_bound(&left, me, v_mid_lo, false));
        child_edges[1][0].push(clip_v_bound(&right, me, v_mid_hi, true));
        child_edges[1][1].push(clip_v_bound(&right, me, v_mid_lo, false));
    }
}

fn clip_v_axis(
    ce: &MergeClippedEdge,
    me: &MergeEdge,
    v_mid_lo: f64,
    v_mid_hi: f64,
    children: &mut [Vec<MergeClippedEdge>; 2],
) {
    if ce.bound[1].hi() <= v_mid_lo {
        children[0].push(ce.clone());
    } else if ce.bound[1].lo() >= v_mid_hi {
        children[1].push(ce.clone());
    } else {
        children[0].push(clip_v_bound(ce, me, v_mid_hi, true));
        children[1].push(clip_v_bound(ce, me, v_mid_lo, false));
    }
}

fn clip_u_bound(
    ce: &MergeClippedEdge,
    me: &MergeEdge,
    u_edge: f64,
    upper: bool,
) -> MergeClippedEdge {
    let a = me.a_uv;
    let b = me.b_uv;
    let v_at_u = interpolate(u_edge, a[0], b[0], a[1], b[1]);
    let u_bound = if upper {
        super::r1interval::R1Interval::new(ce.bound[0].lo(), u_edge)
    } else {
        super::r1interval::R1Interval::new(u_edge, ce.bound[0].hi())
    };
    let v_lo = ce.bound[1].lo().min(v_at_u);
    let v_hi = ce.bound[1].hi().max(v_at_u);
    MergeClippedEdge {
        edge_index: ce.edge_index,
        bound: R2Rect::new(u_bound, super::r1interval::R1Interval::new(v_lo, v_hi)),
    }
}

fn clip_v_bound(
    ce: &MergeClippedEdge,
    me: &MergeEdge,
    v_edge: f64,
    upper: bool,
) -> MergeClippedEdge {
    let a = me.a_uv;
    let b = me.b_uv;
    let u_at_v = interpolate(v_edge, a[1], b[1], a[0], b[0]);
    let v_bound = if upper {
        super::r1interval::R1Interval::new(ce.bound[1].lo(), v_edge)
    } else {
        super::r1interval::R1Interval::new(v_edge, ce.bound[1].hi())
    };
    let u_lo = ce.bound[0].lo().min(u_at_v);
    let u_hi = ce.bound[0].hi().max(u_at_v);
    MergeClippedEdge {
        edge_index: ce.edge_index,
        bound: R2Rect::new(super::r1interval::R1Interval::new(u_lo, u_hi), v_bound),
    }
}

fn interpolate(x: f64, x0: f64, x1: f64, y0: f64, y1: f64) -> f64 {
    if (x0 - x1).abs() < f64::EPSILON {
        return (y0 + y1) * 0.5;
    }
    y0 + (y1 - y0) * (x - x0) / (x1 - x0)
}

fn subdivide_cell(cell: &IndexCell, edge_cache: &RefCell<EdgeCache<'_, Sphere>>) -> Vec<IndexCell> {
    let face = cell.cell_id.face();
    let pcell = S2PaddedCell::new(cell.cell_id, CELL_PADDING);
    let parent_center = pcell.get_center();

    let mut merge_edges: Vec<MergeEdge> = Vec::new();

    for shape in &cell.shapes {
        let vertices = {
            let mut cache = edge_cache.borrow_mut();
            let (head, geo_set) = cache.get_geometry_set(shape.geometry_id);
            let member_idx = (shape.geometry_id.1 - head) as usize;
            geo_set.members[member_idx].vertices.clone()
        };

        for &edge_idx in &shape.edge_indices {
            let v0 = vertices[edge_idx as usize];
            let v1 = if (edge_idx as usize) + 1 < vertices.len() {
                vertices[(edge_idx as usize) + 1]
            } else {
                assert_eq!(
                    edge_idx, 0,
                    "edge index past vertex count is only valid for single-vertex points"
                );
                v0
            };
            let (a_u, a_v) = valid_face_xyz_to_uv(face, &v0);
            let (b_u, b_v) = valid_face_xyz_to_uv(face, &v1);

            merge_edges.push(MergeEdge {
                geometry_id: shape.geometry_id,
                edge_index: edge_idx,
                #[allow(unused)]
                max_level: get_edge_max_level(&v0, &v1),
                v0,
                v1,
                a_uv: [a_u, a_v],
                b_uv: [b_u, b_v],
            });
        }
    }

    let clipped: Vec<MergeClippedEdge> = merge_edges
        .iter()
        .enumerate()
        .map(|(i, e)| MergeClippedEdge {
            edge_index: i,
            bound: R2Rect::from_point_pair(e.a_uv, e.b_uv),
        })
        .collect();

    let parent_contains: Vec<(GeometryId, bool)> = cell
        .shapes
        .iter()
        .map(|s| (s.geometry_id, s.contains_center))
        .collect();

    let middle = pcell.middle();
    let mut child_edges: [[Vec<MergeClippedEdge>; 2]; 2] = Default::default();
    for ce in &clipped {
        let me = &merge_edges[ce.edge_index];
        clip_to_children(ce, me, &middle, &mut child_edges);
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
                );
                if contains {
                    index_cell.add_shape(ClippedShape::new(gid, true));
                }
            }
        } else {
            let mut edge_map: Vec<(GeometryId, Vec<u16>)> = Vec::new();
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
                    .map(|(_, c)| *c)
                    .unwrap_or(false);

                let contains = compute_contains_center(
                    &parent_center,
                    &child_center,
                    parent_flag,
                    &merge_edges,
                    &clipped,
                    *geometry_id,
                );

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

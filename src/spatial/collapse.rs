//! Sibling grouper with dynamic ceiling. Groups cells by parent, promotes when the short edge
//! count allows, clamps when promotion overflows.

use std::cell::RefCell;
use std::collections::{BTreeMap, VecDeque};

use rustc_hash::FxHashSet;

use super::cell_index::{get_edge_max_level, BuildOptions, GeometryId, IndexCell};
use super::edge_cache::EdgeCache;
use super::s2cell_id::S2CellId;
use super::sphere::Sphere;

struct LeveledEdge {
    edge_index: u16,
    max_level: i32,
}

struct LevelGeometry {
    edges: Vec<LeveledEdge>,
}

struct LevelIndexCell {
    cell: IndexCell,
    geometries: BTreeMap<GeometryId, LevelGeometry>,
    geometry_ids: Vec<GeometryId>,
}

struct Level {
    cell_id: S2CellId,
    clamped_cell_id: S2CellId,
    groups: Vec<Vec<LevelIndexCell>>,
}

impl Level {
    fn new() -> Self {
        Level {
            cell_id: S2CellId::none(),
            clamped_cell_id: S2CellId::none(),
            groups: Vec::new(),
        }
    }

    fn clear(&mut self) {
        self.cell_id = S2CellId::none();
        self.groups.clear();
    }
}

/// Groups cells by parent in Hilbert order and promotes groups up the level hierarchy when the
/// short edge count stays under threshold. Produces (parent_id, cells) pairs for the merge stage.
pub struct Collapse<'a, I: Iterator<Item = IndexCell>> {
    inner: I,
    edge_cache: &'a RefCell<EdgeCache<'a, Sphere>>,
    levels: Vec<Level>,
    queue: VecDeque<(S2CellId, Vec<IndexCell>)>,
    last_face: Option<i32>,
    max_edges: usize,
    min_short_edge_fraction: f64,
    cells_in: u64,
    edge_scratch: FxHashSet<(GeometryId, u16)>,
}

impl<'a, I: Iterator<Item = IndexCell>> Collapse<'a, I> {
    /// Creates a new grouper over the given cell iterator with a shared edge cache.
    pub fn new(inner: I, edge_cache: &'a RefCell<EdgeCache<'a, Sphere>>) -> Self {
        let options = BuildOptions::default();
        let levels = S2CellId::MAX_LEVEL as usize + 1;
        Collapse {
            inner,
            edge_cache,
            levels: (0..levels).map(|_| Level::new()).collect(),
            queue: VecDeque::new(),
            last_face: None,
            max_edges: options.max_edges_per_cell,
            min_short_edge_fraction: options.min_short_edge_fraction,
            cells_in: 0,
            edge_scratch: FxHashSet::default(),
        }
    }

    /// Number of cells received from the inner iterator.
    pub fn cells_in(&self) -> u64 {
        self.cells_in
    }

    fn ingest_cell(&mut self, cell: IndexCell) {
        self.cells_in += 1;
        let mut geometries = BTreeMap::new();
        let mut geometry_ids = Vec::with_capacity(cell.shapes.len());
        for shape in &cell.shapes {
            geometry_ids.push(shape.geometry_id);

            if shape.edge_indices.is_empty() {
                continue;
            }

            let mut cache = self.edge_cache.borrow_mut();
            let (head, geo_set) = cache.get_geometry_set(shape.geometry_id);
            let member_idx = (shape.geometry_id.1 - head) as usize;
            let vertices = &geo_set.members[member_idx].vertices;

            let level_geometry = geometries
                .entry(shape.geometry_id)
                .or_insert_with(|| LevelGeometry { edges: Vec::new() });

            for &edge_index in &shape.edge_indices {
                let v0 = vertices[edge_index as usize];
                let v1 = if (edge_index as usize) + 1 < vertices.len() {
                    vertices[(edge_index as usize) + 1]
                } else {
                    v0
                };
                let max_level = get_edge_max_level(&v0, &v1);
                level_geometry.edges.push(LeveledEdge {
                    edge_index,
                    max_level,
                });
            }
        }
        let parent_id = cell.cell_id.immediate_parent();
        self.ingest_group(
            parent_id,
            vec![LevelIndexCell {
                cell,
                geometries,
                geometry_ids,
            }],
        );
    }

    fn ingest_group(&mut self, parent_id: S2CellId, group: Vec<LevelIndexCell>) {
        let level = &mut self.levels[parent_id.level() as usize];
        // If we have clamped at this level, then we have to emit the given cells immediately.
        if level.clamped_cell_id == parent_id {
            self.send_group(group[0].cell.cell_id, group);
            return;
        }
        // If we have crossed out of our parent, we promote our parent.
        if parent_id != level.cell_id && !level.groups.is_empty() {
            let promote_id = level.cell_id.immediate_parent();
            let groups = std::mem::take(&mut level.groups);
            let flattened = groups.into_iter().flatten().collect();
            level.clear();
            self.ingest_group(promote_id, flattened);
        }
        // If we will overflow the group, send accumulated groups, the new group, and clamp.
        let level = &mut self.levels[parent_id.level() as usize];
        let group = vec![group];
        let mut scratch = std::mem::take(&mut self.edge_scratch);
        // TODO Wrong, need to merge.
        let ingest_short_edges = short_edge_count(&group, parent_id.level(), &mut scratch);
        let level_short_edges = short_edge_count(&level.groups, parent_id.level(), &mut scratch);
        self.edge_scratch = scratch;
        let short_edges = ingest_short_edges + level_short_edges;
        let mut geometry_ids = FxHashSet::default();
        collect_geometry_ids(&mut geometry_ids, &group);
        collect_geometry_ids(&mut geometry_ids, &level.groups);
        let threshold = self.max_edges.max(
            (self.min_short_edge_fraction * (short_edges + geometry_ids.len()) as f64) as usize,
        );
        if short_edges > threshold {
            let accumulated = std::mem::take(&mut level.groups);
            level.clamped_cell_id = parent_id;
            level.clear();
            for group in accumulated {
                self.send_group(parent_id, group);
            }
            for group in group {
                self.send_group(parent_id, group);
            }
            return;
        }
        // Otherwise append this group to the list of groups for the level.
        level.cell_id = parent_id;
        for g in group {
            level.groups.push(g);
        }
    }

    fn send_group(&mut self, parent: S2CellId, group: Vec<LevelIndexCell>) {
        self.queue
            .push_back((parent, group.into_iter().map(|c| c.cell).collect()));
    }

    fn flush_all(&mut self) {
        for l in (0..self.levels.len()).rev() {
            let level = &mut self.levels[l];
            if level.groups.is_empty() {
                continue;
            }
            let cell_id = level.cell_id;
            let groups = std::mem::take(&mut level.groups);
            level.clear();
            for group in groups {
                self.send_group(cell_id, group);
            }
        }
    }
}

impl<'a, I: Iterator<Item = IndexCell>> Iterator for Collapse<'a, I> {
    type Item = (S2CellId, Vec<IndexCell>);

    fn next(&mut self) -> Option<(S2CellId, Vec<IndexCell>)> {
        loop {
            if let Some(batch) = self.queue.pop_front() {
                return Some(batch);
            }

            let cell = match self.inner.next() {
                Some(c) => c,
                None => {
                    self.flush_all();
                    return self.queue.pop_front();
                }
            };

            let level = cell.cell_id.level();
            let face = cell.cell_id.face();

            if let Some(last) = self.last_face {
                if last != face {
                    self.flush_all();
                }
            }
            self.last_face = Some(face);

            if level == 0 {
                return Some((cell.cell_id, vec![cell]));
            }

            self.ingest_cell(cell);
        }
    }
}

fn short_edge_count(
    groups: &[Vec<LevelIndexCell>],
    level: i32,
    scratch: &mut FxHashSet<(GeometryId, u16)>,
) -> usize {
    scratch.clear();
    for group in groups {
        for cell in group {
            for (&gid, geo) in &cell.geometries {
                for edge in &geo.edges {
                    if level < edge.max_level {
                        scratch.insert((gid, edge.edge_index));
                    }
                }
            }
        }
    }
    scratch.len()
}

fn collect_geometry_ids(ids: &mut FxHashSet<GeometryId>, groups: &[Vec<LevelIndexCell>]) {
    for group in groups {
        for cell in group {
            ids.extend(&cell.geometry_ids);
        }
    }
}

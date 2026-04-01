//! Merges sibling cell groups into parent cells. `merge_siblings` is the free function that
//! does the work. `CellIndexMerge` wraps an iterator of `(S2CellId, Vec<IndexCell>)` groups and
//! emits `IndexCell`s, collapsing groups with more than one cell.

use super::cell_index::{ClippedShape, GeometryId, IndexCell, CELL_PADDING};
use super::crossings::S2EdgeCrosser;
use super::edge_cache::EdgeCache;
use super::s2cell_id::S2CellId;
use super::s2padded_cell::S2PaddedCell;
use super::sphere::Sphere;

struct CollapseEntry {
    geometry_id: GeometryId,
    anchor_flag: bool,
    anchor_center: [f64; 3],
    edge_indices: Vec<u16>,
}

fn merge_siblings(
    parent_id: S2CellId,
    cells: &[IndexCell],
    edge_cache: &EdgeCache<'_, Sphere>,
) -> IndexCell {
    let parent_pcell = S2PaddedCell::new(parent_id, CELL_PADDING);
    let parent_center = parent_pcell.get_center();

    let mut entries: Vec<CollapseEntry> = Vec::new();

    for cell in cells {
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
        if entry.edge_indices.is_empty() {
            parent_cell.add_shape(ClippedShape::new(entry.geometry_id, entry.anchor_flag));
            continue;
        }

        let entry_geo = edge_cache.get(entry.geometry_id);

        let mut crosser = S2EdgeCrosser::new(&entry.anchor_center, &parent_center);
        let mut crossings = 0u32;
        for &edge_idx in &entry.edge_indices {
            let (v0, v1) = entry_geo.edge(edge_idx);
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

/// Consumes `(S2CellId, Vec<IndexCell>)` groups from a `Collapse` and emits `IndexCell`s. Groups
/// with more than one cell are collapsed into a parent. Single-cell groups pass through.
pub struct CellIndexMerge<'a, I: Iterator<Item = (S2CellId, Vec<IndexCell>)>> {
    inner: I,
    edge_cache: &'a EdgeCache<'a, Sphere>,
}

impl<'a, I: Iterator<Item = (S2CellId, Vec<IndexCell>)>> CellIndexMerge<'a, I> {
    /// Creates a merge from source cell index iterators and a shared edge cache.
    pub fn new(iter: I, edge_cache: &'a EdgeCache<'a, Sphere>) -> Self {
        CellIndexMerge {
            inner: iter,
            edge_cache,
        }
    }

    /// Access the inner iterator.
    pub fn inner(&self) -> &I {
        &self.inner
    }
}

impl<'a, I: Iterator<Item = (S2CellId, Vec<IndexCell>)>> Iterator for CellIndexMerge<'a, I> {
    type Item = IndexCell;

    fn next(&mut self) -> Option<IndexCell> {
        let (parent_id, cells) = self.inner.next()?;
        let mut cells = cells;
        if cells.len() == 1 {
            Some(cells.remove(0))
        } else {
            Some(merge_siblings(parent_id, &cells, self.edge_cache))
        }
    }
}

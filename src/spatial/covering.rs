//! Covering cell types and subdivision for spatial queries.

use super::edge_crosser::EdgeCrosser;
use super::r2rect::R2Rect;
use super::s2cell_id::S2CellId;
use super::s2padded_cell::S2PaddedCell;
use super::surface::Surface;
use crate::spatial::s2edge_clipping::clip_edge_bound;

/// An entry in the covering descent. Carries the padded cell for splitting, the query edge
/// indices that overlap it, and the range of index cells it covers in the segment directory.
pub(crate) struct CoveringCell<S: Surface> {
    pub pcell: S2PaddedCell<S>,
    pub query_edges: Vec<u32>,
    pub contains_center: bool,
    pub index_start: u32,
    pub index_end: u32,
    pub first_index_level: i32,
}

impl<S: Surface> PartialEq for CoveringCell<S> {
    fn eq(&self, other: &Self) -> bool {
        self.pcell.id() == other.pcell.id()
    }
}

impl<S: Surface> Eq for CoveringCell<S> {}

impl<S: Surface> PartialOrd for CoveringCell<S> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<S: Surface> Ord for CoveringCell<S> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let self_interior = self.query_edges.is_empty() && self.contains_center;
        let other_interior = other.query_edges.is_empty() && other.contains_center;
        self_interior
            .cmp(&other_interior)
            .then(other.pcell.id().cmp(&self.pcell.id()))
    }
}

/// Subdivide a covering cell into four children. Distributes query edge indices into child
/// quadrants using UV bounds. Computes index ranges from the parent range. Children inherit the
/// parent's contains_center; call covering_contains_center afterward to update via crossings.
/// Discards exterior children (no query edges and not interior) and empty ranges.
pub(crate) fn covering_split<S: Surface>(
    entry: &CoveringCell<S>,
    get_edge: &impl Fn(u32) -> (S::Point, S::Point),
    start_for_cell: &impl Fn(S2CellId, u32, u32) -> (u32, i32),
) -> Vec<CoveringCell<S>> {
    if entry.pcell.id().is_leaf() {
        return vec![];
    }

    let face = entry.pcell.id().face();
    let cell_bound = entry.pcell.bound();
    let middle = entry.pcell.middle();
    let u_mid_lo = middle[0].lo();
    let u_mid_hi = middle[0].hi();
    let v_mid_lo = middle[1].lo();
    let v_mid_hi = middle[1].hi();

    let mut child_edges: [[Vec<u32>; 2]; 2] = Default::default();

    for &edge_idx in &entry.query_edges {
        let (v0, v1) = get_edge(edge_idx);
        let (a_uv, b_uv) = match S::clip_to_face(&v0, &v1, face, S::CELL_PADDING) {
            Some(ab) => ab,
            None => continue,
        };
        let mut bound = R2Rect::from_point_pair(a_uv, b_uv);
        if !clip_edge_bound(a_uv, b_uv, &cell_bound, &mut bound) {
            continue;
        }
        if bound[0].lo() < u_mid_hi && bound[1].lo() < v_mid_hi {
            child_edges[0][0].push(edge_idx);
        }
        if bound[0].lo() < u_mid_hi && bound[1].hi() > v_mid_lo {
            child_edges[0][1].push(edge_idx);
        }
        if bound[0].hi() > u_mid_lo && bound[1].lo() < v_mid_hi {
            child_edges[1][0].push(edge_idx);
        }
        if bound[0].hi() > u_mid_lo && bound[1].hi() > v_mid_lo {
            child_edges[1][1].push(edge_idx);
        }
    }

    let mut child_ij: [(usize, usize); 4] = [(0, 0); 4];
    let mut child_ids: [S2CellId; 4] = [S2CellId(0); 4];
    for pos in 0..4 {
        let (i, j) = entry.pcell.get_child_ij(pos);
        child_ij[pos as usize] = (i, j);
        child_ids[pos as usize] = entry.pcell.id().child(pos);
    }

    let mut starts = [(0u32, 0i32); 4];
    starts[0] = (entry.index_start, entry.first_index_level);
    for k in 1..4 {
        starts[k] = start_for_cell(child_ids[k], starts[k - 1].0, entry.index_end);
    }

    let mut result = Vec::with_capacity(4);
    for k in 0..4 {
        let (i, j) = child_ij[k];
        let edges = std::mem::take(&mut child_edges[i][j]);
        let (start, first_index_level) = starts[k];
        let end = if k < 3 {
            starts[k + 1].0
        } else {
            entry.index_end
        };

        let is_interior = edges.is_empty() && entry.contains_center;

        if start == end || (edges.is_empty() && !is_interior) {
            continue;
        }

        let child_pcell = S2PaddedCell::from_parent(&entry.pcell, i, j);

        result.push(CoveringCell {
            pcell: child_pcell,
            query_edges: edges,
            contains_center: entry.contains_center,
            index_start: start,
            index_end: end,
            first_index_level,
        });
    }

    result
}

/// Update contains_center on child cells by testing edge crossings from the parent center
/// to each child center.
pub(crate) fn covering_contains_center<S: Surface>(
    parent_center: &S::Point,
    children: &mut [CoveringCell<S>],
    get_edge: &impl Fn(u32) -> (S::Point, S::Point),
) {
    for child in children.iter_mut() {
        if child.query_edges.is_empty() {
            continue;
        }
        let child_center = child.pcell.get_center();
        let mut crosser = S::EdgeCrosser::new(parent_center, &child_center);
        for &edge_idx in &child.query_edges {
            let (v0, v1) = get_edge(edge_idx);
            if crosser.edge_or_vertex_crossing_two(&v0, &v1) {
                child.contains_center = !child.contains_center;
            }
        }
    }
}

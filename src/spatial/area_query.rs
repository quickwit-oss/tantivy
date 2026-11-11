//! Search for the geometries that meet an area of the sphere that is not a polygon: a rectangle
//! of latitude and longitude, or what lies within a distance of a point.
//!
//! A covering of the area finds the candidates. A covering cell within the area decides what it
//! holds without decoding it: whatever lies in the cell has a point in the area. A point, most
//! often the whole of its document, is read from its entry in the edge index and nothing else.
//! Only lines and polygons in the cells that the area's boundary crosses are decoded and tested
//! edge by edge.

use std::collections::HashSet;

use common::BitSet;

use super::cell_index_reader::CellIndexReader;
use super::edge_cache::EdgeCache;
use super::geometry_set::EdgeSet;
use super::region::Region;
use super::region_coverer::{CovererOptions, RegionCoverer};
use super::s2cell::S2Cell;
use super::s2cell_id::S2CellId;
use super::shape_index_region::{contains_point, SegmentIndex};
use super::sphere::Sphere;

/// Cells in the covering of an area. More cells follow its boundary more closely, and leave
/// fewer geometries outside it to read and reject.
const COVERING_CELLS: i32 = 64;

/// A connected area of the sphere that tests points and edges against itself.
pub(crate) trait Area {
    /// Whether the area contains the point.
    fn contains(&self, point: &[f64; 3]) -> bool;

    /// Whether the area and the geodesic edge between two points outside it have a point in
    /// common.
    fn meets_edge(&self, a: &[f64; 3], b: &[f64; 3]) -> bool;

    /// A point of the area. A polygon that contains it, and has no edge that meets the area,
    /// contains the area.
    fn inside(&self) -> &[f64; 3];
}

/// The cells of a covering of an area, and whether the area contains each.
pub(crate) struct Covering(Vec<(S2CellId, bool)>);

impl Covering {
    /// Cover a region. `within` tells whether the area contains a cell: it may say no of a cell
    /// that the area does contain, whose geometries are then tested one by one.
    pub(crate) fn new<R: Region>(region: &R, within: impl Fn(&S2Cell) -> bool) -> Self {
        let coverer = RegionCoverer::new(CovererOptions::default().max_cells(COVERING_CELLS));
        let cells = coverer.get_covering(region).into_cell_ids();
        Covering(
            cells
                .into_iter()
                .map(|id| (id, within(&S2Cell::new(id))))
                .collect(),
        )
    }

    /// Calls `visit` with the position of each geometry in the segment's cells that the covering
    /// overlaps, and whether the cell lies in a covering cell within the area. A geometry in
    /// several cells is visited once for each.
    pub(crate) fn visit(&self, cell_reader: &CellIndexReader, mut visit: impl FnMut(u32, bool)) {
        for &(covering_cell, within) in &self.0 {
            cell_reader.visit_range(covering_cell, |index_cell, position| {
                visit(position, within && covering_cell.contains(index_cell))
            });
        }
    }
}

/// The edges of a member: consecutive vertices of each ring that differ.
pub(crate) fn edges(member: &EdgeSet) -> impl Iterator<Item = (&[f64; 3], &[f64; 3])> {
    member.ring_offsets.windows(2).flat_map(move |ring| {
        (ring[0]..ring[1].saturating_sub(1))
            .map(move |i| (&member.vertices[i], &member.vertices[i + 1]))
            .filter(|(a, b)| a != b)
    })
}

/// Add the documents of one segment that have a point in common with the area to `docs`. A
/// document meets the area when any member of its set does.
pub(crate) fn search_meeting<A: Area>(
    area: &A,
    covering: &Covering,
    cell_reader: &CellIndexReader,
    edge_cache: &mut EdgeCache<'_, Sphere>,
    docs: &mut BitSet,
) {
    // Lines and polygons tested, each of which may lie in many cells.
    let mut tested: HashSet<u32> = HashSet::new();
    covering.visit(cell_reader, |position, within| {
        let summary = edge_cache.summary((0, position));
        if docs.contains(summary.doc_id) {
            return;
        }
        let meets = if within {
            true
        } else if let Some(point) = summary.point {
            area.contains(&point)
        } else {
            tested.insert(position) && member_meets(area, position, cell_reader, edge_cache)
        };
        if meets {
            docs.insert(summary.doc_id);
        }
    });
}

/// Whether the area and a line or polygon have a point in common: a vertex within the area, an
/// edge that meets it, or for a polygon the area within it.
fn member_meets<A: Area>(
    area: &A,
    position: u32,
    cell_reader: &CellIndexReader,
    edge_cache: &mut EdgeCache<'_, Sphere>,
) -> bool {
    let entry = edge_cache.get((0, position));
    let member = entry.edge_set();
    if member.vertices.iter().any(|v| area.contains(v))
        || edges(member).any(|(a, b)| area.meets_edge(a, b))
    {
        return true;
    }
    if !member.closed {
        return false;
    }
    let mut segment = SegmentIndex {
        cell_reader,
        edge_cache,
    };
    contains_point(&mut segment, (0, position), area.inside())
}

//! Spatial index for geometry on the unit sphere.
//!
//! Maps S2CellIds to the shapes that intersect each cell. Cells are subdivided until no cell
//! contains more than a configurable number of edges.
use std::io::Write;

use common::CountingWriter;

use super::containment::brute_force_contains;
use super::crossings::S2EdgeCrosser;
use super::math::normalize;
use super::r1interval::R1Interval;
use super::r2rect::R2Rect;
use super::s2cell_id::S2CellId;
use super::s2coords::{face_uv_to_xyz, get_face, NUM_FACES};
use super::s2edge_clipping::{
    clip_to_padded_face, EDGE_CLIP_ERROR_UV_COORD, FACE_CLIP_ERROR_UV_COORD,
};
use super::s2padded_cell::S2PaddedCell;
use crate::spatial::s2coords::valid_face_xyz_to_uv;

/// The amount by which cells are "padded" to compensate for numerical errors
/// when clipping line segments to cell boundaries.
pub const CELL_PADDING: f64 = 2.0 * (FACE_CLIP_ERROR_UV_COORD + EDGE_CLIP_ERROR_UV_COORD);

/// Default maximum number of edges per cell (not counting "long" edges). Reasonable values range
/// from 10 to about 50 or so.
pub const DEFAULT_MAX_EDGES_PER_CELL: usize = 10;

/// Average edge length metric derivative for quadratic projection.
/// From s2metrics.cc: kAvgEdge for S2_QUADRATIC_PROJECTION
pub(crate) const K_AVG_EDGE_DERIV: f64 = 1.459_213_746_386_106_1;

/// Default ratio of cell size to edge length for "long" edge classification.
/// From FLAGS_s2shape_index_cell_size_to_long_edge_ratio default value.
pub(crate) const CELL_SIZE_TO_LONG_EDGE_RATIO: f64 = 1.0;

/// Returns the minimum level such that the average edge metric is at most the given value.
/// Port of S2::Metric<1>::GetLevelForMaxValue from s2metrics.h.
pub(crate) fn get_level_for_max_value(value: f64) -> i32 {
    // Catches non-positive values, including NaN.
    if value <= 0.0 || value.is_nan() {
        return S2CellId::MAX_LEVEL;
    }

    // This code is equivalent to computing a floating-point "level" value and
    // rounding up. ilogb() returns the exponent corresponding to a fraction in
    // the range [1,2).
    let ratio = value / K_AVG_EDGE_DERIV;
    let level = ratio.log2().floor() as i32;
    // For dim=1, (level >> (dim - 1)) == (level >> 0) == level
    (-level).clamp(0, S2CellId::MAX_LEVEL)
}

/// Computes the maximum level at which an edge is "short" relative to the
/// cell size. Above this level the edge is "long" and doesn't count toward
/// the subdivision threshold. Port of MutableS2ShapeIndex::GetEdgeMaxLevel.
pub(crate) fn get_edge_max_level(v0: &[f64; 3], v1: &[f64; 3]) -> i32 {
    let length =
        ((v0[0] - v1[0]).powi(2) + (v0[1] - v1[1]).powi(2) + (v0[2] - v1[2]).powi(2)).sqrt();
    let max_cell_edge = length * CELL_SIZE_TO_LONG_EDGE_RATIO;
    get_level_for_max_value(max_cell_edge)
}

/// Represents the part of a shape that intersects an S2Cell.
///
/// It consists of the set of edge ids that intersect that cell, and a boolean indicating whether
/// the center of the cell is inside the shape (for shapes that have an interior).
///
/// Note that the edges themselves are not clipped; we always use the original edges for
/// intersection tests so that the results will be the same as the original shape.
#[derive(Clone, Debug, PartialEq)]
pub struct ClippedShape {
    /// The geometry id maps back to a doc id through the edge index.
    pub geometry_id: u32,
    /// True if the geometry contains the center of the cell.
    pub contains_center: bool,
    /// Indicies of the edges stored per geometry id in the edge index.
    pub edge_indices: Vec<u16>,
}

/// Stores the index contents for a particular S2CellId.
///
/// It consists of a set of clipped shapes.
#[derive(Clone, Debug, PartialEq)]
pub struct IndexCell {
    /// Cell id.
    pub cell_id: S2CellId,
    /// Shapes stored in the cell.
    pub shapes: Vec<ClippedShape>,
}

impl IndexCell {
    /// Creates a new empty IndexCell for the given cell ID.
    pub fn new(cell_id: S2CellId) -> Self {
        Self {
            cell_id,
            shapes: Vec::new(),
        }
    }

    /// Adds a clipped shape to this cell.
    pub fn add_shape(&mut self, shape: ClippedShape) {
        self.shapes.push(shape);
    }

    /// Returns the clipped shape corresponding to the given geometry ID, or None if the geometry
    /// does not intersect this cell.
    pub fn find_shape(&self, geometry_id: u32) -> Option<&ClippedShape> {
        self.shapes.iter().find(|s| s.geometry_id == geometry_id)
    }

    /// Returns the number of clipped shapes in this cell.
    #[inline]
    pub fn num_shapes(&self) -> usize {
        self.shapes.len()
    }

    /// Returns true if this cell contains no clipped shapes.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.shapes.is_empty()
    }
}

impl ClippedShape {
    /// Creates a new ClippedShape with the given geometry ID and contains_center flag.
    pub fn new(geometry_id: u32, contains_center: bool) -> Self {
        Self {
            geometry_id,
            contains_center,
            edge_indices: Vec::new(),
        }
    }

    /// Adds an edge index to this clipped shape.
    pub fn add_edge(&mut self, edge_id: u16) {
        self.edge_indices.push(edge_id);
    }

    /// Returns the number of edges that intersect the S2CellId.
    #[inline]
    pub fn num_edges(&self) -> usize {
        self.edge_indices.len()
    }
}

/// A spatial index mapping S2CellIds to the shapes that intersect each cell.
#[derive(Clone, Debug, Default)]
pub struct CellIndex {
    /// Cells in the index.
    pub cells: Vec<IndexCell>,
}

/// An edge projected onto a cube face.
#[derive(Clone, Debug)]
struct FaceEdge {
    /// Geometry the edge belongs to.
    geometry_id: u32,
    /// Edge index in the original polygon.
    edge_id: u16,
    /// Maximum level for subdivision (based on edge length).
    max_level: i32,
    /// Determine if we are indexing a polygon.
    has_interior: bool,
    /// Edge endpoints in (u,v) coordinates.
    a: [f64; 2],
    b: [f64; 2],
    /// Original edge endpoints in 3D.
    v0: [f64; 3],
    v1: [f64; 3],
}

/// A clipped portion of an edge within a cell.
#[derive(Clone, Debug)]
struct ClippedEdge {
    geometry_id: u32,
    /// Original unclipped edge.
    face_edge_index: usize,
    /// Bounding box of the clipped edge in (u,v) coordinates.
    bound: R2Rect,
}

/// Tracks which cells' centers are inside the polygon as we traverse in S2CellId order.
///
/// `is_active` is used for incremental updates in S2, so it can go from true to false. Here it
/// only ever goes from false to true if any of the indexed geometries is a polygon. If there are
/// no polygons to index, it will disable the tracker. It is more for purity of the port than for
/// optimization.
///
/// The C++ MutableS2ShapeIndex uses a sorted vector rather than a set for tracking contained
/// shapes. The S2 authors benchmarked this and found sorted vector faster for the typical case of
/// 0-2 shapes. This set of contained shapes does not stack deep at any single point, it is the set
/// of overlapping polygons at a certian point on the face. This holds for bulk load as well as
/// incremental update.
///
/// The C++ uses linear search. We use binary_search because the function is there and reads better
/// than a loop. For 0-2 elements the difference is negligible.
struct InteriorTracker {
    /// Determine if any shape with an interior is being tracked.
    is_active: bool,
    /// Current focus point.
    focus: [f64; 3],
    /// Edge crosser for testing edge crossings.
    crosser: S2EdgeCrosser,
    /// Shapes whose interiors contain the current focus point.
    shape_ids: Vec<u32>,
    /// The next cell ID we expect to process.
    next_cellid: S2CellId,
}

impl InteriorTracker {
    fn new() -> Self {
        let origin = normalize(&face_uv_to_xyz(0, -1.0, -1.0));
        Self {
            focus: origin,
            crosser: S2EdgeCrosser::new(&origin, &origin),
            shape_ids: Vec::new(),
            is_active: false,
            next_cellid: S2CellId::begin(S2CellId::MAX_LEVEL),
        }
    }

    fn toggle_shape(&mut self, geometry_id: u32) {
        match self.shape_ids.binary_search(&geometry_id) {
            Ok(i) => {
                self.shape_ids.remove(i);
            }
            Err(i) => {
                self.shape_ids.insert(i, geometry_id);
            }
        }
    }

    fn test_edge(&mut self, geometry_id: u32, v0: &[f64; 3], v1: &[f64; 3]) {
        if self.crosser.edge_or_vertex_crossing_two(v0, v1) {
            self.toggle_shape(geometry_id);
        }
    }

    fn shape_ids(&self) -> &[u32] {
        &self.shape_ids
    }

    #[inline]
    fn at_cellid(&self, id: S2CellId) -> bool {
        id.range_min() == self.next_cellid
    }

    #[inline]
    fn set_next_cellid(&mut self, id: S2CellId) {
        self.next_cellid = id.range_min();
    }

    #[inline]
    fn move_to(&mut self, p: &[f64; 3]) {
        self.focus = *p;
    }

    fn draw_to(&mut self, p: &[f64; 3]) {
        self.crosser = S2EdgeCrosser::new(&self.focus, p);
        self.focus = *p;
    }
}

/// Options that affect construction of the CellIndex.
#[derive(Clone, Debug)]
pub struct BuildOptions {
    /// Maximum edges per cell before subdivision.
    pub max_edges_per_cell: usize,
    /// Minimum fraction of 'short' edges required for subdivision.
    /// If this parameter is non-zero then the total index size and construction
    /// time are guaranteed to be linear in the number of input edges.
    /// Default: 0.2 (from FLAGS_s2shape_index_min_short_edge_fraction)
    pub min_short_edge_fraction: f64,
}

impl Default for BuildOptions {
    fn default() -> Self {
        Self {
            max_edges_per_cell: DEFAULT_MAX_EDGES_PER_CELL,
            min_short_edge_fraction: 0.2,
        }
    }
}

impl BuildOptions {
    /// Creates new build options with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the maximum number of edges per cell.
    ///
    /// If a cell has more than this many edges that are not considered "long" relative to the cell
    /// size, then it is subdivided.
    pub fn with_max_edges_per_cell(mut self, max: usize) -> Self {
        self.max_edges_per_cell = max;
        self
    }

    /// Sets the minimum short edge fraction.
    pub fn with_min_short_edge_fraction(mut self, fraction: f64) -> Self {
        self.min_short_edge_fraction = fraction;
        self
    }
}

impl CellIndex {
    /// Creates a new empty CellIndex.
    pub fn new() -> Self {
        Self { cells: Vec::new() }
    }

    /// Returns true if the index contains no cells.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.cells.is_empty()
    }

    /// Returns the number of cells in the index.
    #[inline]
    pub fn num_cells(&self) -> usize {
        self.cells.len()
    }

    /// Returns the index cell containing the given cell ID, or None if not found.
    pub fn find_cell(&self, target: S2CellId) -> Option<&IndexCell> {
        let mut lo = 0;
        let mut hi = self.cells.len();

        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let cell = &self.cells[mid];

            if target < cell.cell_id.range_min() {
                hi = mid;
            } else if target > cell.cell_id.range_max() {
                lo = mid + 1;
            } else {
                return Some(cell);
            }
        }

        None
    }

    /// Writes the cell index to a CountingWriter with a dictionary at the end.
    pub fn write<W: Write>(&self, write: &mut CountingWriter<W>) {
        let mut offsets: Vec<(u64, u64)> = Vec::with_capacity(self.cells.len());

        for cell in &self.cells {
            let offset = write.written_bytes();

            write.write_all(&cell.cell_id.0.to_le_bytes()).unwrap();
            write
                .write_all(&(cell.shapes.len() as u16).to_le_bytes())
                .unwrap();

            for shape in &cell.shapes {
                write.write_all(&shape.geometry_id.to_le_bytes()).unwrap();
                write.write_all(&[shape.contains_center as u8]).unwrap();
                write
                    .write_all(&(shape.edge_indices.len() as u16).to_le_bytes())
                    .unwrap();
                for &edge_id in &shape.edge_indices {
                    write.write_all(&edge_id.to_le_bytes()).unwrap();
                }
            }

            offsets.push((cell.cell_id.0, offset));
        }

        // Dictionary: (cell_id, offset) pairs.
        let dir_offset = write.written_bytes();
        for &(cell_id, offset) in &offsets {
            write.write_all(&cell_id.to_le_bytes()).unwrap();
            write.write_all(&offset.to_le_bytes()).unwrap();
        }

        // Footer: cell count, directory offset.
        write
            .write_all(&(self.cells.len() as u32).to_le_bytes())
            .unwrap();
        write.write_all(&dir_offset.to_le_bytes()).unwrap();
    }
}

/// Input geometry data for indexing, consisting of rings and metadata.
#[derive(Clone)]
pub struct GeometryData {
    /// The rings that define the geometry. A simple polygon has one ring. A polygon with holes
    /// has an outer ring followed by hole rings. A line string has one ring (open, not wrapped).
    pub rings: Vec<Vec<[f64; 3]>>,
    /// Whether the reference origin is inside each ring, parallel to `rings`.
    pub origin_inside: Vec<bool>,
    /// The geometry type.
    pub dimension: u8,
}

/// Builder for constructing a CellIndex from a collection of geometries.
pub struct IndexBuilder {
    /// Geometries grouped by document. Each entry is (doc_id, geometries).
    documents: Vec<(u32, Vec<GeometryData>)>,
    options: BuildOptions,
    /// Tracks which cells' centers are inside the polygon as we traverse in S2CellId order.
    tracker: InteriorTracker,
    /// The index as an array of cells.
    cells: Vec<IndexCell>,
}

impl IndexBuilder {
    /// Creates a new IndexBuilder with the given options.
    pub fn new(options: BuildOptions) -> Self {
        Self {
            documents: Vec::new(),
            options,
            tracker: InteriorTracker::new(),
            cells: Vec::new(),
        }
    }

    /// Adds a document's geometries to the builder.
    pub fn add(&mut self, doc_id: u32, geometries: Vec<GeometryData>) {
        self.documents.push((doc_id, geometries));
    }

    /// Consumes the builder and returns the constructed CellIndex.
    pub fn build(mut self) -> CellIndex {
        // Reject the degenerates.
        if self.documents.is_empty() {
            return CellIndex { cells: Vec::new() };
        }

        // Flatten geometries with sequential geometry_ids.
        let mut geometry_id: u32 = 0;

        // Brute force to determine if the focal point starts inside or outside of each polygon.
        // Each ring toggles the geometry_id independently: outer ring toggles on, hole ring
        // toggles back off if the origin falls inside the hole.
        let tracker_origin = self.tracker.focus;
        for (_doc_id, geometries) in &self.documents {
            for geo in geometries {
                if geo.dimension < 2 {
                    geometry_id += 1;
                    continue;
                }
                self.tracker.is_active = true;
                for (ring, &ring_origin_inside) in geo.rings.iter().zip(geo.origin_inside.iter()) {
                    let inside = brute_force_contains(&tracker_origin, ring, ring_origin_inside);
                    if inside {
                        self.tracker.toggle_shape(geometry_id);
                    }
                }
                geometry_id += 1;
            }
        }

        // Collection of edges per face.
        let mut face_edges: [Vec<FaceEdge>; 6] = Default::default();

        // Stuff our faces. Edge IDs are flat across all rings within a geometry.
        geometry_id = 0;
        for (_doc_id, geometries) in &self.documents {
            for geo in geometries {
                let mut edge_id: u16 = 0;

                for ring in &geo.rings {
                    let n = ring.len();

                    let min_vertices = if geo.dimension == 2 { 3 } else { 2 };
                    if n < min_vertices {
                        continue;
                    }

                    let edge_count = if geo.dimension == 2 { n } else { n - 1 };

                    for i in 0..edge_count {
                        let v0 = ring[i];
                        let v1 = if geo.dimension == 2 {
                            ring[(i + 1) % n]
                        } else {
                            ring[i + 1]
                        };
                        let max_level = self.get_edge_max_level(&v0, &v1);
                        self.add_face_edge(
                            geometry_id,
                            edge_id,
                            max_level,
                            geo.dimension == 2,
                            &v0,
                            &v1,
                            &mut face_edges,
                        );
                        edge_id += 1;
                    }

                    // Skip the duplicated first-vertex position for polygon rings.
                    // The stored format appends ring[0] after each ring so edge lookup
                    // is always position and position + 1, never modulo.
                    if geo.dimension == 2 {
                        edge_id += 1;
                    }
                }

                geometry_id += 1;
            }
        }

        for face in 0..NUM_FACES {
            self.update_face_edges(face, &face_edges[face as usize]);
        }

        self.cells.sort_by_key(|c| c.cell_id);

        CellIndex { cells: self.cells }
    }

    /// Delegates to the free function `get_edge_max_level`.
    fn get_edge_max_level(&self, v0: &[f64; 3], v1: &[f64; 3]) -> i32 {
        get_edge_max_level(v0, v1)
    }

    /// Clips an edge to all cube faces.
    #[allow(clippy::too_many_arguments)]
    fn add_face_edge(
        &self,
        geometry_id: u32,
        edge_id: u16,
        max_level: i32,
        has_interior: bool,
        v0: &[f64; 3],
        v1: &[f64; 3],
        face_edges: &mut [Vec<FaceEdge>; 6],
    ) {
        let a_face = get_face(v0);
        let b_face = get_face(v1);

        if a_face == b_face {
            let (a_u, a_v) = valid_face_xyz_to_uv(a_face, v0);
            let (b_u, b_v) = valid_face_xyz_to_uv(a_face, v1);
            let max_uv = 1.0 - CELL_PADDING;
            if a_u.abs() <= max_uv
                && a_v.abs() <= max_uv
                && b_u.abs() <= max_uv
                && b_v.abs() <= max_uv
            {
                face_edges[a_face as usize].push(FaceEdge {
                    geometry_id,
                    edge_id,
                    max_level,
                    has_interior,
                    a: [a_u, a_v],
                    b: [b_u, b_v],
                    v0: *v0,
                    v1: *v1,
                });
                return;
            }
        }

        for face in 0..NUM_FACES {
            if let Some((a_uv, b_uv)) = clip_to_padded_face(v0, v1, face, CELL_PADDING) {
                face_edges[face as usize].push(FaceEdge {
                    geometry_id,
                    edge_id,
                    max_level,
                    has_interior,
                    a: a_uv,
                    b: b_uv,
                    v0: *v0,
                    v1: *v1,
                });
            }
        }
    }

    fn update_face_edges(&mut self, face: i32, face_edges: &[FaceEdge]) {
        if face_edges.is_empty() {
            return;
        }

        let mut clipped: Vec<ClippedEdge> = Vec::with_capacity(face_edges.len());
        let mut bound = R2Rect::empty();

        for (i, fe) in face_edges.iter().enumerate() {
            let edge_bound = R2Rect::from_point_pair(fe.a, fe.b);
            clipped.push(ClippedEdge {
                geometry_id: fe.geometry_id,
                face_edge_index: i,
                bound: edge_bound,
            });
            bound = bound.union(&edge_bound);
        }

        let face_id = S2CellId::from_face(face);
        let pcell = S2PaddedCell::new(face_id, CELL_PADDING);

        let shrunk_id = pcell.shrink_to_fit(&bound);
        if shrunk_id != face_id {
            self.skip_cell_range(face_id.range_min(), shrunk_id.range_min());

            let pcell = S2PaddedCell::new(shrunk_id, CELL_PADDING);
            self.update_edges(&pcell, &mut clipped, face_edges);

            self.skip_cell_range(shrunk_id.range_max().next(), face_id.range_max().next());
        } else {
            self.update_edges(&pcell, &mut clipped, face_edges);
        }
    }

    fn skip_cell_range(&mut self, begin: S2CellId, end: S2CellId) {
        if self.tracker.shape_ids().is_empty() {
            return;
        }

        for cell_id in cell_union_from_range(begin, end) {
            let pcell = S2PaddedCell::new(cell_id, CELL_PADDING);
            let mut empty_edges: Vec<ClippedEdge> = Vec::new();
            self.update_edges(&pcell, &mut empty_edges, &[]);
        }
    }

    fn update_edges(
        &mut self,
        pcell: &S2PaddedCell,
        edges: &mut [ClippedEdge],
        face_edges: &[FaceEdge],
    ) {
        if edges.is_empty() && self.tracker.shape_ids().is_empty() {
            return;
        }

        if self.should_subdivide(pcell, edges, face_edges, &self.tracker) {
            let middle = pcell.middle();

            let mut child_edges: [[Vec<ClippedEdge>; 2]; 2] = Default::default();
            for i in 0..2 {
                for j in 0..2 {
                    child_edges[i][j].reserve(edges.len());
                }
            }

            for edge in edges.iter() {
                self.clip_to_children(edge, &middle, face_edges, &mut child_edges);
            }

            for pos in 0..4 {
                let (i, j) = pcell.get_child_ij(pos);

                let dominated_by_geometry_with_no_edges =
                    !self.tracker.shape_ids().is_empty() && child_edges[i][j].is_empty();

                if !child_edges[i][j].is_empty() || dominated_by_geometry_with_no_edges {
                    let child_pcell = S2PaddedCell::from_parent(pcell, i, j);
                    self.update_edges(&child_pcell, &mut child_edges[i][j], face_edges);
                }
            }
        } else {
            self.make_index_cell(pcell, edges, face_edges);
        }
    }

    /// Returns true if the cell should be subdivided.
    fn should_subdivide(
        &self,
        pcell: &S2PaddedCell,
        edges: &[ClippedEdge],
        face_edges: &[FaceEdge],
        tracker: &InteriorTracker,
    ) -> bool {
        if edges.len() <= self.options.max_edges_per_cell {
            return false;
        }

        // Compute the threshold for short edges. This ensures linear index size. The formula is:
        // max(max_edges_per_cell, min_short_edge_fraction * (edges + shapes))
        let max_short_edges = self.options.max_edges_per_cell.max(
            (self.options.min_short_edge_fraction
                * (edges.len() + tracker.shape_ids().len()) as f64) as usize,
        );

        // Count short edges (those that could benefit from subdivision).
        let mut short_count = 0;
        for edge in edges {
            let fe = &face_edges[edge.face_edge_index];
            if pcell.level() < fe.max_level {
                short_count += 1;
                // Early exit: if we exceed max_short_edges, keep subdividing
                if short_count > max_short_edges {
                    return true;
                }
            }
        }

        // If short_count <= max_short_edges, stop subdividing (make a cell)
        false
    }

    fn clip_to_children(
        &self,
        edge: &ClippedEdge,
        middle: &R2Rect,
        face_edges: &[FaceEdge],
        child_edges: &mut [[Vec<ClippedEdge>; 2]; 2],
    ) {
        // Get the middle boundaries (the padding region shared by all children)
        let u_mid_lo = middle[0].lo();
        let u_mid_hi = middle[0].hi();
        let v_mid_lo = middle[1].lo();
        let v_mid_hi = middle[1].hi();

        let fe = &face_edges[edge.face_edge_index];

        // Determine which children the edge goes to along the u-axis
        if edge.bound[0].hi() <= u_mid_lo {
            // Edge is entirely in left children (i=0)
            self.clip_v_axis(edge, fe, v_mid_lo, v_mid_hi, &mut child_edges[0]);
        } else if edge.bound[0].lo() >= u_mid_hi {
            // Edge is entirely in right children (i=1)
            self.clip_v_axis(edge, fe, v_mid_lo, v_mid_hi, &mut child_edges[1]);
        } else if edge.bound[1].hi() <= v_mid_lo {
            // Edge is entirely in lower children (j=0)
            child_edges[0][0].push(self.clip_u_bound(edge, fe, u_mid_hi, true));
            child_edges[1][0].push(self.clip_u_bound(edge, fe, u_mid_lo, false));
        } else if edge.bound[1].lo() >= v_mid_hi {
            // Edge is entirely in upper children (j=1)
            child_edges[0][1].push(self.clip_u_bound(edge, fe, u_mid_hi, true));
            child_edges[1][1].push(self.clip_u_bound(edge, fe, u_mid_lo, false));
        } else {
            // Edge spans all four children
            let left = self.clip_u_bound(edge, fe, u_mid_hi, true);
            self.clip_v_axis(&left, fe, v_mid_lo, v_mid_hi, &mut child_edges[0]);
            let right = self.clip_u_bound(edge, fe, u_mid_lo, false);
            self.clip_v_axis(&right, fe, v_mid_lo, v_mid_hi, &mut child_edges[1]);
        }
    }

    fn clip_v_axis(
        &self,
        edge: &ClippedEdge,
        fe: &FaceEdge,
        v_mid_lo: f64,
        v_mid_hi: f64,
        child_edges: &mut [Vec<ClippedEdge>; 2],
    ) {
        if edge.bound[1].hi() <= v_mid_lo {
            // Edge is entirely in lower child (j=0)
            child_edges[0].push(edge.clone());
        } else if edge.bound[1].lo() >= v_mid_hi {
            // Edge is entirely in upper child (j=1)
            child_edges[1].push(edge.clone());
        } else {
            // Edge spans both children
            child_edges[0].push(self.clip_v_bound(edge, fe, v_mid_hi, true));
            child_edges[1].push(self.clip_v_bound(edge, fe, v_mid_lo, false));
        }
    }

    /// Clip the u-axis bound of an edge. If `clip_hi` is true, clip the high end to `u`; otherwise
    /// clip the low end.
    fn clip_u_bound(
        &self,
        edge: &ClippedEdge,
        fe: &FaceEdge,
        u: f64,
        clip_hi: bool,
    ) -> ClippedEdge {
        // Check if clipping is actually needed
        if clip_hi {
            if edge.bound[0].hi() <= u {
                return edge.clone();
            }
        } else if edge.bound[0].lo() >= u {
            return edge.clone();
        }

        // Interpolate to find the v-coordinate where the edge crosses u
        let v = interpolate_double(u, fe.a[0], fe.b[0], fe.a[1], fe.b[1]);
        let v_clamped = edge.bound[1].clamp(v);

        // Determine which end of the v-bound to update based on edge slope
        let slopes_same_sign = (fe.a[0] > fe.b[0]) == (fe.a[1] > fe.b[1]);
        let clip_v_hi = if clip_hi {
            slopes_same_sign
        } else {
            !slopes_same_sign
        };

        let new_bound = if clip_hi {
            R2Rect::new(
                R1Interval::new(edge.bound[0].lo(), u),
                if clip_v_hi {
                    R1Interval::new(edge.bound[1].lo(), v_clamped)
                } else {
                    R1Interval::new(v_clamped, edge.bound[1].hi())
                },
            )
        } else {
            R2Rect::new(
                R1Interval::new(u, edge.bound[0].hi()),
                if clip_v_hi {
                    R1Interval::new(edge.bound[1].lo(), v_clamped)
                } else {
                    R1Interval::new(v_clamped, edge.bound[1].hi())
                },
            )
        };

        ClippedEdge {
            geometry_id: edge.geometry_id,
            face_edge_index: edge.face_edge_index,
            bound: new_bound,
        }
    }

    /// Clip the v-axis bound of an edge. If `clip_hi` is true, clip the high end to `v`; otherwise
    /// clip the low end.
    fn clip_v_bound(
        &self,
        edge: &ClippedEdge,
        fe: &FaceEdge,
        v: f64,
        clip_hi: bool,
    ) -> ClippedEdge {
        // Check if clipping is actually needed
        if clip_hi {
            if edge.bound[1].hi() <= v {
                return edge.clone();
            }
        } else if edge.bound[1].lo() >= v {
            return edge.clone();
        }

        // Interpolate to find the u-coordinate where the edge crosses v
        let u = interpolate_double(v, fe.a[1], fe.b[1], fe.a[0], fe.b[0]);
        let u_clamped = edge.bound[0].clamp(u);

        // Determine which end of the u-bound to update based on edge slope
        let slopes_same_sign = (fe.a[0] > fe.b[0]) == (fe.a[1] > fe.b[1]);
        let clip_u_hi = if clip_hi {
            slopes_same_sign
        } else {
            !slopes_same_sign
        };

        let new_bound = if clip_hi {
            R2Rect::new(
                if clip_u_hi {
                    R1Interval::new(edge.bound[0].lo(), u_clamped)
                } else {
                    R1Interval::new(u_clamped, edge.bound[0].hi())
                },
                R1Interval::new(edge.bound[1].lo(), v),
            )
        } else {
            R2Rect::new(
                if clip_u_hi {
                    R1Interval::new(edge.bound[0].lo(), u_clamped)
                } else {
                    R1Interval::new(u_clamped, edge.bound[0].hi())
                },
                R1Interval::new(v, edge.bound[1].hi()),
            )
        };

        ClippedEdge {
            geometry_id: edge.geometry_id,
            face_edge_index: edge.face_edge_index,
            bound: new_bound,
        }
    }

    fn make_index_cell(
        &mut self,
        pcell: &S2PaddedCell,
        edges: &[ClippedEdge],
        face_edges: &[FaceEdge],
    ) {
        let cell_id = pcell.id();

        if self.tracker.is_active && !edges.is_empty() {
            if !self.tracker.at_cellid(cell_id) {
                self.tracker.move_to(&pcell.get_entry_vertex());
            }
            self.tracker.draw_to(&pcell.get_center());

            for edge in edges {
                let fe = &face_edges[edge.face_edge_index];
                if fe.has_interior {
                    self.tracker.test_edge(fe.geometry_id, &fe.v0, &fe.v1);
                }
            }
        }

        let containing_ids: Vec<u32> = self.tracker.shape_ids().to_vec();

        if edges.is_empty() && containing_ids.is_empty() {
            return;
        }

        let mut cell = IndexCell::new(cell_id);

        let mut edge_idx = 0;
        let mut containing_iter = containing_ids.iter().peekable();

        while edge_idx < edges.len() || containing_iter.peek().is_some() {
            let edge_geo_id = if edge_idx < edges.len() {
                face_edges[edges[edge_idx].face_edge_index].geometry_id
            } else {
                u32::MAX
            };

            let containing_geo_id = containing_iter.peek().copied().copied().unwrap_or(u32::MAX);

            if containing_geo_id < edge_geo_id {
                cell.add_shape(ClippedShape::new(containing_geo_id, true));
                containing_iter.next();
            } else {
                let geo_id = edge_geo_id;
                let mut shape = ClippedShape::new(geo_id, false);

                while edge_idx < edges.len()
                    && face_edges[edges[edge_idx].face_edge_index].geometry_id == geo_id
                {
                    shape.add_edge(face_edges[edges[edge_idx].face_edge_index].edge_id);
                    edge_idx += 1;
                }

                if containing_geo_id == geo_id {
                    shape.contains_center = true;
                    containing_iter.next();
                }

                cell.add_shape(shape);
            }
        }

        self.cells.push(cell);

        if self.tracker.is_active && !edges.is_empty() {
            self.tracker.draw_to(&pcell.get_exit_vertex());
            for edge in edges {
                let fe = &face_edges[edge.face_edge_index];
                self.tracker.test_edge(fe.geometry_id, &fe.v0, &fe.v1);
            }
            self.tracker.set_next_cellid(cell_id.next());
        }
    }
}

/// Interpolates a value along a line segment. Given points (x0, y0) and (x1, y1), returns the
/// y-value at the given x.
///
/// This function makes the following guarantees:
///  - If x == x0, then result == y0 (exactly).
///  - If x == x1, then result == y1 (exactly).
///  - If x0 <= x <= x1 and y0 <= y1, then y0 <= result <= y1.
///  - More generally, if x is between x0 and x1, then result is between y0 and y1.
///
/// Port of S2::InterpolateDouble from s2edge_clipping.h.
#[inline]
fn interpolate_double(x: f64, x0: f64, x1: f64, y0: f64, y1: f64) -> f64 {
    // If x0 == x1 == x all we can return is the single point.
    if x0 == x1 {
        // C++ asserts: x == x0 && y0 == y1
        debug_assert!(x == x0 && y0 == y1);
        return y0;
    }

    // To get results that are accurate near both endpoints, we interpolate
    // starting from the closer of the two points.
    if (x0 - x).abs() <= (x1 - x).abs() {
        y0 + (y1 - y0) * ((x - x0) / (x1 - x0))
    } else {
        y1 + (y0 - y1) * ((x - x1) / (x0 - x1))
    }
}

fn cell_union_from_range(begin: S2CellId, end: S2CellId) -> Vec<S2CellId> {
    let mut result = Vec::new();
    let mut id = begin;

    while id < end {
        // Find largest cell that fits.
        let mut level = id.level();

        while level > 0 {
            let parent = id.parent(level - 1);
            if parent.range_min() != id || parent.range_max().next() > end {
                break;
            }
            level -= 1;
        }

        if level < id.level() {
            id = id.parent(level);
        }

        result.push(id);
        id = id.range_max().next();
    }

    result
}

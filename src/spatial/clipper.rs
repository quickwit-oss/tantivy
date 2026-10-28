//! Spatial index for geometry on the unit sphere.
//!
//! Maps S2CellIds to the shapes that intersect each cell. Cells are subdivided until no cell
//! contains more than a configurable number of edges.
use super::clip_options::ClipOptions;
use super::clipped_shape::ClippedShape;
use super::edge_crosser::EdgeCrosser;
use super::geometry_set::GeometrySet;
use super::r1interval::R1Interval;
use super::r2rect::R2Rect;
use super::s2cell_id::S2CellId;
use super::s2edge_clipping::{
    interpolate_double, EDGE_CLIP_ERROR_UV_COORD, FACE_CLIP_ERROR_UV_COORD,
};
use super::s2padded_cell::S2PaddedCell;
use super::surface::Surface;
use crate::spatial::shape_index::{ShapeCell, ShapeIndex};

/// The amount by which cells are "padded" to compensate for numerical errors
/// when clipping line segments to cell boundaries.
pub const CELL_PADDING: f64 = 2.0 * (FACE_CLIP_ERROR_UV_COORD + EDGE_CLIP_ERROR_UV_COORD);

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

    // This code is equivalent to computing a floating-point "level" value and rounding up. ilogb()
    // returns the exponent corresponding to a fraction in the range [1,2).
    let ratio = value / K_AVG_EDGE_DERIV;
    let level = ilogb(ratio);
    // For dim=1, (level >> (dim - 1)) == (level >> 0) == level
    level.saturating_neg().clamp(0, S2CellId::MAX_LEVEL)
}

fn ilogb(value: f64) -> i32 {
    let bits = value.to_bits();
    let exponent = ((bits >> 52) & 0x7ff) as i32;
    if exponent == 0x7ff {
        return i32::MAX;
    }
    if exponent != 0 {
        return exponent - 1023;
    }

    let mantissa = bits & ((1u64 << 52) - 1);
    if mantissa == 0 {
        return i32::MIN;
    }
    let highest_bit = 63 - mantissa.leading_zeros() as i32;
    highest_bit - 1074
}

/// Computes the maximum level at which an edge is "short" relative to the cell size. Above this
/// level the edge is "long" and doesn't count toward the subdivision threshold. Port of
/// MutableS2ShapeIndex::GetEdgeMaxLevel.
pub(crate) fn get_edge_max_level<S: Surface>(v0: &S::Point, v1: &S::Point) -> i32 {
    let length = S::edge_length(v0, v1);
    let max_cell_edge = length * CELL_SIZE_TO_LONG_EDGE_RATIO;
    get_level_for_max_value(max_cell_edge)
}

/// An edge projected onto a cube face.
#[derive(Clone, Debug)]
struct FaceEdge<S: Surface> {
    /// Geometry the edge belongs to.
    geometry_id: u32,
    /// Edge index in the original polygon.
    edge_index: u32,
    /// Maximum level for subdivision (based on edge length).
    max_level: i32,
    /// Determine if we are indexing a polygon.
    has_interior: bool,
    /// Edge endpoints in (u,v) coordinates.
    a: [f64; 2],
    b: [f64; 2],
    /// Original edge endpoints on the surface.
    v0: S::Point,
    v1: S::Point,
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
struct InteriorTracker<S: Surface> {
    /// Determine if any shape with an interior is being tracked.
    is_active: bool,
    /// Current focus point.
    focus: S::Point,
    /// Edge crosser for testing edge crossings.
    crosser: S::EdgeCrosser,
    /// Shapes whose interiors contain the current focus point.
    shape_ids: Vec<u32>,
    /// The next cell ID we expect to process.
    next_cellid: S2CellId,
}

impl<S: Surface> InteriorTracker<S> {
    fn new() -> Self {
        let origin = S::hilbert_start();
        Self {
            focus: origin,
            crosser: S::EdgeCrosser::new(&origin, &origin),
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

    fn test_edge(&mut self, geometry_id: u32, v0: &S::Point, v1: &S::Point) {
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
    fn move_to(&mut self, p: &S::Point) {
        self.focus = *p;
    }

    fn draw_to(&mut self, p: &S::Point) {
        self.crosser = S::EdgeCrosser::new(&self.focus, p);
        self.focus = *p;
    }
}

impl ClipOptions {
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

/// Builder for constructing a ShapeIndex from a collection of geometries.
pub struct Clipper<S: Surface> {
    options: ClipOptions,
    /// Tracks which cells' centers are inside the polygon as we traverse in S2CellId order.
    tracker: InteriorTracker<S>,
    /// The index as an array of cells.
    cells: Vec<ShapeCell>,
}

impl<S: Surface> Clipper<S> {
    /// Creates a new IndexBuilder with the given options.
    pub fn new(options: ClipOptions) -> Self {
        Self {
            options,
            tracker: InteriorTracker::new(),
            cells: Vec::new(),
        }
    }

    /// Build a ShapeIndex from smashed GeometrySets. Geometry IDs are arrival order. The first
    /// member of the first set is 0, and the count increments from there.
    pub fn build(mut self, sets: &[GeometrySet<S>]) -> ShapeIndex {
        if sets.is_empty() {
            return ShapeIndex { cells: Vec::new() };
        }

        // Seed the InteriorTracker from precomputed contains_hilbert_start flags.
        let mut gid: u32 = 0;
        for set in sets {
            for member in &set.members {
                if member.closed {
                    self.tracker.is_active = true;
                    if member.contains_hilbert_start {
                        self.tracker.toggle_shape(gid);
                    }
                }
                gid += 1;
            }
        }

        let mut face_edges: [Vec<FaceEdge<S>>; 6] = Default::default();

        gid = 0;
        for set in sets {
            for member in &set.members {
                let closed = member.closed;
                let vertices = &member.vertices;
                let offsets = &member.ring_offsets;
                let mut edge_index: u32 = 0;

                for ring_idx in 0..offsets.len() - 1 {
                    let ring_start = offsets[ring_idx];
                    let ring_end = offsets[ring_idx + 1];
                    let ring_len = ring_end - ring_start;

                    if ring_len < 2 {
                        if ring_len == 1 {
                            let v = vertices[ring_start];
                            let max_level = get_edge_max_level::<S>(&v, &v);
                            self.add_face_edge(
                                gid,
                                edge_index,
                                max_level,
                                false,
                                &v,
                                &v,
                                &mut face_edges,
                            );
                            edge_index += 1;
                        }
                        continue;
                    }

                    let edge_count = ring_len - 1;
                    for i in 0..edge_count {
                        let v0 = vertices[ring_start + i];
                        let v1 = vertices[ring_start + i + 1];
                        let max_level = get_edge_max_level::<S>(&v0, &v1);
                        self.add_face_edge(
                            gid,
                            edge_index,
                            max_level,
                            closed,
                            &v0,
                            &v1,
                            &mut face_edges,
                        );
                        edge_index += 1;
                    }

                    if closed {
                        edge_index += 1;
                    }
                }

                gid += 1;
            }
        }

        for face in 0..S::FACE_COUNT {
            self.update_face_edges(face, &face_edges[face as usize]);
        }

        self.cells.sort_by_key(|c| c.cell_id);

        ShapeIndex { cells: self.cells }
    }

    /// Clips an edge to all cube faces.
    #[allow(clippy::too_many_arguments)]
    fn add_face_edge(
        &self,
        geometry_id: u32,
        edge_index: u32,
        max_level: i32,
        has_interior: bool,
        v0: &S::Point,
        v1: &S::Point,
        face_edges: &mut [Vec<FaceEdge<S>>],
    ) {
        let a_face = S::get_face(v0);
        let b_face = S::get_face(v1);

        if a_face == b_face {
            let (a_u, a_v) = S::point_to_face_uv(a_face, v0);
            let (b_u, b_v) = S::point_to_face_uv(a_face, v1);
            let max_uv = 1.0 - S::CELL_PADDING;
            if a_u.abs() <= max_uv
                && a_v.abs() <= max_uv
                && b_u.abs() <= max_uv
                && b_v.abs() <= max_uv
            {
                face_edges[a_face as usize].push(FaceEdge {
                    geometry_id,
                    edge_index,
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

        for face in 0..S::FACE_COUNT {
            if let Some((a_uv, b_uv)) = S::clip_to_face(v0, v1, face, S::CELL_PADDING) {
                face_edges[face as usize].push(FaceEdge {
                    geometry_id,
                    edge_index,
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

    fn update_face_edges(&mut self, face: i32, face_edges: &[FaceEdge<S>]) {
        let num_edges = face_edges.len();
        if num_edges == 0 && self.tracker.shape_ids().is_empty() {
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
        let pcell = S2PaddedCell::new(face_id, S::CELL_PADDING);

        if num_edges > 0 {
            // shrink_to_fit skips empty levels of the hierarchy. If all edges on this face are
            // clustered in a small region the recursion can start at level 10 instead of level 0.
            // The edge bounds are not tightened to the shrunk cell. They keep their full
            // from_point_pair extent. A wide bound means clip_to_children over-includes an edge in
            // children it barely touches. Deeper levels filter out the extras. Over-inclusion is
            // safe. Under-inclusion loses edges.
            let shrunk_id = pcell.shrink_to_fit(&bound);
            if shrunk_id != face_id {
                // All the edges are contained by some descendant of the face cell. We can save a
                // lot of work by starting directly with that cell, but if we are in the interior of
                // at least one shape then we need to create index entries for the cells we are
                // skipping over.
                self.skip_cell_range(face_id.range_min(), shrunk_id.range_min());

                let pcell = S2PaddedCell::new(shrunk_id, S::CELL_PADDING);
                self.update_edges(&pcell, &mut clipped, face_edges);

                self.skip_cell_range(shrunk_id.range_max().next(), face_id.range_max().next());
                return;
            }
        }

        // Otherwise (no edges, or no shrinking is possible), subdivide normally.
        self.update_edges(&pcell, &mut clipped, face_edges);
    }

    fn skip_cell_range(&mut self, begin: S2CellId, end: S2CellId) {
        if self.tracker.shape_ids().is_empty() {
            return;
        }

        for cell_id in cell_union_from_range(begin, end) {
            let pcell = S2PaddedCell::new(cell_id, S::CELL_PADDING);
            let mut empty_edges: Vec<ClippedEdge> = Vec::new();
            self.update_edges(&pcell, &mut empty_edges, &[]);
        }
    }

    fn update_edges(
        &mut self,
        pcell: &S2PaddedCell<S>,
        edges: &mut [ClippedEdge],
        face_edges: &[FaceEdge<S>],
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
        pcell: &S2PaddedCell<S>,
        edges: &[ClippedEdge],
        face_edges: &[FaceEdge<S>],
        tracker: &InteriorTracker<S>,
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
        face_edges: &[FaceEdge<S>],
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
        fe: &FaceEdge<S>,
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
        fe: &FaceEdge<S>,
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
        fe: &FaceEdge<S>,
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
        pcell: &S2PaddedCell<S>,
        edges: &[ClippedEdge],
        face_edges: &[FaceEdge<S>],
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

        let mut cell = ShapeCell::new(cell_id);

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
                cell.add_shape(ClippedShape::new((0, containing_geo_id), true));
                containing_iter.next();
            } else {
                let geo_id = edge_geo_id;
                let mut shape = ClippedShape::new((0, geo_id), false);

                while edge_idx < edges.len()
                    && face_edges[edges[edge_idx].face_edge_index].geometry_id == geo_id
                {
                    shape.add_edge(face_edges[edges[edge_idx].face_edge_index].edge_index);
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
                if fe.has_interior {
                    self.tracker.test_edge(fe.geometry_id, &fe.v0, &fe.v1);
                }
            }
            self.tracker.set_next_cellid(cell_id.next());
        }
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

#[cfg(test)]
mod tests {
    use super::{get_level_for_max_value, ilogb, K_AVG_EDGE_DERIV};
    use crate::spatial::edge_provider::EdgeProvider;
    use crate::spatial::geometry_set::{EdgeSet, GeometrySet};
    use crate::spatial::query_edge_provider::QueryEdgeProvider;
    use crate::spatial::s2cell::S2Cell;
    use crate::spatial::s2cell_id::S2CellId;
    use crate::spatial::shape_index_region::{get_intersecting_shapes, index_contains_point};
    use crate::spatial::sphere::Sphere;
    use crate::spatial::surface::Surface;

    #[test]
    fn test_ilogb_normal_values() {
        assert_eq!(ilogb(1.0), 0);
        assert_eq!(ilogb(2.0), 1);
        assert_eq!(ilogb(0.5), -1);
        assert_eq!(ilogb(f64::INFINITY), i32::MAX);
    }

    #[test]
    fn test_ilogb_subnormal_values() {
        assert_eq!(ilogb(f64::from_bits(1)), -1074);
        assert_eq!(ilogb(f64::from_bits(0x0008_0000_0000_0000)), -1023);
    }

    #[test]
    fn test_get_level_for_max_value_special_values() {
        assert_eq!(get_level_for_max_value(0.0), S2CellId::MAX_LEVEL);
        assert_eq!(get_level_for_max_value(-1.0), S2CellId::MAX_LEVEL);
        assert_eq!(get_level_for_max_value(f64::NAN), S2CellId::MAX_LEVEL);
        assert_eq!(get_level_for_max_value(f64::INFINITY), 0);
    }

    #[test]
    fn test_get_level_for_max_value_exact_powers() {
        assert_eq!(get_level_for_max_value(K_AVG_EDGE_DERIV), 0);
        assert_eq!(get_level_for_max_value(K_AVG_EDGE_DERIV * 0.5), 1);
        assert_eq!(get_level_for_max_value(K_AVG_EDGE_DERIV * 0.25), 2);
    }

    #[test]
    fn test_get_level_for_max_value_subnormal() {
        let ratio = f64::from_bits(0x0008_0000_0000_0000);
        assert!(ratio.is_subnormal());
        assert_eq!(
            get_level_for_max_value(ratio * K_AVG_EDGE_DERIV),
            S2CellId::MAX_LEVEL
        );
    }

    #[test]
    fn test_full_face_interior_without_face_edges() {
        let ring = vec![
            Sphere::face_uv_to_point(3, -0.1, -0.1),
            Sphere::face_uv_to_point(3, 0.1, -0.1),
            Sphere::face_uv_to_point(3, 0.1, 0.1),
            Sphere::face_uv_to_point(3, -0.1, 0.1),
            Sphere::face_uv_to_point(3, -0.1, -0.1),
        ];
        let set = GeometrySet {
            members: vec![EdgeSet {
                vertices: ring,
                closed: true,
                contains_hilbert_start: true,
                ring_offsets: vec![0, 5],
            }],
            doc_id: 0,
        };

        let index = super::Clipper::<Sphere>::new(super::ClipOptions::default())
            .build(std::slice::from_ref(&set));
        let edge_provider = QueryEdgeProvider { set };
        let face_center = Sphere::face_uv_to_point(0, 0.0, 0.0);
        let face_center_cell = Sphere::cell_id_from_point(&face_center);
        let indexed_cell = index
            .find_cell(face_center_cell)
            .expect("covered face should have an interior cell");
        assert_eq!(indexed_cell.cell_id, S2CellId::from_face(0));
        let clipped = indexed_cell
            .find_shape((0, 0))
            .expect("covered face cell should contain the geometry");
        assert!(clipped.contains_center);
        assert!(clipped.edge_indices.is_empty());

        assert!(index_contains_point::<Sphere, QueryEdgeProvider<Sphere>>(
            &index,
            &edge_provider,
            (0, 0),
            &face_center,
        ));

        let target = S2Cell::new(face_center_cell.parent(10));
        let hits = get_intersecting_shapes::<Sphere, QueryEdgeProvider<Sphere>>(
            &index,
            &edge_provider,
            &target,
        );
        assert_eq!(hits, vec![((0, 0), true)]);

        let edge_faces: Vec<_> = (0..4)
            .map(|idx| edge_provider.get_edge((0, 0), idx).0)
            .map(|p| Sphere::get_face(&p))
            .collect();
        assert!(!edge_faces.contains(&0));
    }
}

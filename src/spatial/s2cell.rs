//! S2Cell: An S2Region object representing a cell.
//!
//! Unlike S2CellId, S2Cell supports efficient containment and intersection tests by caching the
//! cell's (u,v) bounds and providing vertex/edge access.
//!
//! S2Cell from s2cell.h and s2cell.cc.
use std::f64::consts::{FRAC_PI_2, FRAC_PI_4, PI};

use super::crossings::S2EdgeCrosser;
use super::latlng_rect::S2LatLngRect;
use super::math::{dot, ldexp, neg, normalize};
use super::r1interval::R1Interval;
use super::r2rect::R2Rect;
use super::s1chord_angle::S1ChordAngle;
use super::s1interval::S1Interval;
use super::s2cap::S2Cap;
use super::s2cell_id::S2CellId;
use super::s2coords::{
    self, face_uv_to_xyz, face_xyz_to_uvw, get_u_axis, get_u_norm, get_v_axis, get_v_norm,
    ij_to_st_min, st_to_uv, MAX_CELL_LEVEL,
};
use super::s2edge_distances::update_min_distance;

/// An S2Cell represents a cell in the S2 cell decomposition.
#[derive(Clone, Copy, Debug)]
pub struct S2Cell {
    id: S2CellId,
    face: u8,
    level: u8,
    orientation: u8,
    uv: R2Rect,
}

impl S2Cell {
    /// Creates an S2Cell from an S2CellId.
    pub fn new(id: S2CellId) -> Self {
        let (face, i, j, orientation) = id.to_face_ij_orientation();
        let level = id.level();
        let uv = ij_level_to_bound_uv(i, j, level);
        Self {
            id,
            face: face as u8,
            level: level as u8,
            orientation: orientation as u8,
            uv,
        }
    }

    /// Creates an S2Cell for the given cube face (level 0).
    pub fn from_face(face: i32) -> Self {
        Self::new(S2CellId::from_face(face))
    }

    /// Creates an S2Cell containing the given point.
    pub fn from_point(p: &[f64; 3]) -> Self {
        Self::new(S2CellId::from_point(p))
    }

    /// Returns the cell ID.
    #[inline]
    pub fn id(&self) -> S2CellId {
        self.id
    }

    /// Returns the cube face (0-5).
    #[inline]
    pub fn face(&self) -> i32 {
        self.face as i32
    }

    /// Returns the subdivision level (0-30).
    #[inline]
    pub fn level(&self) -> i32 {
        self.level as i32
    }

    /// Returns the Hilbert curve orientation.
    #[inline]
    pub fn orientation(&self) -> i32 {
        self.orientation as i32
    }

    /// Returns true if this is a leaf cell.
    #[inline]
    pub fn is_leaf(&self) -> bool {
        self.level as i32 == MAX_CELL_LEVEL
    }

    /// Returns the bounds in (u,v)-space.
    #[inline]
    pub fn get_bound_uv(&self) -> R2Rect {
        self.uv
    }

    /// Returns the size of the cell in (i,j) coordinates.
    #[inline]
    pub fn get_size_ij(&self) -> i32 {
        S2CellId::size_ij_for_level(self.level as i32)
    }

    /// Returns the size of the cell in (s,t) coordinates.
    #[inline]
    pub fn get_size_st(&self) -> f64 {
        ij_to_st_min(self.get_size_ij())
    }

    /// Returns the k-th vertex (k = 0,1,2,3) in CCW order.
    /// Lower left, lower right, upper right, upper left in UV plane.
    #[inline]
    pub fn get_vertex(&self, k: usize) -> [f64; 3] {
        normalize(&self.get_vertex_raw(k))
    }

    /// Returns the k-th vertex without normalization.
    #[inline]
    pub fn get_vertex_raw(&self, k: usize) -> [f64; 3] {
        let v = self.uv.get_vertex(k % 4);
        face_uv_to_xyz(self.face as i32, v[0], v[1])
    }

    /// Returns the center of the cell.
    #[inline]
    pub fn get_center(&self) -> [f64; 3] {
        normalize(&self.get_center_raw())
    }

    /// Returns the center without normalization.
    #[inline]
    pub fn get_center_raw(&self) -> [f64; 3] {
        self.id.to_point_raw()
    }

    /// Returns the inward-facing normal of edge k (k = 0,1,2,3).
    /// Edge k connects vertex k to vertex k+1.
    #[inline]
    pub fn get_edge(&self, k: usize) -> [f64; 3] {
        normalize(&self.get_edge_raw(k))
    }

    /// Returns the edge normal without normalization.
    #[inline]
    pub fn get_edge_raw(&self, k: usize) -> [f64; 3] {
        match k & 3 {
            0 => get_v_norm(self.face as i32, self.uv[1].lo()),
            1 => get_u_norm(self.face as i32, self.uv[0].hi()),
            2 => neg(get_v_norm(self.face as i32, self.uv[1].hi())),
            _ => neg(get_u_norm(self.face as i32, self.uv[0].lo())),
        }
    }

    /// Returns the UV coordinate of edge k (the constant coord along that edge).
    pub fn get_uv_coord_of_edge(&self, k: usize) -> f64 {
        match k & 3 {
            0 => self.uv[1].lo(), // Bottom: v = min
            1 => self.uv[0].hi(), // Right: u = max
            2 => self.uv[1].hi(), // Top: v = max
            _ => self.uv[0].lo(), // Left: u = min
        }
    }

    /// Subdivides this cell into its four children. Returns None if this is a leaf cell.
    pub fn subdivide(&self) -> Option<[S2Cell; 4]> {
        if self.is_leaf() {
            return None;
        }

        // Compute the cell midpoint in uv-space.
        let uv_mid = self.id.get_center_uv();
        let child_ids = self.id.children();

        // Create children using the same optimization as S2.
        let mut children = [S2Cell::new(child_ids[0]); 4];

        for (pos, child_id) in child_ids.iter().enumerate() {
            let ij = s2coords::pos_to_ij(self.orientation as i32, pos as i32);
            let i = (ij >> 1) as usize;
            let j = (ij & 1) as usize;

            children[pos] = S2Cell {
                id: *child_id,
                face: self.face,
                level: self.level + 1,
                orientation: (self.orientation as i32 ^ s2coords::pos_to_orientation(pos as i32))
                    as u8,
                uv: R2Rect::new(
                    R1Interval::new(
                        if i == 0 { self.uv[0].lo() } else { uv_mid[0] },
                        if i == 0 { uv_mid[0] } else { self.uv[0].hi() },
                    ),
                    R1Interval::new(
                        if j == 0 { self.uv[1].lo() } else { uv_mid[1] },
                        if j == 0 { uv_mid[1] } else { self.uv[1].hi() },
                    ),
                ),
            };
        }

        Some(children)
    }

    /// Returns true if this cell contains the given point. Note that S2Cells are closed regions
    /// (include their boundary).
    pub fn contains_point(&self, p: &[f64; 3]) -> bool {
        // Project point onto this face and check UV containment.
        if let Some(uv) = s2coords::face_xyz_to_uv(self.face as i32, p) {
            // Expand slightly to account for numerical error.
            self.uv.expanded(f64::EPSILON).contains_point(uv)
        } else {
            false
        }
    }

    /// Returns true if this cell contains the given cell.
    pub fn contains_cell(&self, other: &S2Cell) -> bool {
        self.id.contains(other.id)
    }

    /// Returns true if this cell may intersect the given cell.
    pub fn may_intersect(&self, other: &S2Cell) -> bool {
        self.id.intersects(other.id)
    }

    /// Returns the average area of cells at the given level.
    #[inline]
    pub fn average_area(level: i32) -> f64 {
        const AVG_AREA_BASE: f64 = 4.0 * PI / 6.0; // ~2.094
        ldexp(AVG_AREA_BASE, -2 * level)
    }

    /// Returns the minimum distance from the cell to the given point. All
    /// arguments should be unit length. Returns zero if the point is inside
    /// the cell.
    ///
    /// Port of S2Cell::GetDistance(const S2Point&) in s2cell.cc.
    pub fn get_distance_to_point(&self, target: &[f64; 3]) -> S1ChordAngle {
        self.get_distance_internal(target, true)
    }

    /// Returns the minimum distance from the cell boundary to the given point.
    /// Returns zero if the point is on the boundary.
    ///
    /// Port of S2Cell::GetBoundaryDistance in s2cell.cc.
    pub fn get_boundary_distance(&self, target: &[f64; 3]) -> S1ChordAngle {
        self.get_distance_internal(target, false)
    }

    /// Returns the minimum distance from the cell to the edge ab. Returns zero
    /// if the edge intersects the cell or if either endpoint is inside.
    ///
    /// Port of S2Cell::GetDistance(const S2Point& a, const S2Point& b) in s2cell.cc.
    pub fn get_distance_to_edge(&self, a: &[f64; 3], b: &[f64; 3]) -> S1ChordAngle {
        // First, check the minimum distance to the edge endpoints A and B.
        // (This also detects whether either endpoint is inside the cell.)
        let mut min_dist = self.get_distance_to_point(a);
        let d = self.get_distance_to_point(b);
        if d < min_dist {
            min_dist = d;
        }
        if min_dist == S1ChordAngle::zero() {
            return min_dist;
        }

        // Otherwise, check whether the edge crosses the cell boundary.
        let v: [[f64; 3]; 4] = [
            self.get_vertex(0),
            self.get_vertex(1),
            self.get_vertex(2),
            self.get_vertex(3),
        ];
        let mut crosser = S2EdgeCrosser::new(a, b);
        crosser.restart_at(&v[3]);
        for i in 0..4 {
            if crosser.crossing_sign(&v[i]) >= 0 {
                return S1ChordAngle::zero();
            }
        }
        // Finally, check whether the minimum distance occurs between a cell vertex
        // and the interior of the edge AB.  (Some of this work is redundant, since
        // it also checks the distance to the endpoints A and B again.)
        //
        // Note that we don't need to check the distance from the interior of AB to
        // the interior of a cell edge, because the only way that this distance can
        // be minimal is if the two edges cross (already checked above).
        for i in 0..4 {
            update_min_distance(&v[i], a, b, &mut min_dist);
        }
        min_dist
    }

    // All calculations are done in the (u,v,w) coordinates of this cell's face.
    //
    // Port of S2Cell::GetDistanceInternal in s2cell.cc.
    fn get_distance_internal(&self, target_xyz: &[f64; 3], to_interior: bool) -> S1ChordAngle {
        let target = face_xyz_to_uvw(self.face as i32, target_xyz);

        // Compute dot products with all four upward or rightward-facing edge
        // normals.  "dirIJ" is the dot product for the edge corresponding to axis
        // I, endpoint J.  For example, dir01 is the right edge of the S2Cell
        // (corresponding to the upper endpoint of the u-axis).
        let dir00 = target[0] - target[2] * self.uv[0].lo();
        let dir01 = target[0] - target[2] * self.uv[0].hi();
        let dir10 = target[1] - target[2] * self.uv[1].lo();
        let dir11 = target[1] - target[2] * self.uv[1].hi();
        let mut inside = true;
        if dir00 < 0.0 {
            inside = false; // Target is to the left of the cell
            if self.v_edge_is_closest(&target, 0) {
                return edge_distance(-dir00, self.uv[0].lo());
            }
        }
        if dir01 > 0.0 {
            inside = false; // Target is to the right of the cell
            if self.v_edge_is_closest(&target, 1) {
                return edge_distance(dir01, self.uv[0].hi());
            }
        }
        if dir10 < 0.0 {
            inside = false; // Target is below the cell
            if self.u_edge_is_closest(&target, 0) {
                return edge_distance(-dir10, self.uv[1].lo());
            }
        }
        if dir11 > 0.0 {
            inside = false; // Target is above the cell
            if self.u_edge_is_closest(&target, 1) {
                return edge_distance(dir11, self.uv[1].hi());
            }
        }
        if inside {
            if to_interior {
                return S1ChordAngle::zero();
            }
            // Although you might think of S2Cells as rectangles, they are actually
            // arbitrary quadrilaterals after they are projected onto the sphere.
            // Therefore the simplest approach is just to find the minimum distance to
            // any of the four edges.
            return edge_distance(-dir00, self.uv[0].lo())
                .min(edge_distance(dir01, self.uv[0].hi()))
                .min(edge_distance(-dir10, self.uv[1].lo()))
                .min(edge_distance(dir11, self.uv[1].hi()));
        }
        // Otherwise, the closest point is one of the four cell vertices.  Note that
        // it is *not* trivial to narrow down the candidates based on the edge sign
        // tests above, because (1) the edges don't meet at right angles and (2)
        // there are points on the far side of the sphere that are both above *and*
        // below the cell, etc.
        self.vertex_chord_dist(&target, 0, 0)
            .min(self.vertex_chord_dist(&target, 1, 0))
            .min(self.vertex_chord_dist(&target, 0, 1))
            .min(self.vertex_chord_dist(&target, 1, 1))
    }

    // Return the squared chord distance from point P to corner vertex (i,j).
    // Port of S2Cell::VertexChordDist in s2cell.cc.
    fn vertex_chord_dist(&self, p: &[f64; 3], i: usize, j: usize) -> S1ChordAngle {
        let vertex = normalize(&[self.uv[0].get_bound(i), self.uv[1].get_bound(j), 1.0]);
        S1ChordAngle::from_points(p, &vertex)
    }

    // Given a point P and either the lower or upper edge of the S2Cell (specified
    // by setting "v_end" to 0 or 1 respectively), return true if P is closer to
    // the interior of that edge than it is to either endpoint.
    // Port of S2Cell::UEdgeIsClosest in s2cell.cc.
    fn u_edge_is_closest(&self, p: &[f64; 3], v_end: usize) -> bool {
        let u0 = self.uv[0].lo();
        let u1 = self.uv[0].hi();
        let v = self.uv[1].get_bound(v_end);
        // These are the normals to the planes that are perpendicular to the edge
        // and pass through one of its two endpoints.
        let dir0 = [v * v + 1.0, -u0 * v, -u0];
        let dir1 = [v * v + 1.0, -u1 * v, -u1];
        dot(p, &dir0) > 0.0 && dot(p, &dir1) < 0.0
    }

    // Given a point P and either the left or right edge of the S2Cell (specified
    // by setting "u_end" to 0 or 1 respectively), return true if P is closer to
    // the interior of that edge than it is to either endpoint.
    // Port of S2Cell::VEdgeIsClosest in s2cell.cc.
    fn v_edge_is_closest(&self, p: &[f64; 3], u_end: usize) -> bool {
        let v0 = self.uv[1].lo();
        let v1 = self.uv[1].hi();
        let u = self.uv[0].get_bound(u_end);
        let dir0 = [-u * v0, u * u + 1.0, -v0];
        let dir1 = [-u * v1, u * u + 1.0, -v1];
        dot(p, &dir0) > 0.0 && dot(p, &dir1) < 0.0
    }

    /// Returns the latitude of the cell vertex at (i, j) where i, j in {0, 1}.
    fn get_latitude(&self, i: usize, j: usize) -> f64 {
        let p = face_uv_to_xyz(
            self.face as i32,
            self.uv[0].get_bound(i),
            self.uv[1].get_bound(j),
        );
        (p[2] + 0.0).atan2((p[0] * p[0] + p[1] * p[1]).sqrt())
    }

    /// Returns the longitude of the cell vertex at (i, j).
    fn get_longitude(&self, i: usize, j: usize) -> f64 {
        let p = face_uv_to_xyz(
            self.face as i32,
            self.uv[0].get_bound(i),
            self.uv[1].get_bound(j),
        );
        (p[1] + 0.0).atan2(p[0] + 0.0)
    }

    /// Returns the latitude-longitude bounding rectangle for this cell.
    pub fn get_rect_bound(&self) -> S2LatLngRect {
        if self.level > 0 {
            let u = self.uv[0].lo() + self.uv[0].hi();
            let v = self.uv[1].lo() + self.uv[1].hi();

            let u_axis = get_u_axis(self.face as i32);
            let v_axis = get_v_axis(self.face as i32);

            let i = if u_axis[2] == 0.0 {
                (u < 0.0) as usize
            } else {
                (u > 0.0) as usize
            };
            let j = if v_axis[2] == 0.0 {
                (v < 0.0) as usize
            } else {
                (v > 0.0) as usize
            };

            let lat = R1Interval::from_point_pair(
                self.get_latitude(i, j),
                self.get_latitude(1 - i, 1 - j),
            );
            let lng = S1Interval::from_point_pair(
                self.get_longitude(i, 1 - j),
                self.get_longitude(1 - i, j),
            );

            S2LatLngRect::new(lat, lng)
                .expanded(2.0 * f64::EPSILON, 2.0 * f64::EPSILON)
                .polar_closure()
        } else {
            let pole_min_lat = (1.0_f64 / 3.0).sqrt().asin() - 0.5 * f64::EPSILON;

            let bound = match self.face {
                0 => S2LatLngRect::new(
                    R1Interval::new(-FRAC_PI_4, FRAC_PI_4),
                    S1Interval::new(-FRAC_PI_4, FRAC_PI_4),
                ),
                1 => S2LatLngRect::new(
                    R1Interval::new(-FRAC_PI_4, FRAC_PI_4),
                    S1Interval::new(FRAC_PI_4, 3.0 * FRAC_PI_4),
                ),
                2 => {
                    S2LatLngRect::new(R1Interval::new(pole_min_lat, FRAC_PI_2), S1Interval::full())
                }
                3 => S2LatLngRect::new(
                    R1Interval::new(-FRAC_PI_4, FRAC_PI_4),
                    S1Interval::new(3.0 * FRAC_PI_4, -3.0 * FRAC_PI_4),
                ),
                4 => S2LatLngRect::new(
                    R1Interval::new(-FRAC_PI_4, FRAC_PI_4),
                    S1Interval::new(-3.0 * FRAC_PI_4, -FRAC_PI_4),
                ),
                _ => S2LatLngRect::new(
                    R1Interval::new(-FRAC_PI_2, -pole_min_lat),
                    S1Interval::full(),
                ),
            };

            bound.expanded(f64::EPSILON, 0.0)
        }
    }

    /// Returns a bounding spherical cap for this cell.
    pub fn get_cap_bound(&self) -> S2Cap {
        let uv_center = self.uv.get_center();
        let center_raw = face_uv_to_xyz(self.face as i32, uv_center[0], uv_center[1]);
        let center = normalize(&center_raw);

        let mut cap = S2Cap::from_point(center);
        for k in 0..4 {
            cap.add_point(&self.get_vertex(k));
        }
        cap
    }
}

// Given the dot product of a point P with the normal of a u- or v-edge at the
// given coordinate value, return the distance from P to that edge.
// Port of EdgeDistance in s2cell.cc.
fn edge_distance(dir_ij: f64, uv: f64) -> S1ChordAngle {
    // Let P be the target point and let R be the closest point on the given
    // edge AB.  The desired distance PR can be expressed as PR^2 = PQ^2 + QR^2
    // where Q is the point P projected onto the plane through the great circle
    // through AB.  We can compute the distance PQ^2 perpendicular to the plane
    // from "dirIJ" (the dot product of the target point P with the edge
    // normal) and the squared length of the edge normal (1 + uv**2).
    let pq2 = (dir_ij * dir_ij) / (1.0 + uv * uv);

    // We can compute the distance QR as (1 - OQ) where O is the sphere origin,
    // and we can compute OQ^2 = 1 - PQ^2 using the Pythagorean theorem.
    // (This calculation loses accuracy as angle POQ approaches Pi/2.)
    let qr = 1.0 - (1.0 - pq2).sqrt();
    S1ChordAngle::from_length2(pq2 + qr * qr)
}

/// Computes the (u,v) bounding rectangle for a cell at level `level` whose (i,j) coordinates are
/// `i` and `j`.
fn ij_level_to_bound_uv(i: i32, j: i32, level: i32) -> R2Rect {
    let cell_size = S2CellId::size_ij_for_level(level);

    // Round down i and j to the nearest multiple of cell_size.
    let i_lo = i & -cell_size;
    let j_lo = j & -cell_size;
    let i_hi = i_lo + cell_size;
    let j_hi = j_lo + cell_size;

    R2Rect::new(
        R1Interval::new(st_to_uv(ij_to_st_min(i_lo)), st_to_uv(ij_to_st_min(i_hi))),
        R1Interval::new(st_to_uv(ij_to_st_min(j_lo)), st_to_uv(ij_to_st_min(j_hi))),
    )
}

impl PartialEq for S2Cell {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for S2Cell {}

impl PartialOrd for S2Cell {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for S2Cell {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.id.cmp(&other.id)
    }
}

//! A triangle encoding with bounding box in the first four words for efficient spatial pruning.
//!
//! Encodes triangles with the bounding box in the first four words, enabling efficient spatial
//! pruning during tree traversal without reconstructing the full triangle. The remaining words
//! contain an additional vertex and packed reconstruction metadata, allowing exact triangle
//! recovery when needed.

use i_triangle::advanced::delaunay::IntDelaunay;
use i_triangle::i_overlay::i_float::int::point::IntPoint;

use crate::DocId;

const MINY_MINX_MAXY_MAXX_Y_X: i32 = 0;
const MINY_MINX_Y_X_MAXY_MAXX: i32 = 1;
const MAXY_MINX_Y_X_MINY_MAXX: i32 = 2;
const MAXY_MINX_MINY_MAXX_Y_X: i32 = 3;
const Y_MINX_MINY_X_MAXY_MAXX: i32 = 4;
const Y_MINX_MINY_MAXX_MAXY_X: i32 = 5;
const MAXY_MINX_MINY_X_Y_MAXX: i32 = 6;
const MINY_MINX_Y_MAXX_MAXY_X: i32 = 7;

/// Converts geographic coordinates (WGS84 lat/lon) to integer spatial coordinates.
///
/// Maps the full globe to the i32 range using linear scaling:
/// - Latitude: -90° to +90° → -2³¹ to +2³¹-1
/// - Longitude: -180° to +180° → -2³¹ to +2³¹-1
///
/// Provides approximately 214 units/meter for latitude and 107 units/meter for longitude, giving
/// millimeter-level precision. Uses `floor()` to ensure consistent quantization.
///
/// Returns `(y, x)` where y=latitude coordinate, x=longitude coordinate.
pub fn latlon_to_point(lat: f64, lon: f64) -> (i32, i32) {
    let y = (lat / (180.0 / (1i64 << 32) as f64)).floor() as i32;
    let x = (lon / (360.0 / (1i64 << 32) as f64)).floor() as i32;
    (y, x)
}

/// Creates a bounding box from two lat/lon corner coordinates.
///
/// Takes two arbitrary corner points and produces a normalized bounding box in the internal
/// coordinate system. Automatically computes min/max for each dimension.
///
/// Returns `[min_y, min_x, max_y, max_x]` matching the internal storage format used throughout the
/// block kd-tree and triangle encoding.
pub fn latlon_to_bbox(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> [i32; 4] {
    let (y1, x1) = latlon_to_point(lat1, lon1);
    let (y2, x2) = latlon_to_point(lat2, lon2);
    [y1.min(y2), x1.min(x2), y1.max(y2), x1.max(x2)]
}

/// A triangle encoded with bounding box in the first four words for efficient spatial pruning.
///
/// Encodes the bounding box, one vertex, boundary edge flags, and a reconstruction code that
/// together allow exact triangle recovery while optimizing for spatial query performance. Finally,
/// it contains the document id.
#[repr(C)]
#[derive(Debug)]
pub struct Triangle {
    /// The bounding box, one vertex, followed by a packed integer containing boundary edge flags
    /// and a reconstruction code.
    pub words: [i32; 7],
    /// The id of the document associated with this triangle.
    pub doc_id: DocId,
}

impl Triangle {
    /// Encodes a triangle with the bounding box in the first four words for efficient spatial
    /// pruning.
    ///
    /// Takes three vertices as `[y0, x0, y1, x1, y2, x2]` and edge boundary flags `[ab, bc, ca]`
    /// indicating which edges are polygon boundaries. Returns a triangle struct with the bounding
    /// box in the first four words as `[min_y, min_x, max_y, max_x]`. When decoded, the vertex
    /// order may differ from the original input to `new()` due to normalized rotation.
    ///
    /// The edge boundary flags are here to express whether an edge is part of the boundaries
    /// in the tesselation of the larger polygon it belongs to.
    pub fn new(doc_id: u32, triangle: [i32; 6], boundaries: [bool; 3]) -> Self {
        let mut ay = triangle[0];
        let mut ax = triangle[1];
        let mut by = triangle[2];
        let mut bx = triangle[3];
        let mut cy = triangle[4];
        let mut cx = triangle[5];
        let mut ab = boundaries[0];
        let mut bc = boundaries[1];
        let mut ca = boundaries[2];
        // rotate edges and place minX at the beginning
        if bx < ax || cx < ax {
            let temp_x = ax;
            let temp_y = ay;
            let temp_boundary = ab;
            if bx < cx {
                ax = bx;
                ay = by;
                ab = bc;
                bx = cx;
                by = cy;
                bc = ca;
                cx = temp_x;
                cy = temp_y;
                ca = temp_boundary;
            } else {
                ax = cx;
                ay = cy;
                ab = ca;
                cx = bx;
                cy = by;
                ca = bc;
                bx = temp_x;
                by = temp_y;
                bc = temp_boundary;
            }
        } else if ax == bx && ax == cx {
            // degenerated case, all points with same longitude
            // we need to prevent that ax is in the middle (not part of the MBS)
            if by < ay || cy < ay {
                let temp_x = ax;
                let temp_y = ay;
                let temp_boundary = ab;
                if by < cy {
                    ax = bx;
                    ay = by;
                    ab = bc;
                    bx = cx;
                    by = cy;
                    bc = ca;
                    cx = temp_x;
                    cy = temp_y;
                    ca = temp_boundary;
                } else {
                    ax = cx;
                    ay = cy;
                    ab = ca;
                    cx = bx;
                    cy = by;
                    ca = bc;
                    bx = temp_x;
                    by = temp_y;
                    bc = temp_boundary;
                }
            }
        }
        // change orientation if clockwise (CW)
        if !is_counter_clockwise(
            IntPoint { y: ay, x: ax },
            IntPoint { y: by, x: bx },
            IntPoint { y: cy, x: cx },
        ) {
            // To change the orientation, we simply swap B and C.
            let temp_x = bx;
            let temp_y = by;
            let temp_boundary = ab;
            // ax and ay do not change, ab becomes bc
            ab = ca;
            bx = cx;
            by = cy;
            // bc does not change, ca becomes ab
            cx = temp_x;
            cy = temp_y;
            ca = temp_boundary;
        }
        let min_x = ax;
        let min_y = ay.min(by).min(cy);
        let max_x = ax.max(bx).max(cx);
        let max_y = ay.max(by).max(cy);
        let (y, x, code) = if min_y == ay {
            if max_y == by && max_x == bx {
                (cy, cx, MINY_MINX_MAXY_MAXX_Y_X)
            } else if max_y == cy && max_x == cx {
                (by, bx, MINY_MINX_Y_X_MAXY_MAXX)
            } else {
                (by, cx, MINY_MINX_Y_MAXX_MAXY_X)
            }
        } else if max_y == ay {
            if min_y == by && max_x == bx {
                (cy, cx, MAXY_MINX_MINY_MAXX_Y_X)
            } else if min_y == cy && max_x == cx {
                (by, bx, MAXY_MINX_Y_X_MINY_MAXX)
            } else {
                (cy, bx, MAXY_MINX_MINY_X_Y_MAXX)
            }
        } else if max_x == bx && min_y == by {
            (ay, cx, Y_MINX_MINY_MAXX_MAXY_X)
        } else if max_x == cx && max_y == cy {
            (ay, bx, Y_MINX_MINY_X_MAXY_MAXX)
        } else {
            panic!("Could not encode the provided triangle");
        };
        let boundaries_bits = (ab as i32) | ((bc as i32) << 1) | ((ca as i32) << 2);
        let packed = code | (boundaries_bits << 3);
        Triangle {
            words: [min_y, min_x, max_y, max_x, y, x, packed],
            doc_id: doc_id,
        }
    }

    /// Builds a degenerated triangle degenerating for a single point.
    /// All vertices are that point, and all vertices are boundaries.
    pub fn from_point(doc_id: DocId, point_x: i32, point_y: i32) -> Triangle {
        Triangle::new(
            doc_id,
            [point_y, point_x, point_y, point_x, point_y, point_x],
            [true, true, true],
        )
    }

    /// Builds a degenerated triangle for a segment.
    /// Line segment AB is represented as the triangle ABA.
    pub fn from_line_segment(doc_id: DocId, a_x: i32, a_y: i32, b_x: i32, b_y: i32) -> Triangle {
        Triangle::new(doc_id, [a_y, a_x, b_y, b_x, a_y, a_x], [true, true, true])
    }

    /// Create a triangle with only the doc_id and the words initialized to zero.
    ///
    /// The doc_id and words in the field are delta-compressed as a series with the doc_id
    /// serialized first. When we reconstruct the triangle we can first reconstruct skeleton
    /// triangles with the doc_id series, then populate the words directly from the decompression
    /// as we decompress each series.
    ///
    /// An immutable constructor would require that we decompress first into parallel `Vec`
    /// instances, then loop through the count of triangles building triangles using a constructor
    /// that takes all eight field values at once. This saves a copy, the triangle is the
    /// decompression destination.
    pub fn skeleton(doc_id: u32) -> Self {
        Triangle {
            doc_id: doc_id,
            words: [0i32; 7],
        }
    }

    /// Decodes the triangle back to vertex coordinates and boundary flags.
    ///
    /// Returns vertices as `[y0, x0, y1, x1, y2, x2]` in CCW order and boundary flags `[ab, bc,
    /// ca]`. The vertex order may differ from the original input to `new()` due to normalized CCW
    /// rotation.
    pub fn decode(&self) -> ([i32; 6], [bool; 3]) {
        let packed = self.words[6];
        let code = packed & 7; // Lower 3 bits
        let boundaries = [
            (packed & (1 << 3)) != 0, // bit 3 = ab
            (packed & (1 << 4)) != 0, // bit 4 = bc
            (packed & (1 << 5)) != 0, // bit 5 = ca
        ];
        let (ay, ax, by, bx, cy, cx) = match code {
            MINY_MINX_MAXY_MAXX_Y_X => (
                self.words[0],
                self.words[1],
                self.words[2],
                self.words[3],
                self.words[4],
                self.words[5],
            ),
            MINY_MINX_Y_X_MAXY_MAXX => (
                self.words[0],
                self.words[1],
                self.words[4],
                self.words[5],
                self.words[2],
                self.words[3],
            ),
            MAXY_MINX_Y_X_MINY_MAXX => (
                self.words[2],
                self.words[1],
                self.words[4],
                self.words[5],
                self.words[0],
                self.words[3],
            ),
            MAXY_MINX_MINY_MAXX_Y_X => (
                self.words[2],
                self.words[1],
                self.words[0],
                self.words[3],
                self.words[4],
                self.words[5],
            ),
            Y_MINX_MINY_X_MAXY_MAXX => (
                self.words[4],
                self.words[1],
                self.words[0],
                self.words[5],
                self.words[2],
                self.words[3],
            ),
            Y_MINX_MINY_MAXX_MAXY_X => (
                self.words[4],
                self.words[1],
                self.words[0],
                self.words[3],
                self.words[2],
                self.words[5],
            ),
            MAXY_MINX_MINY_X_Y_MAXX => (
                self.words[2],
                self.words[1],
                self.words[0],
                self.words[5],
                self.words[4],
                self.words[3],
            ),
            MINY_MINX_Y_MAXX_MAXY_X => (
                self.words[0],
                self.words[1],
                self.words[4],
                self.words[3],
                self.words[2],
                self.words[5],
            ),
            _ => panic!("Could not decode the provided triangle"),
        };
        ([ay, ax, by, bx, cy, cx], boundaries)
    }

    /// Returns the bounding box coordinates of the encoded triangle.
    ///
    /// Provides access to the bounding box `[min_y, min_x, max_y, max_x]` stored in the first four
    /// words of the structure. The bounding box is stored first for efficient spatial pruning,
    /// determining whether it is necessary to decode the triangle for precise intersection or
    /// containment tests.
    pub fn bbox(&self) -> [i32; 4] {
        [self.words[0], self.words[1], self.words[2], self.words[3]]
    }
}

/// Encodes the triangles of a Delaunay triangulation into block kd-tree triangles.
///
/// Takes the output of a Delaunay triangulation from `i_triangle` and encodes each triangle into
/// the normalized triangle used by the block kd-tree. Each triangle includes its bounding box,
/// vertex coordinates, and boundary edge flags that distinguish original polygon edges from
/// internal tessellation edges.
///
/// The boundary edge information provided by the `i_triangle` Delaunay triangulation is essential
/// for CONTAINS and WITHIN queries to work correctly.
pub fn delaunay_to_triangles(doc_id: u32, delaunay: &IntDelaunay, triangles: &mut Vec<Triangle>) {
    for triangle in delaunay.triangles.iter() {
        let bounds = [
            triangle.neighbors[0] == usize::MAX,
            triangle.neighbors[1] == usize::MAX,
            triangle.neighbors[2] == usize::MAX,
        ];
        let v0 = &delaunay.points[triangle.vertices[0].index];
        let v1 = &delaunay.points[triangle.vertices[1].index];
        let v2 = &delaunay.points[triangle.vertices[2].index];
        triangles.push(Triangle::new(
            doc_id,
            [v0.y, v0.x, v1.y, v1.x, v2.y, v2.x],
            bounds,
        ))
    }
}

/// Returns true if the path A -> B -> C is Counter-Clockwise (CCW) or collinear.
/// Returns false if it is Clockwise (CW).
#[inline(always)]
fn is_counter_clockwise(a: IntPoint, b: IntPoint, c: IntPoint) -> bool {
    // We calculate the 2D cross product (determinant) of vectors AB and AC.
    // Formula: (bx - ax)(cy - ay) - (by - ay)(cx - ax)

    // We cast to i64 to prevent overflow, as multiplying two i32s can exceed i32::MAX.
    let val = (b.x as i64 - a.x as i64) * (c.y as i64 - a.y as i64)
        - (b.y as i64 - a.y as i64) * (c.x as i64 - a.x as i64);

    // If the result is positive, the triangle is CCW.
    // If negative, it is CW.
    // If zero, the points are collinear (we return true in that case).
    val >= 0
}

#[cfg(test)]
mod tests {
    use i_triangle::i_overlay::i_float::int::point::IntPoint;
    use i_triangle::int::triangulatable::IntTriangulatable;

    use super::*;

    #[test]
    fn encode_triangle() {
        let test_cases = [
            ([1, 1, 3, 2, 2, 4], [true, false, false]),
            ([1, 1, 2, 4, 3, 2], [false, false, true]),
            ([2, 4, 1, 1, 3, 2], [false, true, false]),
            ([2, 4, 3, 2, 1, 1], [false, true, false]),
            ([3, 2, 1, 1, 2, 4], [true, false, false]),
            ([3, 2, 2, 4, 1, 1], [false, false, true]),
        ];
        let ccw_coords = [1, 1, 2, 4, 3, 2];
        let ccw_bounds = [false, false, true];
        for (coords, bounds) in test_cases {
            let triangle = Triangle::new(1, coords, bounds);
            let (decoded_coords, decoded_bounds) = triangle.decode();
            assert_eq!(decoded_coords, ccw_coords);
            assert_eq!(decoded_bounds, ccw_bounds);
        }
    }

    #[test]
    fn test_cw_triangle_boundary_and_coord_flip() {
        // 1. Define distinct coordinates for a Clockwise triangle
        // Visual layout:
        // A(50,40): Top Center-ish
        // B(10,60): Bottom Right
        // C(20,10): Bottom Left (Has the Minimum X=10)
        // Path A->B->C is Clockwise.
        let input_coords = [
            50, 40, // A (y, x)
            10, 60, // B
            20, 10, // C
        ];

        // 2. Define Boundaries [ab, bc, ca]
        // We set BC=true and CA=false.
        // The bug (ab=bc) would erroneously put 'true' into the first slot.
        // The fix (ab=ca) should put 'false' into the first slot.
        let input_bounds = [false, true, false];

        // 3. Encode
        let triangle = Triangle::new(1, input_coords, input_bounds);
        let (decoded_coords, decoded_bounds) = triangle.decode();

        // 4. Expected Coordinates
        // The internal logic detects CW, swaps B/C to make it CCW:
        //    A(50,40) -> C(20,10) -> B(10,60)
        // Then it rotates to put Min-X first.
        // Min X is 10 (Vertex C).
        // Final Sequence: C -> B -> A
        let expected_coords = [
            20, 10, // C
            10, 60, // B
            50, 40, // A
        ];

        // 5. Expected Boundaries
        // After Flip (A->C->B):
        //    Edge AC (was CA) = false
        //    Edge CB (was BC) = true
        //    Edge BA (was AB) = false
        //    Unrotated: [false, true, false]
        // After Rotation (shifting to start at C):
        //    Shift left by 1: [true, false, false]
        let expected_bounds = [true, false, false];

        assert_eq!(
            decoded_coords, expected_coords,
            "Coordinates did not decode as expected"
        );
        assert_eq!(
            decoded_bounds, expected_bounds,
            "Boundary flags were incorrect (likely swap bug)"
        );
    }

    #[test]
    fn degenerate_triangle() {
        let test_cases = [
            (
                [1, 1, 2, 1, 3, 1],
                [true, false, false],
                [1, 1, 2, 1, 3, 1],
                [true, false, false],
            ),
            (
                [2, 1, 1, 1, 3, 1],
                [true, false, false],
                [1, 1, 3, 1, 2, 1],
                [false, false, true],
            ),
            (
                [2, 1, 3, 1, 1, 1],
                [false, false, true],
                [1, 1, 2, 1, 3, 1],
                [true, false, false],
            ),
        ];
        for (coords, bounds, ccw_coords, ccw_bounds) in test_cases {
            let triangle = Triangle::new(1, coords, bounds);
            let (decoded_coords, decoded_bounds) = triangle.decode();
            assert_eq!(decoded_coords, ccw_coords);
            assert_eq!(decoded_bounds, ccw_bounds);
        }
    }

    #[test]
    fn decode_triangle() {
        // distinct values for each coordinate to catch transposition
        let test_cases = [
            [11, 10, 60, 80, 41, 40],
            [1, 0, 11, 20, 31, 30],
            [30, 0, 11, 10, 1, 20],
            [30, 0, 1, 20, 21, 11],
            [20, 0, 1, 30, 41, 40],
            [20, 0, 1, 30, 31, 10],
            [30, 0, 1, 10, 11, 20],
            [1, 0, 10, 20, 21, 11],
        ];
        for coords in test_cases {
            let triangle = Triangle::new(1, coords, [true, true, true]);
            let (decoded_coords, _) = triangle.decode();
            assert_eq!(decoded_coords, coords);
        }
    }

    #[test]
    fn triangulate_box() {
        let i_polygon = vec![vec![
            IntPoint::new(0, 0),
            IntPoint::new(10, 0),
            IntPoint::new(10, 10),
            IntPoint::new(0, 10),
        ]];
        let mut triangles = Vec::new();
        let delaunay = i_polygon.triangulate().into_delaunay();
        delaunay_to_triangles(1, &delaunay, &mut triangles);
        assert_eq!(triangles.len(), 2);
    }
}

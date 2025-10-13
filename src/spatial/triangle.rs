//! A triangle encoding with bounding box in the first four words for efficient spatial pruning.
//!
//! Encodes triangles with the bounding box in the first four words, enabling efficient spatial
//! pruning during tree traversal without reconstructing the full triangle. The remaining words
//! contain an additional vertex and packed reconstruction metadata, allowing exact triangle
//! recovery when needed.

use robust::{orient2d, Coord};

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
    pub doc_id: u32,
}

impl Triangle {
    /// Encodes a triangle with the bounding box in the first four words for efficient spatial
    /// pruning.
    ///
    /// Takes three vertices as `[y0, x0, y1, x1, y2, x2]` and edge boundary flags `[ab, bc, ca]`
    /// indicating which edges are polygon boundaries. Returns a triangle struct with the bounding
    /// box in the first four words as `[min_y, min_x, max_y, max_x]`. When decoded, the vertex
    /// order may differ from the original input to `new()` due to normalized rotation.
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
        // change orientation if CW
        if orient2d(
            Coord { y: ay, x: ax },
            Coord { y: by, x: bx },
            Coord { y: cy, x: cx },
        ) < 0.0
        {
            let temp_x = bx;
            let temp_y = by;
            let temp_boundary = ab;
            // ax and ay do not change, ab becomes bc
            ab = bc;
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
    pub fn bbox(&self) -> &[i32] {
        &self.words[..4]
    }
}

#[cfg(test)]
mod tests {
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
}

//! The universal currency for stored-format geometry.
//!
//! A GeometrySet holds all members of a document's geometry in stored format: polygon rings closed
//! per RFC 7946 (first == last), flattened into a single vertex array per member, edge_id gaps at
//! ring boundaries. After smashing, the ring structure is gone. Edge access is always
//! vertices[edge_idx] and vertices[edge_idx + 1]. One Geometry in, one GeometrySet out.

use std::sync::LazyLock;

use super::containment::{brute_force_contains, compute_origin_inside};
use super::geometry::Geometry;
use super::math::normalize;
use super::s2coords::face_uv_to_xyz;
use super::sphere::Sphere;

/// The Hilbert curve start point: face 0, UV (-1, -1), normalized to the unit sphere.
static HILBERT_START: LazyLock<[f64; 3]> =
    LazyLock::new(|| normalize(&face_uv_to_xyz(0, -1.0, -1.0)));

/// One smashed geometry's edges. After smashing, the type distinctions (point, line string,
/// polygon) are gone. Everything is consecutive vertex pairs and a closed flag.
#[derive(Clone)]
pub struct EdgeSet {
    /// Flattened vertex array. Edges are consecutive pairs: vertices[i] to vertices[i+1].
    pub vertices: Vec<[f64; 3]>,
    /// Whether this geometry has interior (polygon).
    pub closed: bool,
    /// Whether the Hilbert curve start point is inside this geometry's interior.
    pub contains_hilbert_start: bool,
    /// Ring boundary offsets. Always starts with 0 and ends with the vertex count.
    pub ring_offsets: Vec<usize>,
}

/// Smashed geometry set for one field of one document. The geometry_id is assigned by the
/// builder or writer that receives the set, not carried here.
#[derive(Clone)]
pub struct GeometrySet {
    /// The smashed members of this document's geometry field.
    pub members: Vec<EdgeSet>,
    /// The document this set belongs to.
    pub doc_id: u32,
}

/// Smash a projected geometry into a GeometrySet in stored format. Reverses CW hole rings per
/// RFC 7946, computes contains_hilbert_start, and flattens rings into a single vertex array per
/// member. One Geometry in, one GeometrySet out.
pub fn to_geometry_set(geometry: &Geometry<Sphere>, doc_id: u32) -> GeometrySet {
    let mut members = Vec::new();
    smash(&mut members, geometry);
    GeometrySet { members, doc_id }
}

fn smash(members: &mut Vec<EdgeSet>, geometry: &Geometry<Sphere>) {
    match geometry {
        Geometry::Point(v) => {
            members.push(EdgeSet {
                vertices: vec![*v],
                closed: false,
                contains_hilbert_start: false,
                ring_offsets: vec![0, 1],
            });
        }
        Geometry::MultiPoint(points) => {
            for v in points {
                members.push(EdgeSet {
                    vertices: vec![*v],
                    closed: false,
                    contains_hilbert_start: false,
                    ring_offsets: vec![0, 1],
                });
            }
        }
        Geometry::LineString(line) => {
            let n = line.len();
            members.push(EdgeSet {
                vertices: line.clone(),
                closed: false,
                contains_hilbert_start: false,
                ring_offsets: vec![0, n],
            });
        }
        Geometry::MultiLineString(lines) => {
            for line in lines {
                let n = line.len();
                members.push(EdgeSet {
                    vertices: line.clone(),
                    closed: false,
                    contains_hilbert_start: false,
                    ring_offsets: vec![0, n],
                });
            }
        }
        Geometry::Polygon(polygon) => {
            let (vertices, hilbert_inside, offsets) = smash_polygon(polygon);
            members.push(EdgeSet {
                vertices,
                closed: true,
                contains_hilbert_start: hilbert_inside,
                ring_offsets: offsets,
            });
        }
        Geometry::MultiPolygon(multi_polygon) => {
            for polygon in multi_polygon {
                let (vertices, hilbert_inside, offsets) = smash_polygon(polygon);
                members.push(EdgeSet {
                    vertices,
                    closed: true,
                    contains_hilbert_start: hilbert_inside,
                    ring_offsets: offsets,
                });
            }
        }
        Geometry::GeometryCollection(collection) => {
            for geo in collection {
                smash(members, geo);
            }
        }
    }
}

/// Smash one polygon (outer ring + holes) into stored-format vertices. Takes already-projected
/// sphere coordinates with rings closed per RFC 7946 (first == last). Reverses CW hole rings,
/// computes whether the Hilbert curve start point is inside, and flattens into a single vertex
/// array.
///
/// Ring offsets always start with 0 and end with the total vertex count.
fn smash_polygon(rings: &[Vec<[f64; 3]>]) -> (Vec<[f64; 3]>, bool, Vec<usize>) {
    let mut flat = Vec::new();
    let mut hilbert_inside = false;
    let mut offsets = vec![0usize];

    for (i, ring) in rings.iter().enumerate() {
        let mut ring = ring.clone();
        // GeoJSON RFC 7946: holes are CW. S2 wants all rings CCW.
        if i > 0 {
            ring.reverse();
        }
        // Rings must be closed: first == last.
        assert!(ring.len() >= 4 && ring.first() == ring.last());
        let origin_inside = compute_origin_inside(&ring);
        if brute_force_contains(&HILBERT_START, &ring, origin_inside) {
            hilbert_inside = !hilbert_inside;
        }
        flat.extend_from_slice(&ring);
        offsets.push(flat.len());
    }

    (flat, hilbert_inside, offsets)
}

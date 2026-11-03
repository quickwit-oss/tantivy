//! The universal currency for stored-format geometry.
//!
//! A GeometrySet holds one input value. Polygon rings remain closed, with offsets
//! into a flat vertex array and gaps in the edge indices at ring boundaries.

use super::geometry::Geometry;
use super::surface::Surface;

/// The declared type of one geometry member.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GeometryKind {
    /// One point.
    Point,
    /// An ordered line chain, including a collapsed or empty chain.
    LineString,
    /// A shell followed by its hole rings.
    Polygon,
}

/// One member's source vertices, chain boundaries and declared type.
#[derive(Clone)]
pub struct EdgeSet<S: Surface> {
    /// The source type, retained even when the member is effectively empty.
    pub kind: GeometryKind,
    /// Flattened vertex array. Edges are consecutive pairs: vertices[i] to vertices[i+1].
    pub vertices: Vec<S::Point>,
    /// Whether this member is a polygon. Its effective material may still be empty.
    pub closed: bool,
    /// Whether the Hilbert curve start point is inside this geometry's interior.
    pub contains_hilbert_start: bool,
    /// Ring boundary offsets. Always starts with 0 and ends with the vertex count.
    pub ring_offsets: Vec<usize>,
}

pub(crate) fn ring_material<S: Surface>(inside: &[bool]) -> bool {
    if S::RING_ROLES {
        inside.first().copied().unwrap_or(false) && !inside[1..].iter().any(|&v| v)
    } else {
        inside.iter().fold(false, |parity, &value| parity ^ value)
    }
}

impl<S: Surface> EdgeSet<S> {
    pub(crate) fn ring_states(&self, point: &S::Point) -> Vec<bool> {
        self.ring_offsets
            .windows(2)
            .map(|range| {
                let ring = &self.vertices[range[0]..range[1]];
                S::contains_point(point, ring, S::origin_inside(ring))
            })
            .collect()
    }

    pub(crate) fn contains_point(&self, point: &S::Point) -> bool {
        self.closed && ring_material::<S>(&self.ring_states(point))
    }
}

/// Smashed geometry set for one input field value. The geometry_id is assigned by the
/// builder or writer that receives the set, not carried here.
#[derive(Clone)]
pub struct GeometrySet<S: Surface> {
    /// The smashed members of this input value.
    pub members: Vec<EdgeSet<S>>,
    /// The document this set belongs to.
    pub doc_id: u32,
}

/// Smash a projected geometry into stored format. Applies the surface's ring normalization,
/// computes contains_hilbert_start, and flattens rings into one vertex array per member.
/// One Geometry in, one GeometrySet out.
pub fn to_geometry_set<S: Surface>(geometry: &Geometry<S>, doc_id: u32) -> GeometrySet<S> {
    let mut members = Vec::new();
    smash(&mut members, geometry);
    GeometrySet { members, doc_id }
}

fn smash<S: Surface>(members: &mut Vec<EdgeSet<S>>, geometry: &Geometry<S>) {
    match geometry {
        Geometry::Point(v) => {
            members.push(EdgeSet {
                kind: GeometryKind::Point,
                vertices: vec![*v],
                closed: false,
                contains_hilbert_start: false,
                ring_offsets: vec![0, 1],
            });
        }
        Geometry::MultiPoint(points) => {
            for v in points {
                members.push(EdgeSet {
                    kind: GeometryKind::Point,
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
                kind: GeometryKind::LineString,
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
                    kind: GeometryKind::LineString,
                    vertices: line.clone(),
                    closed: false,
                    contains_hilbert_start: false,
                    ring_offsets: vec![0, n],
                });
            }
        }
        Geometry::Polygon(polygon) => {
            let (vertices, hilbert_inside, offsets) = smash_polygon::<S>(polygon);
            members.push(EdgeSet {
                kind: GeometryKind::Polygon,
                vertices,
                closed: true,
                contains_hilbert_start: hilbert_inside,
                ring_offsets: offsets,
            });
        }
        Geometry::MultiPolygon(multi_polygon) => {
            for polygon in multi_polygon {
                let (vertices, hilbert_inside, offsets) = smash_polygon::<S>(polygon);
                members.push(EdgeSet {
                    kind: GeometryKind::Polygon,
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
/// coordinates with rings closed per RFC 7946 (first == last). On the unit sphere, corrects
/// winding order that projection may have inverted. Computes whether the Hilbert curve start
/// point is inside and flattens into a single vertex array.
///
/// Ring offsets always start with 0 and end with the total vertex count.
fn smash_polygon<S: Surface>(rings: &[Vec<S::Point>]) -> (Vec<S::Point>, bool, Vec<usize>) {
    let mut flat = Vec::new();
    let mut ring_inside = Vec::new();
    let mut offsets = vec![0usize];
    let hilbert_start = S::hilbert_start();

    for ring in rings {
        let mut ring = ring.clone();
        S::normalize_ring(&mut ring);
        assert!(ring.is_empty() || (ring.len() >= 4 && ring.first() == ring.last()));
        let origin_inside = S::origin_inside(&ring);
        ring_inside.push(S::contains_point(&hilbert_start, &ring, origin_inside));
        flat.extend_from_slice(&ring);
        offsets.push(flat.len());
    }

    (flat, ring_material::<S>(&ring_inside), offsets)
}

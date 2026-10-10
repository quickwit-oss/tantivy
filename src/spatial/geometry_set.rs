//! The universal currency for stored-format geometry.
//!
//! A GeometrySet holds all members of a document's geometry in stored format: polygon rings closed
//! per RFC 7946 (first == last), flattened into a single vertex array per member, edge_id gaps at
//! ring boundaries. After smashing, the ring structure is gone. Edge access is always
//! vertices[edge_idx] and vertices[edge_idx + 1]. One Geometry in, one GeometrySet out.

use std::sync::LazyLock;

use super::containment::{
    brute_force_contains, compute_origin_inside, curvature, curvature_max_error,
    on_one_great_circle, prune_degeneracies,
};
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

impl GeometrySet {
    /// Heap held by the set: its members and their vertex and ring offset arrays, spare capacity
    /// included.
    pub fn mem_usage(&self) -> usize {
        heap_size(&self.members)
            + self
                .members
                .iter()
                .map(|member| heap_size(&member.vertices) + heap_size(&member.ring_offsets))
                .sum::<usize>()
    }
}

/// Heap held by a Vec's buffer, spare capacity included.
fn heap_size<T>(vec: &Vec<T>) -> usize {
    vec.capacity() * std::mem::size_of::<T>()
}

/// Which of the two regions a polygon ring separates on the sphere is the polygon's. A ring and
/// its reverse have the same vertices, so only the direction it runs can tell a small polygon from
/// the rest of the sphere around it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum RingOrientation {
    /// The smaller region, whichever way the ring runs. RFC 7946 has parsers accept rings of
    /// either direction, and much data has them, but no polygon is larger than a hemisphere.
    #[default]
    Smaller,
    /// The right-hand rule of RFC 7946: an exterior ring runs counter-clockwise around the
    /// polygon and a hole clockwise, so the polygon lies to the left of every ring. A polygon may
    /// be larger than a hemisphere, but a ring running the wrong way bounds the rest of the
    /// sphere.
    RightHand,
}

/// Smash a projected geometry into a GeometrySet in stored format, with rings bounding the
/// smaller region. See `to_geometry_set_with_orientation`.
pub fn to_geometry_set(geometry: &Geometry<Sphere>, doc_id: u32) -> GeometrySet {
    to_geometry_set_with_orientation(geometry, doc_id, RingOrientation::Smaller)
}

/// Smash a projected geometry into a GeometrySet in stored format. Orients polygon rings CCW
/// around the regions the orientation has them bound, computes contains_hilbert_start, and
/// flattens rings into a single vertex array per member. One Geometry in, one GeometrySet out.
/// Every array is allocated at its exact length, but for degenerate polygon rings, since the
/// writer holds the sets until the segment is flushed.
pub fn to_geometry_set_with_orientation(
    geometry: &Geometry<Sphere>,
    doc_id: u32,
    orientation: RingOrientation,
) -> GeometrySet {
    let mut members = Vec::with_capacity(member_count(geometry));
    smash(&mut members, geometry, orientation);
    GeometrySet { members, doc_id }
}

/// Number of members a geometry smashes into.
fn member_count(geometry: &Geometry<Sphere>) -> usize {
    match geometry {
        Geometry::Point(_) | Geometry::LineString(_) | Geometry::Polygon(_) => 1,
        Geometry::MultiPoint(points) => points.len(),
        Geometry::MultiLineString(lines) => lines.len(),
        Geometry::MultiPolygon(polygons) => polygons.len(),
        Geometry::GeometryCollection(collection) => collection.iter().map(member_count).sum(),
    }
}

fn smash(members: &mut Vec<EdgeSet>, geometry: &Geometry<Sphere>, orientation: RingOrientation) {
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
        // A line with no vertices is empty.
        Geometry::LineString(line) => {
            if !line.is_empty() {
                members.push(open_member(line.clone()));
            }
        }
        Geometry::MultiLineString(lines) => {
            for line in lines.iter().filter(|line| !line.is_empty()) {
                members.push(open_member(line.clone()));
            }
        }
        Geometry::Polygon(polygon) => {
            if !polygon.is_empty() {
                members.extend(smash_polygon(polygon, orientation));
            }
        }
        Geometry::MultiPolygon(multi_polygon) => {
            for polygon in multi_polygon.iter().filter(|polygon| !polygon.is_empty()) {
                members.extend(smash_polygon(polygon, orientation));
            }
        }
        Geometry::GeometryCollection(collection) => {
            for geo in collection {
                smash(members, geo, orientation);
            }
        }
    }
}

/// Smash one polygon (outer ring + holes) into stored-format members. Takes already-projected
/// sphere coordinates with rings closed per RFC 7946 (first == last), or unclosed. Orients every
/// ring CCW around the region it bounds, computes whether the Hilbert curve start point is inside,
/// and flattens into a single vertex array of closed rings. The orientation decides which region
/// a ring bounds.
///
/// Degeneracies are made valid as ST_MakeValid does. Repeated vertices are removed. A spike,
/// where an edge doubles back along the one before it, bounds no area: it leaves the ring, and
/// follows the polygon as a line member, where queries still meet it. An exterior that bounds no
/// area at all is the point or line it is, without its holes, and a hole that bounds none is left
/// out. A polygon with no rings is empty.
///
/// Ring offsets always start with 0 and end with the total vertex count.
fn smash_polygon(rings: &[Vec<[f64; 3]>], orientation: RingOrientation) -> Vec<EdgeSet> {
    let mut members = Vec::new();
    // Room for every ring closed, as GeoJSON rings are.
    let closed_len = |ring: &Vec<[f64; 3]>| ring.len() + usize::from(ring.first() != ring.last());
    let mut flat = Vec::with_capacity(rings.iter().map(closed_len).sum());
    let mut hilbert_inside = false;
    let mut offsets = Vec::with_capacity(rings.len() + 1);
    offsets.push(0);

    for (i, ring) in rings.iter().enumerate() {
        let (mut ring, spikes) = prune_degeneracies(ring);
        if ring.is_empty() || on_one_great_circle(&ring) {
            if i == 0 {
                return vec![open_member(remove_repeated_vertices(&rings[0]))];
            }
            continue;
        }
        members.extend(spikes.iter().map(|spike| open_member(spike.to_vec())));
        let curvature = curvature(&ring);
        let reverse = match orientation {
            RingOrientation::Smaller => curvature < -curvature_max_error(ring.len()),
            // A hole runs clockwise around the region it bounds.
            RingOrientation::RightHand => i > 0,
        };
        if reverse {
            // Keep the first vertex first.
            ring[1..].reverse();
        }
        ring.push(ring[0]);
        let mut origin_inside = compute_origin_inside(&ring);
        if orientation == RingOrientation::Smaller && curvature.abs() < std::f64::consts::PI {
            // A ring this close to a hemisphere may cross itself, which defeats the angle test
            // at a vertex. Take the inside to be the smaller side.
            let inside = |origin_inside| {
                SAMPLE_POINTS
                    .iter()
                    .filter(|point| brute_force_contains(point, &ring, origin_inside))
                    .count()
            };
            if inside(origin_inside) > SAMPLE_POINTS.len() * 3 / 4 {
                origin_inside = !origin_inside;
            }
        }
        if brute_force_contains(&HILBERT_START, &ring, origin_inside) {
            hilbert_inside = !hilbert_inside;
        }
        flat.extend_from_slice(&ring);
        offsets.push(flat.len());
    }

    let polygon = EdgeSet {
        vertices: flat,
        closed: true,
        contains_hilbert_start: hilbert_inside,
        ring_offsets: offsets,
    };
    members.insert(0, polygon);
    members
}

/// Directions spread over the sphere: the centers, edge midpoints and corners of the cube.
static SAMPLE_POINTS: LazyLock<Vec<[f64; 3]>> = LazyLock::new(|| {
    let mut points = Vec::new();
    for x in -1..=1 {
        for y in -1..=1 {
            for z in -1..=1 {
                if (x, y, z) != (0, 0, 0) {
                    points.push(normalize(&[x as f64, y as f64, z as f64]));
                }
            }
        }
    }
    points
});

/// A path without consecutive repeated vertices. A ring of a single position becomes that
/// position alone.
fn remove_repeated_vertices(ring: &[[f64; 3]]) -> Vec<[f64; 3]> {
    let mut vertices: Vec<[f64; 3]> = Vec::with_capacity(ring.len());
    for v in ring {
        if vertices.last() != Some(v) {
            vertices.push(*v);
        }
    }
    vertices
}

/// A member with no interior: a point for a single vertex, otherwise a line through the vertices.
fn open_member(vertices: Vec<[f64; 3]>) -> EdgeSet {
    let n = vertices.len();
    EdgeSet {
        vertices,
        closed: false,
        contains_hilbert_start: false,
        ring_offsets: vec![0, n],
    }
}

#[cfg(test)]
mod tests {
    use std::mem::size_of;

    use super::*;
    use crate::spatial::plane::Plane;

    fn smashed(geometry: Geometry<Plane>) -> GeometrySet {
        to_geometry_set(&geometry.project::<Sphere>(), 0)
    }

    #[test]
    fn test_mem_usage_counts_exactly_sized_arrays() {
        let point = smashed(Geometry::Point([19.94, 50.06]));
        assert_eq!(
            point.mem_usage(),
            size_of::<EdgeSet>() + size_of::<[f64; 3]>() + 2 * size_of::<usize>()
        );

        let square = vec![[0.0, 0.0], [4.0, 0.0], [4.0, 4.0], [0.0, 4.0], [0.0, 0.0]];
        let hole = vec![[1.0, 1.0], [1.0, 2.0], [2.0, 2.0], [2.0, 1.0], [1.0, 1.0]];
        let polygons = smashed(Geometry::MultiPolygon(vec![
            vec![square.clone(), hole],
            vec![square],
        ]));
        assert_eq!(
            polygons.mem_usage(),
            2 * size_of::<EdgeSet>() + 15 * size_of::<[f64; 3]>() + 5 * size_of::<usize>()
        );
    }

    #[test]
    fn test_smash_polygon_reverses_holes() {
        let square = vec![[0.0, 0.0], [4.0, 0.0], [4.0, 4.0], [0.0, 4.0], [0.0, 0.0]];
        let hole = vec![[1.0, 1.0], [1.0, 2.0], [2.0, 2.0], [2.0, 1.0], [1.0, 1.0]];
        let projected =
            Geometry::<Plane>::Polygon(vec![square.clone(), hole.clone()]).project::<Sphere>();
        let Geometry::Polygon(rings) = &projected else {
            unreachable!()
        };
        let set = to_geometry_set(&projected, 0);
        let member = &set.members[0];
        assert_eq!(member.ring_offsets, [0, 5, 10]);
        assert_eq!(member.vertices[..5], rings[0][..]);
        let reversed: Vec<[f64; 3]> = rings[1].iter().rev().copied().collect();
        assert_eq!(member.vertices[5..], reversed[..]);
    }

    #[test]
    fn test_smash_polygon_orients_rings_ccw() {
        // A clockwise exterior is reversed. A counter-clockwise hole, which RFC 7946 orders
        // the other way, already runs CCW around the hole, as the stored format has it.
        let square = vec![[0.0, 0.0], [0.0, 4.0], [4.0, 4.0], [4.0, 0.0], [0.0, 0.0]];
        let hole = vec![[1.0, 1.0], [2.0, 1.0], [2.0, 2.0], [1.0, 2.0], [1.0, 1.0]];
        let projected =
            Geometry::<Plane>::Polygon(vec![square.clone(), hole.clone()]).project::<Sphere>();
        let set = to_geometry_set(&projected, 0);
        let member = &set.members[0];
        let expected = Geometry::<Plane>::Polygon(vec![
            vec![[0.0, 0.0], [4.0, 0.0], [4.0, 4.0], [0.0, 4.0], [0.0, 0.0]],
            hole,
        ])
        .project::<Sphere>();
        let Geometry::Polygon(rings) = &expected else {
            unreachable!()
        };
        assert_eq!(member.vertices[..5], rings[0][..]);
        assert_eq!(member.vertices[5..], rings[1][..]);
        assert!(!member.contains_hilbert_start);
    }

    #[test]
    fn test_smash_polygon_keeps_what_degeneracies_leave() {
        // A repeated leading vertex is removed. A spike leaves the ring, and stays as a line.
        let spiked = vec![
            [0.0, 0.0],
            [0.0, 0.0],
            [10.0, 0.0],
            [12.0, 0.0],
            [10.0, 0.0],
            [10.0, 10.0],
            [0.0, 10.0],
            [0.0, 0.0],
        ];
        let set = smashed(Geometry::Polygon(vec![spiked]));
        let square = vec![
            [0.0, 0.0],
            [10.0, 0.0],
            [10.0, 10.0],
            [0.0, 10.0],
            [0.0, 0.0],
        ];
        let unspiked = smashed(Geometry::Polygon(vec![square.clone()]));
        assert_eq!(set.members[0].vertices, unspiked.members[0].vertices);
        assert_eq!(
            set.members[0].contains_hilbert_start,
            unspiked.members[0].contains_hilbert_start
        );
        let spike = smashed(Geometry::LineString(vec![[10.0, 0.0], [12.0, 0.0]]));
        assert_eq!(set.members.len(), 2);
        assert!(!set.members[1].closed);
        assert_eq!(set.members[1].vertices, spike.members[0].vertices);

        // An unclosed ring is closed.
        let unclosed = smashed(Geometry::Polygon(vec![square[..4].to_vec()]));
        assert_eq!(unclosed.members[0].vertices, unspiked.members[0].vertices);

        // A polygon with no area is the line or point it is. A hole with no area is left out.
        let flat = vec![[0.0, 0.0], [1.0, 1.0], [0.0, 0.0], [0.0, 0.0]];
        let set = smashed(Geometry::Polygon(vec![flat.clone()]));
        let line = smashed(Geometry::LineString(flat[..3].to_vec()));
        assert!(!set.members[0].closed);
        assert_eq!(set.members[0].vertices, line.members[0].vertices);
        let collinear = vec![[0.0, 0.0], [2.0, 0.0], [1.0, 0.0], [0.0, 0.0]];
        assert!(!smashed(Geometry::Polygon(vec![collinear])).members[0].closed);
        let point = smashed(Geometry::Polygon(vec![vec![[5.0, 5.0]; 4]]));
        assert_eq!(point.members[0].vertices.len(), 1);
        let square = vec![[0.0, 0.0], [4.0, 0.0], [4.0, 4.0], [0.0, 4.0], [0.0, 0.0]];
        let set = smashed(Geometry::Polygon(vec![square, flat]));
        assert_eq!(set.members[0].ring_offsets, [0, 5]);

        // Empty polygons and lines are empty.
        assert!(smashed(Geometry::Polygon(vec![])).members.is_empty());
        let multi = smashed(Geometry::MultiPolygon(vec![
            vec![],
            vec![vec![[5.0, 5.0]; 4]],
        ]));
        assert_eq!(multi.members.len(), 1);
        assert!(smashed(Geometry::LineString(vec![])).members.is_empty());
    }
}

//! HUSH
use std::collections::HashMap;
use std::io;

use i_triangle::i_overlay::i_float::int::point::IntPoint;
use i_triangle::int::triangulatable::IntTriangulatable;

use crate::schema::Field;
use crate::spatial::geometry::Geometry;
use crate::spatial::point::GeoPoint;
use crate::spatial::serializer::SpatialSerializer;
use crate::spatial::triangle::{delaunay_to_triangles, Triangle};
use crate::DocId;

/// HUSH
pub struct SpatialWriter {
    /// Map from field to its triangles buffer
    triangles_by_field: HashMap<Field, Vec<Triangle>>,
}

impl SpatialWriter {
    /// HUST
    pub fn add_geometry(&mut self, doc_id: DocId, field: Field, geometry: Geometry) {
        let triangles = &mut self.triangles_by_field.entry(field).or_default();
        match geometry {
            Geometry::Point(point) => {
                append_point(triangles, doc_id, point);
            }
            Geometry::MultiPoint(multi_point) => {
                for point in multi_point {
                    append_point(triangles, doc_id, point);
                }
            }
            Geometry::LineString(line_string) => {
                append_line_string(triangles, doc_id, line_string);
            }
            Geometry::MultiLineString(multi_line_string) => {
                for line_string in multi_line_string {
                    append_line_string(triangles, doc_id, line_string);
                }
            }
            Geometry::Polygon(polygon) => {
                append_polygon(triangles, doc_id, &polygon);
            }
            Geometry::MultiPolygon(multi_polygon) => {
                for polygon in multi_polygon {
                    append_polygon(triangles, doc_id, &polygon);
                }
            }
            Geometry::GeometryCollection(geometries) => {
                for geometry in geometries {
                    self.add_geometry(doc_id, field, geometry);
                }
            }
        }
    }

    /// Memory usage estimate
    pub fn mem_usage(&self) -> usize {
        self.triangles_by_field
            .values()
            .map(|triangles| triangles.len() * std::mem::size_of::<Triangle>())
            .sum()
    }

    /// Serializing our field.
    pub fn serialize(&mut self, mut serializer: SpatialSerializer) -> io::Result<()> {
        for (field, triangles) in &mut self.triangles_by_field {
            serializer.serialize_field(*field, triangles)?;
        }
        serializer.close()?;
        Ok(())
    }
}

impl Default for SpatialWriter {
    /// HUSH
    fn default() -> Self {
        SpatialWriter {
            triangles_by_field: HashMap::new(),
        }
    }
}

/// Convert a point of `(longitude, latitude)` to a integer point.
pub fn as_point_i32(point: GeoPoint) -> (i32, i32) {
    (
        (point.lon / (360.0 / (1i64 << 32) as f64)).floor() as i32,
        (point.lat / (180.0 / (1i64 << 32) as f64)).floor() as i32,
    )
}

fn append_point(triangles: &mut Vec<Triangle>, doc_id: DocId, point: GeoPoint) {
    let point = as_point_i32(point);
    triangles.push(Triangle::from_point(doc_id, point.0, point.1));
}

fn append_line_string(
    triangles: &mut Vec<Triangle>,
    doc_id: DocId,
    line_string: Vec<GeoPoint>,
) {
    let mut previous = as_point_i32(line_string[0]);
    for point in line_string.into_iter().skip(1) {
        let point = as_point_i32(point);
        triangles.push(Triangle::from_line_segment(
            doc_id, previous.0, previous.1, point.0, point.1,
        ));
        previous = point
    }
}

fn append_ring(i_polygon: &mut Vec<Vec<IntPoint>>, ring: &[GeoPoint]) {
    let mut i_ring = Vec::with_capacity(ring.len() + 1);
    for &point in ring {
        let point = as_point_i32(point);
        i_ring.push(IntPoint::new(point.0, point.1));
    }
    i_polygon.push(i_ring);
}

fn append_polygon(triangles: &mut Vec<Triangle>, doc_id: DocId, polygon: &[Vec<GeoPoint>]) {
    let mut i_polygon: Vec<Vec<IntPoint>> = Vec::new();
    for ring in polygon {
        append_ring(&mut i_polygon, ring);
    }
    let delaunay = i_polygon.triangulate().into_delaunay();
    delaunay_to_triangles(doc_id, &delaunay, triangles);
}

//! HUSH
use std::collections::HashMap;
use std::io;

use i_triangle::i_overlay::i_float::int::point::IntPoint;
use i_triangle::int::triangulatable::IntTriangulatable;

use crate::schema::Field;
use crate::spatial::geometry::Geometry;
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
                into_point(triangles, doc_id, point);
            }
            Geometry::MultiPoint(multi_point) => {
                for point in multi_point {
                    into_point(triangles, doc_id, point);
                }
            }
            Geometry::LineString(line_string) => {
                into_line_string(triangles, doc_id, line_string);
            }
            Geometry::MultiLineString(multi_line_string) => {
                for line_string in multi_line_string {
                    into_line_string(triangles, doc_id, line_string);
                }
            }
            Geometry::Polygon(polygon) => {
                into_polygon(triangles, doc_id, polygon);
            }
            Geometry::MultiPolygon(multi_polygon) => {
                for polygon in multi_polygon {
                    into_polygon(triangles, doc_id, polygon);
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

    /// HUSH
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

/// HUSH
pub fn as_point_i32(point: (f64, f64)) -> (i32, i32) {
    (
        (point.0 / (360.0 / (1i64 << 32) as f64)).floor() as i32,
        (point.1 / (180.0 / (1i64 << 32) as f64)).floor() as i32,
    )
}

fn into_point(triangles: &mut Vec<Triangle>, doc_id: DocId, point: (f64, f64)) {
    let point = as_point_i32(point);
    triangles.push(Triangle::new(
        doc_id,
        [point.1, point.0, point.1, point.0, point.1, point.0],
        [true, true, true],
    ));
}

fn into_line_string(triangles: &mut Vec<Triangle>, doc_id: DocId, line_string: Vec<(f64, f64)>) {
    let mut previous = as_point_i32(line_string[0]);
    for point in line_string.into_iter().skip(1) {
        let point = as_point_i32(point);
        triangles.push(Triangle::new(
            doc_id,
            [
                previous.1, previous.0, point.1, point.0, previous.1, previous.0,
            ],
            [true, true, true],
        ));
        previous = point
    }
}

fn into_ring(i_polygon: &mut Vec<Vec<IntPoint>>, ring: Vec<(f64, f64)>) {
    let mut i_ring = Vec::new();
    for point in ring {
        let point = as_point_i32(point);
        i_ring.push(IntPoint::new(point.0, point.1));
    }
    i_polygon.push(i_ring);
}

fn into_polygon(triangles: &mut Vec<Triangle>, doc_id: DocId, polygon: Vec<Vec<(f64, f64)>>) {
    let mut i_polygon = Vec::new();
    for ring in polygon {
        into_ring(&mut i_polygon, ring);
    }
    let delaunay = i_polygon.triangulate().into_delaunay();
    delaunay_to_triangles(doc_id, &delaunay, triangles);
}

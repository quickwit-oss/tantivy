//! Spatial field writer.
//!
//! Accumulates geometry per field during indexing. At segment flush, builds cell indexes and
//! streams edge data through the SpatialSerializer.
use std::collections::HashMap;
use std::io;

use crate::schema::Field;
use crate::spatial::cell_index::GeometryData;
use crate::spatial::containment::compute_origin_inside;
use crate::spatial::geometry::Geometry;
use crate::spatial::plane::Plane;
use crate::spatial::serializer::SpatialSerializer;
use crate::DocId;

/// Converts longitude/latitude in degrees to a unit sphere point.
fn lonlat_to_sphere(lon: f64, lat: f64) -> [f64; 3] {
    let lat = lat.to_radians();
    let lon = lon.to_radians();
    let cos_lat = lat.cos();
    [cos_lat * lon.cos(), cos_lat * lon.sin(), lat.sin()]
}

/// Converts a ring of lon/lat points to unit sphere vertices.
fn ring_to_sphere(ring: &[[f64; 2]]) -> Vec<[f64; 3]> {
    ring.iter().map(|p| lonlat_to_sphere(p[0], p[1])).collect()
}

/// Per-field accumulated documents with their geometries.
struct FieldData {
    documents: Vec<(u32, Vec<GeometryData>)>,
}

impl FieldData {
    fn new() -> Self {
        Self {
            documents: Vec::new(),
        }
    }

    fn ensure_doc(&mut self, doc_id: u32) -> &mut Vec<GeometryData> {
        if self.documents.last().map_or(true, |&(id, _)| id != doc_id) {
            self.documents.push((doc_id, Vec::new()));
        }
        &mut self.documents.last_mut().unwrap().1
    }
}

/// Accumulates spatial geometry during indexing, then serializes at segment flush.
pub struct SpatialWriter {
    data_by_field: HashMap<Field, FieldData>,
}

impl SpatialWriter {
    /// Add a geometry for a document and field.
    pub fn add_geometry(&mut self, doc_id: DocId, field: Field, geometry: Geometry<Plane>) {
        let data = self
            .data_by_field
            .entry(field)
            .or_insert_with(FieldData::new);
        let geometries = data.ensure_doc(doc_id);
        collect_geometry(geometries, &geometry);
    }

    /// Memory usage estimate.
    pub fn mem_usage(&self) -> usize {
        self.data_by_field
            .values()
            .map(|data| {
                data.documents
                    .iter()
                    .map(|(_, geos)| {
                        geos.iter()
                            .map(|g| g.rings.iter().map(|r| r.len() * 24).sum::<usize>())
                            .sum::<usize>()
                    })
                    .sum::<usize>()
            })
            .sum()
    }

    /// Serialize all fields.
    pub fn serialize(&mut self, mut serializer: SpatialSerializer) -> io::Result<()> {
        for (field, data) in &self.data_by_field {
            serializer.serialize_field(*field, &data.documents)?;
        }
        serializer.close()?;
        Ok(())
    }
}

impl Default for SpatialWriter {
    fn default() -> Self {
        SpatialWriter {
            data_by_field: HashMap::new(),
        }
    }
}

/// Recursively collect GeometryData from a Geometry<Plane>.
fn collect_geometry(geometries: &mut Vec<GeometryData>, geometry: &Geometry<Plane>) {
    match geometry {
        Geometry::Point(point) => {
            let v = lonlat_to_sphere(point[0], point[1]);
            geometries.push(GeometryData {
                rings: vec![vec![v]],
                origin_inside: vec![false],
                dimension: 0,
            });
        }
        Geometry::MultiPoint(points) => {
            for point in points {
                let v = lonlat_to_sphere(point[0], point[1]);
                geometries.push(GeometryData {
                    rings: vec![vec![v]],
                    origin_inside: vec![false],
                    dimension: 0,
                });
            }
        }
        Geometry::LineString(line) => {
            let ring = ring_to_sphere(line);
            geometries.push(GeometryData {
                rings: vec![ring],
                origin_inside: vec![false],
                dimension: 1,
            });
        }
        Geometry::MultiLineString(lines) => {
            for line in lines {
                let ring = ring_to_sphere(line);
                geometries.push(GeometryData {
                    rings: vec![ring],
                    origin_inside: vec![false],
                    dimension: 1,
                });
            }
        }
        Geometry::Polygon(polygon) => {
            let rings: Vec<Vec<[f64; 3]>> = polygon
                .iter()
                .enumerate()
                .map(|(i, r)| {
                    let mut ring = ring_to_sphere(r);
                    // GeoJSON RFC 7946: holes are CW. S2 wants all rings CCW.
                    if i > 0 {
                        ring.reverse();
                    }
                    ring
                })
                .collect();
            let origin_inside: Vec<bool> = rings.iter().map(|r| compute_origin_inside(r)).collect();
            geometries.push(GeometryData {
                rings,
                origin_inside,
                dimension: 2,
            });
        }
        Geometry::MultiPolygon(multi_polygon) => {
            for polygon in multi_polygon {
                let rings: Vec<Vec<[f64; 3]>> = polygon
                    .iter()
                    .enumerate()
                    .map(|(i, r)| {
                        let mut ring = ring_to_sphere(r);
                        if i > 0 {
                            ring.reverse();
                        }
                        ring
                    })
                    .collect();
                let origin_inside: Vec<bool> =
                    rings.iter().map(|r| compute_origin_inside(r)).collect();
                geometries.push(GeometryData {
                    rings,
                    origin_inside,
                    dimension: 2,
                });
            }
        }
        Geometry::GeometryCollection(collection) => {
            for geo in collection {
                collect_geometry(geometries, geo);
            }
        }
    }
}

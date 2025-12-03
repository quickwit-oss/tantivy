//! HUSH

use std::io::{self, Read, Write};

use common::{BinarySerializable, VInt};
use serde_json::{json, Map, Value};

use crate::spatial::point::GeoPoint;
use crate::spatial::xor::{compress_f64, decompress_f64};

/// HUSH
#[derive(Debug)]
pub enum GeometryError {
    /// HUSH
    MissingType,
    /// HUSH
    MissingField(String), // "expected array", "wrong nesting depth", etc
    /// HUSH
    UnsupportedType(String),
    /// HUSH
    InvalidCoordinate(String), // Can report the actual bad value
    /// HUSH
    InvalidStructure(String), // "expected array", "wrong nesting depth", etc
}

/// HUSH
#[derive(Debug, Clone, PartialEq)]
pub enum Geometry {
    /// HUSH
    Point(GeoPoint),
    /// HUSH
    MultiPoint(Vec<GeoPoint>),
    /// HUSH
    LineString(Vec<GeoPoint>),
    /// HUSH
    MultiLineString(Vec<Vec<GeoPoint>>),
    /// HUSH
    Polygon(Vec<Vec<GeoPoint>>),
    /// HUSH
    MultiPolygon(Vec<Vec<Vec<GeoPoint>>>),
    /// HUSH
    GeometryCollection(Vec<Self>),
}

impl Geometry {
    /// HUSH
    pub fn from_geojson(object: &Map<String, Value>) -> Result<Self, GeometryError> {
        let geometry_type = object
            .get("type")
            .and_then(|v| v.as_str())
            .ok_or(GeometryError::MissingType)?;
        match geometry_type {
            "Point" => {
                let coordinates = get_coordinates(object)?;
                let point = to_point(coordinates)?;
                Ok(Geometry::Point(point))
            }
            "MultiPoint" => {
                let coordinates = get_coordinates(object)?;
                let multi_point = to_line_string(coordinates)?;
                Ok(Geometry::MultiPoint(multi_point))
            }
            "LineString" => {
                let coordinates = get_coordinates(object)?;
                let line_string = to_line_string(coordinates)?;
                if line_string.len() < 2 {
                    return Err(GeometryError::InvalidStructure(
                        "a line string contains at least 2 points".to_string(),
                    ));
                }
                Ok(Geometry::LineString(line_string))
            }
            "MultiLineString" => {
                let coordinates = get_coordinates(object)?;
                let multi_line_string = to_multi_line_string(coordinates)?;
                for line_string in &multi_line_string {
                    if line_string.len() < 2 {
                        return Err(GeometryError::InvalidStructure(
                            "a line string contains at least 2 points".to_string(),
                        ));
                    }
                }
                Ok(Geometry::MultiLineString(multi_line_string))
            }
            "Polygon" => {
                let coordinates = get_coordinates(object)?;
                let polygon = to_multi_line_string(coordinates)?;
                for ring in &polygon {
                    if ring.len() < 3 {
                        return Err(GeometryError::InvalidStructure(
                            "a polygon ring contains at least 3 points".to_string(),
                        ));
                    }
                }
                Ok(Geometry::Polygon(polygon))
            }
            "MultiPolygon" => {
                let mut result = Vec::new();
                let multi_polygons = get_coordinates(object)?;
                let multi_polygons =
                    multi_polygons
                        .as_array()
                        .ok_or(GeometryError::InvalidStructure(
                            "expected an array of polygons".to_string(),
                        ))?;
                for polygon in multi_polygons {
                    let polygon = to_multi_line_string(polygon)?;
                    for ring in &polygon {
                        if ring.len() < 3 {
                            return Err(GeometryError::InvalidStructure(
                                "a polygon ring contains at least 3 points".to_string(),
                            ));
                        }
                    }
                    result.push(polygon);
                }
                Ok(Geometry::MultiPolygon(result))
            }
            "GeometriesCollection" => {
                let geometries = object
                    .get("geometries")
                    .ok_or(GeometryError::MissingField("geometries".to_string()))?;
                let geometries = geometries
                    .as_array()
                    .ok_or(GeometryError::InvalidStructure(
                        "geometries is not an array".to_string(),
                    ))?;
                let mut result = Vec::new();
                for geometry in geometries {
                    let object = geometry.as_object().ok_or(GeometryError::InvalidStructure(
                        "geometry is not an object".to_string(),
                    ))?;
                    result.push(Geometry::from_geojson(object)?);
                }
                Ok(Geometry::GeometryCollection(result))
            }
            _ => Err(GeometryError::UnsupportedType(geometry_type.to_string())),
        }
    }

    /// Serialize the geometry to GeoJSON format.
    /// https://fr.wikipedia.org/wiki/GeoJSON
    pub fn to_geojson(&self) -> Map<String, Value> {
        let mut map = Map::new();
        match self {
            Geometry::Point(point) => {
                map.insert("type".to_string(), Value::String("Point".to_string()));
                let coords = json!([point.lon, point.lat]);
                map.insert("coordinates".to_string(), coords);
            }
            Geometry::MultiPoint(points) => {
                map.insert("type".to_string(), Value::String("MultiPoint".to_string()));
                let coords: Vec<Value> = points.iter().map(|p| json!([p.lon, p.lat])).collect();
                map.insert("coordinates".to_string(), Value::Array(coords));
            }
            Geometry::LineString(line) => {
                map.insert("type".to_string(), Value::String("LineString".to_string()));
                let coords: Vec<Value> = line.iter().map(|p| json!([p.lon, p.lat])).collect();
                map.insert("coordinates".to_string(), Value::Array(coords));
            }
            Geometry::MultiLineString(lines) => {
                map.insert(
                    "type".to_string(),
                    Value::String("MultiLineString".to_string()),
                );
                let coords: Vec<Value> = lines
                    .iter()
                    .map(|line| Value::Array(line.iter().map(|p| json!([p.lon, p.lat])).collect()))
                    .collect();
                map.insert("coordinates".to_string(), Value::Array(coords));
            }
            Geometry::Polygon(rings) => {
                map.insert("type".to_string(), Value::String("Polygon".to_string()));
                let coords: Vec<Value> = rings
                    .iter()
                    .map(|ring| Value::Array(ring.iter().map(|p| json!([p.lon, p.lat])).collect()))
                    .collect();
                map.insert("coordinates".to_string(), Value::Array(coords));
            }
            Geometry::MultiPolygon(polygons) => {
                map.insert(
                    "type".to_string(),
                    Value::String("MultiPolygon".to_string()),
                );
                let coords: Vec<Value> = polygons
                    .iter()
                    .map(|polygon| {
                        Value::Array(
                            polygon
                                .iter()
                                .map(|ring| {
                                    Value::Array(ring.iter().map(|p| json!([p.lon, p.lat])).collect())
                                })
                                .collect(),
                        )
                    })
                    .collect();
                map.insert("coordinates".to_string(), Value::Array(coords));
            }
            Geometry::GeometryCollection(geometries) => {
                map.insert(
                    "type".to_string(),
                    Value::String("GeometryCollection".to_string()),
                );
                let geoms: Vec<Value> = geometries
                    .iter()
                    .map(|g| Value::Object(g.to_geojson()))
                    .collect();
                map.insert("geometries".to_string(), Value::Array(geoms));
            }
        }
        map
    }
}

fn get_coordinates(object: &Map<String, Value>) -> Result<&Value, GeometryError> {
    let coordinates = object
        .get("coordinates")
        .ok_or(GeometryError::MissingField("coordinates".to_string()))?;
    Ok(coordinates)
}

fn to_point(value: &Value) -> Result<GeoPoint, GeometryError> {
    let lonlat = value.as_array().ok_or(GeometryError::InvalidStructure(
        "expected 2 element array pair of lon/lat".to_string(),
    ))?;
    if lonlat.len() != 2 {
        return Err(GeometryError::InvalidStructure(
            "expected 2 element array pair of lon/lat".to_string(),
        ));
    }
    let lon = lonlat[0].as_f64().ok_or(GeometryError::InvalidCoordinate(
        "longitude must be f64".to_string(),
    ))?;
    if !lon.is_finite() || !(-180.0..=180.0).contains(&lon) {
        return Err(GeometryError::InvalidCoordinate(format!(
            "invalid longitude: {}",
            lon
        )));
    }
    let lat = lonlat[1].as_f64().ok_or(GeometryError::InvalidCoordinate(
        "latitude must be f64".to_string(),
    ))?;
    if !lat.is_finite() || !(-90.0..=90.0).contains(&lat) {
        return Err(GeometryError::InvalidCoordinate(format!(
            "invalid latitude: {}",
            lat
        )));
    }
    Ok(GeoPoint { lon, lat })
}

fn to_line_string(value: &Value) -> Result<Vec<GeoPoint>, GeometryError> {
    let mut result = Vec::new();
    let coordinates = value.as_array().ok_or(GeometryError::InvalidStructure(
        "expected an array of lon/lat arrays".to_string(),
    ))?;
    for coordinate in coordinates {
        result.push(to_point(coordinate)?);
    }
    Ok(result)
}

fn to_multi_line_string(value: &Value) -> Result<Vec<Vec<GeoPoint>>, GeometryError> {
    let mut result = Vec::new();
    let coordinates = value.as_array().ok_or(GeometryError::InvalidStructure(
        "expected an array of an array of lon/lat arrays".to_string(),
    ))?;
    for coordinate in coordinates {
        result.push(to_line_string(coordinate)?);
    }
    Ok(result)
}

impl BinarySerializable for Geometry {
    fn serialize<W: Write + ?Sized>(&self, writer: &mut W) -> io::Result<()> {
        match self {
            Geometry::Point(point) => {
                0u8.serialize(writer)?;
                point.lon.serialize(writer)?;
                point.lat.serialize(writer)?;
                Ok(())
            }
            Geometry::MultiPoint(points) => {
                1u8.serialize(writer)?;
                serialize_line_string(points, writer)
            }
            Geometry::LineString(line_string) => {
                2u8.serialize(writer)?;
                serialize_line_string(line_string, writer)
            }
            Geometry::MultiLineString(multi_line_string) => {
                3u8.serialize(writer)?;
                serialize_polygon(&multi_line_string[..], writer)
            }
            Geometry::Polygon(polygon) => {
                4u8.serialize(writer)?;
                serialize_polygon(polygon, writer)
            }
            Geometry::MultiPolygon(multi_polygon) => {
                5u8.serialize(writer)?;
                BinarySerializable::serialize(&VInt(multi_polygon.len() as u64), writer)?;
                for polygon in multi_polygon {
                    BinarySerializable::serialize(&VInt(polygon.len() as u64), writer)?;
                    for ring in polygon {
                        BinarySerializable::serialize(&VInt(ring.len() as u64), writer)?;
                    }
                }
                let mut lon = Vec::new();
                let mut lat = Vec::new();
                for polygon in multi_polygon {
                    for ring in polygon {
                        for point in ring {
                            lon.push(point.lon);
                            lat.push(point.lat);
                        }
                    }
                }
                let lon = compress_f64(&lon);
                let lat = compress_f64(&lat);
                VInt(lon.len() as u64).serialize(writer)?;
                writer.write_all(&lon)?;
                VInt(lat.len() as u64).serialize(writer)?;
                writer.write_all(&lat)?;
                Ok(())
            }
            Geometry::GeometryCollection(geometries) => {
                6u8.serialize(writer)?;
                BinarySerializable::serialize(&VInt(geometries.len() as u64), writer)?;
                for geometry in geometries {
                    geometry.serialize(writer)?;
                }
                Ok(())
            }
        }
    }

    fn deserialize<R: Read>(reader: &mut R) -> io::Result<Self> {
        let discriminant: u8 = BinarySerializable::deserialize(reader)?;
        match discriminant {
            0 => {
                let lon = BinarySerializable::deserialize(reader)?;
                let lat = BinarySerializable::deserialize(reader)?;
                Ok(Geometry::Point(GeoPoint { lon, lat }))
            }
            1 => Ok(Geometry::MultiPoint(deserialize_line_string(reader)?)),
            2 => Ok(Geometry::LineString(deserialize_line_string(reader)?)),
            3 => Ok(Geometry::MultiLineString(deserialize_polygon(reader)?)),
            4 => Ok(Geometry::Polygon(deserialize_polygon(reader)?)),
            5 => {
                let polygon_count = VInt::deserialize(reader)?.0 as usize;
                let mut polygons = Vec::new();
                let mut count = 0;
                for _ in 0..polygon_count {
                    let ring_count = VInt::deserialize(reader)?.0 as usize;
                    let mut rings = Vec::new();
                    for _ in 0..ring_count {
                        let point_count = VInt::deserialize(reader)?.0 as usize;
                        rings.push(point_count);
                        count += point_count;
                    }
                    polygons.push(rings);
                }
                let lon_bytes: Vec<u8> = BinarySerializable::deserialize(reader)?;
                let lat_bytes: Vec<u8> = BinarySerializable::deserialize(reader)?;
                let lon = decompress_f64(&lon_bytes, count);
                let lat = decompress_f64(&lat_bytes, count);
                let mut multi_polygon = Vec::new();
                let mut offset = 0;
                for rings in polygons {
                    let mut polygon = Vec::new();
                    for point_count in rings {
                        let mut ring = Vec::new();
                        for _ in 0..point_count {
                            ring.push(GeoPoint {
                                lon: lon[offset],
                                lat: lat[offset],
                            });
                            offset += 1;
                        }
                        polygon.push(ring);
                    }
                    multi_polygon.push(polygon);
                }
                Ok(Geometry::MultiPolygon(multi_polygon))
            }
            6 => {
                let geometry_count = VInt::deserialize(reader)?.0 as usize;
                let mut geometries = Vec::new();
                for _ in 0..geometry_count {
                    geometries.push(Geometry::deserialize(reader)?);
                }
                Ok(Geometry::GeometryCollection(geometries))
            }
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid geometry type",
            )),
        }
    }
}

fn serialize_line_string<W: Write + ?Sized>(
    line: &[GeoPoint],
    writer: &mut W,
) -> io::Result<()> {
    BinarySerializable::serialize(&VInt(line.len() as u64), writer)?;
    let mut lon = Vec::new();
    let mut lat = Vec::new();
    for point in line {
        lon.push(point.lon);
        lat.push(point.lat);
    }
    let lon = compress_f64(&lon);
    let lat = compress_f64(&lat);
    VInt(lon.len() as u64).serialize(writer)?;
    writer.write_all(&lon)?;
    VInt(lat.len() as u64).serialize(writer)?;
    writer.write_all(&lat)?;
    Ok(())
}

fn serialize_polygon<W: Write + ?Sized>(
    line_string: &[Vec<GeoPoint>],
    writer: &mut W,
) -> io::Result<()> {
    BinarySerializable::serialize(&VInt(line_string.len() as u64), writer)?;
    for ring in line_string {
        BinarySerializable::serialize(&VInt(ring.len() as u64), writer)?;
    }
    let mut lon: Vec<f64> = Vec::new();
    let mut lat: Vec<f64> = Vec::new();
    for ring in line_string {
        for point in ring {
            lon.push(point.lon);
            lat.push(point.lat);
        }
    }
    let lon: Vec<u8> = compress_f64(&lon);
    let lat: Vec<u8> = compress_f64(&lat);
    VInt(lon.len() as u64).serialize(writer)?;
    writer.write_all(&lon)?;
    VInt(lat.len() as u64).serialize(writer)?;
    writer.write_all(&lat)?;
    Ok(())
}

fn deserialize_line_string<R: Read>(reader: &mut R) -> io::Result<Vec<GeoPoint>> {
    let point_count = VInt::deserialize(reader)?.0 as usize;
    let lon_bytes: Vec<u8> = BinarySerializable::deserialize(reader)?;
    let lat_bytes: Vec<u8> = BinarySerializable::deserialize(reader)?;
    let lon: Vec<f64> = decompress_f64(&lon_bytes, point_count);
    let lat: Vec<f64> = decompress_f64(&lat_bytes, point_count);
    let mut line_string: Vec<GeoPoint> = Vec::new();
    for offset in 0..point_count {
        line_string.push(GeoPoint { lon: lon[offset], lat: lat[offset] });
    }
    Ok(line_string)
}

fn deserialize_polygon<R: Read>(reader: &mut R) -> io::Result<Vec<Vec<GeoPoint>>> {
    let ring_count = VInt::deserialize(reader)?.0 as usize;
    let mut rings = Vec::new();
    let mut count = 0;
    for _ in 0..ring_count {
        let point_count = VInt::deserialize(reader)?.0 as usize;
        rings.push(point_count);
        count += point_count;
    }
    let lon_bytes: Vec<u8> = BinarySerializable::deserialize(reader)?;
    let lat_bytes: Vec<u8> = BinarySerializable::deserialize(reader)?;
    let lon: Vec<f64> = decompress_f64(&lon_bytes, count);
    let lat: Vec<f64> = decompress_f64(&lat_bytes, count);
    let mut polygon: Vec<Vec<GeoPoint>> = Vec::new();
    let mut offset = 0;
    for point_count in rings {
        let mut ring = Vec::new();
        for _ in 0..point_count {
            ring.push(GeoPoint { lon: lon[offset], lat: lat[offset] });
            offset += 1;
        }
        polygon.push(ring);
    }
    Ok(polygon)
}

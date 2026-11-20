use crate::spatial::geometry_set::GeometrySet;
use crate::spatial::plane::Plane;
use crate::spatial::surface::Surface;

/// A GeoJSON geometry, parameterized by surface. On the Plane, coordinates are lon/lat in degrees.
/// On the Sphere, coordinates are unit vectors.
#[derive(Debug, Clone, PartialEq)]
pub enum Geometry<S: Surface> {
    /// A single position.
    Point(S::Point),
    /// Multiple positions.
    MultiPoint(Vec<S::Point>),
    /// An ordered sequence of positions forming a line.
    LineString(Vec<S::Point>),
    /// Multiple line strings.
    MultiLineString(Vec<Vec<S::Point>>),
    /// A closed ring (outer) with optional holes (inner rings).
    Polygon(Vec<Vec<S::Point>>),
    /// Multiple polygons.
    MultiPolygon(Vec<Vec<Vec<S::Point>>>),
    /// A heterogeneous collection of geometries.
    GeometryCollection(Vec<Self>),
}

/// Convert a projected geometry into the representation consumed by the spatial index.
pub fn to_geometry_set<S: Surface>(geometry: &Geometry<S>, doc_id: u32) -> GeometrySet<S> {
    let mut set = GeometrySet::new(doc_id);
    append_to(geometry, &mut set);
    set
}

fn append_to<S: Surface>(geometry: &Geometry<S>, set: &mut GeometrySet<S>) {
    match geometry {
        Geometry::Point(point) => set.push_point(*point),
        Geometry::MultiPoint(points) => {
            for point in points {
                set.push_point(*point);
            }
        }
        Geometry::LineString(line) => set.push_line_string(line.clone()),
        Geometry::MultiLineString(lines) => {
            for line in lines {
                set.push_line_string(line.clone());
            }
        }
        Geometry::Polygon(rings) => set.push_polygon(rings),
        Geometry::MultiPolygon(polygons) => {
            for rings in polygons {
                set.push_polygon(rings);
            }
        }
        Geometry::GeometryCollection(geometries) => {
            for geometry in geometries {
                append_to(geometry, set);
            }
        }
    }
}

impl Geometry<Plane> {
    /// Project this geometry onto a surface. For Sphere, converts lon/lat to unit sphere
    /// coordinates. For Plane, returns the geometry unchanged.
    pub fn project<S: Surface>(&self) -> Geometry<S> {
        match self {
            Geometry::Point(p) => Geometry::Point(S::project(p[0], p[1])),
            Geometry::MultiPoint(points) => {
                Geometry::MultiPoint(points.iter().map(|p| S::project(p[0], p[1])).collect())
            }
            Geometry::LineString(line) => Geometry::LineString(Self::project_ring::<S>(line)),
            Geometry::MultiLineString(lines) => Geometry::MultiLineString(
                lines.iter().map(|l| Self::project_ring::<S>(l)).collect(),
            ),
            Geometry::Polygon(rings) => {
                Geometry::Polygon(rings.iter().map(|r| Self::project_ring::<S>(r)).collect())
            }
            Geometry::MultiPolygon(polygons) => Geometry::MultiPolygon(
                polygons
                    .iter()
                    .map(|p| p.iter().map(|r| Self::project_ring::<S>(r)).collect())
                    .collect(),
            ),
            Geometry::GeometryCollection(geometries) => {
                Geometry::GeometryCollection(geometries.iter().map(|g| g.project::<S>()).collect())
            }
        }
    }

    fn project_ring<S: Surface>(ring: &[[f64; 2]]) -> Vec<S::Point> {
        ring.iter().map(|p| S::project(p[0], p[1])).collect()
    }
}

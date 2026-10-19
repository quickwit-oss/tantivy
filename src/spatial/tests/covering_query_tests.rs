use common::BitSet;

use crate::indexer::NoMergePolicy;
use crate::schema::{Schema, PLANE, SPHERE};
use crate::spatial::cell_index_reader::CellIndexReader;
use crate::spatial::clip_options::ClipOptions;
use crate::spatial::clipper::Clipper;
use crate::spatial::contains::Contains;
use crate::spatial::edge_cache::EdgeCache;
use crate::spatial::edge_reader::EdgeReader;
use crate::spatial::geometry::Geometry;
use crate::spatial::geometry_set::to_geometry_set;
use crate::spatial::intersects::Intersects;
use crate::spatial::plane::Plane;
use crate::spatial::region_coverer::CovererOptions;
use crate::spatial::s2cell_id::S2CellId;
use crate::spatial::sphere::Sphere;
use crate::spatial::surface::Surface;
use crate::spatial::within::Within;
use crate::{Index, IndexWriter, TantivyDocument};

fn polygon<S: Surface>(uv: &[[f64; 2]]) -> Geometry<Plane> {
    let ring = uv
        .iter()
        .map(|&[u, v]| {
            if S::DIMENSIONS == 2 {
                [u * 180.0, v * 90.0]
            } else {
                [
                    u.atan().to_degrees(),
                    (v / (1.0 + u * u).sqrt()).atan().to_degrees(),
                ]
            }
        })
        .collect();
    Geometry::Polygon(vec![ring])
}

fn rectangle(bounds: [f64; 4], steps: usize) -> Vec<[f64; 2]> {
    let [x0, y0, x1, y1] = bounds;
    let corners = [[x0, y0], [x1, y0], [x1, y1], [x0, y1]];
    let mut ring = Vec::new();
    for i in 0..4 {
        let a = corners[i];
        let b = corners[(i + 1) % 4];
        for k in 0..steps {
            let t = k as f64 / steps as f64;
            ring.push([a[0] + t * (b[0] - a[0]), a[1] + t * (b[1] - a[1])]);
        }
    }
    ring.push(ring[0]);
    ring
}

fn small_polygon(center: [f64; 2]) -> Vec<[f64; 2]> {
    let mut ring: Vec<_> = (0..11)
        .map(|i| {
            let angle = std::f64::consts::TAU * i as f64 / 11.0;
            [
                center[0] + 0.02 * angle.cos(),
                center[1] + 0.02 * angle.sin(),
            ]
        })
        .collect();
    ring.push(ring[0]);
    ring
}

fn convex_contains(ring: &[[f64; 2]], [u, v]: [f64; 2]) -> Option<bool> {
    let mut positive = false;
    let mut negative = false;
    for edge in ring.windows(2) {
        let [a, b] = [edge[0], edge[1]];
        let side = (b[0] - a[0]) * (v - a[1]) - (b[1] - a[1]) * (u - a[0]);
        if side.abs() < 1e-12 {
            return None;
        }
        positive |= side > 0.0;
        negative |= side < 0.0;
    }
    Some(!(positive && negative))
}

struct Fixture {
    cells: Vec<u8>,
    edges: Vec<u8>,
    max_doc: u32,
}

impl Fixture {
    fn new<S: Surface>(rings: &[Vec<[f64; 2]>]) -> Self {
        let mut schema = Schema::builder();
        let field =
            schema.add_spatial_field("geometry", if S::DIMENSIONS == 2 { PLANE } else { SPHERE });
        let index = Index::create_in_ram(schema.build());
        let mut writer: IndexWriter = index.writer_with_num_threads(1, 50_000_000).unwrap();
        writer.set_merge_policy(Box::new(NoMergePolicy));
        for ring in rings {
            let raw = serde_json::json!({"geometry": polygon::<S>(ring).to_geojson()});
            writer
                .add_document(
                    TantivyDocument::parse_json(&index.schema(), &raw.to_string()).unwrap(),
                )
                .unwrap();
        }
        writer.commit().unwrap();
        writer.wait_merging_threads().unwrap();
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        assert_eq!(searcher.segment_readers().len(), 1);
        let segment = &searcher.segment_readers()[0];
        let spatial = segment.spatial_fields().get_field(field).unwrap().unwrap();
        let fixture = Self {
            cells: spatial.cells_bytes().to_vec(),
            edges: spatial.edges_bytes().to_vec(),
            max_doc: segment.max_doc(),
        };
        let cells = fixture.reader();
        let cache = fixture.cache::<S>();
        // Establish the stored center states independently before checking query descent.
        for pos in 0..cells.cell_count() {
            let cell = cells.cell_at(pos);
            let (u, v) = S::point_to_face_uv(0, &S::cell_center(cell.cell_id));
            for (doc, ring) in rings.iter().enumerate() {
                if let Some(expected) = convex_contains(ring, [u, v]) {
                    let actual = cell.shapes.iter().any(|shape| {
                        cache.doc_id_for(shape.geometry_id) == doc as u32 && shape.contains_center
                    });
                    assert_eq!(
                        actual, expected,
                        "stored center at {:?}, doc {doc}",
                        cell.cell_id
                    );
                }
            }
        }
        for ring in rings {
            let n = ring.len() - 1;
            let center = ring[..n].iter().fold([0.0, 0.0], |a, b| {
                [a[0] + b[0] / n as f64, a[1] + b[1] / n as f64]
            });
            let id = S::cell_id_from_point(&S::face_uv_to_point(0, center[0], center[1]));
            assert!((0..cells.cell_count()).any(|pos| cells.cell_id_at(pos).contains(id)));
        }
        fixture
    }

    fn reader(&self) -> CellIndexReader<'_> {
        CellIndexReader::open(&self.cells)
    }

    fn cache<S: Surface>(&self) -> EdgeCache<'_, S> {
        EdgeCache::new(vec![EdgeReader::<S>::open(&self.edges)], 100_000)
    }
}

fn check_aligned_queries<S: Surface + Clone>() {
    let divider = S::st_to_uv(0.625);
    for right in [false, true] {
        for swap in [false, true] {
            let (x0, x1, center) = if right {
                (divider, 0.8, divider + 0.11)
            } else {
                (-0.3, divider, divider - 0.11)
            };
            let mut query_ring = rectangle([x0, -0.2, x1, 0.8], 1);
            let mut rings = vec![small_polygon([center, 0.31]), small_polygon([-0.6, 0.6])];
            if swap {
                for ring in rings.iter_mut().chain(std::iter::once(&mut query_ring)) {
                    for point in ring {
                        point.swap(0, 1);
                    }
                }
            }
            let fixture = Fixture::new::<S>(&rings);
            let set = to_geometry_set(&polygon::<S>(&query_ring).project::<S>(), 0);
            let seed = Clipper::new(ClipOptions::default()).build(std::slice::from_ref(&set));
            assert_eq!(seed.cells.len(), 1);
            assert_eq!(seed.cells[0].cell_id, S2CellId::from_face(0));
            let cells = fixture.reader();
            assert!((0..cells.cell_count()).all(|pos| cells.cell_id_at(pos).level() > 0));
            let intersects = Intersects::new(set.clone(), CovererOptions::default());
            let within = Within::new(set, CovererOptions::default());
            for exclude in [false, true] {
                let mut filter = BitSet::with_max_value(fixture.max_doc);
                filter.insert(1);
                let filter = if exclude { Some(&filter) } else { None };
                let a =
                    intersects.search(&cells, filter, &mut fixture.cache::<S>(), fixture.max_doc);
                let b = within.search(&cells, filter, &mut fixture.cache::<S>(), fixture.max_doc);
                for hits in [a, b] {
                    let actual: Vec<_> = (0..fixture.max_doc)
                        .filter(|&doc| hits.contains(doc))
                        .collect();
                    assert_eq!(
                        actual,
                        if exclude { vec![] } else { vec![0] },
                        "right {right}, swap {swap}"
                    );
                }
            }
        }
    }
}

#[test]
fn test_plane_aligned_queries() {
    check_aligned_queries::<Plane>();
}

#[test]
fn test_sphere_aligned_queries() {
    check_aligned_queries::<Sphere>();
}

fn check_contains<S: Surface>(x1: f64, steps: usize) {
    let fixture = Fixture::new::<S>(&[
        rectangle([-0.7, -0.7, 0.9, 0.9], steps),
        rectangle([-0.7, -0.7, 0.9, 0.61], steps),
    ]);
    let ring = rectangle([S::st_to_uv(0.625), -0.2, x1, 0.8], 1);
    let set = to_geometry_set(&polygon::<S>(&ring).project::<S>(), 0);
    if steps == 31 {
        // Keep the exclusion witness separate from candidate-vertex contact.
        let cache = fixture.cache::<S>();
        let candidate = cache.locate((0, 1));
        let mut crossings = 0;
        for i in 0..candidate.vertex_count - 1 {
            let (a, b) = candidate.edge(i as u32);
            let (u, _) = S::point_to_face_uv(0, &a);
            assert_ne!(u, S::st_to_uv(0.625));
            assert_ne!(u, x1);
            for edge in set.members[0].vertices.windows(2) {
                use crate::spatial::edge_crosser::EdgeCrosser;
                crossings += usize::from(
                    S::EdgeCrosser::new(&a, &b).crossing_sign_two(&edge[0], &edge[1]) > 0,
                );
            }
        }
        assert_eq!(crossings, 2);
    }
    let seed = Clipper::new(ClipOptions::default()).build(std::slice::from_ref(&set));
    assert_eq!(seed.cells.len(), 1);
    assert_eq!(seed.cells[0].cell_id, S2CellId::from_face(0));
    let query = Contains::new(set, CovererOptions::default());
    let hits = query.search(
        &fixture.reader(),
        None,
        &mut fixture.cache::<S>(),
        fixture.max_doc,
    );
    let actual: Vec<_> = (0..fixture.max_doc)
        .filter(|&doc| hits.contains(doc))
        .collect();
    assert_eq!(actual, vec![0]);
}

#[test]
fn test_plane_contains_excludes_crossing_at_dividers() {
    check_contains::<Plane>(0.5, 31);
}

#[test]
#[ignore = "Contains misses exclusion crossings through candidate vertices"]
fn test_plane_contains_crosses_candidate_vertices() {
    check_contains::<Plane>(0.5, 32);
}

#[test]
fn test_sphere_contains_after_subdivision() {
    check_contains::<Sphere>(0.8, 32);
}

#[test]
fn test_sphere_contains_narrow_query() {
    check_contains::<Sphere>(Sphere::st_to_uv(0.75), 32);
}

fn check_coarse_contains<S: Surface>() {
    let fixture = Fixture::new::<S>(&[rectangle([-0.9, -0.9, 0.9, 0.9], 1)]);
    let cells = fixture.reader();
    assert_eq!(cells.cell_count(), 1);
    assert_eq!(cells.cell_id_at(0), S2CellId::from_face(0));
    for x_sign in [-1.0, 1.0] {
        for y_sign in [-1.0, 1.0] {
            let mut ring = rectangle([0.2, 0.2, 0.6, 0.6], 1);
            for p in &mut ring {
                p[0] *= x_sign;
                p[1] *= y_sign;
            }
            let set = to_geometry_set(&polygon::<S>(&ring).project::<S>(), 0);
            let query = Contains::new(set, CovererOptions::default());
            for exclude in [false, true] {
                let filter = BitSet::with_max_value(fixture.max_doc);
                let filter = if exclude { Some(&filter) } else { None };
                let hits = query.search(&cells, filter, &mut fixture.cache::<S>(), fixture.max_doc);
                assert_eq!(
                    hits.contains(0),
                    !exclude,
                    "query quadrant ({x_sign}, {y_sign}), exclude {exclude}"
                );
            }
        }
    }
}

#[test]
fn test_plane_contains_with_coarse_index_cell() {
    check_coarse_contains::<Plane>();
}

#[test]
fn test_sphere_contains_with_coarse_index_cell() {
    check_coarse_contains::<Sphere>();
}

use super::*;
use crate::merge_policy::NoMergePolicy;
use crate::schema::{Schema, SPHERE};
use crate::spatial::crossings::S2EdgeCrosser;
use crate::spatial::edge_reader::EdgeReader;
use crate::spatial::geometry::Geometry;
use crate::spatial::geometry_set::{to_geometry_set, EdgeSet};
use crate::spatial::intersects::Intersects;
use crate::spatial::plane::Plane;
use crate::spatial::sphere::Sphere;
use crate::{Index, IndexWriter, TantivyDocument};

const EARTH_RADIUS: f64 = 6_371_000.0;

struct Fixture {
    cells: Vec<u8>,
    edges: Vec<u8>,
    doc_ids: Vec<u8>,
    max_doc: u32,
}

impl Fixture {
    fn new(geometries: &[Geometry<Plane>]) -> Self {
        Self::build(geometries, false)
    }

    fn build(geometries: &[Geometry<Plane>], merge: bool) -> Self {
        let mut builder = Schema::builder();
        let field = builder.add_spatial_field("geometry", SPHERE);
        let schema = builder.build();
        let index = Index::create_in_ram(schema.clone());
        let mut writer: IndexWriter = index.writer_with_num_threads(1, 50_000_000).unwrap();
        writer.set_merge_policy(Box::new(NoMergePolicy));
        for (i, geometry) in geometries.iter().enumerate() {
            if merge && i == geometries.len() / 2 {
                writer.commit().unwrap();
            }
            let json = serde_json::json!({ "geometry": geometry.to_geojson() }).to_string();
            writer
                .add_document(TantivyDocument::parse_json(&schema, &json).unwrap())
                .unwrap();
        }
        writer.commit().unwrap();
        if merge {
            let segments = index.searchable_segment_ids().unwrap();
            assert_eq!(segments.len(), 2);
            writer.merge(&segments).wait().unwrap();
        }
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        assert_eq!(searcher.segment_readers().len(), 1);
        let segment = &searcher.segment_readers()[0];
        let spatial = segment.spatial_fields().get_field(field).unwrap().unwrap();
        Self {
            cells: spatial.cells_bytes().to_vec(),
            edges: spatial.edges_bytes().to_vec(),
            doc_ids: spatial.doc_ids_bytes().to_vec(),
            max_doc: segment.max_doc(),
        }
    }

    fn reference_matches(&self, geometry: &Geometry<Plane>, meters: f64) -> Vec<u32> {
        let set = to_geometry_set(&geometry.project::<Sphere>(), 0);
        // Read complete geometries without consulting the cell index. This checks traversal
        // and document IDs. The distance calculation itself is shared with the query.
        let reader = EdgeReader::<Sphere>::open(&self.edges);
        let mut actual = Vec::new();
        let mut pos = 0;
        while pos < reader.geometry_count() {
            let (head, candidate) = reader.read_geometry_set(pos);
            if full_distance(&set, &candidate) <= S1ChordAngle::from_radians(meters / EARTH_RADIUS)
            {
                actual.push(candidate.doc_id);
            }
            pos = head + candidate.members.len() as u32;
        }
        actual.sort_unstable();
        actual.dedup();
        actual
    }

    fn check_reference(&self, geometry: &Geometry<Plane>, meters: f64) {
        self.check(geometry, meters, &self.reference_matches(geometry, meters));
    }

    fn check(&self, geometry: &Geometry<Plane>, meters: f64, expected: &[u32]) {
        let set = to_geometry_set(&geometry.project::<Sphere>(), 0);
        if meters > 0.0 {
            assert_eq!(
                self.reference_matches(geometry, meters),
                expected,
                "reference fixture at {meters}m"
            );
        }
        let mut query = Distance::new(set, meters / EARTH_RADIUS, CovererOptions::default());
        let reader = CellIndexReader::open_with_doc_ids(&self.cells, &self.doc_ids);
        let mut cache = EdgeCache::new(vec![EdgeReader::<Sphere>::open(&self.edges)], 100_000);
        let mut filter = BitSet::with_max_value(self.max_doc);
        for doc in 0..self.max_doc {
            filter.insert(doc);
        }
        for first_only in [false, true] {
            query.first_only = first_only;
            for terms_filter in [None, Some(&filter)] {
                let hits = query.search(&reader, terms_filter, &mut cache, self.max_doc);
                assert_eq!(
                    hits.len(),
                    if first_only {
                        expected.len().min(1)
                    } else {
                        expected.len()
                    },
                    "distance {meters}, first_only {first_only}"
                );
                for doc in 0..self.max_doc {
                    if hits.contains(doc) {
                        assert!(expected.contains(&doc), "unexpected doc {doc}");
                    }
                }
            }
        }
        for &doc in expected {
            filter.remove(doc);
        }
        assert_eq!(
            query
                .search(&reader, Some(&filter), &mut cache, self.max_doc)
                .len(),
            0
        );
    }
}

fn member_edges(member: &EdgeSet<Sphere>) -> Vec<([f64; 3], [f64; 3])> {
    let mut edges = Vec::new();
    for ring in member.ring_offsets.windows(2) {
        let vertices = &member.vertices[ring[0]..ring[1]];
        if vertices.len() == 1 {
            edges.push((vertices[0], vertices[0]));
        }
        edges.extend(vertices.windows(2).map(|v| (v[0], v[1])));
    }
    edges
}

fn inside(point: &[f64; 3], member: &EdgeSet<Sphere>) -> bool {
    if !member.closed {
        return false;
    }
    let mut inside = member.contains_hilbert_start;
    let mut crosser = S2EdgeCrosser::new(&Sphere::hilbert_start(), point);
    for (a, b) in member_edges(member) {
        inside ^= crosser.edge_or_vertex_crossing_two(&a, &b);
    }
    inside
}

fn full_distance(a: &GeometrySet<Sphere>, b: &GeometrySet<Sphere>) -> S1ChordAngle {
    let mut distance = S1ChordAngle::infinity();
    for am in &a.members {
        for bm in &b.members {
            if am.vertices.iter().any(|v| inside(v, bm))
                || bm.vertices.iter().any(|v| inside(v, am))
            {
                return S1ChordAngle::zero();
            }
            for (a0, a1) in member_edges(am) {
                for (b0, b1) in member_edges(bm) {
                    update_edge_pair_min_distance(&a0, &a1, &b0, &b1, &mut distance);
                }
            }
        }
    }
    distance
}

fn square(x: f64, y: f64, radius: f64) -> Geometry<Plane> {
    Geometry::Polygon(vec![vec![
        [x - radius, y - radius],
        [x + radius, y - radius],
        [x + radius, y + radius],
        [x - radius, y + radius],
        [x - radius, y - radius],
    ]])
}

fn check_query_inside_candidate(query: &Geometry<Plane>, contained_member: usize) {
    // The candidate straddles the equator, keeping its edges in the face cell.
    // The cell center at (0, 0) lies outside both indexed polygons.
    let fixture = Fixture::new(&[square(10.0, 0.001, 0.01), square(30.0, -20.0, 0.01)]);
    let set = to_geometry_set(&query.project::<Sphere>(), 0);
    let vertex = &set.members[contained_member].vertices[0];
    let reader = CellIndexReader::open_with_doc_ids(&fixture.cells, &fixture.doc_ids);
    let mut cache = EdgeCache::new(vec![EdgeReader::<Sphere>::open(&fixture.edges)], 100_000);
    let cell = reader.find(Sphere::cell_id_from_point(vertex)).unwrap();
    let shape = cell.find_shape((0, 0)).unwrap();
    let candidate = cache.locate(shape.geometry_id);
    assert_eq!(candidate.doc_id, 0);
    assert!(candidate.closed);
    assert!(!shape.contains_center);
    assert!(!shape.edge_indices.is_empty());

    let center = Sphere::cell_center(cell.cell_id);
    let mut crosser = S2EdgeCrosser::new(&center, vertex);
    let crossings = shape
        .edge_indices
        .iter()
        .filter(|&&edge| {
            let (a, b) = candidate.edge(edge);
            crosser.edge_or_vertex_crossing_two(&a, &b)
        })
        .count();
    assert_eq!(crossings, 1, "the query vertex must be inside by parity");

    // Neither edge crossings nor an indexed vertex inside the query can supply the hit.
    for gid in 0..2 {
        let indexed = cache.get((0, gid));
        let indexed = indexed.edge_set();
        assert!(!inside(&center, indexed));
        for (member_id, member) in set.members.iter().enumerate() {
            for vertex in &member.vertices {
                assert_eq!(
                    inside(vertex, indexed),
                    gid == 0 && member_id == contained_member,
                    "query member {member_id}, indexed geometry {gid}"
                );
            }
            assert!(indexed
                .vertices
                .iter()
                .all(|vertex| !inside(vertex, member)));
            for (a, b) in member_edges(indexed) {
                let mut crosser = S2EdgeCrosser::new(&a, &b);
                for (c, d) in member_edges(member) {
                    assert_eq!(crosser.crossing_sign_two(&c, &d), -1);
                }
            }
        }
    }

    let intersects = Intersects::new(set, CovererOptions::default());
    let hits = intersects.search(&reader, None, &mut cache, fixture.max_doc);
    assert_eq!(hits.len(), 1);
    assert!(hits.contains(0));
    fixture.check(query, 0.0, &[0]);
    fixture.check(query, 1.0, &[0]);
}

#[test]
fn test_query_inside_candidate_with_boundary_edges() {
    check_query_inside_candidate(&square(10.003, 0.004, 0.002), 0);
}

#[test]
fn test_later_query_member_inside_candidate_with_boundary_edges() {
    let Geometry::Polygon(outside) = square(-20.0, 25.0, 0.002) else {
        unreachable!()
    };
    let Geometry::Polygon(inside) = square(10.003, 0.004, 0.002) else {
        unreachable!()
    };
    // Member 0 is disjoint; only member 1 exercises the pre-scan.
    check_query_inside_candidate(&Geometry::MultiPolygon(vec![outside, inside]), 1);
}

#[test]
fn test_boundary_cell_points_and_holes() {
    let fixture = Fixture::new(&[
        Geometry::Point([0.0101, 0.0]),
        Geometry::Point([0.0099, 0.0]),
        Geometry::Point([0.0, 0.0]),
    ]);
    fixture.check(&square(0.0, 0.0, 0.01), 60.96, &[0, 1, 2]);
    let Geometry::Polygon(mut rings) = square(0.0, 0.0, 0.1) else {
        unreachable!()
    };
    let Geometry::Polygon(hole) = square(0.0, 0.0, 0.01) else {
        unreachable!()
    };
    rings.push(hole[0].iter().rev().copied().collect());
    fixture.check(&Geometry::Polygon(rings), 60.96, &[0, 1]);
}

#[test]
fn test_point_queries_and_zero_distance_convention() {
    let fixture = Fixture::new(&[Geometry::LineString(vec![[-0.01, 0.0], [0.01, 0.0]])]);
    fixture.check(&Geometry::Point([0.0, 0.0001]), 60.96, &[0]);
    fixture.check(&Geometry::Point([0.0, 0.0001]), 0.0, &[]);
    fixture.check(
        &Geometry::LineString(vec![[0.01, 0.0], [0.02, 0.01]]),
        0.0,
        &[],
    );
    fixture.check(
        &Geometry::LineString(vec![[0.0, -0.01], [0.0, 0.01]]),
        0.0,
        &[0],
    );
}

#[test]
fn test_multipart_members_keep_edge_identity_and_union_interiors() {
    let fixture = Fixture::new(&[
        Geometry::LineString(vec![[-10.0, 0.0], [10.0, 2.0]]),
        Geometry::Point([1.0, 1.0]),
        Geometry::Point([-1.5, -1.5]),
    ]);
    let query = Geometry::MultiPolygon(vec![
        vec![vec![[-2.0, -2.0], [-1.0, -2.0], [-1.0, -1.0], [-2.0, -2.0]]],
        vec![vec![
            [0.5, 0.5],
            [1.5, 0.5],
            [1.5, 1.5],
            [0.5, 1.5],
            [0.5, 0.5],
        ]],
    ]);
    fixture.check(&query, 2000.0, &[0, 1, 2]);
    let fixture = Fixture::new(&[Geometry::Point([0.0, 0.0])]);
    fixture.check(
        &Geometry::GeometryCollection(vec![square(-0.01, 0.0, 0.03), square(0.01, 0.0, 0.03)]),
        61.0,
        &[0],
    );
    fixture.check(
        &Geometry::MultiPoint(vec![[10.0, 10.0], [0.0, 0.0001]]),
        61.0,
        &[0],
    );
}

#[test]
fn test_ancestor_cell_does_not_imply_a_match() {
    let lon = -100.123456;
    let lat = 29.123456;
    let fixture = Fixture::new(&[Geometry::LineString(vec![
        [lon, lat],
        [lon, lat + (20_000.0 / EARTH_RADIUS).to_degrees()],
    ])]);
    let reader = CellIndexReader::open(&fixture.cells);
    assert_eq!(reader.cell_count(), 1);
    assert_eq!(reader.cell_id_at(0).level(), 8);
    fixture.check(
        &square(-100.096648471341865, 29.123725796481775, 15.0 / 111_195.0),
        61.0,
        &[],
    );
    fixture.check(
        &square(-99.939694714843384, 29.127287110041216, 15.0 / 111_195.0),
        2000.0,
        &[],
    );
    fixture.check(&square(lon + 0.0003, lat + 0.001, 0.0001), 61.0, &[0]);
}

#[test]
fn test_query_boundary_cover_is_uniform_and_preserves_sparse_edges() {
    let query = Distance::new(
        to_geometry_set(&square(44.99, 0.0, 0.02).project::<Sphere>(), 0),
        61.0 / EARTH_RADIUS,
        CovererOptions::default(),
    );
    let boundary = query.boundary.as_ref().unwrap();
    assert!(boundary.cells.iter().all(|id| id.level() == boundary.level));
    let ids: HashSet<_> = boundary.cells.iter().copied().collect();
    for &(member, edge) in &query.edges {
        let (a, b) = query.query_edges.get_edge((0, member), edge);
        for i in 0..=100 {
            let t = i as f64 / 100.0;
            let p = std::array::from_fn::<_, 3, _>(|axis| a[axis] * (1.0 - t) + b[axis] * t);
            let norm = p.iter().map(|v| v * v).sum::<f64>().sqrt();
            let p = p.map(|v| v / norm);
            assert!(ids.contains(&Sphere::cell_id_from_point(&p).parent(boundary.level)));
        }
    }
    let fixture = Fixture::new(&[Geometry::Point([45.0101, 0.0])]);
    fixture.check(&square(44.99, 0.0, 0.02), 61.0, &[0]);
}

#[test]
fn test_whole_sphere_radius_and_empty_query() {
    let fixture = Fixture::new(&[Geometry::Point([0.0, 0.0]), Geometry::Point([180.0, 0.0])]);
    fixture.check(&Geometry::Point([0.0, 0.0]), 8_000_000.0, &[0]);
    fixture.check(&Geometry::Point([0.0, 0.0]), f64::INFINITY, &[0, 1]);
    fixture.check(&Geometry::GeometryCollection(vec![]), 61.0, &[]);
}

#[test]
fn test_merged_mixed_levels_preserve_interior_and_sparse_boundary_hits() {
    let mut geometries = vec![
        Geometry::Point([0.0101, 0.0]),
        Geometry::Point([-0.0101, 0.0]),
        Geometry::Point([0.0, 0.0101]),
        Geometry::Point([0.0, -0.0101]),
        square(0.0, 0.0, 0.001),
        square(0.0, 0.0, 0.1),
        Geometry::LineString(vec![[-0.2, 0.1], [0.2, 0.12]]),
    ];
    for i in 0..80 {
        geometries.push(Geometry::Point([0.009 + i as f64 * 0.0000001, 0.009]));
    }
    let fixture = Fixture::build(&geometries, true);
    assert!(
        !fixture.doc_ids.is_empty(),
        "expected a doc-id sidecar after merging"
    );
    let cells = CellIndexReader::open(&fixture.cells);
    let levels: HashSet<_> = cells.iter().map(|cell| cell.cell_id.level()).collect();
    assert!(levels.len() > 1, "expected cells at different levels");
    fixture.check_reference(&square(0.0, 0.0, 0.01), 61.0);
    fixture.check_reference(&Geometry::Point([0.0, 0.0]), 61.0);
}

#[test]
fn test_flood_crosses_faces_dateline_and_poles() {
    let wrap = |lon: f64| (lon + 180.0).rem_euclid(360.0) - 180.0;
    for [lon, lat] in [
        [45.0, 0.0],
        [135.0, 0.0],
        [179.9999, 0.0],
        [45.0, 35.264389682754654],
        [0.0, 89.98],
        [-45.0, -35.264389682754654],
    ] {
        let query = Geometry::LineString(vec![[wrap(lon - 0.03), lat], [wrap(lon + 0.03), lat]]);
        let geometries: Vec<_> = (-4..=4)
            .flat_map(|i| {
                (-4..=4).map(move |j| {
                    Geometry::Point([wrap(lon + i as f64 * 0.01), lat + j as f64 * 0.004])
                })
            })
            .collect();
        Fixture::new(&geometries).check_reference(&query, 1000.0);
    }
}

#[test]
fn test_distance_rim_and_leaf_scale_queries() {
    for meters in [0.000001, 61.0, 2000.0] {
        let degrees = (meters / EARTH_RADIUS).to_degrees();
        let fixture = Fixture::new(&[
            Geometry::Point([degrees * 0.99999999, 0.0]),
            Geometry::Point([degrees * 1.00000001, 0.0]),
            Geometry::Point([-degrees * 0.99999999, 0.0]),
        ]);
        fixture.check(&Geometry::Point([0.0, 0.0]), meters, &[0, 2]);
    }
}

#[test]
fn test_computed_distance_threshold_is_inclusive() {
    for meters in [61.0, 2000.0] {
        let candidate = Geometry::Point([(meters / EARTH_RADIUS).to_degrees(), 0.0]);
        let fixture = Fixture::new(&[candidate.clone()]);
        let set = to_geometry_set(&Geometry::<Plane>::Point([0.0, 0.0]).project::<Sphere>(), 0);
        let distance = full_distance(&set, &to_geometry_set(&candidate.project::<Sphere>(), 0));
        let mut query = Distance::new(set, meters / EARTH_RADIUS, CovererOptions::default());
        let reader = CellIndexReader::open(&fixture.cells);
        let mut cache = EdgeCache::new(vec![EdgeReader::<Sphere>::open(&fixture.edges)], 100_000);
        // Set the chord threshold directly so conversion to radians and back cannot change it.
        // The boundary search still includes its rounding allowance around this distance.
        for (limit, expected) in [
            (
                S1ChordAngle::from_length2(distance.length2().next_down()),
                0,
            ),
            (distance, 1),
            (S1ChordAngle::from_length2(distance.length2().next_up()), 1),
        ] {
            query.max_distance = Some(limit);
            for first_only in [false, true] {
                query.first_only = first_only;
                assert_eq!(
                    query
                        .search(&reader, None, &mut cache, fixture.max_doc)
                        .len(),
                    expected
                );
            }
        }
    }
}

#[test]
fn test_long_fiber_parcel_grid_matches_full_geometry() {
    let lon = -100.123456;
    let lat = 29.123456;
    let fixture = Fixture::new(&[Geometry::LineString(vec![
        [lon, lat],
        [lon, lat + (20_000.0 / EARTH_RADIUS).to_degrees()],
    ])]);
    // Test nearby parcels and distant parcels against a line stored in one large index cell.
    for row in 0..10 {
        for column in 0..10 {
            let query = square(
                lon + column as f64 * 0.001,
                lat + row as f64 * 0.01,
                0.00015,
            );
            fixture.check_reference(&query, 61.0);
            fixture.check_reference(&query, 2000.0);
        }
    }
}

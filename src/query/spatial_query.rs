//! Spatial polygon query.
//!
//! Finds indexed geometries that match a query polygon via contains or intersects predicates. The
//! query polygon is specified as lon/lat vertices, converted to unit sphere coordinates, and
//! searched against each segment's cell index and edge index.

use common::BitSet;

use crate::query::explanation::does_not_match;
use crate::query::{BitSetDocSet, Explanation, Query, Scorer, Weight};
use crate::schema::Field;
use crate::spatial::cell_index_reader::CellIndexReader;
use crate::spatial::closest_edge_query::ClosestEdgeQuery;
use crate::spatial::contains_query::ContainsQuery;
use crate::spatial::edge_cache::EdgeCache;
use crate::spatial::edge_reader::EdgeReader;
use crate::spatial::executor::{PlanNode, SpatialExecutor};
use crate::spatial::geometry::Geometry;
use crate::spatial::geometry_set::to_geometry_set;
use crate::spatial::intersects_query::IntersectsQuery;
use crate::spatial::plane::Plane;
use crate::spatial::region_coverer::CovererOptions;
use crate::spatial::s1chord_angle::S1ChordAngle;
use crate::spatial::sphere::Sphere;
use crate::{DocId, DocSet, Score, TERMINATED};

/// The spatial predicate to apply.
#[derive(Clone, Debug)]
pub enum SpatialPredicate {
    /// Return geometries contained by the query polygon.
    Contains,
    /// Return geometries that intersect the query polygon.
    Intersects,
    /// Return geometries within a distance of a point. Distance in radians.
    Within(f64),
    /// Return geometries between two distances of a point. Distances in radians.
    Between(f64, f64),
    /// Return the K nearest geometries to a point, across all segments. A hit's score is the
    /// negated squared chord length of its distance, so the nearest scores highest.
    Knn(usize),
}

/// Spatial query.
#[derive(Clone, Debug)]
pub struct SpatialQuery {
    field: Field,
    predicate: SpatialPredicate,
    /// Polygon vertices (lon/lat) for Contains and Intersects, or a single point for distance.
    coordinates: Vec<[f64; 2]>,
}

impl SpatialQuery {
    /// Create a contains query from a polygon specified as lon/lat vertices.
    pub fn new(field: Field, polygon: Vec<[f64; 2]>) -> Self {
        SpatialQuery {
            field,
            coordinates: polygon,
            predicate: SpatialPredicate::Contains,
        }
    }

    /// Create a spatial query with the given predicate.
    pub fn with_predicate(
        field: Field,
        coordinates: Vec<[f64; 2]>,
        predicate: SpatialPredicate,
    ) -> Self {
        SpatialQuery {
            field,
            coordinates,
            predicate,
        }
    }

    /// Create a spatial query from a bounding box. The box is converted to a 4-vertex polygon.
    pub fn from_bounds(field: Field, bounds: [[f64; 2]; 2]) -> Self {
        let [lo, hi] = bounds;
        let polygon = vec![
            [lo[0], lo[1]],
            [hi[0], lo[1]],
            [hi[0], hi[1]],
            [lo[0], hi[1]],
        ];
        SpatialQuery {
            field,
            coordinates: polygon,
            predicate: SpatialPredicate::Contains,
        }
    }

    /// Create an intersects query from a bounding box.
    pub fn intersects_bounds(field: Field, bounds: [[f64; 2]; 2]) -> Self {
        let [lo, hi] = bounds;
        let polygon = vec![
            [lo[0], lo[1]],
            [hi[0], lo[1]],
            [hi[0], hi[1]],
            [lo[0], hi[1]],
        ];
        SpatialQuery {
            field,
            coordinates: polygon,
            predicate: SpatialPredicate::Intersects,
        }
    }
}

impl Query for SpatialQuery {
    fn weight(
        &self,
        enable_scoring: super::EnableScoring<'_>,
    ) -> crate::Result<Box<dyn super::Weight>> {
        let prepared: Box<dyn PreparedSpatialQuery> = match &self.predicate {
            SpatialPredicate::Contains | SpatialPredicate::Intersects => {
                let mut ring: Vec<[f64; 2]> = self.coordinates.clone();
                if ring.first() != ring.last() {
                    ring.push(ring[0]);
                }
                let plane_geometry = Geometry::<Plane>::Polygon(vec![ring]);
                let projected = plane_geometry.project::<Sphere>();
                let set = to_geometry_set(&projected, 0);
                match &self.predicate {
                    SpatialPredicate::Contains => {
                        Box::new(ContainsQuery::new(set, CovererOptions::default()))
                    }
                    SpatialPredicate::Intersects => {
                        Box::new(IntersectsQuery::new(set, CovererOptions::default()))
                    }
                    _ => unreachable!(),
                }
            }
            SpatialPredicate::Within(radius_radians) => {
                let query_geometry = Geometry::<Plane>::Point(self.coordinates[0]);
                let projected = query_geometry.project::<Sphere>();
                let set = to_geometry_set(&projected, 0);
                let radius = S1ChordAngle::from_radians(*radius_radians);
                Box::new(ClosestEdgeQuery::within(set, radius))
            }
            SpatialPredicate::Between(inner_radians, outer_radians) => {
                let query_geometry = Geometry::<Plane>::Point(self.coordinates[0]);
                let projected = query_geometry.project::<Sphere>();
                let set = to_geometry_set(&projected, 0);
                let inner = S1ChordAngle::from_radians(*inner_radians);
                let outer = S1ChordAngle::from_radians(*outer_radians);
                Box::new(ClosestEdgeQuery::between(set, inner, outer))
            }
            SpatialPredicate::Knn(k) => {
                // The nearest K are ranked across segments, so the executor runs the query on
                // every segment, keeps the global K, and scores each by its distance.
                let query_geometry = Geometry::<Plane>::Point(self.coordinates[0]);
                let projected = query_geometry.project::<Sphere>();
                let knn = PlanNode::Knn {
                    field: self.field,
                    geometry: to_geometry_set(&projected, 0),
                    k: *k,
                    filter: None,
                };
                return SpatialExecutor::new(knn).weight(enable_scoring);
            }
        };
        Ok(Box::new(SpatialWeight {
            field: self.field,
            query: prepared,
        }))
    }
}

/// Shared interface for prepared spatial queries.
trait PreparedSpatialQuery: Send + Sync {
    fn search_segment<'a>(
        &self,
        cell_reader: &'a CellIndexReader<'a>,
        edge_cache: &mut EdgeCache<'a, Sphere>,
    ) -> Vec<u32>;
}

impl PreparedSpatialQuery for ContainsQuery {
    fn search_segment<'a>(
        &self,
        cell_reader: &'a CellIndexReader<'a>,
        edge_cache: &mut EdgeCache<'a, Sphere>,
    ) -> Vec<u32> {
        ContainsQuery::search_segment(self, cell_reader, edge_cache)
    }
}

impl PreparedSpatialQuery for IntersectsQuery {
    fn search_segment<'a>(
        &self,
        cell_reader: &'a CellIndexReader<'a>,
        edge_cache: &mut EdgeCache<'a, Sphere>,
    ) -> Vec<u32> {
        IntersectsQuery::search_segment(self, cell_reader, edge_cache)
    }
}

impl PreparedSpatialQuery for ClosestEdgeQuery {
    fn search_segment<'a>(
        &self,
        cell_reader: &'a CellIndexReader<'a>,
        edge_cache: &mut EdgeCache<'a, Sphere>,
    ) -> Vec<u32> {
        ClosestEdgeQuery::search_segment(self, cell_reader, edge_cache)
            .into_iter()
            .map(|r| r.doc_id)
            .collect()
    }
}

struct SpatialWeight {
    field: Field,
    query: Box<dyn PreparedSpatialQuery>,
}

impl Weight for SpatialWeight {
    fn scorer(
        &self,
        reader: &crate::SegmentReader,
        boost: crate::Score,
    ) -> crate::Result<Box<dyn super::Scorer>> {
        let spatial_reader = match reader.spatial_fields().get_field(self.field)? {
            Some(reader) => reader,
            None => {
                let empty_bitset = BitSet::with_max_value(reader.max_doc());
                return Ok(Box::new(SpatialScorer::new(boost, empty_bitset)));
            }
        };

        let cell_reader = CellIndexReader::open(spatial_reader.cells_bytes())?;
        let edge_reader = EdgeReader::<Sphere>::open(spatial_reader.edges_bytes())?;
        let mut edge_cache = EdgeCache::new(vec![edge_reader], 100_000);

        let doc_ids = self.query.search_segment(&cell_reader, &mut edge_cache);

        let mut include = BitSet::with_max_value(reader.max_doc());
        for doc_id in doc_ids {
            include.insert(doc_id);
        }

        Ok(Box::new(SpatialScorer::new(boost, include)))
    }

    fn explain(
        &self,
        reader: &crate::SegmentReader,
        doc: DocId,
    ) -> crate::Result<super::Explanation> {
        let mut scorer = self.scorer(reader, 1.0)?;
        if scorer.seek(doc) != doc {
            return Err(does_not_match(doc));
        }
        let score = scorer.score();
        let explanation = Explanation::new("SpatialQuery", score);
        Ok(explanation)
    }
}

struct SpatialScorer {
    include: BitSetDocSet,
    doc_id: DocId,
    score: Score,
}

impl SpatialScorer {
    pub fn new(score: Score, include: BitSet) -> Self {
        let mut scorer = SpatialScorer {
            include: BitSetDocSet::from(include),
            doc_id: 0,
            score,
        };
        scorer.doc_id = scorer.include.doc();
        scorer
    }
}

impl Scorer for SpatialScorer {
    fn score(&mut self) -> Score {
        self.score
    }
}

impl DocSet for SpatialScorer {
    fn advance(&mut self) -> DocId {
        if self.doc_id == TERMINATED {
            return TERMINATED;
        }
        self.doc_id = self.include.advance();
        self.doc_id
    }

    fn size_hint(&self) -> u32 {
        self.include.size_hint()
    }

    fn doc(&self) -> DocId {
        self.doc_id
    }
}

#[cfg(test)]
mod tests {
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};

    use super::*;
    use crate::collector::{DocSetCollector, TopDocs};
    use crate::query::{AllQuery, TermQuery};
    use crate::schema::{IndexRecordOption, Schema, Value, SPATIAL, STORED, STRING};
    use crate::spatial::containment::{brute_force_contains, compute_origin_inside};
    use crate::spatial::executor::SpatialRelation;
    use crate::spatial::geometry_set::{EdgeSet, GeometrySet};
    use crate::spatial::s2edge_distances::{update_edge_pair_min_distance, update_min_distance};
    use crate::{DocAddress, Index, IndexWriter, Searcher, TantivyDocument, Term};

    /// Indexes each inner slice of `(name, GeoJSON geometry)` pairs as one segment.
    fn index_segments(segments: &[&[(&str, &str)]]) -> crate::Result<(Index, Field, Field)> {
        let mut schema_builder = Schema::builder();
        let name = schema_builder.add_text_field("name", STRING | STORED);
        let geometry = schema_builder.add_spatial_field("geometry", SPATIAL);
        let index = Index::create_in_ram(schema_builder.build());
        let mut writer: IndexWriter = index.writer_for_tests()?;
        writer.set_merge_policy(Box::new(crate::indexer::NoMergePolicy));
        for docs in segments {
            for (doc_name, doc_geometry) in *docs {
                let json = format!(r#"{{"name": "{doc_name}", "geometry": {doc_geometry}}}"#);
                writer.add_document(TantivyDocument::parse_json(&index.schema(), &json)?)?;
            }
            writer.commit()?;
        }
        Ok((index, name, geometry))
    }

    fn name_of(searcher: &Searcher, name: Field, doc_address: DocAddress) -> crate::Result<String> {
        let doc: TantivyDocument = searcher.doc(doc_address)?;
        Ok(doc
            .get_first(name)
            .and_then(|v| v.as_str())
            .unwrap()
            .to_string())
    }

    /// Names of the documents the query matches, sorted.
    fn matching(index: &Index, name: Field, query: &dyn Query) -> crate::Result<Vec<String>> {
        let searcher = index.reader()?.searcher();
        let mut names = searcher
            .search(query, &DocSetCollector)?
            .into_iter()
            .map(|doc_address| name_of(&searcher, name, doc_address))
            .collect::<crate::Result<Vec<_>>>()?;
        names.sort();
        Ok(names)
    }

    fn name_query(name: Field, value: &str) -> Box<dyn Query> {
        Box::new(TermQuery::new(
            Term::from_field_text(name, value),
            IndexRecordOption::Basic,
        ))
    }

    const BOX: &str =
        r#"{"type": "Polygon", "coordinates": [[[0, 0], [10, 0], [10, 10], [0, 10], [0, 0]]]}"#;

    #[test]
    fn test_within_matches_a_polygon_containing_the_point() -> crate::Result<()> {
        let (index, name, geometry) = index_segments(&[&[("box", BOX)]])?;
        let query = SpatialQuery::with_predicate(
            geometry,
            vec![[5.0, 5.0]],
            SpatialPredicate::Within(1e-6),
        );
        assert_eq!(matching(&index, name, &query)?, ["box"]);
        Ok(())
    }

    #[test]
    fn test_within_matches_a_line_near_the_point() -> crate::Result<()> {
        let line = r#"{"type": "LineString", "coordinates": [[0, 0], [10, 0]]}"#;
        let (index, name, geometry) = index_segments(&[&[("equator", line)]])?;
        let near = SpatialQuery::with_predicate(
            geometry,
            vec![[5.0, 0.001]],
            SpatialPredicate::Within(0.01f64.to_radians()),
        );
        assert_eq!(matching(&index, name, &near)?, ["equator"]);
        let far = SpatialQuery::with_predicate(
            geometry,
            vec![[5.0, 1.0]],
            SpatialPredicate::Within(0.01f64.to_radians()),
        );
        assert!(matching(&index, name, &far)?.is_empty());
        Ok(())
    }

    #[test]
    fn test_knn_returns_the_nearest_across_segments() -> crate::Result<()> {
        let point = |lat: f64| format!(r#"{{"type": "Point", "coordinates": [0, {lat}]}}"#);
        let (a, b, c, d) = (point(1.0), point(2.0), point(3.0), point(4.0));
        let (index, name, geometry) =
            index_segments(&[&[("a", &a), ("c", &c)], &[("b", &b), ("d", &d)]])?;
        let searcher = index.reader()?.searcher();
        let knn =
            SpatialQuery::with_predicate(geometry, vec![[0.0, 0.0]], SpatialPredicate::Knn(3));
        let hits = searcher.search(&knn, &TopDocs::with_limit(10).order_by_score())?;
        let names = hits
            .iter()
            .map(|&(_, doc_address)| name_of(&searcher, name, doc_address))
            .collect::<crate::Result<Vec<_>>>()?;
        assert_eq!(names, ["a", "b", "c"]);

        // Explaining a hit gives its score; explaining the fourth nearest fails.
        let (score, nearest) = hits[0];
        assert_eq!(knn.explain(&searcher, nearest)?.value(), score);
        let d = searcher.search(&name_query(name, "d"), &DocSetCollector)?;
        assert!(knn.explain(&searcher, *d.iter().next().unwrap()).is_err());

        let none =
            SpatialQuery::with_predicate(geometry, vec![[0.0, 0.0]], SpatialPredicate::Knn(0));
        assert!(matching(&index, name, &none)?.is_empty());
        Ok(())
    }

    #[test]
    fn test_contains_needs_every_member_of_a_multi_polygon() -> crate::Result<()> {
        let split = r#"{"type": "MultiPolygon", "coordinates": [
            [[[1, 1], [2, 1], [2, 2], [1, 2], [1, 1]]],
            [[[20, 20], [21, 20], [21, 21], [20, 21], [20, 20]]]]}"#;
        let inside = r#"{"type": "MultiPolygon", "coordinates": [
            [[[1, 1], [2, 1], [2, 2], [1, 2], [1, 1]]],
            [[[3, 3], [4, 3], [4, 4], [3, 4], [3, 3]]]]}"#;
        let (index, name, geometry) = index_segments(&[&[("split", split), ("inside", inside)]])?;
        let query = SpatialQuery::from_bounds(geometry, [[0.0, 0.0], [5.0, 5.0]]);
        assert_eq!(matching(&index, name, &query)?, ["inside"]);
        Ok(())
    }

    #[test]
    fn test_near_join_matches_a_polygon_containing_the_point() -> crate::Result<()> {
        let point = r#"{"type": "Point", "coordinates": [5, 5]}"#;
        let (index, name, geometry) = index_segments(&[&[("box", BOX), ("point", point)]])?;
        let join = SpatialExecutor::new(PlanNode::Join {
            field: geometry,
            outer: Box::new(PlanNode::Query(name_query(name, "box"))),
            inner: Box::new(PlanNode::Query(name_query(name, "point"))),
            relation: SpatialRelation::Near(1e-6),
        });
        assert_eq!(matching(&index, name, &join)?, ["box"]);
        Ok(())
    }

    /// A closed regular ring around (lon, lat): counterclockwise, or clockwise for a hole.
    fn regular_ring(lon: f64, lat: f64, sides: usize, radius: f64, clockwise: bool) -> String {
        let mut positions: Vec<String> = (0..=sides)
            .map(|i| {
                let angle = std::f64::consts::TAU * (i % sides) as f64 / sides as f64;
                let (sin, cos) = angle.sin_cos();
                format!("[{}, {}]", lon + radius * cos, lat + radius * sin)
            })
            .collect();
        if clockwise {
            positions.reverse();
        }
        format!("[{}]", positions.join(", "))
    }

    /// A random GeoJSON point, line string, polygon (half of them with a hole) or two-polygon
    /// multi-polygon, a few degrees from the origin.
    fn random_geojson(rng: &mut StdRng) -> String {
        let (lon, lat): (f64, f64) = (rng.random_range(0.0..3.0), rng.random_range(0.0..3.0));
        let sides = rng.random_range(3..9);
        match rng.random_range(0..4) {
            0 => format!(r#"{{"type": "Point", "coordinates": [{lon}, {lat}]}}"#),
            1 => {
                let positions: Vec<String> = (0..rng.random_range(2..6))
                    .map(|_| {
                        let (dlon, dlat): (f64, f64) =
                            (rng.random_range(-0.3..0.3), rng.random_range(-0.3..0.3));
                        format!("[{}, {}]", lon + dlon, lat + dlat)
                    })
                    .collect();
                format!(
                    r#"{{"type": "LineString", "coordinates": [{}]}}"#,
                    positions.join(", ")
                )
            }
            2 => {
                let radius = rng.random_range(0.05..0.5);
                let mut rings = vec![regular_ring(lon, lat, sides, radius, false)];
                if rng.random_bool(0.5) {
                    rings.push(regular_ring(lon, lat, sides, radius / 3.0, true));
                }
                format!(
                    r#"{{"type": "Polygon", "coordinates": [{}]}}"#,
                    rings.join(", ")
                )
            }
            _ => {
                let radius = rng.random_range(0.05..0.2);
                format!(
                    r#"{{"type": "MultiPolygon", "coordinates": [[{}], [{}]]}}"#,
                    regular_ring(lon, lat, sides, radius, false),
                    regular_ring(lon + 0.6, lat - 0.4, sides, radius, false)
                )
            }
        }
    }

    fn geometry_set(geojson: &str) -> GeometrySet {
        let object: serde_json::Map<String, serde_json::Value> =
            serde_json::from_str(geojson).unwrap();
        let geometry = Geometry::<Plane>::from_geojson(&object).unwrap();
        to_geometry_set(&geometry.project::<Sphere>(), 0)
    }

    /// The edges of a member, ring by ring. A point is an edge from itself to itself.
    fn member_edges(member: &EdgeSet) -> Vec<([f64; 3], [f64; 3])> {
        if let [point] = member.vertices[..] {
            return vec![(point, point)];
        }
        member
            .ring_offsets
            .windows(2)
            .flat_map(|ring| ring[0]..ring[1] - 1)
            .map(|i| (member.vertices[i], member.vertices[i + 1]))
            .collect()
    }

    fn member_contains(member: &EdgeSet, point: &[f64; 3]) -> bool {
        member.closed
            && member.ring_offsets.windows(2).fold(false, |inside, ring| {
                let ring = &member.vertices[ring[0]..ring[1]];
                inside ^ brute_force_contains(point, ring, compute_origin_inside(ring))
            })
    }

    /// The distance between two geometry sets, interiors included, from every pair of edges.
    fn brute_force_distance(a: &GeometrySet, b: &GeometrySet) -> f64 {
        let mut min_dist = S1ChordAngle::infinity();
        for a_member in &a.members {
            for b_member in &b.members {
                if member_contains(a_member, &b_member.vertices[0])
                    || member_contains(b_member, &a_member.vertices[0])
                {
                    return 0.0;
                }
                for (a0, a1) in member_edges(a_member) {
                    for (b0, b1) in member_edges(b_member) {
                        if a0 == a1 {
                            update_min_distance(&a0, &b0, &b1, &mut min_dist);
                        } else if b0 == b1 {
                            update_min_distance(&b0, &a0, &a1, &mut min_dist);
                        } else {
                            update_edge_pair_min_distance(&a0, &a1, &b0, &b1, &mut min_dist);
                        }
                    }
                }
            }
        }
        min_dist.to_radians()
    }

    /// Asserts that the found names hold every geometry whose distance is in [min, max) and none
    /// outside it, leaving out distances within rounding of either bound.
    fn assert_found_in_range(found: &[String], distances: &[(f64, String)], min: f64, max: f64) {
        const ROUNDING: f64 = 1e-12;
        for (distance, name) in distances {
            let is_found = found.contains(name);
            if *distance >= min + ROUNDING && *distance < max - ROUNDING {
                assert!(is_found, "{name} at {distance} is in [{min}, {max})");
            }
            if *distance < min - ROUNDING || *distance >= max + ROUNDING {
                assert!(!is_found, "{name} at {distance} is not in [{min}, {max})");
            }
        }
    }

    /// Asserts that the hits are the k nearest geometries, nearest first. Ties may be broken
    /// either way, so the distances are compared rather than the names.
    fn assert_nearest(
        searcher: &Searcher,
        name: Field,
        hits: &[(Score, DocAddress)],
        distances: &[(f64, String)],
        k: usize,
    ) -> crate::Result<()> {
        assert_eq!(hits.len(), k.min(distances.len()));
        for (rank, (_, doc_address)) in hits.iter().enumerate() {
            let hit = name_of(searcher, name, *doc_address)?;
            let distance = distances.iter().find(|(_, n)| *n == hit).unwrap().0;
            assert!(
                (distance - distances[rank].0).abs() <= 1e-12,
                "hit {rank} is {hit} at {distance}, the nearest are {:?}",
                &distances[..k]
            );
        }
        Ok(())
    }

    #[test]
    fn test_distance_queries_agree_with_brute_force() -> crate::Result<()> {
        check_distance_queries_against_brute_force(7)
    }

    /// Indexes random geometries and checks distance queries from random points and random
    /// geometries against brute force.
    fn check_distance_queries_against_brute_force(seed: u64) -> crate::Result<()> {
        let mut rng = StdRng::seed_from_u64(seed);
        let docs: Vec<(String, String)> = (0..120)
            .map(|i| (format!("g{i}"), random_geojson(&mut rng)))
            .collect();
        let segments: Vec<Vec<(&str, &str)>> = docs
            .chunks(40)
            .map(|chunk| {
                chunk
                    .iter()
                    .map(|(n, g)| (n.as_str(), g.as_str()))
                    .collect()
            })
            .collect();
        let segments: Vec<&[(&str, &str)]> = segments.iter().map(Vec::as_slice).collect();
        let (index, name, geometry) = index_segments(&segments)?;
        let searcher = index.reader()?.searcher();
        let stored: Vec<(String, GeometrySet)> = docs
            .iter()
            .map(|(n, g)| (n.clone(), geometry_set(g)))
            .collect();
        let distances_from = |query: &GeometrySet| {
            let mut distances: Vec<(f64, String)> = stored
                .iter()
                .map(|(n, set)| (brute_force_distance(query, set), n.clone()))
                .collect();
            distances.sort_by(|a, b| a.0.total_cmp(&b.0));
            distances
        };

        for _ in 0..40 {
            let point = [rng.random_range(-0.5..3.5), rng.random_range(-0.5..3.5)];
            let query = format!(
                r#"{{"type": "Point", "coordinates": [{}, {}]}}"#,
                point[0], point[1]
            );
            let distances = distances_from(&geometry_set(&query));
            let radius = rng.random_range(0.0..0.5f64).to_radians();

            let within = SpatialQuery::with_predicate(
                geometry,
                vec![point],
                SpatialPredicate::Within(radius),
            );
            let found = matching(&index, name, &within)?;
            assert_found_in_range(&found, &distances, 0.0, radius);

            let between = SpatialQuery::with_predicate(
                geometry,
                vec![point],
                SpatialPredicate::Between(radius / 2.0, radius),
            );
            let found = matching(&index, name, &between)?;
            assert_found_in_range(&found, &distances, radius / 2.0, radius);

            let k = rng.random_range(1..6);
            let knn = SpatialQuery::with_predicate(geometry, vec![point], SpatialPredicate::Knn(k));
            let hits = searcher.search(&knn, &TopDocs::with_limit(10).order_by_score())?;
            assert_nearest(&searcher, name, &hits, &distances, k)?;
        }

        for _ in 0..40 {
            let query = geometry_set(&random_geojson(&mut rng));
            let distances = distances_from(&query);
            let radius = rng.random_range(0.0..0.5f64).to_radians();

            let within = SpatialExecutor::new(PlanNode::Within {
                field: geometry,
                geometry: query.clone(),
                radius: S1ChordAngle::from_radians(radius).length2(),
                filter: Box::new(PlanNode::Query(Box::new(AllQuery))),
            });
            let found = matching(&index, name, &within)?;
            assert_found_in_range(&found, &distances, 0.0, radius);

            let k = rng.random_range(1..6);
            let knn = SpatialExecutor::new(PlanNode::Knn {
                field: geometry,
                geometry: query,
                k,
                filter: None,
            });
            let hits = searcher.search(&knn, &TopDocs::with_limit(10).order_by_score())?;
            assert_nearest(&searcher, name, &hits, &distances, k)?;
        }
        Ok(())
    }

    #[test]
    fn test_between_join_skips_geometries_inside_the_inner_radius() -> crate::Result<()> {
        let point = |lat: f64| format!(r#"{{"type": "Point", "coordinates": [0, {lat}]}}"#);
        let (origin, near, far) = (point(0.0), point(0.5), point(2.0));
        let (index, name, geometry) =
            index_segments(&[&[("origin", &origin), ("near", &near), ("far", &far)]])?;
        let join = SpatialExecutor::new(PlanNode::Join {
            field: geometry,
            outer: Box::new(PlanNode::Query(name_query(name, "origin"))),
            inner: Box::new(PlanNode::Query(Box::new(AllQuery))),
            relation: SpatialRelation::Between(1f64.to_radians(), 3f64.to_radians()),
        });
        assert_eq!(matching(&index, name, &join)?, ["origin"]);
        Ok(())
    }
}

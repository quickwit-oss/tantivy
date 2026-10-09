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
        let min_coordinates = match self.predicate {
            SpatialPredicate::Contains | SpatialPredicate::Intersects => 3,
            _ => 1,
        };
        if self.coordinates.len() < min_coordinates {
            return Err(crate::TantivyError::InvalidArgument(format!(
                "Spatial predicate {:?} needs at least {min_coordinates} coordinates",
                self.predicate
            )));
        }
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
    fn search_segment(
        &self,
        cell_reader: &CellIndexReader,
        edge_cache: &mut EdgeCache<'_, Sphere>,
    ) -> Vec<u32>;
}

impl PreparedSpatialQuery for ContainsQuery {
    fn search_segment(
        &self,
        cell_reader: &CellIndexReader,
        edge_cache: &mut EdgeCache<'_, Sphere>,
    ) -> Vec<u32> {
        ContainsQuery::search_segment(self, cell_reader, edge_cache)
    }
}

impl PreparedSpatialQuery for IntersectsQuery {
    fn search_segment(
        &self,
        cell_reader: &CellIndexReader,
        edge_cache: &mut EdgeCache<'_, Sphere>,
    ) -> Vec<u32> {
        IntersectsQuery::search_segment(self, cell_reader, edge_cache)
    }
}

impl PreparedSpatialQuery for ClosestEdgeQuery {
    fn search_segment(
        &self,
        cell_reader: &CellIndexReader,
        edge_cache: &mut EdgeCache<'_, Sphere>,
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
    use crate::query::{AllQuery, BoostQuery, TermQuery};
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
    fn test_knn_skips_deleted_documents_before_ranking() -> crate::Result<()> {
        let point = |lat: f64| format!(r#"{{"type": "Point", "coordinates": [0, {lat}]}}"#);
        let (deleted, near, middle, far) = (point(1.0), point(2.0), point(3.0), point(4.0));
        let (index, name, geometry) = index_segments(&[
            &[("deleted", &deleted), ("far", &far)],
            &[("near", &near), ("middle", &middle)],
        ])?;
        let mut writer: IndexWriter = index.writer_for_tests()?;
        writer.set_merge_policy(Box::new(crate::indexer::NoMergePolicy));
        writer.delete_term(Term::from_field_text(name, "deleted"));
        writer.commit()?;

        let searcher = index.reader()?.searcher();
        assert_eq!(searcher.segment_readers().len(), 2);
        assert_eq!(searcher.num_docs(), 3);
        for k in 1..=4 {
            let unfiltered =
                SpatialQuery::with_predicate(geometry, vec![[0.0, 0.0]], SpatialPredicate::Knn(k));
            let filtered = SpatialExecutor::new(PlanNode::Knn {
                field: geometry,
                geometry: geometry_set(r#"{"type": "Point", "coordinates": [0, 0]}"#),
                k,
                filter: Some(Box::new(PlanNode::Query(Box::new(AllQuery)))),
            });
            for query in [&unfiltered as &dyn Query, &filtered] {
                let hits = searcher.search(query, &TopDocs::with_limit(10).order_by_score())?;
                let names = hits
                    .iter()
                    .map(|&(_, address)| name_of(&searcher, name, address))
                    .collect::<crate::Result<Vec<_>>>()?;
                assert_eq!(names, ["near", "middle", "far"][..k.min(3)]);
            }
        }
        // The deletion mask must intersect the caller's filter rather than replace it.
        for only in ["deleted", "far"] {
            let filtered = SpatialExecutor::new(PlanNode::Knn {
                field: geometry,
                geometry: geometry_set(r#"{"type": "Point", "coordinates": [0, 0]}"#),
                k: 1,
                filter: Some(Box::new(PlanNode::Query(name_query(name, only)))),
            });
            let expected = if only == "deleted" {
                vec![]
            } else {
                vec!["far"]
            };
            assert_eq!(matching(&index, name, &filtered)?, expected);
        }
        Ok(())
    }

    #[test]
    fn test_knn_applies_boosts_and_explains_the_same_score() -> crate::Result<()> {
        let a = r#"{"type": "Point", "coordinates": [0, 1]}"#;
        let b = r#"{"type": "Point", "coordinates": [0, 2]}"#;
        let (index, _, geometry) = index_segments(&[&[("a", a)], &[("b", b)]])?;
        let searcher = index.reader()?.searcher();
        let knn =
            SpatialQuery::with_predicate(geometry, vec![[0.0, 0.0]], SpatialPredicate::Knn(2));
        let plain = searcher.search(&knn, &TopDocs::with_limit(10).order_by_score())?;
        assert_eq!(plain.len(), 2);
        for boost in [0.0, 3.0, -1.0] {
            let boosted = BoostQuery::new(Box::new(knn.clone()), boost);
            let hits = searcher.search(&boosted, &TopDocs::with_limit(10).order_by_score())?;
            assert_eq!(hits.len(), plain.len());
            for (score, address) in hits {
                let unboosted = plain.iter().find(|(_, doc)| *doc == address).unwrap().0;
                assert_eq!(score, unboosted * boost);
                assert_eq!(boosted.explain(&searcher, address)?.value(), score);
            }
        }
        Ok(())
    }

    #[test]
    fn test_empty_distance_ranges_match_no_geometries() -> crate::Result<()> {
        let point = r#"{"type": "Point", "coordinates": [5, 5]}"#;
        let (index, name, geometry) = index_segments(&[&[("box", BOX), ("point", point)]])?;
        for predicate in [
            SpatialPredicate::Within(0.0),
            SpatialPredicate::Within(-1.0),
            SpatialPredicate::Between(0.0, 0.0),
            SpatialPredicate::Between(-1.0, 0.0),
            SpatialPredicate::Between(1.0, 1.0),
            SpatialPredicate::Between(1.0, 0.5),
        ] {
            let query = SpatialQuery::with_predicate(geometry, vec![[5.0, 5.0]], predicate);
            assert!(matching(&index, name, &query)?.is_empty(), "{query:?}");
        }
        let nonempty = SpatialQuery::with_predicate(
            geometry,
            vec![[5.0, 5.0]],
            SpatialPredicate::Between(0.0, 1e-6),
        );
        assert_eq!(matching(&index, name, &nonempty)?, ["box", "point"]);
        Ok(())
    }

    #[test]
    fn test_overlapping_polygons_match_knn_and_within() -> crate::Result<()> {
        let docs: Vec<(String, &str)> = (0..256).map(|i| (format!("box{i}"), BOX)).collect();
        let docs: Vec<(&str, &str)> = docs
            .iter()
            .map(|(name, shape)| (name.as_str(), *shape))
            .collect();
        let (index, _, geometry) = index_segments(&[&docs])?;
        let searcher = index.reader()?.searcher();
        for k in [1, 10, docs.len(), docs.len() + 1] {
            let knn =
                SpatialQuery::with_predicate(geometry, vec![[5.0, 5.0]], SpatialPredicate::Knn(k));
            let hits = searcher.search(&knn, &TopDocs::with_limit(docs.len()).order_by_score())?;
            assert_eq!(hits.len(), k.min(docs.len()));
            assert!(hits.iter().all(|(score, _)| *score == 0.0));
        }
        let within = SpatialQuery::with_predicate(
            geometry,
            vec![[5.0, 5.0]],
            SpatialPredicate::Within(1e-6),
        );
        assert_eq!(
            searcher.search(&within, &DocSetCollector)?.len(),
            docs.len()
        );
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

    fn parse(index: &Index, name: Field, query: &str) -> crate::Result<Vec<String>> {
        let parser = crate::query::QueryParser::for_index(index, vec![name]);
        matching(index, name, &*parser.parse_query(query)?)
    }

    #[test]
    fn test_join_keeps_the_occur_of_the_join_and_its_siblings() -> crate::Result<()> {
        let point = r#"{"type": "Point", "coordinates": [5, 5]}"#;
        let far = r#"{"type": "Point", "coordinates": [50, 50]}"#;
        let (index, name, _) = index_segments(&[&[("box", BOX), ("point", point), ("far", far)]])?;
        let near_point = "geometry:$within(1km, $query(name:point))";
        assert_eq!(parse(&index, name, near_point)?, ["box"]);
        assert_eq!(
            parse(&index, name, &format!("+name:box +{near_point}"))?,
            ["box"]
        );
        assert!(parse(&index, name, &format!("-name:box {near_point}"))?.is_empty());
        assert_eq!(
            parse(&index, name, &format!("name:far OR {near_point}"))?,
            ["box", "far"]
        );
        assert_eq!(
            parse(&index, name, &format!("name:far -{near_point}"))?,
            ["far"]
        );
        assert_eq!(
            parse(&index, name, &format!("name:box name:far -{near_point}"))?,
            ["far"]
        );
        let any_near = "geometry:$within(1km, $query(*))";
        assert_eq!(
            parse(&index, name, &format!("+(name:box name:point) +{any_near}"))?,
            ["box", "point"]
        );
        // A join with a boost is not narrowed, and still matches.
        assert_eq!(
            parse(&index, name, &format!("+name:box +{near_point}^2"))?,
            ["box"]
        );
        Ok(())
    }

    #[test]
    fn test_join_rejects_an_invalid_inner_query() -> crate::Result<()> {
        let (index, name, _) = index_segments(&[&[("box", BOX)]])?;
        let parser = crate::query::QueryParser::for_index(&index, vec![name]);
        let query = "geometry:$within(1km, $query(name:point AND))";
        assert!(parser.parse_query(query).is_err());
        assert!(!parser.parse_query_lenient(query).1.is_empty());
        assert!(parser
            .parse_query("geometry:$within(1km, $query(nofield:x))")
            .is_err());

        let knn_join = query_grammar::UserInputAst::from(query_grammar::UserInputLeaf::Spatial {
            field: Some("geometry".to_string()),
            predicate: query_grammar::SpatialPredicateKind::Knn(3),
            coordinates: vec![],
            inner_query: Some("name:box".to_string()),
        });
        assert!(parser.build_query_from_user_input_ast(knn_join).is_err());
        let no_coordinates =
            query_grammar::UserInputAst::from(query_grammar::UserInputLeaf::Spatial {
                field: Some("geometry".to_string()),
                predicate: query_grammar::SpatialPredicateKind::Intersects,
                coordinates: vec![],
                inner_query: None,
            });
        assert!(parser
            .build_query_from_user_input_ast(no_coordinates)
            .is_err());
        Ok(())
    }

    #[test]
    fn test_a_query_with_too_few_coordinates_is_an_error() -> crate::Result<()> {
        let (index, _, geometry) = index_segments(&[&[("box", BOX)]])?;
        let searcher = index.reader()?.searcher();
        for (coordinates, predicate) in [
            (vec![], SpatialPredicate::Within(1.0)),
            (vec![], SpatialPredicate::Knn(1)),
            (vec![[0.0, 0.0], [1.0, 1.0]], SpatialPredicate::Contains),
        ] {
            let query = SpatialQuery::with_predicate(geometry, coordinates, predicate);
            assert!(searcher.search(&query, &DocSetCollector).is_err());
        }
        Ok(())
    }

    #[test]
    fn test_joins_with_point_and_line_outer_geometries() -> crate::Result<()> {
        let point = r#"{"type": "Point", "coordinates": [5, 5]}"#;
        let line = r#"{"type": "LineString", "coordinates": [[-5, 5.001], [15, 5.001]]}"#;
        let (index, name, _) =
            index_segments(&[&[("box", BOX), ("point", point), ("line", line)]])?;
        assert_eq!(
            parse(&index, name, "geometry:$intersects($query(name:box))")?,
            ["line", "point"]
        );
        assert_eq!(
            parse(&index, name, "geometry:$intersects($query(name:line))")?,
            ["box"]
        );
        assert_eq!(
            parse(&index, name, "geometry:$intersects($query(name:point))")?,
            ["box"]
        );
        assert!(parse(&index, name, "geometry:$contains($query(name:line))")?.is_empty());
        assert_eq!(
            parse(&index, name, "geometry:$contains($query(name:point))")?,
            ["box"]
        );
        Ok(())
    }

    #[test]
    fn test_queries_use_every_member_of_a_multi_polygon() -> crate::Result<()> {
        let multi = r#"{"type": "MultiPolygon", "coordinates": [
            [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]],
            [[[20, 20], [30, 20], [30, 30], [20, 30], [20, 20]]]]}"#;
        let point = r#"{"type": "Point", "coordinates": [25, 25]}"#;
        let square = r#"{"type": "Polygon", "coordinates": [
            [[24, 24], [26, 24], [26, 26], [24, 26], [24, 24]]]}"#;
        let (index, name, geometry) =
            index_segments(&[&[("multi", multi), ("point", point), ("square", square)]])?;
        let intersects = SpatialExecutor::new(PlanNode::Intersects {
            field: geometry,
            geometry: geometry_set(multi),
            filter: Box::new(PlanNode::Query(Box::new(AllQuery))),
        });
        assert_eq!(
            matching(&index, name, &intersects)?,
            ["multi", "point", "square"]
        );
        for relation in [SpatialRelation::Intersects, SpatialRelation::Contains] {
            for inner in ["point", "square"] {
                let join = SpatialExecutor::new(PlanNode::Join {
                    field: geometry,
                    outer: Box::new(PlanNode::Query(name_query(name, "multi"))),
                    inner: Box::new(PlanNode::Query(name_query(name, inner))),
                    relation: relation.clone(),
                });
                assert_eq!(
                    matching(&index, name, &join)?,
                    ["multi"],
                    "{relation:?} {inner}"
                );
            }
        }
        Ok(())
    }

    #[test]
    fn test_a_line_query_has_no_interior() -> crate::Result<()> {
        let point =
            |lon: f64, lat: f64| format!(r#"{{"type": "Point", "coordinates": [{lon}, {lat}]}}"#);
        let (east, far_east) = (point(10.0, 0.0), point(20.0, 1.0));
        let line = r#"{"type": "LineString", "coordinates": [[5, -5], [5, 5]]}"#;
        let crossing = r#"{"type": "LineString", "coordinates": [[0, 0], [10, 0]]}"#;
        let (index, name, geometry) = index_segments(&[&[
            ("east", &east),
            ("far_east", &far_east),
            ("box", BOX),
            ("crossing", crossing),
            ("line", line),
        ]])?;
        let intersects = SpatialExecutor::new(PlanNode::Intersects {
            field: geometry,
            geometry: geometry_set(line),
            filter: Box::new(PlanNode::Query(Box::new(AllQuery))),
        });
        // A line meets itself only along its edges, which do not cross.
        assert_eq!(matching(&index, name, &intersects)?, ["box", "crossing"]);
        for relation in [SpatialRelation::Intersects, SpatialRelation::Contains] {
            let join = SpatialExecutor::new(PlanNode::Join {
                field: geometry,
                outer: Box::new(PlanNode::Query(name_query(name, "line"))),
                inner: Box::new(PlanNode::Query(Box::new(AllQuery))),
                relation: relation.clone(),
            });
            let expected: &[&str] = match relation {
                SpatialRelation::Intersects => &["line"],
                _ => &[],
            };
            assert_eq!(matching(&index, name, &join)?, expected, "{relation:?}");
        }
        Ok(())
    }

    #[test]
    fn test_join_ignores_deleted_inner_documents() -> crate::Result<()> {
        let point = r#"{"type": "Point", "coordinates": [5, 5]}"#;
        let (index, name, geometry) = index_segments(&[&[("box", BOX), ("point", point)]])?;
        let join = SpatialExecutor::new(PlanNode::Join {
            field: geometry,
            outer: Box::new(PlanNode::Query(name_query(name, "box"))),
            inner: Box::new(PlanNode::Query(Box::new(AllQuery))),
            relation: SpatialRelation::Near(1e-6),
        });
        assert_eq!(matching(&index, name, &join)?, ["box"]);
        let mut writer: IndexWriter = index.writer_for_tests()?;
        writer.set_merge_policy(Box::new(crate::indexer::NoMergePolicy));
        writer.delete_term(Term::from_field_text(name, "point"));
        writer.commit()?;
        assert!(matching(&index, name, &join)?.is_empty());
        Ok(())
    }

    #[test]
    fn test_join_does_not_match_a_document_to_itself() -> crate::Result<()> {
        let point = |lon: f64| format!(r#"{{"type": "Point", "coordinates": [{lon}, 0]}}"#);
        let (a, b, c) = (point(0.0), point(10.0), point(10.000001));
        let (index, name, geometry) = index_segments(&[&[("a", &a), ("b", &b)], &[("c", &c)]])?;
        let join = SpatialExecutor::new(PlanNode::Join {
            field: geometry,
            outer: Box::new(PlanNode::Query(Box::new(AllQuery))),
            inner: Box::new(PlanNode::Query(Box::new(AllQuery))),
            relation: SpatialRelation::Near(1e-6),
        });
        assert_eq!(matching(&index, name, &join)?, ["b", "c"]);
        Ok(())
    }

    #[test]
    fn test_contains_rejects_a_polygon_leaving_through_a_shared_vertex() -> crate::Result<()> {
        let polygon = |ring: &str| format!(r#"{{"type": "Polygon", "coordinates": [{ring}]}}"#);
        let diamond = polygon("[[5, 5], [10, 0], [15, 5], [10, 10], [5, 5]]");
        let corner = polygon("[[5, 5], [10, 0], [10, 10], [5, 5]]");
        let inside_corner = polygon("[[10, 0], [8, 5], [5, 5], [10, 0]]");
        let outside_corner = polygon("[[10, 0], [15, 0], [15, 5], [10, 0]]");
        let (index, name, geometry) = index_segments(&[&[
            ("diamond", &diamond),
            ("corner", &corner),
            ("inside_corner", &inside_corner),
            ("outside_corner", &outside_corner),
            ("same", BOX),
        ]])?;
        let query = SpatialQuery::from_bounds(geometry, [[0.0, 0.0], [10.0, 10.0]]);
        assert_eq!(
            matching(&index, name, &query)?,
            ["corner", "inside_corner", "same"]
        );
        Ok(())
    }

    #[test]
    fn test_polygons_sharing_boundary_intersect_only_where_interiors_overlap() -> crate::Result<()>
    {
        let polygon = |ring: &str| format!(r#"{{"type": "Polygon", "coordinates": [{ring}]}}"#);
        let east = polygon("[[10, 0], [20, 0], [20, 10], [10, 10], [10, 0]]");
        let corner_touch = polygon("[[10, 10], [20, 10], [20, 20], [10, 20], [10, 10]]");
        let inner_touch = polygon("[[10, 0], [8, 5], [5, 5], [10, 0]]");
        let surround = polygon("[[-5, -5], [15, -5], [15, 15], [-5, 15], [-5, -5]]");
        let surround_touch = polygon("[[0, 0], [15, -5], [15, 15], [-5, 15], [0, 0]]");
        let (index, name, geometry) = index_segments(&[&[
            ("same", BOX),
            ("east", &east),
            ("corner_touch", &corner_touch),
            ("inner_touch", &inner_touch),
            ("surround", &surround),
            ("surround_touch", &surround_touch),
        ]])?;
        let bounds = [[0.0, 0.0], [10.0, 10.0]];
        let intersects = SpatialQuery::intersects_bounds(geometry, bounds);
        assert_eq!(
            matching(&index, name, &intersects)?,
            ["inner_touch", "same", "surround", "surround_touch"]
        );
        let contains = SpatialQuery::from_bounds(geometry, bounds);
        assert_eq!(matching(&index, name, &contains)?, ["inner_touch", "same"]);
        Ok(())
    }

    #[test]
    fn test_thin_polygons_sharing_edges_use_their_interior_sides() -> crate::Result<()> {
        for height in [1e-6, 1e-8, 1e-12] {
            let triangle = format!(
                r#"{{"type": "Polygon", "coordinates": [[[0, 0], [10, 0], [5, {height}], [0, 0]]]}}"#
            );
            let below = format!(
                r#"{{"type": "Polygon", "coordinates": [[[10, 0], [0, 0], [5, {}], [10, 0]]]}}"#,
                -height
            );
            let (index, name, geometry) =
                index_segments(&[&[("same", &triangle), ("below", &below)]])?;
            for predicate in [SpatialPredicate::Intersects, SpatialPredicate::Contains] {
                let query = SpatialQuery::with_predicate(
                    geometry,
                    vec![[0.0, 0.0], [10.0, 0.0], [5.0, height]],
                    predicate,
                );
                assert_eq!(matching(&index, name, &query)?, ["same"], "height {height}");
            }
        }
        Ok(())
    }

    #[test]
    fn test_contains_rejects_a_polygon_around_a_hole_of_the_query() -> crate::Result<()> {
        let holed = format!(
            r#"{{"type": "Polygon", "coordinates": [{}, {}]}}"#,
            "[[0, 0], [20, 0], [20, 20], [0, 20], [0, 0]]",
            "[[8, 8], [8, 12], [12, 12], [12, 8], [8, 8]]"
        );
        let square = |lo: f64, hi: f64| {
            format!(
                r#"{{"type": "Polygon", "coordinates": [[[{lo}, {lo}], [{hi}, {lo}], [{hi}, {hi}], [{lo}, {hi}], [{lo}, {lo}]]]}}"#
            )
        };
        let (around, beside, in_hole) = (square(5.0, 15.0), square(1.0, 3.0), square(9.0, 11.0));
        let hole = square(8.0, 12.0);
        let (index, name, geometry) = index_segments(&[&[
            ("holed", &holed),
            ("around", &around),
            ("beside", &beside),
            ("in_hole", &in_hole),
            ("hole", &hole),
            ("copy", &holed),
        ]])?;
        let contains = |inner: &str| {
            SpatialExecutor::new(PlanNode::Join {
                field: geometry,
                outer: Box::new(PlanNode::Query(name_query(name, "holed"))),
                inner: Box::new(PlanNode::Query(name_query(name, inner))),
                relation: SpatialRelation::Contains,
            })
        };
        assert!(matching(&index, name, &contains("around"))?.is_empty());
        assert!(matching(&index, name, &contains("in_hole"))?.is_empty());
        assert!(matching(&index, name, &contains("hole"))?.is_empty());
        assert_eq!(matching(&index, name, &contains("beside"))?, ["holed"]);
        assert_eq!(matching(&index, name, &contains("copy"))?, ["holed"]);
        Ok(())
    }

    #[test]
    fn test_contains_joins_use_the_union_of_polygon_values() -> crate::Result<()> {
        let square = |lo: f64, hi: f64| {
            format!(
                r#"{{"type": "Polygon", "coordinates": [[[{lo}, {lo}], [{hi}, {lo}], [{hi}, {hi}], [{lo}, {hi}], [{lo}, {lo}]]]}}"#
            )
        };
        let inner =
            r#"{"type": "Polygon", "coordinates": [[[2, 2], [8, 2], [8, 8], [2, 8], [2, 2]]]}"#;
        for (outer, expected) in [
            // The candidate crosses boundaries of both members but stays inside their union.
            (
                r#"[
                {"type": "Polygon", "coordinates": [[[0, 0], [6, 0], [6, 10], [0, 10], [0, 0]]]},
                {"type": "Polygon", "coordinates": [[[4, 0], [10, 0], [10, 10], [4, 10], [4, 0]]]}
            ]"#,
                true,
            ),
            // The oppositely directed shared edge is internal to the union.
            (
                r#"[
                {"type": "Polygon", "coordinates": [[[0, 0], [5, 0], [5, 10], [0, 10], [0, 0]]]},
                {"type": "Polygon", "coordinates": [[[5, 0], [10, 0], [10, 10], [5, 10], [5, 0]]]}
            ]"#,
                true,
            ),
            // A gap between the members must still reject containment.
            (
                r#"[
                {"type": "Polygon", "coordinates": [[[0, 0], [3, 0], [3, 10], [0, 10], [0, 0]]]},
                {"type": "Polygon", "coordinates": [[[7, 0], [10, 0], [10, 10], [7, 10], [7, 0]]]}
            ]"#,
                false,
            ),
            // No individual member has a hole, but the union encloses an uncovered gap.
            (
                r#"[
                {"type": "Polygon", "coordinates": [[[0, 0], [10, 0], [10, 3], [0, 3], [0, 0]]]},
                {"type": "Polygon", "coordinates": [[[0, 7], [10, 7], [10, 10], [0, 10], [0, 7]]]},
                {"type": "Polygon", "coordinates": [[[0, 2], [3, 2], [3, 8], [0, 8], [0, 2]]]},
                {"type": "Polygon", "coordinates": [[[7, 2], [10, 2], [10, 8], [7, 8], [7, 2]]]}
            ]"#,
                false,
            ),
        ] {
            let (index, name, _) =
                index_segments(&[&[("outer", outer)], &[("inner", inner), ("copy", outer)]])?;
            let query = "+name:outer +geometry:$contains($query(name:inner))";
            let wanted = if expected { vec!["outer"] } else { vec![] };
            assert_eq!(parse(&index, name, query)?, wanted, "{outer}");
            assert_eq!(
                parse(
                    &index,
                    name,
                    "+name:outer +geometry:$contains($query(name:copy))"
                )?,
                ["outer"],
                "contains a copy: {outer}"
            );

            let mut writer: IndexWriter = index.writer_for_tests()?;
            writer.merge(&index.searchable_segment_ids()?).wait()?;
            assert_eq!(parse(&index, name, query)?, wanted, "merged {outer}");
            assert_eq!(
                parse(
                    &index,
                    name,
                    "+name:outer +geometry:$contains($query(name:copy))"
                )?,
                ["outer"],
                "merged contains a copy: {outer}"
            );
        }

        let donut = r#"{"type": "Polygon", "coordinates": [
            [[0, 0], [20, 0], [20, 20], [0, 20], [0, 0]],
            [[8, 8], [8, 12], [12, 12], [12, 8], [8, 8]]]}"#;
        for (filling, expected) in [(square(7.0, 13.0), true), (square(7.0, 10.0), false)] {
            let outer = format!("[{donut}, {filling}]");
            let inner = square(5.0, 15.0);
            let (index, name, _) = index_segments(&[&[("outer", &outer), ("inner", &inner)]])?;
            let wanted = if expected { vec!["outer"] } else { vec![] };
            assert_eq!(
                parse(
                    &index,
                    name,
                    "+name:outer +geometry:$contains($query(name:inner))"
                )?,
                wanted,
                "{outer}"
            );
        }
        Ok(())
    }

    #[test]
    fn test_queries_with_many_vertices_find_crossings() -> crate::Result<()> {
        let polygon = |ring: &str| format!(r#"{{"type": "Polygon", "coordinates": [{ring}]}}"#);
        let straddling = polygon("[[4.5, -0.5], [5.5, -0.5], [5.5, 0.5], [4.5, 0.5], [4.5, -0.5]]");
        let inside = polygon("[[-1, -1], [1, -1], [1, 1], [-1, 1], [-1, -1]]");
        let outside = polygon("[[5.5, -0.5], [6.5, -0.5], [6.5, 0.5], [5.5, 0.5], [5.5, -0.5]]");
        let (index, name, geometry) = index_segments(&[&[
            ("straddling", &straddling),
            ("inside", &inside),
            ("outside", &outside),
        ]])?;
        let circle: Vec<[f64; 2]> = (0..200)
            .map(|i| {
                let (sin, cos) = (std::f64::consts::TAU * i as f64 / 200.0).sin_cos();
                [5.0 * cos, 5.0 * sin]
            })
            .collect();
        let contains = SpatialQuery::new(geometry, circle.clone());
        assert_eq!(matching(&index, name, &contains)?, ["inside"]);
        let intersects =
            SpatialQuery::with_predicate(geometry, circle, SpatialPredicate::Intersects);
        assert_eq!(
            matching(&index, name, &intersects)?,
            ["inside", "straddling"]
        );
        Ok(())
    }

    #[test]
    fn test_contains_needs_every_value_of_a_document() -> crate::Result<()> {
        let point = |lon: f64| format!(r#"{{"type": "Point", "coordinates": [{lon}, 5]}}"#);
        let split = format!("[{}, {}]", point(5.0), point(50.0));
        let both = format!("[{}, {}]", point(4.0), point(6.0));
        let (index, name, geometry) = index_segments(&[&[("split", &split), ("both", &both)]])?;
        let query = SpatialQuery::from_bounds(geometry, [[0.0, 0.0], [10.0, 10.0]]);
        assert_eq!(matching(&index, name, &query)?, ["both"]);
        let intersects = SpatialQuery::intersects_bounds(geometry, [[0.0, 0.0], [10.0, 10.0]]);
        assert_eq!(matching(&index, name, &intersects)?, ["both", "split"]);
        Ok(())
    }

    #[test]
    fn test_geometry_collection_is_indexed_and_round_trips() -> crate::Result<()> {
        let collection = r#"{"type": "GeometryCollection", "geometries": [
            {"type": "Point", "coordinates": [50, 50]},
            {"type": "LineString", "coordinates": [[1, 1], [2, 2]]}]}"#;
        let (index, name, geometry) = index_segments(&[&[("collection", collection)]])?;
        let query = SpatialQuery::intersects_bounds(geometry, [[0.0, 0.0], [10.0, 10.0]]);
        assert_eq!(matching(&index, name, &query)?, ["collection"]);

        let object: serde_json::Map<String, serde_json::Value> =
            serde_json::from_str(collection).unwrap();
        let parsed = Geometry::<Plane>::from_geojson(&object).unwrap();
        assert_eq!(
            Geometry::<Plane>::from_geojson(&parsed.to_geojson()).unwrap(),
            parsed
        );
        Ok(())
    }

    #[test]
    fn test_index_a_polygon_with_more_vertices_than_a_u16() -> crate::Result<()> {
        let ring = regular_ring(0.0, 0.0, 70_000, 5.0, false);
        let big = format!(r#"{{"type": "Polygon", "coordinates": [{ring}]}}"#);
        let point = |lon: f64| format!(r#"{{"type": "Point", "coordinates": [{lon}, 0]}}"#);
        let (center, edge, out) = (point(0.0), point(4.99999), point(5.01));
        let (index, name, geometry) = index_segments(&[&[
            ("big", &big),
            ("center", &center),
            ("edge", &edge),
            ("out", &out),
        ]])?;
        let at = |lon: f64| {
            SpatialQuery::with_predicate(geometry, vec![[lon, 0.0]], SpatialPredicate::Within(1e-9))
        };
        assert_eq!(matching(&index, name, &at(0.0))?, ["big", "center"]);
        // The vertices near (5, 0) have the highest edge ids.
        assert_eq!(matching(&index, name, &at(4.999995))?, ["big"]);
        assert!(matching(&index, name, &at(5.005))?.is_empty());
        let near_edge = SpatialQuery::intersects_bounds(geometry, [[4.9, -0.1], [5.1, 0.1]]);
        assert_eq!(matching(&index, name, &near_edge)?, ["big", "edge", "out"]);
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

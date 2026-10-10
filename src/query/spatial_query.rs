//! Spatial polygon query.
//!
//! Finds indexed geometries that match a query polygon via contains or intersects predicates. The
//! query polygon is specified as lon/lat vertices, converted to unit sphere coordinates, and
//! searched against each segment's cell index and edge index.

use common::BitSet;

use crate::query::explanation::does_not_match;
use crate::query::{BitSetDocSet, Explanation, Query, Scorer, Weight};
use crate::schema::{Field, FieldType};
use crate::spatial::cell_index_reader::CellIndexReader;
use crate::spatial::closest_edge_query::ClosestEdgeQuery;
use crate::spatial::contains_query::{ContainingQuery, ContainsQuery};
use crate::spatial::edge_cache::EdgeCache;
use crate::spatial::edge_reader::EdgeReader;
use crate::spatial::executor::{SpatialExecutor, QUERY_EDGE_CACHE_BYTES};
use crate::spatial::geometry::Geometry;
use crate::spatial::geometry_set::{
    to_geometry_set, to_geometry_set_with_orientation, RingOrientation,
};
use crate::spatial::intersects_query::IntersectsQuery;
use crate::spatial::plane::Plane;
use crate::spatial::region_coverer::CovererOptions;
use crate::spatial::s1chord_angle::S1ChordAngle;
use crate::spatial::sphere::Sphere;
use crate::{DocId, DocSet, Score, TERMINATED};

/// The spatial predicate to apply.
#[derive(Clone, Debug)]
pub enum SpatialPredicate {
    /// Return geometries inside the query polygon, its boundary included (ST_CoveredBy).
    CoveredBy,
    /// Return geometries that contain the query polygon.
    Contains,
    /// Return geometries that intersect the query polygon.
    Intersects,
    /// Return geometries within a distance of a point. Distance in radians.
    Within(f64),
    /// Return geometries between two distances of a point. Distances in radians.
    Between(f64, f64),
    /// Return the K nearest geometries to a point, across all segments. A hit scores by its
    /// rank by distance, from 1 for the nearest down to 1/K, so that the nearest score highest
    /// and a kNN clause lifts the nearest documents of a boolean query. Without a searcher, as
    /// when deleting by query, each segment keeps its own K nearest.
    Knn(usize),
}

/// Spatial query.
#[derive(Clone, Debug)]
pub struct SpatialQuery {
    field: Field,
    predicate: SpatialPredicate,
    /// Polygon vertices (lon/lat) for Contains and Intersects, or a single point for distance.
    coordinates: Vec<[f64; 2]>,
    /// How the polygon's ring picks the region it bounds, if not as the field's rings do.
    orientation: Option<RingOrientation>,
}

impl SpatialQuery {
    /// Create a query for the geometries inside a polygon specified as lon/lat vertices.
    pub fn new(field: Field, polygon: Vec<[f64; 2]>) -> Self {
        SpatialQuery {
            field,
            coordinates: polygon,
            predicate: SpatialPredicate::CoveredBy,
            orientation: None,
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
            orientation: None,
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
            predicate: SpatialPredicate::CoveredBy,
            orientation: None,
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
            orientation: None,
        }
    }

    /// Have the query polygon's ring pick the region it bounds as given, rather than as the
    /// field's rings do. With [`RingOrientation::RightHand`], a query polygon may be larger than
    /// a hemisphere on any field.
    #[must_use]
    pub fn with_ring_orientation(mut self, orientation: RingOrientation) -> Self {
        self.orientation = Some(orientation);
        self
    }
}

impl Query for SpatialQuery {
    fn weight(
        &self,
        enable_scoring: super::EnableScoring<'_>,
    ) -> crate::Result<Box<dyn super::Weight>> {
        let min_coordinates = match self.predicate {
            SpatialPredicate::CoveredBy
            | SpatialPredicate::Contains
            | SpatialPredicate::Intersects => 3,
            _ => 1,
        };
        if self.coordinates.len() < min_coordinates {
            return Err(crate::TantivyError::InvalidArgument(format!(
                "Spatial predicate {:?} needs at least {min_coordinates} coordinates",
                self.predicate
            )));
        }
        // GeoJSON values are checked as they are parsed; query coordinates are checked here.
        if let Some([lon, lat]) = self
            .coordinates
            .iter()
            .find(|[lon, lat]| !(-180.0..=180.0).contains(lon) || !(-90.0..=90.0).contains(lat))
        {
            return Err(crate::TantivyError::InvalidArgument(format!(
                "Invalid coordinate {lon} {lat}: longitude must be within [-180, 180] and \
                 latitude within [-90, 90]"
            )));
        }
        let distances = match self.predicate {
            SpatialPredicate::Within(radius) => vec![radius],
            SpatialPredicate::Between(inner, outer) => vec![inner, outer],
            _ => Vec::new(),
        };
        if distances.iter().any(|distance| distance.is_nan()) {
            return Err(crate::TantivyError::InvalidArgument(format!(
                "Spatial predicate {:?} needs distances that are numbers",
                self.predicate
            )));
        }
        let prepared: Box<dyn PreparedSpatialQuery> = match &self.predicate {
            SpatialPredicate::CoveredBy
            | SpatialPredicate::Contains
            | SpatialPredicate::Intersects => {
                let mut ring: Vec<[f64; 2]> = self.coordinates.clone();
                if ring.first() != ring.last() {
                    ring.push(ring[0]);
                }
                let orientation = self.orientation.unwrap_or_else(|| {
                    // A field the schema does not have matches nothing, however it is read.
                    let schema = enable_scoring.schema();
                    if self.field.field_id() as usize >= schema.num_fields() {
                        return RingOrientation::default();
                    }
                    match schema.get_field_entry(self.field).field_type() {
                        FieldType::Spatial(options) => options.ring_orientation(),
                        _ => RingOrientation::default(),
                    }
                });
                let plane_geometry = Geometry::<Plane>::Polygon(vec![ring]);
                let projected = plane_geometry.project::<Sphere>();
                let set = to_geometry_set_with_orientation(&projected, 0, orientation);
                // Smashing keeps a polygon with no area as the line or point it is, so that the
                // bounding box of a point queries the point, as it would be indexed.
                match &self.predicate {
                    SpatialPredicate::CoveredBy => {
                        Box::new(ContainsQuery::new(set, CovererOptions::default()))
                    }
                    SpatialPredicate::Contains => {
                        Box::new(ContainingQuery::new(set, CovererOptions::default())?)
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
                return SpatialExecutor::knn(self.field, self.coordinates[0], *k)
                    .weight(enable_scoring);
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

impl PreparedSpatialQuery for ContainingQuery {
    fn search_segment(
        &self,
        cell_reader: &CellIndexReader,
        edge_cache: &mut EdgeCache<'_, Sphere>,
    ) -> Vec<u32> {
        ContainingQuery::search_segment(self, cell_reader, edge_cache)
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
        let mut edge_cache = EdgeCache::new(vec![edge_reader], QUERY_EDGE_CACHE_BYTES);

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
        // Seeking backward is not allowed: a scorer may start past the document.
        if scorer.doc() > doc || scorer.seek(doc) != doc {
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

    fn seek(&mut self, target: DocId) -> DocId {
        if self.doc_id >= target {
            return self.doc_id;
        }
        self.doc_id = self.include.seek(target);
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
    use crate::spatial::executor::{PlanNode, SpatialRelation};
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
            // All at distance zero, the hits share the top rank.
            assert!(hits.iter().all(|(score, _)| *score == 1.0));
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
        // A join with a boost is narrowed too, and still matches.
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
            (vec![[0.0, 0.0], [1.0, 1.0]], SpatialPredicate::CoveredBy),
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
        // The line has no interior, but it meets the lines it crosses, and itself.
        assert_eq!(
            matching(&index, name, &intersects)?,
            ["box", "crossing", "line"]
        );
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
            for predicate in [SpatialPredicate::Intersects, SpatialPredicate::CoveredBy] {
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

    /// Merges every segment of the index into one.
    fn merge_all(index: &Index) -> crate::Result<()> {
        let mut writer: IndexWriter = index.writer_for_tests()?;
        writer.merge(&index.searchable_segment_ids()?).wait()?;
        writer.wait_merging_threads()
    }

    /// A query for the geometries within a few meters of a point.
    fn near(geometry: Field, [lon, lat]: [f64; 2]) -> SpatialQuery {
        SpatialQuery::with_predicate(geometry, vec![[lon, lat]], SpatialPredicate::Within(1e-6))
    }

    /// A query for the geometries that intersect a small box around a point.
    fn box_around(geometry: Field, [lon, lat]: [f64; 2]) -> SpatialQuery {
        let d = 0.01;
        SpatialQuery::intersects_bounds(geometry, [[lon - d, lat - d], [lon + d, lat + d]])
    }

    #[test]
    fn test_a_polygon_around_a_whole_cube_face_is_indexed_on_it() -> crate::Result<()> {
        // Face 0 lies within 45 degrees of (0, 0), so the polygon has no edges on it. The point
        // is in a segment of its own, so that it adds none either.
        let ring = [[-50.0, -50.0], [50.0, -50.0], [50.0, 50.0], [-50.0, 50.0]];
        let big = r#"{"type": "Polygon", "coordinates":
            [[[-50, -50], [50, -50], [50, 50], [-50, 50], [-50, -50]]]}"#;
        let origin = r#"{"type": "Point", "coordinates": [0, 0]}"#;
        let (index, name, geometry) = index_segments(&[&[("big", big)], &[("origin", origin)]])?;
        assert_eq!(
            matching(&index, name, &near(geometry, [0.0, 0.0]))?,
            ["big", "origin"]
        );
        assert_eq!(
            matching(&index, name, &box_around(geometry, [0.0, 0.0]))?,
            ["big", "origin"]
        );
        // As a query, the polygon covers the face too.
        let intersects =
            SpatialQuery::with_predicate(geometry, ring.to_vec(), SpatialPredicate::Intersects);
        assert_eq!(matching(&index, name, &intersects)?, ["big", "origin"]);
        let contains = SpatialQuery::new(geometry, ring.to_vec());
        assert!(matching(&index, name, &contains)?.contains(&"origin".to_string()));
        Ok(())
    }

    #[test]
    fn test_rings_of_either_orientation_bound_the_smaller_region() -> crate::Result<()> {
        let clockwise = r#"{"type": "Polygon", "coordinates":
            [[[0, 0], [0, 10], [10, 10], [10, 0], [0, 0]]]}"#;
        let (index, name, geometry) = index_segments(&[&[("clockwise", clockwise)]])?;
        assert_eq!(
            matching(&index, name, &box_around(geometry, [4.5, 4.5]))?,
            ["clockwise"]
        );
        assert!(matching(&index, name, &box_around(geometry, [50.0, 50.0]))?.is_empty());

        let inside = r#"{"type": "Point", "coordinates": [5, 5]}"#;
        let far = r#"{"type": "Point", "coordinates": [50, 50]}"#;
        let (index, name, _) = index_segments(&[&[("inside", inside), ("far", far)]])?;
        let query = "geometry:$intersects(0 0, 0 10, 10 10, 10 0)";
        assert_eq!(parse(&index, name, query)?, ["inside"]);
        Ok(())
    }

    #[test]
    fn test_a_ring_with_a_repeated_vertex_is_indexed() -> crate::Result<()> {
        let repeated = r#"{"type": "Polygon", "coordinates":
            [[[0, 0], [0, 0], [10, 0], [10, 10], [0, 10], [0, 0]]]}"#;
        let (index, name, geometry) = index_segments(&[&[("repeated", repeated)]])?;
        assert_eq!(
            matching(&index, name, &box_around(geometry, [4.5, 4.5]))?,
            ["repeated"]
        );
        assert!(matching(&index, name, &box_around(geometry, [50.0, 50.0]))?.is_empty());
        Ok(())
    }

    #[test]
    fn test_a_query_polygon_with_no_area_is_what_is_left_of_it() -> crate::Result<()> {
        let (index, name, geometry) = index_segments(&[&[("box", BOX)]])?;
        // A line inside the box, and the bounding box of a point inside it.
        let line = SpatialQuery::with_predicate(
            geometry,
            vec![[2.0, 2.0], [3.0, 3.0], [2.0, 2.0]],
            SpatialPredicate::Intersects,
        );
        assert_eq!(matching(&index, name, &line)?, ["box"]);
        let point = SpatialQuery::intersects_bounds(geometry, [[5.0, 5.0], [5.0, 5.0]]);
        assert_eq!(matching(&index, name, &point)?, ["box"]);
        Ok(())
    }

    #[test]
    fn test_query_coordinates_and_distances_are_checked() -> crate::Result<()> {
        let (index, name, geometry) = index_segments(&[&[("box", BOX)]])?;
        let searcher = index.reader()?.searcher();
        for (coordinates, predicate) in [
            (
                vec![[0.0, 0.0], [1.0, 0.0], [f64::INFINITY, 1.0]],
                SpatialPredicate::Intersects,
            ),
            (vec![[0.0, 95.0]], SpatialPredicate::Within(1e-6)),
            (vec![[f64::NAN, 0.0]], SpatialPredicate::Knn(1)),
            (vec![[0.0, 0.0]], SpatialPredicate::Within(f64::NAN)),
        ] {
            let query = SpatialQuery::with_predicate(geometry, coordinates, predicate);
            assert!(searcher.search(&query, &DocSetCollector).is_err());
        }
        let huge = format!("geometry:$intersects(0 0, 1 0, 1{} 1)", "0".repeat(400));
        let parser = crate::query::QueryParser::for_index(&index, vec![name]);
        assert!(searcher
            .search(&*parser.parse_query(&huge)?, &DocSetCollector)
            .is_err());
        Ok(())
    }

    #[test]
    fn test_a_line_beside_a_polygon_never_contains_a_cell_center() -> crate::Result<()> {
        use crate::spatial::cell_index_reader::CellIndexReader;

        let zigzag: Vec<String> = (0..200)
            .map(|i| {
                format!(
                    "[{}, {}]",
                    0.5 + i as f64 / 25.0,
                    1.0 + (i % 2) as f64 / 10.0
                )
            })
            .collect();
        let line = format!(
            r#"{{"type": "LineString", "coordinates": [{}]}}"#,
            zigzag.join(", ")
        );
        let (index, _, geometry) = index_segments(&[&[("box", BOX), ("line", &line)]])?;
        let searcher = index.reader()?.searcher();
        let spatial = searcher
            .segment_reader(0)
            .spatial_fields()
            .get_field(geometry)?
            .unwrap();
        let cells = CellIndexReader::open(spatial.cells_bytes())?;
        let edges = EdgeReader::<Sphere>::open(spatial.edges_bytes())?;
        let mut line_cells = 0;
        for cell in cells.iter() {
            for shape in &cell.shapes {
                let (_, set) = edges.read_geometry_set(shape.geometry_id.1);
                if !set.members[0].closed {
                    assert!(!shape.contains_center, "{:?}", cell.cell_id);
                    line_cells += 1;
                }
            }
        }
        assert!(line_cells > 1);
        Ok(())
    }

    #[test]
    fn test_merge_keeps_a_long_line_in_every_cell_it_crosses() -> crate::Result<()> {
        use crate::spatial::math::normalize;
        use crate::spatial::surface::Surface;

        let line = r#"{"type": "LineString", "coordinates": [[1, -1], [100, -1]]}"#;
        let points: Vec<String> = (0..80)
            .map(|i| {
                let lon = 30.0 + i as f64 / 8.0;
                format!(r#"{{"type": "Point", "coordinates": [{lon}, -0.3]}}"#)
            })
            .collect();
        let points: Vec<(&str, &str)> = points.iter().map(|p| ("point", p.as_str())).collect();
        let (index, name, geometry) = index_segments(&[&[("line", line)], &points])?;

        // Points on the line, from 10 to 42 degrees of longitude.
        let (a, b) = (Sphere::project(1.0, -1.0), Sphere::project(100.0, -1.0));
        let on_line: Vec<[f64; 2]> = [0.09, 0.19, 0.29, 0.34, 0.37, 0.41]
            .iter()
            .map(|&t| {
                let p = normalize(&[
                    a[0] + t * (b[0] - a[0]),
                    a[1] + t * (b[1] - a[1]),
                    a[2] + t * (b[2] - a[2]),
                ]);
                [p[1].atan2(p[0]).to_degrees(), p[2].asin().to_degrees()]
            })
            .collect();
        let finds_line = |index: &Index| -> crate::Result<bool> {
            for &point in &on_line {
                if !matching(index, name, &near(geometry, point))?.contains(&"line".to_string()) {
                    return Ok(false);
                }
            }
            Ok(true)
        };
        assert!(finds_line(&index)?);
        merge_all(&index)?;
        assert_eq!(index.searchable_segment_ids()?.len(), 1);
        assert!(finds_line(&index)?);
        Ok(())
    }

    #[test]
    fn test_points_and_lines_intersect_where_they_meet() -> crate::Result<()> {
        let point = r#"{"type": "Point", "coordinates": [5, 5]}"#;
        let line = r#"{"type": "LineString", "coordinates": [[5, 5], [6, 6]]}"#;
        let next = r#"{"type": "LineString", "coordinates": [[6, 6], [7, 5]]}"#;
        let on_equator = r#"{"type": "Point", "coordinates": [20.5, 0]}"#;
        let equator = r#"{"type": "LineString", "coordinates": [[20, 0], [21, 0]]}"#;
        let far = r#"{"type": "Point", "coordinates": [50, 50]}"#;
        let (index, name, _) = index_segments(&[
            &[("a", point), ("b", point), ("line", line)],
            &[
                ("next", next),
                ("on_equator", on_equator),
                ("equator", equator),
            ],
            &[("far", far)],
        ])?;
        let intersecting = |inner: &str| {
            parse(
                &index,
                name,
                &format!("geometry:$intersects($query(name:{inner}))"),
            )
        };
        assert_eq!(intersecting("a")?, ["b", "line"]);
        assert_eq!(intersecting("line")?, ["a", "b", "next"]);
        assert_eq!(intersecting("next")?, ["line"]);
        // A point on the interior of an edge.
        assert_eq!(intersecting("on_equator")?, ["equator"]);
        assert_eq!(intersecting("equator")?, ["on_equator"]);
        assert!(intersecting("far")?.is_empty());
        Ok(())
    }

    #[test]
    fn test_geometry_outside_a_spatial_field_is_not_indexed() -> crate::Result<()> {
        use crate::schema::{OwnedValue, FAST, TEXT};

        // Fast fields, and the fields of a JSON object, skip geometry.
        let mut schema_builder = Schema::builder();
        let number = schema_builder.add_u64_field("number", FAST);
        let text = schema_builder.add_text_field("text", STRING | FAST);
        let json = schema_builder.add_json_field("json", TEXT | FAST);
        let index = Index::create_in_ram(schema_builder.build());
        let mut writer: IndexWriter = index.writer_for_tests()?;
        let geometry = OwnedValue::Geometry(Geometry::Point([1.0, 2.0]));
        let mut doc = TantivyDocument::new();
        doc.add_field_value(number, &geometry);
        doc.add_field_value(text, &geometry);
        doc.add_field_value(
            json,
            &OwnedValue::Object(vec![("place".to_string(), geometry.clone())]),
        );
        writer.add_document(doc)?;
        writer.commit()?;
        assert_eq!(index.reader()?.searcher().num_docs(), 1);
        Ok(())
    }

    #[test]
    fn test_knn_finds_the_nearest_of_the_documents_its_siblings_allow() -> crate::Result<()> {
        let point = |lat: f64| format!(r#"{{"type": "Point", "coordinates": [0, {lat}]}}"#);
        let (a, b, cafe1, cafe2, cafe3) =
            (point(1.0), point(2.0), point(3.0), point(4.0), point(5.0));
        let (index, name, _) = index_segments(&[
            &[("a", &a), ("cafe2", &cafe2)],
            &[("b", &b), ("cafe1", &cafe1), ("cafe3", &cafe3)],
        ])?;
        assert_eq!(parse(&index, name, "geometry:$knn(2, 0 0)")?, ["a", "b"]);
        let cafes = "+(name:cafe1 name:cafe2 name:cafe3)";
        assert_eq!(
            parse(&index, name, &format!("{cafes} +geometry:$knn(2, 0 0)"))?,
            ["cafe1", "cafe2"]
        );
        assert_eq!(
            parse(&index, name, "+name:cafe3 +geometry:$knn(1, 0 0)")?,
            ["cafe3"]
        );
        assert_eq!(parse(&index, name, "-name:a geometry:$knn(1, 0 0)")?, ["b"]);
        Ok(())
    }

    #[test]
    fn test_contains_reads_from_the_field_side_with_coordinates_and_joins() -> crate::Result<()> {
        let small = r#"{"type": "Polygon", "coordinates":
            [[[4, 4], [5, 4], [5, 5], [4, 5], [4, 4]]]}"#;
        let halves = r#"{"type": "MultiPolygon", "coordinates": [
            [[[20, 0], [25, 0], [25, 5], [20, 5], [20, 0]]],
            [[[25, 0], [30, 0], [30, 5], [25, 5], [25, 0]]]]}"#;
        let point = r#"{"type": "Point", "coordinates": [4.5, 4.5]}"#;
        let (index, name, _) = index_segments(&[
            &[("big", BOX), ("small", small)],
            &[("halves", halves), ("point", point)],
        ])?;
        let found = |query: &str| parse(&index, name, query);
        assert_eq!(
            found("geometry:$contains(4.2 4.2, 4.8 4.2, 4.8 4.8, 4.2 4.8)")?,
            ["big", "small"]
        );
        assert_eq!(found("geometry:$contains(1 1, 6 1, 6 6, 1 6)")?, ["big"]);
        // A polygon contains itself.
        assert_eq!(
            found("geometry:$contains(4 4, 5 4, 5 5, 4 5)")?,
            ["big", "small"]
        );
        assert_eq!(found("geometry:$contains($query(name:small))")?, ["big"]);
        // The members of a set may contain the polygon jointly.
        assert_eq!(
            found("geometry:$contains(24 1, 26 1, 26 2, 24 2)")?,
            ["halves"]
        );
        assert!(found("geometry:$contains(9 9, 11 9, 11 11, 9 11)")?.is_empty());
        // `$within` a polygon is the other way around.
        assert_eq!(
            found("geometry:$within(3 3, 6 3, 6 6, 3 6)")?,
            ["point", "small"]
        );
        assert_eq!(
            found("geometry:$within(-1 -1, 11 -1, 11 11, -1 11)")?,
            ["big", "point", "small"]
        );
        Ok(())
    }

    #[test]
    fn test_oriented_rings_may_bound_more_than_a_hemisphere() -> crate::Result<()> {
        // On an oriented field, a clockwise ring around the box bounds the rest of the sphere.
        let mut schema_builder = Schema::builder();
        let name = schema_builder.add_text_field("name", STRING | STORED);
        let geometry = schema_builder.add_spatial_field("geometry", SPATIAL.set_oriented());
        let index = Index::create_in_ram(schema_builder.build());
        let mut writer: IndexWriter = index.writer_for_tests()?;
        let everything_else = r#"{"type": "Polygon", "coordinates":
            [[[0, 0], [0, 10], [10, 10], [10, 0], [0, 0]]]}"#;
        let point =
            |lon: f64, lat: f64| format!(r#"{{"type": "Point", "coordinates": [{lon}, {lat}]}}"#);
        for (doc_name, doc_geometry) in [
            ("everything_else", everything_else.to_string()),
            ("inside", point(5.0, 5.0)),
            ("far", point(50.0, 50.0)),
            ("antipode", point(-175.0, -5.0)),
        ] {
            let json = format!(r#"{{"name": "{doc_name}", "geometry": {doc_geometry}}}"#);
            writer.add_document(TantivyDocument::parse_json(&index.schema(), &json)?)?;
        }
        writer.commit()?;

        let near_everything_else = |at: [f64; 2]| -> crate::Result<bool> {
            Ok(matching(&index, name, &near(geometry, at))?
                .contains(&"everything_else".to_string()))
        };
        assert!(!near_everything_else([5.0, 5.0])?);
        assert!(near_everything_else([50.0, 50.0])?);
        assert!(near_everything_else([-175.0, -5.0])?);

        // Query polygons on the field are read the same way.
        let clockwise = vec![[0.0, 0.0], [0.0, 10.0], [10.0, 10.0], [10.0, 0.0]];
        let contained = SpatialQuery::new(geometry, clockwise.clone());
        assert_eq!(
            matching(&index, name, &contained)?,
            ["antipode", "everything_else", "far"]
        );
        let parsed = parse(&index, name, "geometry:$within(0 0, 0 10, 10 10, 10 0)")?;
        assert_eq!(parsed, ["antipode", "everything_else", "far"]);
        // A query may read its polygon otherwise.
        let smaller = contained.with_ring_orientation(RingOrientation::Smaller);
        assert_eq!(matching(&index, name, &smaller)?, ["inside"]);
        Ok(())
    }

    #[test]
    fn test_a_query_polygon_may_be_oriented_on_any_field() -> crate::Result<()> {
        let inside = r#"{"type": "Point", "coordinates": [5, 5]}"#;
        let far = r#"{"type": "Point", "coordinates": [50, 50]}"#;
        let (index, name, geometry) = index_segments(&[&[("inside", inside), ("far", far)]])?;
        let clockwise = vec![[0.0, 0.0], [0.0, 10.0], [10.0, 10.0], [10.0, 0.0]];
        let query = SpatialQuery::with_predicate(geometry, clockwise, SpatialPredicate::Intersects);
        assert_eq!(matching(&index, name, &query)?, ["inside"]);
        let oriented = query.with_ring_orientation(RingOrientation::RightHand);
        assert_eq!(matching(&index, name, &oriented)?, ["far"]);
        Ok(())
    }

    #[test]
    fn test_knn_lifts_the_nearest_documents_of_a_boolean_query() -> crate::Result<()> {
        // Without the lift, the tie on the text clauses goes to the first document, b.
        let point = |lat: f64| format!(r#"{{"type": "Point", "coordinates": [0, {lat}]}}"#);
        let (b, a) = (point(2.0), point(1.0));
        let (index, name, _) = index_segments(&[&[("b", &b), ("a", &a)]])?;
        let searcher = index.reader()?.searcher();
        let parser = crate::query::QueryParser::for_index(&index, vec![name]);
        for query in [
            "name:a name:b geometry:$knn(1, 0 0)",
            "name:a name:b geometry:$knn(1, 0 0)^2",
        ] {
            let query = parser.parse_query(query)?;
            let hits = searcher.search(&*query, &TopDocs::with_limit(2).order_by_score())?;
            assert_eq!(name_of(&searcher, name, hits[0].1)?, "a");
            assert!(hits[0].0 > hits[1].0);
        }
        Ok(())
    }

    #[test]
    fn test_knn_is_restricted_however_it_is_boosted_or_nested() -> crate::Result<()> {
        let point = |lat: f64| format!(r#"{{"type": "Point", "coordinates": [0, {lat}]}}"#);
        let (a, b, cafe1, cafe2, cafe3) =
            (point(1.0), point(2.0), point(3.0), point(4.0), point(5.0));
        let (index, name, _) = index_segments(&[
            &[("a", &a), ("cafe2", &cafe2)],
            &[("b", &b), ("cafe1", &cafe1), ("cafe3", &cafe3)],
        ])?;
        assert_eq!(
            parse(&index, name, "+name:cafe3 +geometry:$knn(1, 0 0)^2")?,
            ["cafe3"]
        );
        assert_eq!(
            parse(&index, name, "+name:cafe3 +(geometry:$knn(1, 0 0) name:b)")?,
            ["cafe3"]
        );
        // The nearest of cafe1 and cafe3 is cafe1, so cafe3 is left.
        let cafes = "(name:cafe1 OR name:cafe3)";
        for query in [
            format!("+{cafes} -geometry:$knn(1, 0 0)"),
            format!("+{cafes} -geometry:$knn(1, 0 0)^2"),
        ] {
            assert_eq!(parse(&index, name, &query)?, ["cafe3"], "{query}");
        }
        Ok(())
    }

    #[test]
    fn test_knn_without_a_searcher_keeps_the_nearest_of_each_segment() -> crate::Result<()> {
        let point = |lat: f64| format!(r#"{{"type": "Point", "coordinates": [0, {lat}]}}"#);
        let (a, b, c, d) = (point(1.0), point(2.0), point(3.0), point(4.0));
        let (index, name, _) = index_segments(&[&[("a", &a), ("c", &c)], &[("b", &b), ("d", &d)]])?;
        let parser = crate::query::QueryParser::for_index(&index, vec![name]);
        let mut writer: IndexWriter = index.writer_for_tests()?;
        writer.delete_query(parser.parse_query("geometry:$knn(1, 0 0)")?)?;
        writer.commit()?;
        assert_eq!(parse(&index, name, "*")?, ["c", "d"]);
        Ok(())
    }

    #[test]
    fn test_polygons_with_no_area_and_spikes_are_found() -> crate::Result<()> {
        let dot = r#"{"type": "Polygon", "coordinates":
            [[[10, 20], [10, 20], [10, 20], [10, 20]]]}"#;
        let sliver =
            r#"{"type": "Polygon", "coordinates": [[[30, 0], [31, 1], [30, 0], [30, 0]]]}"#;
        let spiked = r#"{"type": "Polygon", "coordinates":
            [[[0, 0], [10, 0], [14, 0], [10, 0], [10, 10], [0, 10], [0, 0]]]}"#;
        let (index, name, geometry) =
            index_segments(&[&[("dot", dot), ("sliver", sliver), ("spiked", spiked)]])?;
        assert_eq!(
            matching(&index, name, &near(geometry, [10.0, 20.0]))?,
            ["dot"]
        );
        assert_eq!(
            matching(&index, name, &near(geometry, [31.0, 1.0]))?,
            ["sliver"]
        );
        assert_eq!(
            matching(&index, name, &near(geometry, [14.0, 0.0]))?,
            ["spiked"]
        );
        // The spike bounds no area: the polygon is the square.
        assert_eq!(
            matching(&index, name, &box_around(geometry, [5.0, 5.0]))?,
            ["spiked"]
        );
        assert!(matching(&index, name, &box_around(geometry, [12.0, 5.0]))?.is_empty());
        Ok(())
    }

    #[test]
    fn test_a_polygon_with_no_rings_is_empty() -> crate::Result<()> {
        let values = r#"[{"type": "Point", "coordinates": [1, 1]},
            {"type": "Polygon", "coordinates": []}]"#;
        let (index, name, geometry) = index_segments(&[&[("point", values)]])?;
        let around = SpatialQuery::from_bounds(geometry, [[0.0, 0.0], [2.0, 2.0]]);
        assert_eq!(matching(&index, name, &around)?, ["point"]);
        Ok(())
    }

    #[test]
    fn test_a_self_crossing_ring_bounds_its_lobes() -> crate::Result<()> {
        let bow_tie = r#"{"type": "Polygon", "coordinates":
            [[[0, 0], [2, 2], [2, 0], [0, 2], [0, 0]]]}"#;
        let (index, name, geometry) = index_segments(&[&[("bow_tie", bow_tie)]])?;
        assert_eq!(
            matching(&index, name, &near(geometry, [1.8, 1.0]))?,
            ["bow_tie"]
        );
        assert!(matching(&index, name, &near(geometry, [100.0, 50.0]))?.is_empty());
        assert!(matching(&index, name, &near(geometry, [-120.0, -60.0]))?.is_empty());
        Ok(())
    }

    #[test]
    fn test_boundaries_meeting_inside_edges_are_contact() -> crate::Result<()> {
        // a and b touch the box's boundary along parts of its edges and at T-junctions.
        let a = r#"{"type": "Polygon", "coordinates": [[[0, 0], [5, 0], [5, 5], [0, 5], [0, 0]]]}"#;
        let b =
            r#"{"type": "Polygon", "coordinates": [[[5, 0], [10, 0], [10, 5], [5, 5], [5, 0]]]}"#;
        let (index, name, _) = index_segments(&[&[("box", BOX), ("a", a), ("b", b)]])?;
        assert_eq!(
            parse(&index, name, "geometry:$within(0 0, 10 0, 10 10, 0 10)")?,
            ["a", "b", "box"]
        );
        assert_eq!(
            parse(
                &index,
                name,
                "geometry:$within(0 0, 5 0, 10 0, 10 10, 0 10, 0 5)"
            )?,
            ["a", "b", "box"]
        );
        assert_eq!(
            parse(&index, name, "geometry:$contains($query(name:a))")?,
            ["box"]
        );

        let line = |coordinates: &str| {
            format!(r#"{{"type": "LineString", "coordinates": {coordinates}}}"#)
        };
        let (equator, south, overlap, bottom) = (
            line("[[0, 0], [10, 0]]"),
            line("[[5, 0], [5, -5]]"),
            line("[[2, 0], [5, 0]]"),
            line("[[0, 0], [10, 0]]"),
        );
        let (index, name, _) = index_segments(&[
            &[
                ("equator", &equator),
                ("south", &south),
                ("overlap", &overlap),
            ],
            &[("box", BOX), ("bottom", &bottom)],
        ])?;
        assert_eq!(
            parse(&index, name, "geometry:$intersects($query(name:equator))")?,
            ["bottom", "box", "overlap", "south"]
        );
        // A line along a polygon's edge is within it, and intersects it.
        assert_eq!(
            parse(&index, name, "geometry:$intersects(0 0, 10 0, 10 10, 0 10)")?,
            ["bottom", "box", "equator", "overlap", "south"]
        );
        Ok(())
    }

    #[test]
    fn test_oriented_query_rings_bound_more_than_a_hemisphere_in_contains() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let name = schema_builder.add_text_field("name", STRING | STORED);
        let geometry = schema_builder.add_spatial_field("geometry", SPATIAL.set_oriented());
        let index = Index::create_in_ram(schema_builder.build());
        let mut writer: IndexWriter = index.writer_for_tests()?;
        // Everything but a small box, and a box that covers the small one.
        let all_but =
            r#"{"type": "Polygon", "coordinates": [[[4, 4], [4, 6], [6, 6], [6, 4], [4, 4]]]}"#;
        let big = r#"{"type": "Polygon", "coordinates": [[[-5, -5], [15, -5], [15, 15], [-5, 15], [-5, -5]]]}"#;
        for (doc_name, doc_geometry) in [("all_but", all_but), ("big", big)] {
            let json = format!(r#"{{"name": "{doc_name}", "geometry": {doc_geometry}}}"#);
            writer.add_document(TantivyDocument::parse_json(&index.schema(), &json)?)?;
        }
        writer.commit()?;
        let inside_box = SpatialQuery::new(
            geometry,
            vec![[0.0, 0.0], [10.0, 0.0], [10.0, 10.0], [0.0, 10.0]],
        );
        assert!(matching(&index, name, &inside_box)?.is_empty());
        // The world but the box [0, 10]²: big covers part of the box.
        assert!(parse(&index, name, "geometry:$within(0 0, 0 10, 10 10, 10 0)")?.is_empty());
        Ok(())
    }

    #[test]
    fn test_knn_restriction_does_not_depend_on_how_the_query_is_written() -> crate::Result<()> {
        let point = |lat: f64| format!(r#"{{"type": "Point", "coordinates": [0, {lat}]}}"#);
        let (a, b, cafe3) = (point(1.0), point(2.0), point(5.0));
        let (index, name, _) = index_segments(&[&[("a", &a), ("b", &b), ("cafe3", &cafe3)]])?;
        // The nearest cafe3 is cafe3, however the clause that requires it is written.
        for query in [
            "+name:cafe3 -geometry:$knn(1, 0 0)",
            "name:cafe3 -geometry:$knn(1, 0 0)",
        ] {
            assert!(parse(&index, name, query)?.is_empty(), "{query}");
        }
        let nested =
            "+(+name:cafe3 +geometry:$within(1000km, $query(name:a))) +geometry:$knn(1, 0 0)";
        assert_eq!(parse(&index, name, nested)?, ["cafe3"]);
        let parser = crate::query::QueryParser::for_index(&index, vec![name]);
        let (lenient, errors) = parser.parse_query_lenient(nested);
        assert!(errors.is_empty());
        assert_eq!(matching(&index, name, &*lenient)?, ["cafe3"]);
        Ok(())
    }
}

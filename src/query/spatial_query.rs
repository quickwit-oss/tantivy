//! Spatial polygon query.
//!
//! Finds indexed geometries that match a query polygon via contains or intersects predicates. The
//! query polygon is specified as lon/lat vertices, projected onto the surface configured for the
//! field, and searched against each segment's cell index and edge index.

use common::BitSet;

use crate::query::explanation::does_not_match;
use crate::query::{BitSetDocSet, Explanation, Query, Scorer, Weight};
use crate::schema::{Field, FieldType};
use crate::spatial::cell_index_reader::CellIndexReader;
use crate::spatial::geometry::Geometry;
use crate::spatial::plane::Plane;
use crate::spatial::spatial_index_manager::PreparedSpatialQuery;
use crate::{DocId, DocSet, Score, TERMINATED};

/// The spatial predicate to apply.
#[derive(Clone, Debug)]
pub enum SpatialPredicate {
    /// Return geometries contained by the query polygon.
    Contains,
    /// Return geometries that intersect the query polygon.
    Intersects,
    /// Return geometries entirely inside the query polygon (ST_Within).
    CoveredBy,
    /// Return geometries within a distance of a query polygon. Distance in radians.
    DistanceWithin(f64),
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
        let mut ring: Vec<[f64; 2]> = self.coordinates.clone();
        if ring.first() != ring.last() {
            ring.push(ring[0]);
        }
        let plane_geometry = Geometry::<Plane>::Polygon(vec![ring]);

        let prepared: Box<dyn PreparedSpatialQuery> = match &self.predicate {
            _ => {
                let searcher = enable_scoring
                    .searcher()
                    .expect("searcher required for spatial query");
                let schema = searcher.index().schema();
                let field_entry = schema.get_field_entry(self.field);
                let spatial_opts = match field_entry.field_type() {
                    FieldType::Spatial(opts) => opts,
                    _ => panic!("field is not spatial"),
                };
                let manager = searcher.index().spatial_indices();
                let spatial_index = manager
                    .get(spatial_opts.spatial_index_name())
                    .unwrap_or_else(|| {
                        panic!(
                            "spatial index '{}' not registered",
                            spatial_opts.spatial_index_name()
                        )
                    });
                match &self.predicate {
                    SpatialPredicate::Intersects => {
                        spatial_index.prepare_intersects(&plane_geometry)
                    }
                    SpatialPredicate::Contains => spatial_index.prepare_contains(&plane_geometry),
                    SpatialPredicate::CoveredBy => spatial_index.prepare_within(&plane_geometry),
                    SpatialPredicate::DistanceWithin(radians) => {
                        spatial_index.prepare_distance(&plane_geometry, *radians)
                    }
                }
            }
        };
        Ok(Box::new(SpatialWeight {
            field: self.field,
            query: prepared,
        }))
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

        let include = self.query.search_segment_bytes(
            spatial_reader.cells_bytes(),
            spatial_reader.edges_bytes(),
            spatial_reader.doc_ids_bytes(),
            reader.max_doc(),
        );

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

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
use crate::spatial::contains_query::ContainsQuery;
use crate::spatial::edge_reader::EdgeReader;
use crate::spatial::intersects_query::IntersectsQuery;
use crate::spatial::region_coverer::CovererOptions;
use crate::{DocId, DocSet, Score, TERMINATED};

/// Converts longitude/latitude in degrees to a unit sphere point.
fn lonlat_to_sphere(lon: f64, lat: f64) -> [f64; 3] {
    let lat = lat.to_radians();
    let lon = lon.to_radians();
    let cos_lat = lat.cos();
    [cos_lat * lon.cos(), cos_lat * lon.sin(), lat.sin()]
}

/// The spatial predicate to apply.
#[derive(Clone, Debug)]
pub enum SpatialPredicate {
    /// Return geometries contained by the query polygon.
    Contains,
    /// Return geometries that intersect the query polygon.
    Intersects,
}

/// Spatial polygon query.
#[derive(Clone, Debug)]
pub struct SpatialQuery {
    field: Field,
    polygon: Vec<[f64; 2]>,
    predicate: SpatialPredicate,
}

impl SpatialQuery {
    /// Create a contains query from a polygon specified as lon/lat vertices.
    pub fn new(field: Field, polygon: Vec<[f64; 2]>) -> Self {
        SpatialQuery {
            field,
            polygon,
            predicate: SpatialPredicate::Contains,
        }
    }

    /// Create a spatial query with the given predicate.
    pub fn with_predicate(
        field: Field,
        polygon: Vec<[f64; 2]>,
        predicate: SpatialPredicate,
    ) -> Self {
        SpatialQuery {
            field,
            polygon,
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
            polygon,
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
            polygon,
            predicate: SpatialPredicate::Intersects,
        }
    }
}

impl Query for SpatialQuery {
    fn weight(
        &self,
        _enable_scoring: super::EnableScoring<'_>,
    ) -> crate::Result<Box<dyn super::Weight>> {
        let vertices: Vec<[f64; 3]> = self
            .polygon
            .iter()
            .map(|p| lonlat_to_sphere(p[0], p[1]))
            .collect();
        let prepared: Box<dyn PreparedSpatialQuery> = match self.predicate {
            SpatialPredicate::Contains => {
                Box::new(ContainsQuery::new(vertices, CovererOptions::default()))
            }
            SpatialPredicate::Intersects => {
                Box::new(IntersectsQuery::new(vertices, CovererOptions::default()))
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
        edge_reader: &mut EdgeReader<'a>,
    ) -> Vec<u32>;
}

impl PreparedSpatialQuery for ContainsQuery {
    fn search_segment<'a>(
        &self,
        cell_reader: &'a CellIndexReader<'a>,
        edge_reader: &mut EdgeReader<'a>,
    ) -> Vec<u32> {
        ContainsQuery::search_segment(self, cell_reader, edge_reader)
    }
}

impl PreparedSpatialQuery for IntersectsQuery {
    fn search_segment<'a>(
        &self,
        cell_reader: &'a CellIndexReader<'a>,
        edge_reader: &mut EdgeReader<'a>,
    ) -> Vec<u32> {
        IntersectsQuery::search_segment(self, cell_reader, edge_reader)
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

        let cell_reader = CellIndexReader::open(spatial_reader.cells_bytes());
        let mut edge_reader = EdgeReader::open(spatial_reader.edges_bytes(), 100_000);

        let doc_ids = self.query.search_segment(&cell_reader, &mut edge_reader);

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

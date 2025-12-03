//! HUSH

use common::BitSet;

use crate::query::explanation::does_not_match;
use crate::query::{BitSetDocSet, Explanation, Query, Scorer, Weight};
use crate::schema::Field;
use crate::spatial::bkd::{search_intersects, Segment};
use crate::spatial::point::GeoPoint;
use crate::spatial::writer::as_point_i32;
use crate::{DocId, DocSet, Score, TantivyError, TERMINATED};

#[derive(Clone, Copy, Debug)]
/// HUSH
pub enum SpatialQueryType {
    /// HUSH
    Intersects,
    //      Within,
    // Contains,
}

#[derive(Clone, Copy, Debug)]
/// HUSH
pub struct SpatialQuery {
    field: Field,
    bounds: [(i32, i32); 2],
    query_type: SpatialQueryType,
}

impl SpatialQuery {
    /// HUSH
    pub fn new(field: Field, bounds: [GeoPoint; 2], query_type: SpatialQueryType) -> Self {
        SpatialQuery {
            field,
            bounds: [as_point_i32(bounds[0]), as_point_i32(bounds[1])],
            query_type,
        }
    }
}

impl Query for SpatialQuery {
    fn weight(
        &self,
        _enable_scoring: super::EnableScoring<'_>,
    ) -> crate::Result<Box<dyn super::Weight>> {
        Ok(Box::new(SpatialWeight::new(
            self.field,
            self.bounds,
            self.query_type,
        )))
    }
}

pub struct SpatialWeight {
    field: Field,
    bounds: [(i32, i32); 2],
    query_type: SpatialQueryType,
}

impl SpatialWeight {
    fn new(field: Field, bounds: [(i32, i32); 2], query_type: SpatialQueryType) -> Self {
        SpatialWeight {
            field,
            bounds,
            query_type,
        }
    }
}

impl Weight for SpatialWeight {
    fn scorer(
        &self,
        reader: &crate::SegmentReader,
        boost: crate::Score,
    ) -> crate::Result<Box<dyn super::Scorer>> {
        let spatial_reader = reader
            .spatial_fields()
            .get_field(self.field)?
            .ok_or_else(|| TantivyError::SchemaError(format!("No spatial data for field")))?;
        let block_kd_tree = Segment::new(spatial_reader.get_bytes());
        match self.query_type {
            SpatialQueryType::Intersects => {
                let mut include = BitSet::with_max_value(reader.max_doc());
                search_intersects(
                    &block_kd_tree,
                    block_kd_tree.root_offset,
                    &[
                        self.bounds[0].1,
                        self.bounds[0].0,
                        self.bounds[1].1,
                        self.bounds[1].0,
                    ],
                    &mut include,
                )?;
                Ok(Box::new(SpatialScorer::new(boost, include, None)))
            }
        }
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
        let query_type_desc = match self.query_type {
            SpatialQueryType::Intersects => "SpatialQuery::Intersects",
        };
        let score = scorer.score();
        let mut explanation = Explanation::new(query_type_desc, score);
        explanation.add_context(format!(
            "bounds: [({}, {}), ({}, {})]",
            self.bounds[0].0, self.bounds[0].1, self.bounds[1].0, self.bounds[1].1,
        ));
        explanation.add_context(format!("field: {:?}", self.field));
        Ok(explanation)
    }
}

struct SpatialScorer {
    include: BitSetDocSet,
    exclude: Option<BitSet>,
    doc_id: DocId,
    score: Score,
}

impl SpatialScorer {
    pub fn new(score: Score, include: BitSet, exclude: Option<BitSet>) -> Self {
        let mut scorer = SpatialScorer {
            include: BitSetDocSet::from(include),
            exclude,
            doc_id: 0,
            score,
        };
        scorer.prime();
        scorer
    }
    fn prime(&mut self) {
        self.doc_id = self.include.doc();
        while self.exclude() {
            self.doc_id = self.include.advance();
        }
    }

    fn exclude(&self) -> bool {
        if self.doc_id == TERMINATED {
            return false;
        }
        match &self.exclude {
            Some(exclude) => exclude.contains(self.doc_id),
            None => false,
        }
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
        while self.exclude() {
            self.doc_id = self.include.advance();
        }
        self.doc_id
    }

    fn size_hint(&self) -> u32 {
        match &self.exclude {
            Some(exclude) => self.include.size_hint() - exclude.len() as u32,
            None => self.include.size_hint(),
        }
    }

    fn doc(&self) -> DocId {
        self.doc_id
    }
}

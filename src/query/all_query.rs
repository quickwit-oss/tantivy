use query::Query;
use query::Weight;
use query::Scorer;
use core::SegmentReader;
use Result;
use DocSet;
use Score;
use DocId;
use std::any::Any;
use core::Searcher;

/// Query that matches all of the documents.
///
/// All of the document get the score 1f32.
#[derive(Debug)]
pub struct AllQuery;

impl Query for AllQuery {
    fn as_any(&self) -> &Any {
        self
    }

    fn weight(&self, _: &Searcher, _: bool) -> Result<Box<Weight>> {
        Ok(box AllWeight)
    }
}

/// Weight associated to the `AllQuery` query.
pub struct AllWeight;

impl Weight for AllWeight {
    fn scorer<'a>(&'a self, reader: &'a SegmentReader) -> Result<Box<Scorer + 'a>> {
        Ok(box AllScorer {
            started: false,
            doc: 0u32,
            max_doc: reader.max_doc(),
        })
    }
}

/// Scorer associated to the `AllQuery` query.
pub struct AllScorer {
    started: bool,
    doc: DocId,
    max_doc: DocId,
}

impl DocSet for AllScorer {
    fn advance(&mut self) -> bool {
        if self.started {
            self.doc += 1u32;
        } else {
            self.started = true;
        }
        self.doc < self.max_doc
    }

    fn doc(&self) -> DocId {
        self.doc
    }

    fn size_hint(&self) -> u32 {
        self.max_doc
    }
}

impl Scorer for AllScorer {
    fn score(&mut self) -> Score {
        1f32
    }
}

use query::Query;
use query::Weight;
use query::Scorer;
use core::SegmentReader;
use docset::DocSet;
use Result;
use Score;
use DocId;
use core::Searcher;
use fastfield::DeleteBitSet;

/// Query that matches all of the documents.
///
/// All of the document get the score 1f32.
#[derive(Debug)]
pub struct AllQuery;

impl Query for AllQuery {
    fn weight(&self, _: &Searcher, _: bool) -> Result<Box<Weight>> {
        Ok(box AllWeight)
    }
}

/// Weight associated to the `AllQuery` query.
pub struct AllWeight;

impl Weight for AllWeight {
    fn scorer(&self, reader: &SegmentReader) -> Result<Box<Scorer>> {
        Ok(box AllScorer {
            state: State::NotStarted,
            doc: 0u32,
            max_doc: reader.max_doc(),
            deleted_bitset: reader.delete_bitset().clone()
        })
    }
}

enum State {
    NotStarted,
    Started,
    Finished
}

/// Scorer associated to the `AllQuery` query.
pub struct AllScorer {
    state: State,
    doc: DocId,
    max_doc: DocId,
    deleted_bitset: DeleteBitSet
}

impl DocSet for AllScorer {
    fn advance(&mut self) -> bool {
        loop {
            match self.state {
                State::NotStarted => {
                    self.state = State::Started;
                    self.doc = 0;
                }
                State::Started => {
                    self.doc += 1u32;
                }
                State::Finished => {
                    return false;
                }
            }
            if self.doc < self.max_doc {
                if !self.deleted_bitset.is_deleted(self.doc) {
                    return true;
                }
            } else {
                self.state = State::Finished;
                return false;
            }
        }
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

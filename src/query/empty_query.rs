use super::Scorer;
use crate::query::explanation::does_not_match;
use crate::query::Weight;
use crate::query::{Explanation, Query};
use crate::DocId;
use crate::DocSet;
use crate::Result;
use crate::Score;
use crate::Searcher;
use crate::SegmentReader;

/// `EmptyQuery` is a dummy `Query` in which no document matches.
///
/// It is useful for tests and handling edge cases.
#[derive(Clone, Debug)]
pub struct EmptyQuery;

impl Query for EmptyQuery {
    fn weight<'a, 'b>(&'a self, _searcher: &'b Searcher, _scoring_enabled: bool) -> Result<Box<dyn Weight + 'a>> {
        Ok(Box::new(EmptyWeight))
    }

    fn count(&self, _searcher: &Searcher) -> Result<usize> {
        Ok(0)
    }
}

/// `EmptyWeight` is a dummy `Weight` in which no document matches.
///
/// It is useful for tests and handling edge cases.
pub struct EmptyWeight;
impl Weight for EmptyWeight {
    fn scorer(&self, _reader: &SegmentReader) -> Result<Box<dyn Scorer>> {
        Ok(Box::new(EmptyScorer))
    }

    fn explain(&self, _reader: &SegmentReader, doc: DocId) -> Result<Explanation> {
        Err(does_not_match(doc))
    }
}

/// `EmptyScorer` is a dummy `Scorer` in which no document matches.
///
/// It is useful for tests and handling edge cases.
pub struct EmptyScorer;

impl DocSet for EmptyScorer {
    fn advance(&mut self) -> bool {
        false
    }

    fn doc(&self) -> DocId {
        panic!(
            "You may not call .doc() on a scorer \
             where the last call to advance() did not return true."
        );
    }

    fn size_hint(&self) -> u32 {
        0
    }
}

impl Scorer for EmptyScorer {
    fn score(&mut self) -> Score {
        0f32
    }
}

#[cfg(test)]
mod tests {
    use crate::query::EmptyScorer;
    use crate::DocSet;

    #[test]
    fn test_empty_scorer() {
        let mut empty_scorer = EmptyScorer;
        assert!(!empty_scorer.advance());
    }

    #[test]
    #[should_panic]
    fn test_empty_scorer_panic_on_doc_call() {
        EmptyScorer.doc();
    }
}

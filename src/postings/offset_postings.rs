use postings::Postings;
use postings::SegmentPostings;
use postings::SkipResult;
use postings::DocSet;
use postings::HasLen;
use DocId;

/// Wraps a posting object and offset all of the doc id with a given offset.
///
/// Assuming the original posting list is `0, 5, 7, 8...`, and the offset is `3`
/// the `OffsetPostings` becomes  `3, 8, 10, 11...`.
pub struct OffsetPostings<'a> {
    underlying: SegmentPostings<'a>,
    offset: DocId,
}

impl<'a> OffsetPostings<'a> {
    /// Constructor
    pub fn new(underlying: SegmentPostings<'a>, offset: DocId) -> OffsetPostings {
        OffsetPostings {
            underlying: underlying,
            offset: offset,
        }
    }
}

impl<'a> DocSet for OffsetPostings<'a> {
    fn advance(&mut self) -> bool {
        self.underlying.advance()
    }

    fn doc(&self) -> DocId {
        self.underlying.doc() + self.offset
    }

    fn skip_next(&mut self, target: DocId) -> SkipResult {
        if target >= self.offset {
            SkipResult::OverStep
        } else {
            self.underlying.skip_next(target - self.offset)
        }
    }
}

impl<'a> HasLen for OffsetPostings<'a> {
    fn len(&self) -> usize {
        self.underlying.len()
    }
}

impl<'a> Postings for OffsetPostings<'a> {
    fn term_freq(&self) -> u32 {
        self.underlying.term_freq()
    }

    fn positions(&self) -> &[u32] {
        self.underlying.positions()
    }
}
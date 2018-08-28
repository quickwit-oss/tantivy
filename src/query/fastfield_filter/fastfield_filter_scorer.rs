use query::Scorer;
use fastfield::FastFieldReader;
use DocId;
use DocSet;
use query::fastfield_filter::RangeU64;

pub(crate) struct FastFieldFilterScorer {
    fastfield_reader: FastFieldReader<u64>,
    range: RangeU64,
    max_doc: DocId,
    doc: DocId,
}

impl FastFieldFilterScorer {
    pub fn new(fastfield_reader: FastFieldReader<u64>,
           range: RangeU64,
           max_doc: DocId) -> FastFieldFilterScorer {
        FastFieldFilterScorer {
            fastfield_reader,
            range,
            max_doc,
            doc: 0u32,
        }
    }

    fn within_range(&self, doc: DocId) -> bool {
        let val = self.fastfield_reader.get(doc);
        self.range.contains(val)
    }

}

impl DocSet for FastFieldFilterScorer {
    fn advance(&mut self) -> bool {
        for doc in (self.doc + 1)..self.max_doc {
            if self.within_range(doc) {
                self.doc = doc;
                return true;
            }
        }
        self.doc = self.max_doc;
        return false;
    }

    fn doc(&self) -> u32 {
        self.doc
    }

    fn size_hint(&self) -> u32 {
        self.max_doc
    }
}

impl Scorer for FastFieldFilterScorer {
    fn score(&mut self) -> f32 {
        1f32
    }
}

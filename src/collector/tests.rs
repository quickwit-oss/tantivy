use super::*;
use core::SegmentReader;
use fastfield::BytesFastFieldReader;
use fastfield::FastFieldReader;
use schema::Field;
use DocAddress;
use DocId;
use Score;
use SegmentLocalId;

/// Stores all of the doc ids.
/// This collector is only used for tests.
/// It is unusable in pr
///
/// actise, as it does not store
/// the segment ordinals
pub struct TestCollector;

pub struct TestSegmentCollector {
    segment_id: SegmentLocalId,
    fruit: TestFruit,
}

#[derive(Default)]
pub struct TestFruit {
    docs: Vec<DocAddress>,
    scores: Vec<Score>,
}

impl TestFruit {
    /// Return the list of matching documents exhaustively.
    pub fn docs(&self) -> &[DocAddress] {
        &self.docs[..]
    }

    pub fn scores(&self) -> &[Score] {
        &self.scores[..]
    }
}

impl Collector for TestCollector {
    type Fruit = TestFruit;
    type SegmentFruit = Self::Fruit;
    type Child = TestSegmentCollector;

    fn for_segment(
        &self,
        segment_id: SegmentLocalId,
        _reader: &SegmentReader,
    ) -> Result<TestSegmentCollector> {
        Ok(TestSegmentCollector {
            segment_id,
            fruit: TestFruit::default(),
        })
    }

    fn requires_scoring(&self) -> bool {
        true
    }

    fn merge_fruits(&self, mut children: Vec<TestFruit>) -> Result<TestFruit> {
        children.sort_by_key(|fruit| {
            if fruit.docs().is_empty() {
                0
            } else {
                fruit.docs()[0].segment_ord()
            }
        });
        let mut docs = vec![];
        let mut scores = vec![];
        for child in children {
            docs.extend(child.docs());
            scores.extend(child.scores);
        }
        Ok(TestFruit { docs, scores })
    }
}

impl SegmentCollector for TestSegmentCollector {
    type Fruit = TestFruit;

    fn collect(&mut self, doc: DocId, score: Score) {
        self.fruit.docs.push(DocAddress(self.segment_id, doc));
        self.fruit.scores.push(score);
    }

    fn harvest(self) -> <Self as SegmentCollector>::Fruit {
        self.fruit
    }
}

/// Collects in order all of the fast fields for all of the
/// doc in the `DocSet`
///
/// This collector is mainly useful for tests.
pub struct FastFieldTestCollector {
    field: Field,
}

pub struct FastFieldSegmentCollector {
    vals: Vec<u64>,
    reader: FastFieldReader<u64>,
}

impl FastFieldTestCollector {
    pub fn for_field(field: Field) -> FastFieldTestCollector {
        FastFieldTestCollector { field }
    }
}

impl Collector for FastFieldTestCollector {
    type Fruit = Vec<u64>;
    type SegmentFruit = Self::Fruit;

    type Child = FastFieldSegmentCollector;

    fn for_segment(
        &self,
        _: SegmentLocalId,
        reader: &SegmentReader,
    ) -> Result<FastFieldSegmentCollector> {
        Ok(FastFieldSegmentCollector {
            vals: Vec::new(),
            reader: reader.fast_field_reader(self.field)?,
        })
    }

    fn requires_scoring(&self) -> bool {
        false
    }

    fn merge_fruits(&self, children: Vec<Vec<u64>>) -> Result<Vec<u64>> {
        Ok(children.into_iter().flat_map(|v| v.into_iter()).collect())
    }
}

impl SegmentCollector for FastFieldSegmentCollector {
    type Fruit = Vec<u64>;

    fn collect(&mut self, doc: DocId, _score: Score) {
        let val = self.reader.get(doc);
        self.vals.push(val);
    }

    fn harvest(self) -> Vec<u64> {
        self.vals
    }
}

/// Collects in order all of the fast field bytes for all of the
/// docs in the `DocSet`
///
/// This collector is mainly useful for tests.
pub struct BytesFastFieldTestCollector {
    field: Field,
}

pub struct BytesFastFieldSegmentCollector {
    vals: Vec<u8>,
    reader: BytesFastFieldReader,
}

impl BytesFastFieldTestCollector {
    pub fn for_field(field: Field) -> BytesFastFieldTestCollector {
        BytesFastFieldTestCollector { field }
    }
}

impl Collector for BytesFastFieldTestCollector {
    type Fruit = Vec<u8>;
    type SegmentFruit = Self::Fruit;
    type Child = BytesFastFieldSegmentCollector;

    fn for_segment(
        &self,
        _segment_local_id: u32,
        segment: &SegmentReader,
    ) -> Result<BytesFastFieldSegmentCollector> {
        Ok(BytesFastFieldSegmentCollector {
            vals: Vec::new(),
            reader: segment.bytes_fast_field_reader(self.field)?,
        })
    }

    fn requires_scoring(&self) -> bool {
        false
    }

    fn merge_fruits(&self, children: Vec<Vec<u8>>) -> Result<Vec<u8>> {
        Ok(children.into_iter().flat_map(|c| c.into_iter()).collect())
    }
}

impl SegmentCollector for BytesFastFieldSegmentCollector {
    type Fruit = Vec<u8>;

    fn collect(&mut self, doc: u32, _score: f32) {
        let data = self.reader.get_val(doc);
        self.vals.extend(data);
    }

    fn harvest(self) -> <Self as SegmentCollector>::Fruit {
        self.vals
    }
}

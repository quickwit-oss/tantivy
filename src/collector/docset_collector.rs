use rustc_hash::FxHashSet;

use super::{Collector, SegmentCollector};
use crate::{DocAddress, DocId, Score};

/// Collectors that returns the set of DocAddress that matches the query.
///
/// This collector is mostly useful for tests.
pub struct DocSetCollector;

impl Collector for DocSetCollector {
    type Fruit = FxHashSet<DocAddress>;
    type Child = DocSetChildCollector;

    fn for_segment(
        &self,
        segment_local_id: crate::SegmentOrdinal,
        _segment: &crate::SegmentReader,
    ) -> crate::Result<Self::Child> {
        Ok(DocSetChildCollector {
            segment_local_id,
            docs: Default::default(),
        })
    }

    fn requires_scoring(&self) -> bool {
        false
    }

    fn merge_fruits(
        &self,
        segment_fruits: Vec<(u32, FxHashSet<DocId>)>,
    ) -> crate::Result<Self::Fruit> {
        let len: usize = segment_fruits.iter().map(|(_, docset)| docset.len()).sum();
        let mut result = FxHashSet::with_capacity_and_hasher(len, Default::default());
        for (segment_local_id, docs) in segment_fruits {
            for doc in docs {
                result.insert(DocAddress::new(segment_local_id, doc));
            }
        }
        Ok(result)
    }
}

pub struct DocSetChildCollector {
    segment_local_id: u32,
    docs: FxHashSet<DocId>,
}

impl SegmentCollector for DocSetChildCollector {
    type Fruit = (u32, FxHashSet<DocId>);

    fn collect(&mut self, doc: crate::DocId, _score: Score) {
        self.docs.insert(doc);
    }

    fn harvest(self) -> (u32, FxHashSet<DocId>) {
        (self.segment_local_id, self.docs)
    }
}

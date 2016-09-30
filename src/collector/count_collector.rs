use std::io;
use super::Collector;
use ScoredDoc;
use SegmentReader;
use SegmentLocalId;

/// `CountCollector` collector only counts how many
/// documents match the query.  
pub struct CountCollector {
    count: usize,
}

impl CountCollector {
    /// Returns the count of documents that were
    /// collected.
    pub fn count(&self,) -> usize {
        self.count
    }
}

impl Default for CountCollector {
    fn default() -> CountCollector {
        CountCollector {count: 0,
        }
    }
}

impl Collector for CountCollector {

    fn set_segment(&mut self, _: SegmentLocalId, _: &SegmentReader) -> io::Result<()> {
        Ok(())
    }

    fn collect(&mut self, _: ScoredDoc) {
        self.count += 1;
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use test::Bencher;
    use ScoredDoc;
    use collector::Collector;

    #[bench]
    fn build_collector(b: &mut Bencher) {
        b.iter(|| {
            let mut count_collector = CountCollector::default();
            let docs: Vec<u32> = (0..1_000_000).collect();
            for doc in docs {
                count_collector.collect(ScoredDoc(1f32, doc));
            }
            count_collector.count()
        });
    }
}

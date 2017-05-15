use super::Collector;
use DocId;
use Score;
use Result;
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
    pub fn count(&self) -> usize {
        self.count
    }
}

impl Default for CountCollector {
    fn default() -> CountCollector {
        CountCollector { count: 0 }
    }
}

impl Collector for CountCollector {
    fn set_segment(&mut self, _: SegmentLocalId, _: &SegmentReader) -> Result<()> {
        Ok(())
    }

    fn collect(&mut self, _: DocId, _: Score) {
        self.count += 1;
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use test::Bencher;
    use collector::Collector;

    #[bench]
    fn build_collector(b: &mut Bencher) {
        b.iter(|| {
                   let mut count_collector = CountCollector::default();
                   for doc in 0..1_000_000 {
                       count_collector.collect(doc, 1f32);
                   }
                   count_collector.count()
               });
    }
}

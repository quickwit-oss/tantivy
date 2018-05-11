use super::Collector;
use super::SegmentCollector;
use DocId;
use Result;
use Score;
use SegmentLocalId;
use SegmentReader;
use std::any::Any;

pub trait AnyCollector: Collector + Any where <Self as Collector>::Child : AnySegmentCollector {
    fn merge_children_anys(&mut self, children: Vec<Box<AnySegmentCollector>>);
}

pub trait AnySegmentCollector: SegmentCollector + Any {
}

/// Multicollector makes it possible to collect on more than one collector.
/// It should only be used for use cases where the Collector types is unknown
/// at compile time.
/// If the type of the collectors is known, you should prefer to use `ChainedCollector`.
pub struct MultiCollector<'a> {
    collectors: Vec<&'a mut AnyCollector>,
}

impl<'a> MultiCollector<'a> {
    /// Constructor
    pub fn from(collectors: Vec<&'a mut AnyCollector>) -> MultiCollector {
        MultiCollector { collectors }
    }
}

pub struct SegmentMultiCollector {
    segment_collectors: Vec<Box<AnySegmentCollector>>,
}

impl<'a> Collector for MultiCollector<'a> {
    type Child = SegmentMultiCollector;

    fn for_segment(&mut self, segment_local_id: u32, segment: &SegmentReader) -> Result<SegmentMultiCollector> {
        let segment_collectors = self.collectors.iter_mut()
            .map(|x| x.for_segment(segment_local_id, segment))
            .collect::<Result<Vec<_>>>()?;
        Ok(SegmentMultiCollector { segment_collectors })
    }

    fn requires_scoring(&self) -> bool {
        self.collectors
            .iter()
            .any(|collector| collector.requires_scoring())
    }

    fn merge_children(&mut self, children: Vec<SegmentMultiCollector>) {
        let mut per_collector_children =
            (0..self.collectors.len())
                .map(|_| Vec::with_capacity(children.len()))
                .collect::<Vec<_>>();
        for child in children.into_iter() {
            for (idx, segment_collector) in child.segment_collectors.into_iter().enumerate() {
                per_collector_children[idx].push(segment_collector);
            }
        }
        for (collector, children) in self.collectors.iter_mut().zip(per_collector_children) {
            collector.merge_children_anys(children);
        }
    }
}

impl SegmentCollector for SegmentMultiCollector {
    fn collect(&mut self, doc: DocId, score: Score) {
        for collector in &mut self.segment_collectors {
            collector.collect(doc, score);
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use collector::{Collector, CountCollector, TopCollector};

    #[test]
    fn test_multi_collector() {
        let mut top_collector = TopCollector::with_limit(2);
        let mut count_collector = CountCollector::default();
        {
            let mut collectors =
                MultiCollector::from(vec![&mut top_collector, &mut count_collector]);
            collectors.collect(1, 0.2);
            collectors.collect(2, 0.1);
            collectors.collect(3, 0.5);
        }
        assert_eq!(count_collector.count(), 3);
        assert!(top_collector.at_capacity());
    }
}

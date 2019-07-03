use crate::collector::top_collector::{TopCollector, TopSegmentCollector};
use crate::collector::{Collector, SegmentCollector};
use crate::Result;
use crate::{DocAddress, DocId, Score, SegmentReader};

pub struct CustomScoreTopCollector<TCustomScorer, TScore = Score> {
    custom_scorer: TCustomScorer,
    collector: TopCollector<TScore>,
}

impl<TCustomScorer, TScore> CustomScoreTopCollector<TCustomScorer, TScore>
where
    TScore: Clone + PartialOrd,
{
    pub fn new(
        custom_scorer: TCustomScorer,
        limit: usize,
    ) -> CustomScoreTopCollector<TCustomScorer, TScore> {
        CustomScoreTopCollector {
            custom_scorer,
            collector: TopCollector::with_limit(limit),
        }
    }
}

pub trait CustomSegmentScorer<TScore>: 'static {
    fn score(&self, doc: DocId) -> TScore;
}

pub trait CustomScorer<TScore>: Sync {
    type Child: CustomSegmentScorer<TScore>;
    fn segment_scorer(&self, segment_reader: &SegmentReader) -> Result<Self::Child>;
}

impl<TCustomScorer, TScore> Collector for CustomScoreTopCollector<TCustomScorer, TScore>
where
    TCustomScorer: CustomScorer<TScore>,
    TScore: 'static + PartialOrd + Clone + Send + Sync,
{
    type Fruit = Vec<(TScore, DocAddress)>;

    type Child = CustomScoreTopSegmentCollector<TCustomScorer::Child, TScore>;

    fn for_segment(
        &self,
        segment_local_id: u32,
        segment_reader: &SegmentReader,
    ) -> Result<Self::Child> {
        let segment_scorer = self.custom_scorer.segment_scorer(segment_reader)?;
        let segment_collector = self
            .collector
            .for_segment(segment_local_id, segment_reader)?;
        Ok(CustomScoreTopSegmentCollector {
            segment_collector,
            segment_scorer,
        })
    }

    fn requires_scoring(&self) -> bool {
        false
    }

    fn merge_fruits(&self, segment_fruits: Vec<Self::Fruit>) -> Result<Self::Fruit> {
        self.collector.merge_fruits(segment_fruits)
    }
}

pub struct CustomScoreTopSegmentCollector<T, TScore>
where
    TScore: 'static + PartialOrd + Clone + Send + Sync + Sized,
    T: CustomSegmentScorer<TScore>,
{
    segment_collector: TopSegmentCollector<TScore>,
    segment_scorer: T,
}

impl<T, TScore> SegmentCollector for CustomScoreTopSegmentCollector<T, TScore>
where
    TScore: 'static + PartialOrd + Clone + Send + Sync,
    T: 'static + CustomSegmentScorer<TScore>,
{
    type Fruit = Vec<(TScore, DocAddress)>;

    fn collect(&mut self, doc: DocId, _score: Score) {
        let score = self.segment_scorer.score(doc);
        self.segment_collector.collect(doc, score);
    }

    fn harvest(self) -> Vec<(TScore, DocAddress)> {
        self.segment_collector.harvest()
    }
}

impl<F, TScore, T> CustomScorer<TScore> for F
where
    F: 'static + Send + Sync + Fn(&SegmentReader) -> T,
    T: CustomSegmentScorer<TScore>,
{
    type Child = T;

    fn segment_scorer(&self, segment_reader: &SegmentReader) -> Result<Self::Child> {
        Ok((self)(segment_reader))
    }
}

impl<F, TScore> CustomSegmentScorer<TScore> for F
where
    F: 'static + Sync + Send + Fn(DocId) -> TScore,
{
    fn score(&self, doc: DocId) -> TScore {
        (self)(doc)
    }
}

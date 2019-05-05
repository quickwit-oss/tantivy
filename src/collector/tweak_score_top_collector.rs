use collector::top_collector::{TopCollector, TopSegmentCollector};
use collector::{Collector, SegmentCollector};
use DocAddress;
use Result;
use {DocId, Score, SegmentReader};

pub struct TweakedScoreTopCollector<TScoreTweaker, TScore = Score> {
    score_tweaker: TScoreTweaker,
    collector: TopCollector<TScore>,
}

impl<TScoreTweaker, TScore> TweakedScoreTopCollector<TScoreTweaker, TScore>
where
    TScore: Clone + PartialOrd,
{
    pub fn new(
        score_tweaker: TScoreTweaker,
        limit: usize,
    ) -> TweakedScoreTopCollector<TScoreTweaker, TScore> {
        TweakedScoreTopCollector {
            score_tweaker,
            collector: TopCollector::with_limit(limit),
        }
    }
}

pub trait ScoreSegmentTweaker<TScore>: 'static {
    fn score(&self, doc: DocId, score: Score) -> TScore;
}

pub trait ScoreTweaker<TScore>: Sync {
    type Child: ScoreSegmentTweaker<TScore>;
    fn segment_scorer(&self, segment_reader: &SegmentReader) -> Result<Self::Child>;
}

impl<TScoreTweaker, TScore> Collector for TweakedScoreTopCollector<TScoreTweaker, TScore>
where
    TScoreTweaker: ScoreTweaker<TScore>,
    TScore: 'static + PartialOrd + Clone + Send + Sync,
{
    type Fruit = Vec<(TScore, DocAddress)>;

    type Child = TopTweakedScoreSegmentCollector<TScoreTweaker::Child, TScore>;

    fn for_segment(
        &self,
        segment_local_id: u32,
        segment_reader: &SegmentReader,
    ) -> Result<Self::Child> {
        let segment_scorer = self.score_tweaker.segment_scorer(segment_reader)?;
        let segment_collector = self
            .collector
            .for_segment(segment_local_id, segment_reader)?;
        Ok(TopTweakedScoreSegmentCollector {
            segment_collector,
            segment_scorer,
        })
    }

    fn requires_scoring(&self) -> bool {
        true
    }

    fn merge_fruits(&self, segment_fruits: Vec<Self::Fruit>) -> Result<Self::Fruit> {
        self.collector.merge_fruits(segment_fruits)
    }
}

pub struct TopTweakedScoreSegmentCollector<TSegmentScoreTweaker, TScore>
where
    TScore: 'static + PartialOrd + Clone + Send + Sync + Sized,
    TSegmentScoreTweaker: ScoreSegmentTweaker<TScore>,
{
    segment_collector: TopSegmentCollector<TScore>,
    segment_scorer: TSegmentScoreTweaker,
}

impl<TSegmentScoreTweaker, TScore> SegmentCollector
    for TopTweakedScoreSegmentCollector<TSegmentScoreTweaker, TScore>
where
    TScore: 'static + PartialOrd + Clone + Send + Sync,
    TSegmentScoreTweaker: 'static + ScoreSegmentTweaker<TScore>,
{
    type Fruit = Vec<(TScore, DocAddress)>;

    fn collect(&mut self, doc: DocId, score: Score) {
        let score = self.segment_scorer.score(doc, score);
        self.segment_collector.collect(doc, score);
    }

    fn harvest(self) -> Vec<(TScore, DocAddress)> {
        self.segment_collector.harvest()
    }
}

impl<F, TScore, TSegmentScoreTweaker> ScoreTweaker<TScore> for F
where
    F: 'static + Send + Sync + Fn(&SegmentReader) -> TSegmentScoreTweaker,
    TSegmentScoreTweaker: ScoreSegmentTweaker<TScore>,
{
    type Child = TSegmentScoreTweaker;

    fn segment_scorer(&self, segment_reader: &SegmentReader) -> Result<Self::Child> {
        Ok((self)(segment_reader))
    }
}

impl<F, TScore> ScoreSegmentTweaker<TScore> for F
where
    F: 'static + Sync + Send + Fn(DocId, Score) -> TScore,
{
    fn score(&self, doc: DocId, score: Score) -> TScore {
        (self)(doc, score)
    }
}

use crate::collector::top_collector::{TopCollector, TopSegmentCollector};
use crate::collector::{Collector, SegmentCollector};
use crate::DocAddress;
use crate::{DocId, Result, Score, SegmentReader};

pub(crate) struct TweakedScoreTopCollector<TScoreTweaker, TScore = Score> {
    score_tweaker: TScoreTweaker,
    collector: TopCollector<TScore>,
}

impl<TScoreTweaker, TScore> TweakedScoreTopCollector<TScoreTweaker, TScore>
where
    TScore: Clone + PartialOrd,
{
    pub fn new(
        score_tweaker: TScoreTweaker,
        collector: TopCollector<TScore>,
    ) -> TweakedScoreTopCollector<TScoreTweaker, TScore> {
        TweakedScoreTopCollector {
            score_tweaker,
            collector,
        }
    }
}

/// A `ScoreSegmentTweaker` makes it possible to modify the default score
/// for a given document belonging to a specific segment.
///
/// It is the segment local version of the [`ScoreTweaker`](./trait.ScoreTweaker.html).
pub trait ScoreSegmentTweaker<TScore>: 'static {
    /// Tweak the given `score` for the document `doc`.
    fn score(&mut self, doc: DocId, score: Score) -> TScore;
}

/// `ScoreTweaker` makes it possible to tweak the score
/// emitted  by the scorer into another one.
///
/// The `ScoreTweaker` itself does not make much of the computation itself.
/// Instead, it helps constructing `Self::Child` instances that will compute
/// the score at a segment scale.
pub trait ScoreTweaker<TScore>: Sync {
    /// Type of the associated [`ScoreSegmentTweaker`](./trait.ScoreSegmentTweaker.html).
    type Child: ScoreSegmentTweaker<TScore>;

    /// Builds a child tweaker for a specific segment. The child scorer is associated to
    /// a specific segment.
    fn segment_tweaker(&self, segment_reader: &SegmentReader) -> Result<Self::Child>;
}

impl<TScoreTweaker, TScore> Collector for TweakedScoreTopCollector<TScoreTweaker, TScore>
where
    TScoreTweaker: ScoreTweaker<TScore> + Send + Sync,
    TScore: 'static + PartialOrd + Clone + Send + Sync,
{
    type Fruit = Vec<(TScore, DocAddress)>;

    type Child = TopTweakedScoreSegmentCollector<TScoreTweaker::Child, TScore>;

    fn for_segment(
        &self,
        segment_local_id: u32,
        segment_reader: &SegmentReader,
    ) -> Result<Self::Child> {
        let segment_scorer = self.score_tweaker.segment_tweaker(segment_reader)?;
        let segment_collector = self.collector.for_segment(segment_local_id, segment_reader);
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

    fn segment_tweaker(&self, segment_reader: &SegmentReader) -> Result<Self::Child> {
        Ok((self)(segment_reader))
    }
}

impl<F, TScore> ScoreSegmentTweaker<TScore> for F
where
    F: 'static + FnMut(DocId, Score) -> TScore,
{
    fn score(&mut self, doc: DocId, score: Score) -> TScore {
        (self)(doc, score)
    }
}

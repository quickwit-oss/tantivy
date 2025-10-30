use std::cmp::Ordering;

use crate::collector::top_collector::{TopCollector, TopSegmentCollector};
use crate::collector::{Collector, SegmentCollector};
use crate::{DocAddress, DocId, Result, Score, SegmentReader};

pub(crate) struct TweakedScoreTopCollector<TScoreTweaker, TScore = Score> {
    score_tweaker: TScoreTweaker,
    collector: TopCollector<TScore>,
}

impl<TScoreTweaker, TScore> TweakedScoreTopCollector<TScoreTweaker, TScore>
where TScore: Clone + PartialOrd
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
/// It is the segment local version of the [`ScoreTweaker`].
pub trait ScoreSegmentTweaker: 'static {
    /// Score used by at the segment level by the `ScoreSegmentTweaker`.
    type SegmentScore: 'static + PartialOrd + Clone + Send + Sync;

    /// Tweak the given `score` for the document `doc`.
    fn score(&mut self, doc: DocId, score: Score) -> Self::SegmentScore;

    /// Returns true if the `ScoreSegmentTweaker` is a good candidate for the lazy evaluation optimization.
    /// See [`ScoreSegmentTweaker::accept_score_lazy`].
    fn is_lazy() -> bool {
        false
    }

    /// Implementing this method makes it possible to avoid computing
    /// a score entirely if we can assess that it won't pass a threshold
    /// with a partial computation.
    ///
    /// This is currently used for lexicographic sorting.
    ///
    /// If REVERSE_ORDER is false (resp. true),
    /// - we return None if the score is below the threshold (resp. above to the threshold)
    /// - we return Some(ordering, score) if the score is above or equal to the threshold (resp. below or equal to)
    fn accept_score_lazy<const REVERSE_ORDER: bool>(
        &mut self,
        doc_id: DocId,
        score: Score,
        threshold: &Self::SegmentScore,
    ) -> Option<(std::cmp::Ordering, Self::SegmentScore)> {
        let excluded_ordering = if REVERSE_ORDER {
            Ordering::Greater
        } else {
            Ordering::Less
        };
        let score = self.score(doc_id, score);
        let cmp = score.partial_cmp(threshold).unwrap_or(excluded_ordering);
        if cmp == excluded_ordering {
            return None;
        } else {
            return Some((cmp, score));
        }
    }
}

/// `ScoreTweaker` makes it possible to tweak the score
/// emitted  by the scorer into another one.
///
/// The `ScoreTweaker` itself does not make much of the computation itself.
/// Instead, it helps constructing `Self::Child` instances that will compute
/// the score at a segment scale.
pub trait ScoreTweaker<TScore>: Sync {
    /// Type of the associated [`ScoreSegmentTweaker`].
    type Child: ScoreSegmentTweaker<SegmentScore = TScore>;

    /// Builds a child tweaker for a specific segment. The child scorer is associated with
    /// a specific segment.
    fn segment_tweaker(&self, segment_reader: &SegmentReader) -> Result<Self::Child>;
}

impl<TScoreTweaker, TScore> Collector for TweakedScoreTopCollector<TScoreTweaker, TScore>
where
    TScoreTweaker: ScoreTweaker<TScore> + Send + Sync,
    TScore: 'static + PartialOrd + Clone + Send + Sync,
{
    type Fruit = Vec<(TScore, DocAddress)>;

    type Child = TopTweakedScoreSegmentCollector<TScoreTweaker::Child>;

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

pub struct TopTweakedScoreSegmentCollector<TSegmentScoreTweaker>
where TSegmentScoreTweaker: ScoreSegmentTweaker
{
    segment_collector: TopSegmentCollector<TSegmentScoreTweaker::SegmentScore>,
    segment_scorer: TSegmentScoreTweaker,
}

impl<TSegmentScoreTweaker> SegmentCollector
    for TopTweakedScoreSegmentCollector<TSegmentScoreTweaker>
where TSegmentScoreTweaker: 'static + ScoreSegmentTweaker
{
    type Fruit = Vec<(TSegmentScoreTweaker::SegmentScore, DocAddress)>;

    fn collect(&mut self, doc: DocId, score: Score) {
        // Thanks to generics, this if-statement is free.
        if TSegmentScoreTweaker::is_lazy() {
            self.segment_collector.topn_computer.push_lazy(doc, score, &mut self.segment_scorer);
        } else {
            let score = self.segment_scorer.score(doc, score);
            self.segment_collector.collect(doc, score);
        }
    }

    fn harvest(self) -> Vec<(TSegmentScoreTweaker::SegmentScore, DocAddress)> {
        self.segment_collector.harvest()
    }
}

impl<F, TScore, TSegmentScoreTweaker> ScoreTweaker<TScore> for F
where
    F: 'static + Send + Sync + Fn(&SegmentReader) -> TSegmentScoreTweaker,
    TSegmentScoreTweaker: ScoreSegmentTweaker<SegmentScore = TScore>,
{
    type Child = TSegmentScoreTweaker;

    fn segment_tweaker(&self, segment_reader: &SegmentReader) -> Result<Self::Child> {
        Ok((self)(segment_reader))
    }
}

impl<F, TScore> ScoreSegmentTweaker for F
where
    F: 'static + FnMut(DocId, Score) -> TScore,
    TScore: 'static + PartialOrd + Clone + Send + Sync,
{
    type SegmentScore = TScore;
    fn score(&mut self, doc: DocId, score: Score) -> TScore {
        (self)(doc, score)
    }
}

use std::cmp::Ordering;

use crate::collector::top_collector::{TopCollector, TopSegmentCollector};
use crate::collector::{Collector, SegmentCollector};
use crate::{DocAddress, DocId, Result, Score, SegmentReader};

pub(crate) struct TweakedScoreTopCollector<TScoreTweaker, TScore> {
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
    /// The final score being emitted.
    type Score: 'static + PartialOrd + Send + Sync + Clone;

    /// Score used by at the segment level by the `ScoreSegmentTweaker`.
    ///
    /// It is typically small like a `u64`, and is meant to be converted
    /// to the final score at the end of the collection of the segment.
    type SegmentScore: 'static + PartialOrd + Clone + Send + Sync + Clone;

    /// Tweak the given `score` for the document `doc`.
    fn score(&mut self, doc: DocId, score: Score) -> Self::SegmentScore;

    /// Returns true if the `ScoreSegmentTweaker` is a good candidate for the lazy evaluation
    /// optimization. See [`ScoreSegmentTweaker::accept_score_lazy`].
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
    /// - we return Some(ordering, score) if the score is above or equal to the threshold (resp.
    ///   below or equal to)
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

    /// Convert a segment level score into the global level score.
    fn convert_score(&self, score: Self::SegmentScore) -> Self::Score;
}

/// `ScoreTweaker` makes it possible to tweak the score
/// emitted  by the scorer into another one.
///
/// The `ScoreTweaker` itself does not make much of the computation itself.
/// Instead, it helps constructing `Self::Child` instances that will compute
/// the score at a segment scale.
pub trait ScoreTweaker: Sync {
    /// The actual score emitted by the Tweaker.
    type Score: 'static + Send + Sync + PartialOrd + Clone;
    /// Type of the associated [`ScoreSegmentTweaker`].
    type Child: ScoreSegmentTweaker<Score = Self::Score>;

    /// Builds a child tweaker for a specific segment. The child scorer is associated with
    /// a specific segment.
    fn segment_tweaker(&self, segment_reader: &SegmentReader) -> Result<Self::Child>;
}

impl<TScoreTweaker, TScore> Collector for TweakedScoreTopCollector<TScoreTweaker, TScore>
where
    TScoreTweaker: ScoreTweaker<Score = TScore> + Send + Sync,
    TScore: 'static + Send + PartialOrd + Sync + Clone,
{
    type Fruit = Vec<(TScoreTweaker::Score, DocAddress)>;

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
    type Fruit = Vec<(TSegmentScoreTweaker::Score, DocAddress)>;

    fn collect(&mut self, doc: DocId, score: Score) {
        self.segment_collector.collect_lazy(doc, score, &mut self.segment_scorer);
    }

    fn harvest(self) -> Self::Fruit {
        let segment_hits: Vec<(TSegmentScoreTweaker::SegmentScore, DocAddress)> =
            self.segment_collector.harvest();
        segment_hits
            .into_iter()
            .map(|(score, doc)| (self.segment_scorer.convert_score(score), doc))
            .collect()
    }
}

impl<F, TSegmentScoreTweaker> ScoreTweaker for F
where
    F: 'static + Send + Sync + Fn(&SegmentReader) -> TSegmentScoreTweaker,
    TSegmentScoreTweaker: ScoreSegmentTweaker,
{

    type Score = TSegmentScoreTweaker::Score;
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
    type Score = TScore;
    type SegmentScore = TScore;

    fn score(&mut self, doc: DocId, score: Score) -> TScore {
        (self)(doc, score)
    }

    /// Convert a segment level score into the global level score.
    fn convert_score(&self, score: Self::SegmentScore) -> Self::Score {
        score
    }
}

impl<HeadScoreTweaker, TailScoreTweaker> ScoreTweaker for (HeadScoreTweaker, TailScoreTweaker)
where
    HeadScoreTweaker: ScoreTweaker,
    TailScoreTweaker: ScoreTweaker,
{
    type Score = (<HeadScoreTweaker::Child as ScoreSegmentTweaker>::Score, <TailScoreTweaker::Child as ScoreSegmentTweaker>::Score);
    type Child = (HeadScoreTweaker::Child, TailScoreTweaker::Child);

    fn segment_tweaker(&self, segment_reader: &SegmentReader) -> Result<Self::Child> {
        Ok((
            self.0.segment_tweaker(segment_reader)?,
            self.1.segment_tweaker(segment_reader)?,
        ))
    }
}

impl<HeadScoreSegmentTweaker, TailScoreSegmentTweaker> ScoreSegmentTweaker
    for (HeadScoreSegmentTweaker, TailScoreSegmentTweaker)
where
    HeadScoreSegmentTweaker: ScoreSegmentTweaker,
    TailScoreSegmentTweaker: ScoreSegmentTweaker,
{
    type Score = (
        HeadScoreSegmentTweaker::Score,
        TailScoreSegmentTweaker::Score,
    );
    type SegmentScore = (
        HeadScoreSegmentTweaker::SegmentScore,
        TailScoreSegmentTweaker::SegmentScore,
    );

    fn score(&mut self, doc: DocId, score: Score) -> Self::SegmentScore {
        let head_score = self.0.score(doc, score);
        let tail_score = self.1.score(doc, score);
        (head_score, tail_score)
    }

    fn accept_score_lazy<const REVERSE_ORDER: bool>(
        &mut self,
        doc_id: DocId,
        score: Score,
        threshold: &Self::SegmentScore,
    ) -> Option<(Ordering, Self::SegmentScore)> {
        let (head_threshold, tail_threshold) = threshold;
        let (head_cmp, head_score) =
            self.0
                .accept_score_lazy::<REVERSE_ORDER>(doc_id, score, head_threshold)?;
        if head_cmp == Ordering::Equal {
            let (tail_cmp, tail_score) =
                self.1
                    .accept_score_lazy::<REVERSE_ORDER>(doc_id, score, tail_threshold)?;
            Some((tail_cmp, (head_score, tail_score)))
        } else {
            let tail_score = self.1.score(doc_id, score);
            Some((head_cmp, (head_score, tail_score)))
        }
    }

    fn is_lazy() -> bool {
        true
    }

    fn convert_score(&self, score: Self::SegmentScore) -> Self::Score {
        let (head_score, tail_score) = score;
        (
            self.0.convert_score(head_score),
            self.1.convert_score(tail_score),
        )
    }
}

/// This struct is used as an adapter to take a segment score tweaker and map its score to another new score.
pub struct MappedSegmentScoreTweaker<T, PreviousScore, NewScore> {
    tweaker: T,
    map: fn(PreviousScore) -> NewScore,
}

impl<T, PreviousScore, NewScore> ScoreSegmentTweaker for MappedSegmentScoreTweaker<T, PreviousScore, NewScore>
where
    T: ScoreSegmentTweaker<Score = PreviousScore>,
    PreviousScore: 'static + Clone + Send + Sync + PartialOrd,
    NewScore: 'static + Clone + Send + Sync + PartialOrd,
{
    type Score = NewScore;
    type SegmentScore = T::SegmentScore;

    fn score(&mut self, doc: DocId, score: Score) -> Self::SegmentScore {
        self.tweaker.score(doc, score)
    }

    fn accept_score_lazy<const REVERSE_ORDER: bool>(
            &mut self,
            doc_id: DocId,
            score: Score,
            threshold: &Self::SegmentScore,
        ) -> Option<(std::cmp::Ordering, Self::SegmentScore)> {
        self.tweaker.accept_score_lazy::<REVERSE_ORDER>(doc_id, score, threshold)
    }

    fn is_lazy() -> bool {
        T::is_lazy()
    }

    fn convert_score(&self, score: Self::SegmentScore) -> Self::Score {
        (self.map)(self.tweaker.convert_score(score))
    }
}


// We then re-use our (head, tail) implement and our mapper by seeing mapping any tuple (a, b, c, ...) as the chain (a, (b, (c, ...)))

impl<ScoreTweaker1, ScoreTweaker2, ScoreTweaker3> ScoreTweaker for (ScoreTweaker1, ScoreTweaker2, ScoreTweaker3)
where
    ScoreTweaker1: ScoreTweaker,
    ScoreTweaker2: ScoreTweaker,
    ScoreTweaker3: ScoreTweaker,
{
    type Child = MappedSegmentScoreTweaker<<(ScoreTweaker1, (ScoreTweaker2, ScoreTweaker3)) as  ScoreTweaker>::Child, (ScoreTweaker1::Score, (ScoreTweaker2::Score, ScoreTweaker3::Score)), Self::Score>;
    type Score = (ScoreTweaker1::Score, ScoreTweaker2::Score, ScoreTweaker3::Score);

    fn segment_tweaker(&self, segment_reader: &SegmentReader) -> Result<Self::Child> {
        let score_tweaker1 = self.0.segment_tweaker(segment_reader)?;
        let score_tweaker2 = self.1.segment_tweaker(segment_reader)?;
        let score_tweaker3 = self.2.segment_tweaker(segment_reader)?;
        Ok(
            MappedSegmentScoreTweaker {
                tweaker: (score_tweaker1, (score_tweaker2, score_tweaker3)),
                map: | (score1, (score2, score3))| (score1, score2, score3),
            }
        )

    }
}

impl<ScoreTweaker1, ScoreTweaker2, ScoreTweaker3, ScoreTweaker4> ScoreTweaker for (ScoreTweaker1, ScoreTweaker2, ScoreTweaker3, ScoreTweaker4)
where
    ScoreTweaker1: ScoreTweaker,
    ScoreTweaker2: ScoreTweaker,
    ScoreTweaker3: ScoreTweaker,
    ScoreTweaker4: ScoreTweaker,
{
    type Child = MappedSegmentScoreTweaker<<(ScoreTweaker1, (ScoreTweaker2, (ScoreTweaker3, ScoreTweaker4))) as  ScoreTweaker>::Child, (ScoreTweaker1::Score, (ScoreTweaker2::Score, (ScoreTweaker3::Score, ScoreTweaker4::Score))), Self::Score>;
    type Score = (ScoreTweaker1::Score, ScoreTweaker2::Score, ScoreTweaker3::Score, ScoreTweaker4::Score);

    fn segment_tweaker(&self, segment_reader: &SegmentReader) -> Result<Self::Child> {
        let score_tweaker1 = self.0.segment_tweaker(segment_reader)?;
        let score_tweaker2 = self.1.segment_tweaker(segment_reader)?;
        let score_tweaker3 = self.2.segment_tweaker(segment_reader)?;
        let score_tweaker4 = self.3.segment_tweaker(segment_reader)?;
        Ok(
            MappedSegmentScoreTweaker {
                tweaker: (score_tweaker1, (score_tweaker2, (score_tweaker3, score_tweaker4))),
                map: | (score1, (score2, (score3, score4)))| (score1, score2, score3, score4),
            }
        )

    }
}

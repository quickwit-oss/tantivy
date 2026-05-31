use crate::docset::{DocSet, TERMINATED};
use crate::query::Scorer;
use crate::{DocId, Score};

struct ScoreOnlyScorer {
    doc: DocId,
    score: Score,
}

impl DocSet for ScoreOnlyScorer {
    fn advance(&mut self) -> DocId {
        self.doc = TERMINATED;
        TERMINATED
    }

    fn doc(&self) -> DocId {
        self.doc
    }

    fn size_hint(&self) -> u32 {
        1
    }
}

impl Scorer for ScoreOnlyScorer {
    fn score(&mut self) -> Score {
        self.score
    }

    fn can_score_doc(&self) -> bool {
        true
    }

    fn score_doc(&mut self, _doc: DocId, _term_freq: u32) -> Score {
        self.score
    }
}

/// The `ScoreCombiner` trait defines how to compute
/// an overall score given a list of scores.
pub trait ScoreCombiner: Default + Clone + Send + Copy + 'static {
    /// Aggregates the score combiner with the given scorer.
    ///
    /// The `ScoreCombiner` may decide to call `.scorer.score()`
    /// or not.
    fn update<TScorer: Scorer>(&mut self, scorer: &mut TScorer);

    /// Aggregates the score combiner with an already computed score.
    fn update_score(&mut self, doc: DocId, score: Score) {
        let mut scorer = ScoreOnlyScorer { doc, score };
        self.update(&mut scorer);
    }

    /// Clears the score combiner state back to its initial state.
    fn clear(&mut self);

    /// Returns the aggregate score.
    fn score(&self) -> Score;
}

/// Just ignores scores. The `DoNothingCombiner` does not
/// even call the scorers `.score()` function.
///
/// It is useful to optimize the case when scoring is disabled.
#[derive(Default, Clone, Copy)] //< these should not be too much work :)
pub struct DoNothingCombiner;

impl ScoreCombiner for DoNothingCombiner {
    fn update<TScorer: Scorer>(&mut self, _scorer: &mut TScorer) {}

    fn update_score(&mut self, _doc: DocId, _score: Score) {}

    fn clear(&mut self) {}

    #[inline]
    fn score(&self) -> Score {
        1.0
    }
}

/// Sums the score of different scorers.
#[derive(Default, Clone, Copy)]
pub struct SumCombiner {
    score: Score,
}

impl ScoreCombiner for SumCombiner {
    #[inline]
    fn update<TScorer: Scorer>(&mut self, scorer: &mut TScorer) {
        self.score += scorer.score();
    }

    #[inline]
    fn update_score(&mut self, _doc: DocId, score: Score) {
        self.score += score;
    }

    fn clear(&mut self) {
        self.score = 0.0;
    }

    #[inline]
    fn score(&self) -> Score {
        self.score
    }
}

/// Take max score of different scorers
/// and optionally sum it with other matches multiplied by `tie_breaker`
#[derive(Default, Clone, Copy)]
pub struct DisjunctionMaxCombiner {
    max: Score,
    sum: Score,
    tie_breaker: Score,
}

impl DisjunctionMaxCombiner {
    /// Creates `DisjunctionMaxCombiner` with tie breaker
    pub fn with_tie_breaker(tie_breaker: Score) -> DisjunctionMaxCombiner {
        DisjunctionMaxCombiner {
            max: 0.0,
            sum: 0.0,
            tie_breaker,
        }
    }
}

impl ScoreCombiner for DisjunctionMaxCombiner {
    #[inline]
    fn update<TScorer: Scorer>(&mut self, scorer: &mut TScorer) {
        let score = scorer.score();
        self.max = Score::max(score, self.max);
        self.sum += score;
    }

    #[inline]
    fn update_score(&mut self, _doc: DocId, score: Score) {
        self.max = Score::max(score, self.max);
        self.sum += score;
    }

    fn clear(&mut self) {
        self.max = 0.0;
        self.sum = 0.0;
    }

    #[inline]
    fn score(&self) -> Score {
        self.max + (self.sum - self.max) * self.tie_breaker
    }
}

use Score;
use query::Scorer;

pub trait ScoreCombiner: Default + Clone + Copy {
    fn update<TScorer: Scorer>(&mut self, scorer: &mut TScorer);
    fn clear(&mut self);
    fn score(&self) -> Score;
}

#[derive(Default, Clone, Copy)] //< these should not be too much work :)
pub struct DoNothingCombiner;

impl ScoreCombiner for DoNothingCombiner {
    fn update<TScorer: Scorer>(&mut self, _scorer: &mut TScorer) {}

    fn clear(&mut self) {}

    fn score(&self) -> Score {
        1f32
    }
}

#[derive(Default, Clone, Copy)]
pub struct SumCombiner {
    score: Score
}


impl ScoreCombiner for SumCombiner {
    fn update<TScorer: Scorer>(&mut self, scorer: &mut TScorer) {
        self.score += scorer.score();
    }

    fn clear(&mut self) {
        self.score = 0f32;
    }

    fn score(&self) -> Score {
        self.score
    }
}

#[derive(Default, Clone, Copy)]
pub struct SumWithCoordsCombiner {
    num_fields: usize,
    score: Score,
}

impl ScoreCombiner for SumWithCoordsCombiner {
    fn update<TScorer: Scorer>(&mut self, scorer: &mut TScorer) {
        self.score += scorer.score();
        self.num_fields += 1;
    }

    fn clear(&mut self) {
        self.score = 0f32;
        self.num_fields = 0;
    }

    fn score(&self) -> Score {
        self.score
    }
}


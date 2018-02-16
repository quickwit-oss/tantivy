use Score;

pub trait ScoreCombiner: Default + Copy {
    fn update(&mut self, score: Score);
    fn clear(&mut self);
    fn score(&self) -> Score;
}


pub struct DoNothingCombiner;
impl ScoreCombiner for DoNothingCombiner {
    fn update(&mut self, score: Score) {}

    fn clear(&mut self) {}

    fn score(&self) -> Score {
        1f32
    }
}

pub struct SumWithCoordsCombiner {
    coords: Vec<Score>,
    num_fields: usize,
    score: Score,
}

impl ScoreCombiner for SumWithCoordsCombiner {
    fn update(&mut self, score: Score) {
        self.score += score;
        self.num_fields += 1;
    }

    fn clear(&mut self) {
        self.score = 0f32;
        self.num_fields = 0;
    }

    fn score(&self) -> Score {
        self.score * self.coord()
    }

}

impl SumWithCoordsCombiner {
    /// Compute the coord term
    fn coord(&self) -> f32 {
        self.coords[self.num_fields]
    }


    pub fn default_for_num_scorers(num_scorers: usize) -> Self {
        let query_coords: Vec<Score> = (0..num_scorers + 1)
            .map(|i| (i as Score) / (num_scorers as Score))
            .collect();
        ScoreCombiner::from(query_coords)
    }
}

impl From<Vec<Score>> for ScoreCombiner {
    fn from(coords: Vec<Score>) -> SumWithCoordsCombiner {
        SumWithCoordsCombiner {
            coords,
            num_fields: 0,
            score: 0f32,
        }
    }
}

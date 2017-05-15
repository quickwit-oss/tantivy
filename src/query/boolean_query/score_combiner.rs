use Score;

pub struct ScoreCombiner {
    coords: Vec<Score>,
    num_fields: usize,
    score: Score,
}

impl ScoreCombiner {
    pub fn update(&mut self, score: Score) {
        self.score += score;
        self.num_fields += 1;
    }

    pub fn clear(&mut self) {
        self.score = 0f32;
        self.num_fields = 0;
    }

    /// Compute the coord term
    fn coord(&self) -> f32 {
        self.coords[self.num_fields]
    }

    pub fn score(&self) -> Score {
        self.score * self.coord()
    }

    pub fn default_for_num_scorers(num_scorers: usize) -> ScoreCombiner {
        let query_coords: Vec<Score> = (0..num_scorers + 1)
            .map(|i| (i as Score) / (num_scorers as Score))
            .collect();
        ScoreCombiner::from(query_coords)
    }
}

impl From<Vec<Score>> for ScoreCombiner {
    fn from(coords: Vec<Score>) -> ScoreCombiner {
        ScoreCombiner {
            coords: coords,
            num_fields: 0,
            score: 0f32,
        }
    }
}

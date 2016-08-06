use super::MultiTermAccumulator;
use super::MultiTermScorer;
use super::Explanation;

pub struct MultiTermExplainer<TScorer: MultiTermScorer + Sized> {
    scorer: TScorer,
    vals: Vec<(usize, u32, u32)>,
}

impl<TScorer: MultiTermScorer + Sized> MultiTermExplainer<TScorer> {
    pub fn explain_score(&self,) -> Explanation {
        self.scorer.explain(&self.vals)
    }
}

impl<TScorer: MultiTermScorer + Sized> From<TScorer> for MultiTermExplainer<TScorer> {
    fn from(multi_term_scorer: TScorer) -> MultiTermExplainer<TScorer> {
        MultiTermExplainer {
            scorer: multi_term_scorer,
            vals: Vec::new(),
        }
    }
}

impl<TScorer: MultiTermScorer + Sized> MultiTermAccumulator for MultiTermExplainer<TScorer> {
    fn update(&mut self, term_ord: usize, term_freq: u32, fieldnorm: u32) {
        self.vals.push((term_ord, term_freq, fieldnorm));
        self.scorer.update(term_ord, term_freq, fieldnorm);
    }
    
    fn clear(&mut self,) {
        self.vals.clear();
        self.scorer.clear();
    }
}


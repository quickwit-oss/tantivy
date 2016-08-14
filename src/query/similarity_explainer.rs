use Score;
use super::MultiTermAccumulator;
use super::Similarity;
use super::Explanation;

pub struct SimilarityExplainer<TSimilarity: Similarity + Sized> {
    scorer: TSimilarity,
    vals: Vec<(usize, u32, u32)>,
}

impl<TSimilarity: Similarity + Sized> SimilarityExplainer<TSimilarity> {
    pub fn explain_score(&self,) -> Explanation {
        self.scorer.explain(&self.vals)
    }
}

impl<TSimilarity: Similarity + Sized> From<TSimilarity> for SimilarityExplainer<TSimilarity> {
    fn from(multi_term_scorer: TSimilarity) -> SimilarityExplainer<TSimilarity> {
        SimilarityExplainer {
            scorer: multi_term_scorer,
            vals: Vec::new(),
        }
    }
}

impl<TSimilarity: Similarity + Sized> MultiTermAccumulator for SimilarityExplainer<TSimilarity> {
    fn update(&mut self, term_ord: usize, term_freq: u32, fieldnorm: u32) {
        self.vals.push((term_ord, term_freq, fieldnorm));
        self.scorer.update(term_ord, term_freq, fieldnorm);
    }
    
    fn clear(&mut self,) {
        self.vals.clear();
        self.scorer.clear();
    }
}


impl<TSimilarity: Similarity + Sized> Similarity for SimilarityExplainer<TSimilarity> {
    
    fn score(&self,) -> Score {
        self.scorer.score()
    } 
    
    fn explain(&self, vals: &Vec<(usize, u32, u32)>) -> Explanation {
        self.scorer.explain(vals)
    }
}

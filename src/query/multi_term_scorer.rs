use query::Scorer;
use query::Explanation; 


pub trait MultiTermScorer: Scorer {
    fn update(&mut self, term_ord: usize, term_freq: u32, fieldnorm: u32);
    fn clear(&mut self,);
    fn explain(&self, vals: &Vec<(usize, u32, u32)>) -> Explanation;
}

#[derive(Clone)]
pub struct TfIdfScorer {
    coords: Vec<f32>,
    idf: Vec<f32>,
    score: f32,
    num_fields: usize,
}

pub struct MultiTermExplainScorer<TScorer: MultiTermScorer + Sized> {
    scorer: TScorer,
    vals: Vec<(usize, u32, u32)>,
}

impl<TScorer: MultiTermScorer + Sized> MultiTermExplainScorer<TScorer> {
    pub fn explain_score(&self,) -> Explanation {
        self.scorer.explain(&self.vals)
    }
}

impl<TScorer: MultiTermScorer + Sized> From<TScorer> for MultiTermExplainScorer<TScorer> {
    fn from(multi_term_scorer: TScorer) -> MultiTermExplainScorer<TScorer> {
        MultiTermExplainScorer {
            scorer: multi_term_scorer,
            vals: Vec::new(),
        }
    }
}

impl<TScorer: MultiTermScorer + Sized> Scorer for MultiTermExplainScorer<TScorer> {
    fn score(&self,) -> f32 {
        self.scorer.score()
    }
}


impl<TScorer: MultiTermScorer + Sized> MultiTermScorer for MultiTermExplainScorer<TScorer> {
    fn update(&mut self, term_ord: usize, term_freq: u32, fieldnorm: u32) {
        self.vals.push((term_ord, term_freq, fieldnorm));
        self.scorer.update(term_ord, term_freq, fieldnorm);
    }
    fn clear(&mut self,) {
        self.vals.clear();
        self.scorer.clear();
    }
    fn explain(&self, vals: &Vec<(usize, u32, u32)>) -> Explanation {
        self.scorer.explain(vals)
    }
}

impl TfIdfScorer {
    pub fn new(mut coords: Vec<f32>, idf: Vec<f32>) -> TfIdfScorer {
        TfIdfScorer {
            coords: coords,
            idf: idf,
            score: 0f32,
            num_fields: 0,
        }
    }

    fn coord(&self,) -> f32 {
        self.coords[self.num_fields]
    }
}

impl Scorer for TfIdfScorer {
    fn score(&self, ) -> f32 {
        self.score * self.coord()
    }
}

impl MultiTermScorer for TfIdfScorer {

    fn explain(&self, vals: &Vec<(usize, u32, u32)>) -> Explanation {
        let mut explain = String::new();
        for &(ord, term_freq, field_norm) in vals.iter() {
            explain += &format!("{} {} {}.\n", ord, term_freq, field_norm);    
        }
        let count = vals.len();
        explain += &format!("coord({}) := {}", count, self.coords[count]); 
        Explanation::Explanation(explain)

    }

    fn update(&mut self, term_ord: usize, term_freq: u32, fieldnorm: u32) {
        assert!(term_freq != 0u32);
        self.score += (term_freq as f32 / fieldnorm as f32).sqrt() * self.idf[term_ord];
        self.num_fields += 1;
    }

    fn clear(&mut self,) {
        self.score = 0f32;
        self.num_fields = 0;
    }

}


#[cfg(test)]
mod tests {
    
    use super::*;
    use query::Scorer;
    
      
    fn abs_diff(left: f32, right: f32) -> f32 {
        (right - left).abs()
    }  

    #[test]
    pub fn test_multiterm_scorer() {
        let mut tfidf_scorer = TfIdfScorer::new(vec!(1f32, 2f32), vec!(1f32, 4f32));
        {
            tfidf_scorer.update(0, 1, 1);
            assert!(abs_diff(tfidf_scorer.score(), 1f32) < 0.001f32);
            tfidf_scorer.clear();
            
        }
        {
            tfidf_scorer.update(1, 1, 1);
            assert_eq!(tfidf_scorer.score(), 4f32);    
            tfidf_scorer.clear();
        }
        {
            tfidf_scorer.update(0, 2, 1);
            assert!(abs_diff(tfidf_scorer.score(), 1.4142135) < 0.001f32);          
            tfidf_scorer.clear();
        }
        {
            tfidf_scorer.update(0, 1, 1);
            tfidf_scorer.update(1, 1, 1);
            assert_eq!(tfidf_scorer.score(), 10f32);    
            tfidf_scorer.clear();
        }


    }

}

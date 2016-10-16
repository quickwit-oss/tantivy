use Score;
use super::MultiTermAccumulator;
use super::Explanation;
use super::Similarity;


/// `TfIdf` is the default pertinence score in tantivy.
///
/// See [Tf-Idf in the global documentation](https://fulmicoton.gitbooks.io/tantivy-doc/content/tfidf.html) 
#[derive(Clone)]
pub struct TfIdf {
    coords: Vec<f32>,
    idf: Vec<f32>,
    score: f32,
    num_fields: usize,
    term_names: Option<Vec<String>>, //< only here for explain
}

impl MultiTermAccumulator for TfIdf {
    
    #[inline]
    fn update(&mut self, term_ord: usize, term_freq: u32, fieldnorm: u32) {
        assert!(term_freq != 0u32);
        self.score += self.term_score(term_ord, term_freq, fieldnorm);
        self.num_fields += 1;
    }

    #[inline]
    fn clear(&mut self,) {
        self.score = 0f32;
        self.num_fields = 0;
    }
}

impl TfIdf {
    /// Constructor
    /// * coords - Coords act as a boosting factor for queries 
    ///   containing many terms. The coords must have a length 
    ///   of `num_terms + 1`
    /// * idf - idf value for each given term. `idf` must
    ///   have a length of `num_terms`. 
    pub fn new(coords: Vec<f32>, idf: Vec<f32>) -> TfIdf {
        TfIdf {
            coords: coords,
            idf: idf,
            score: 0f32,
            num_fields: 0,
            term_names: None,
        }
    }
    
    /// Compute the coord term
    fn coord(&self,) -> f32 {
        self.coords[self.num_fields]
    }
    
    /// Set the term names for the explain function
    pub fn set_term_names(&mut self, term_names: Vec<String>) {
        self.term_names = Some(term_names);
    }
    
    /// Return the name for the ordinal `ord` 
    fn term_name(&self, ord: usize) -> String {
        match self.term_names {
            Some(ref term_names_vec) => term_names_vec[ord].clone(),
                None => format!("Field({})", ord)
        }
    }
    
    #[inline]
    fn term_score(&self, term_ord: usize, term_freq: u32, field_norm: u32) -> f32 {
        (term_freq as f32 / field_norm as f32).sqrt() * self.idf[term_ord]
    }
}

impl Similarity for TfIdf {
    
    #[inline]
    fn score(&self, ) -> Score {
        self.score * self.coord()
    }
    
    fn explain(&self, vals: &[(usize, u32, u32)]) -> Explanation {
        let score = self.score();
        let mut explanation = Explanation::with_val(score);
        let formula_components: Vec<String> = vals.iter()
            .map(|&(ord, _, _)| ord)
            .map(|ord| format!("<score for ({}>", self.term_name(ord)))
            .collect();
        let formula = format!("<coord> * ({})", formula_components.join(" + "));
        explanation.set_formula(&formula);
        for &(ord, term_freq, field_norm) in vals {
            let term_score = self.term_score(ord, term_freq, field_norm);
            let term_explanation = explanation.add_child(&self.term_name(ord), term_score);
            term_explanation.set_formula(" sqrt(<term_freq> / <field_norm>) * <idf>");    
        }
        explanation
    }
}




#[cfg(test)]
mod tests {
    
    use super::*;
    use query::MultiTermAccumulator;
    use query::Similarity;
    
    fn abs_diff(left: f32, right: f32) -> f32 {
        (right - left).abs()
    }  

    #[test]
    pub fn test_tfidf() {
        let mut tfidf = TfIdf::new(vec!(0f32, 1f32, 2f32), vec!(1f32, 4f32));
        {
            tfidf.update(0, 1, 1);
            assert!(abs_diff(tfidf.score(), 1f32) < 0.001f32);
            tfidf.clear();
        }
        {
            tfidf.update(1, 1, 1);
            assert_eq!(tfidf.score(), 4f32);    
            tfidf.clear();
        }
        {
            tfidf.update(0, 2, 1);
            assert!(abs_diff(tfidf.score(), 1.4142135) < 0.001f32);          
            tfidf.clear();
        }
        {
            tfidf.update(0, 1, 1);
            tfidf.update(1, 1, 1);
            assert_eq!(tfidf.score(), 10f32);    
            tfidf.clear();
        }


    }

}
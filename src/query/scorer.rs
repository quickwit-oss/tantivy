use DocSet;


/// Scored `DocSet`
pub trait Scorer: DocSet {
    
    /// Returns the score.
    /// 
    /// This method will perform a bit of computation and is not cached.
    fn score(&self,) -> f32;
} 



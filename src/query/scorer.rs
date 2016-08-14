use DocSet;

pub trait Scorer: DocSet {
    fn score(&self,) -> f32;
} 



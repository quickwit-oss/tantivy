use DocSet;
use collector::Collector;

/// Scored `DocSet`
pub trait Scorer: DocSet {
    
    /// Returns the score.
    /// 
    /// This method will perform a bit of computation and is not cached.
    fn score(&self,) -> f32;
    
    fn collect(&mut self, collector: &mut Collector) {
        while self.advance() {
            collector.collect(self.doc(), self.score());
        }
    }
} 


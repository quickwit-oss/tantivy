use Result;
use collector::Collector;
use core::searcher::Searcher;
use common::TimerTree;
use DocAddress;
use query::Explanation;


/// Queries represent the query of the user, and are in charge
/// of the logic defining the set of documents that should be
/// sent to the collector, as well as the way to score the
/// documents. 
pub trait Query {
    
    /// Perform the search operation
    fn search<C: Collector>(
        &self,
        searcher: &Searcher,
        collector: &mut C) -> Result<TimerTree>;
        
    /// Explain the score of a specific document
    fn explain(
        &self,
        searcher: &Searcher,
        doc_address: &DocAddress) -> Result<Explanation>;
}

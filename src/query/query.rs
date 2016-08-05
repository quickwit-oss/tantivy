use std::io;
use collector::Collector;
use core::searcher::Searcher;
use common::TimerTree;
use DocAddress;
use query::Explanation;

pub trait Query {
    
    fn search<C: Collector>(
        &self,
        searcher: &Searcher,
        collector: &mut C) -> io::Result<TimerTree>;

    fn explain(
        &self,
        searcher: &Searcher,
        doc_address: &DocAddress) -> Result<Explanation, String> {
            // TODO check that the document is there or return an error.
            panic!("Not implemented");
    }
}

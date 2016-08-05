use std::io;
use collector::Collector;
use core::searcher::Searcher;
use common::TimerTree;
use DocAddress;
use query::Explanation;
use Score;

pub trait Query {
    
    fn search<C: Collector>(
        &self,
        searcher: &Searcher,
        collector: &mut C) -> io::Result<TimerTree>;

    fn explain(
        &self,
        searcher: &Searcher,
        doc_address: &DocAddress) -> Result<Option<(Score, Explanation)>, io::Error> {
            // TODO check that the document is there or return an error.
            panic!("Not implemented");
    }
}

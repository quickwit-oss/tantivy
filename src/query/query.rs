use std::io;
use collector::Collector;
use core::searcher::Searcher;
use common::TimerTree;

pub trait Query {
    fn search<C: Collector>(&self, searcher: &Searcher, collector: &mut C) -> io::Result<TimerTree>;
}

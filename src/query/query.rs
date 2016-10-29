use Result;
use collector::Collector;
use core::searcher::Searcher;
use common::TimerTree;
use SegmentLocalId;
use super::Weight;


/// Queries represent the query of the user, and are in charge
/// of the logic defining the set of documents that should be
/// sent to the collector, as well as the way to score the
/// documents. 
pub trait Query {


    fn weight(&self, searcher: &Searcher) -> Result<Box<Weight>>;
        
    /// Perform the search operation
    fn search(
        &self,
        searcher: &Searcher,
        collector: &mut Collector) -> Result<TimerTree> {
            
        let mut timer_tree = TimerTree::default();    
        let weight = try!(self.weight(searcher));
        
        {
            let mut search_timer = timer_tree.open("search");
            for (segment_ord, segment_reader) in searcher.segment_readers().iter().enumerate() {
                let mut segment_search_timer = search_timer.open("segment_search");
                {
                    let _ = segment_search_timer.open("set_segment");
                    try!(collector.set_segment(segment_ord as SegmentLocalId, &segment_reader));
                }
                let mut scorer = try!(weight.scorer(segment_reader));
                {
                    let _collection_timer = segment_search_timer.open("collection");
                    scorer.collect(collector);
                }
            }
        }
        Ok(timer_tree)
    }
        
}

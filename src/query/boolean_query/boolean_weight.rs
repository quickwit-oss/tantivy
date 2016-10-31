use query::Weight;
use core::SegmentReader;
use query::Scorer;
use query::OccurFilter;
use Result;

pub struct BooleanWeight {
    weights: Vec<Box<Weight>>,
    occur_filter: OccurFilter,
}

impl BooleanWeight {
    pub fn new(weights: Vec<Box<Weight>>, 
           occur_filter: OccurFilter) -> BooleanWeight {
        BooleanWeight {
            weights: weights,
            occur_filter: occur_filter,
        }
    }
}


impl Weight for BooleanWeight {

    fn scorer<'a>(&'a self, reader: &'a SegmentReader) -> Result<Box<Scorer + 'a>> {
        // BooleanScorer {
            
        // }
        panic!("");
        
    }
    
}
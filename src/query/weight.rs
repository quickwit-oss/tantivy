use super::Scorer;
use Result;
use core::SegmentReader;

pub trait Weight {
    
    
    fn scorer<'a>(&'a self, reader: &'a SegmentReader) -> Result<Box<Scorer + 'a>>;
    
    
}

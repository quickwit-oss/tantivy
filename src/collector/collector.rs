use SegmentReader;
use SegmentLocalId;
use std::io;
use ScoredDoc;

pub trait Collector {
    fn set_segment(&mut self, segment_local_id: SegmentLocalId, segment: &SegmentReader) -> io::Result<()>;
    fn collect(&mut self, scored_doc: ScoredDoc);
}
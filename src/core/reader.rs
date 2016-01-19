use core::directory::Directory;
use core::directory::Segment;
use core::schema::Term;
// use fst::stream::Streamer;
use fst;

pub struct SegmentIndexReader {
    segment: Segment,
    term_offsets: fst::Map,
}
//
// pub struct SegmentDocCursor<'a> {
//     data: &'a [u8],
// }
//
// struct SegmentTermCur<'a> {
//     segment: &'a Segment,
//     fst_streamer: fst::map::Stream<'a>,
//     term: Term<'a>,
//
// }
//
// impl<'a> SegmentTermCur<'a> {
//
//     fn next(&mut self,) -> Option<(Term<'a>, SegmentDocCursor<'a>)> {
//         match self.fst_streamer.next() {
//             Some(_) => None,
//             None => None
//         }
//     }
// }
//
//
// impl SegmentIndexReader {
//
//     fn term_cursor<'a>(&'a self) -> SegmentTermCur<'a> {
//         let term: Term<'a> {
//             self.
//         };
//         SegmentTermCur {
//             segment: &self.segment,
//             fst_streamer: self.term_offsets.stream(),
//             term:
//         }
//     }
//
// }

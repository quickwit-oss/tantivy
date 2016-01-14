use std::io;
use core::serial::SerializableSegment;
use std::io::Write;

pub struct SegmentOutput<'a> {
    terms: &'a Write,
    postings: &'a Write,
    // TODO positions, docvalues, ...
}


pub trait Codec {
    fn write<'a, 'b, I: SerializableSegment<'a>>(index: &I, output: &'b SegmentOutput) -> Result<usize, io::Error> {
        Ok(0)
    }
}

pub struct DebugCodec;

// impl DebugCodec {
//     fn write_field(field_name) {
//
//     }
//
//     fn serialize_postings_for_term() -> Result<usize, io::Error> {
//
//     }
// }
//
// impl Codec for DebugCodec {
//     fn write<'a, I: SerializableSegment<'a>>(index: &I, output: &SegmentOutput) -> Result<usize, io::Error> {
//         let mut field_cursor = index.field_cursor();
//         let mut posting_offset: usize = 0;
//         loop {
//             match field_cursor.next() {
//                 Some(field) => {
//                     let field_name = field_cursor.get_field();
//                     try!(DebugCodec::write_term(field_name, posting_offset, output.terms));
//                     let term_cursor = field_cursor.term_cursor();
//                     let len = try!(DebugCodec::serialize_postings_for_term(term_cursor));
//                     posting_offset += len;
//                 },
//                 None => { break; },
//             }
//         }
//         Ok(1)
//     }
// }

//
// let mut field_cursor = closed_index_writer.field_cursor();
// loop {
//     match field_cursor.next() {
//         Some(field) => {
//             println!("  {:?}", field);
//             show_term_cursor(field_cursor.term_cursor());
//         },
//         None => { break; },
//     }
// }

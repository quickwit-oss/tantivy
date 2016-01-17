use std::io;
use core::serial::*;
use std::io::Write;
use fst::MapBuilder;
use core::error::*;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

pub trait SegmentOutput<'a, W: Write> {
    fn terms(&self,) -> W;
    fn postings(&self,) -> W;
    // TODO positions, docvalues, ...
}


pub trait Codec {
    fn write<'a, I: SerializableSegment<'a>, W: Write>(index: &'a I, output: &'a SegmentOutput<'a, W>) -> Result<usize>;
}

pub struct SimpleCodec;

impl SimpleCodec {
    fn  write_postings<D: DocCursor, W: Write>(mut doc_it: D, postings: &mut W) -> Result<usize> {
        let mut written_bytes: usize = 4;
        postings.write_u32::<LittleEndian>(doc_it.len() as u32);
        // TODO handle error correctly
        for doc_id in doc_it {
            postings.write_u32::<LittleEndian>(doc_id as u32);
            written_bytes += 4;
        }
        Ok(written_bytes)
    }
}

impl Codec for SimpleCodec {
    fn write<'a, I: SerializableSegment<'a>, W: Write>(index: &'a I, output: &'a SegmentOutput<'a, W>) -> Result<usize> {
        let term_trie_builder_result = MapBuilder::new(output.terms());
        if term_trie_builder_result.is_err() {
            // TODO include cause somehow
            return Err(Error::IOError(String::from("Failed creating the term builder")));
        }
        let mut term_buffer: String = String::new();
        let mut term_trie_builder = term_trie_builder_result.unwrap();
        let mut term_cursor = index.term_cursor();
        let mut offset: usize = 0;
        let mut postings_output = output.postings();
        loop {
            match term_cursor.next() {
                Some((term, doc_it)) => {
                    term.write_into(&mut term_buffer);
                    match term_trie_builder.insert(&term_buffer, offset as u64) {
                        Ok(_) => {}
                        Err(_) => {
                            return Err(Error::IOError(String::from("Failed while inserting into the fst")))
                        },
                    }
                    offset += try!(SimpleCodec::write_postings(doc_it, &mut postings_output));
                },
                None => {
                    break;
                }
            }
        }
        Ok(0)

    }
}





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

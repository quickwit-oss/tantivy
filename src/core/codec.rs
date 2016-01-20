use std::io;
use core::serial::*;
use std::io::Write;
use fst::MapBuilder;
use core::error::*;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use core::directory::Segment;
use core::directory::SegmentComponent;
use core::reader::*;


pub struct SimpleCodec;

impl SimpleCodec {
    fn  write_postings<D: DocCursor, W: Write>(doc_it: D, postings: &mut W) -> Result<usize> {
        let mut written_bytes: usize = 4;
        match postings.write_u32::<LittleEndian>(doc_it.len() as u32) {
            Ok(_) => {},
            Err(_) => {
                let msg = String::from("Failed writing posting list length");
                return Err(Error::WriteError(msg));
            },
        }
        for doc_id in doc_it {
            println!("  Doc {}", doc_id);
            match postings.write_u32::<LittleEndian>(doc_id as u32) {
                Ok(_) => {},
                Err(_) => {
                    let msg = String::from("Failed while writing posting list");
                    return Err(Error::WriteError(msg));
                },
            }
            written_bytes += 4;
        }
        Ok(written_bytes)
    }

    // TODO impl packed int
    // TODO skip lists



    pub fn write<'a, I: SerializableSegment<'a>>(index: &'a I, segment: &'a Segment) -> Result<usize> {
        let term_write = try!(segment.open_writable(SegmentComponent::TERMS));
        let mut postings_write = try!(segment.open_writable(SegmentComponent::POSTINGS));
        let term_trie_builder_result = MapBuilder::new(term_write);
        if term_trie_builder_result.is_err() {
            // TODO include cause somehow
            return Err(Error::WriteError(String::from("Failed creating the term builder")));
        }
        let mut term_buffer: Vec<u8> = Vec::new();
        let mut term_trie_builder = term_trie_builder_result.unwrap();
        let mut term_cursor = index.term_cursor();
        let mut offset: usize = 0;
        loop {
            match term_cursor.next() {
                Some((term, doc_it)) => {
                    println!("{:?}", term);
                    term.write_into(&mut term_buffer);
                    match term_trie_builder.insert(&term_buffer, offset as u64) {
                        Ok(_) => {}
                        Err(_) => {
                            return Err(Error::WriteError(String::from("Failed while inserting into the fst")))
                        },
                    }
                    offset += try!(SimpleCodec::write_postings(doc_it, &mut postings_write));
                },
                None => {
                    break;
                }
            }
        }
        term_trie_builder.finish();
        Ok(0)

    }
}

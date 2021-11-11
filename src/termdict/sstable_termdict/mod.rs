use std::io;

mod merger;
mod sstable;
mod streamer;
mod termdict;

use std::iter::ExactSizeIterator;

use common::VInt;

pub use self::merger::TermMerger;
use self::sstable::value::{ValueReader, ValueWriter};
use self::sstable::{BlockReader, SSTable};
pub use self::streamer::{TermStreamer, TermStreamerBuilder};
pub use self::termdict::{TermDictionary, TermDictionaryBuilder};
use crate::postings::TermInfo;

pub struct TermSSTable;

impl SSTable for TermSSTable {
    type Value = TermInfo;
    type Reader = TermInfoReader;
    type Writer = TermInfoWriter;
}

#[derive(Default)]
pub struct TermInfoReader {
    term_infos: Vec<TermInfo>,
}

impl ValueReader for TermInfoReader {
    type Value = TermInfo;

    fn value(&self, idx: usize) -> &TermInfo {
        &self.term_infos[idx]
    }

    fn read(&mut self, reader: &mut BlockReader) -> io::Result<()> {
        self.term_infos.clear();
        let num_els = VInt::deserialize_u64(reader)?;
        let mut postings_start = VInt::deserialize_u64(reader)? as usize;
        let mut positions_start = VInt::deserialize_u64(reader)? as usize;
        for _ in 0..num_els {
            let doc_freq = VInt::deserialize_u64(reader)? as u32;
            let postings_num_bytes = VInt::deserialize_u64(reader)?;
            let positions_num_bytes = VInt::deserialize_u64(reader)?;
            let postings_end = postings_start + postings_num_bytes as usize;
            let positions_end = positions_start + positions_num_bytes as usize;
            let term_info = TermInfo {
                doc_freq,
                postings_range: postings_start..postings_end,
                positions_range: positions_start..positions_end,
            };
            self.term_infos.push(term_info);
            postings_start = postings_end;
            positions_start = positions_end;
        }
        Ok(())
    }
}

#[derive(Default)]
pub struct TermInfoWriter {
    term_infos: Vec<TermInfo>,
}

impl ValueWriter for TermInfoWriter {
    type Value = TermInfo;

    fn write(&mut self, term_info: &TermInfo) {
        self.term_infos.push(term_info.clone());
    }

    fn write_block(&mut self, buffer: &mut Vec<u8>) {
        VInt(self.term_infos.len() as u64).serialize_into_vec(buffer);
        if self.term_infos.is_empty() {
            return;
        }
        VInt(self.term_infos[0].postings_range.start as u64).serialize_into_vec(buffer);
        VInt(self.term_infos[0].positions_range.start as u64).serialize_into_vec(buffer);
        for term_info in &self.term_infos {
            VInt(term_info.doc_freq as u64).serialize_into_vec(buffer);
            VInt(term_info.postings_range.len() as u64).serialize_into_vec(buffer);
            VInt(term_info.positions_range.len() as u64).serialize_into_vec(buffer);
        }
        self.term_infos.clear();
    }
}

#[cfg(test)]
mod tests {
    use std::io;

    use super::BlockReader;
    use crate::directory::OwnedBytes;
    use crate::postings::TermInfo;
    use crate::termdict::sstable_termdict::sstable::value::{ValueReader, ValueWriter};
    use crate::termdict::sstable_termdict::TermInfoReader;

    #[test]
    fn test_block_terminfos() -> io::Result<()> {
        let mut term_info_writer = super::TermInfoWriter::default();
        term_info_writer.write(&TermInfo {
            doc_freq: 120u32,
            postings_range: 17..45,
            positions_range: 10..122,
        });
        term_info_writer.write(&TermInfo {
            doc_freq: 10u32,
            postings_range: 45..450,
            positions_range: 122..1100,
        });
        term_info_writer.write(&TermInfo {
            doc_freq: 17u32,
            postings_range: 450..462,
            positions_range: 1100..1302,
        });
        let mut buffer = Vec::new();
        term_info_writer.write_block(&mut buffer);
        let mut block_reader = make_block_reader(&buffer[..]);
        let mut term_info_reader = TermInfoReader::default();
        term_info_reader.read(&mut block_reader)?;
        assert_eq!(
            term_info_reader.value(0),
            &TermInfo {
                doc_freq: 120u32,
                postings_range: 17..45,
                positions_range: 10..122
            }
        );
        assert!(block_reader.buffer().is_empty());
        Ok(())
    }

    fn make_block_reader(data: &[u8]) -> BlockReader {
        let mut buffer = (data.len() as u32).to_le_bytes().to_vec();
        buffer.extend_from_slice(data);
        let owned_bytes = OwnedBytes::new(buffer);
        let mut block_reader = BlockReader::new(Box::new(owned_bytes));
        block_reader.read_block().unwrap();
        block_reader
    }
}

use std::io;

mod sstable;
mod streamer;
mod termdict;

use self::sstable::value::{ValueReader, ValueWriter};
use self::sstable::{BlockReader, SSTable};

use crate::common::VInt;
use crate::postings::TermInfo;

pub use self::streamer::{TermStreamer, TermStreamerBuilder};
pub use self::termdict::{TermDictionary, TermDictionaryBuilder};

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
        let mut start_offset = VInt::deserialize_u64(reader)?;
        let mut positions_idx = 0;
        for _ in 0..num_els {
            let doc_freq = VInt::deserialize_u64(reader)? as u32;
            let posting_num_bytes = VInt::deserialize_u64(reader)?;
            let stop_offset = start_offset + posting_num_bytes;
            let delta_positions_idx = VInt::deserialize_u64(reader)?;
            positions_idx += delta_positions_idx;
            let term_info = TermInfo {
                doc_freq,
                postings_start_offset: start_offset,
                postings_stop_offset: stop_offset,
                positions_idx,
            };
            self.term_infos.push(term_info);
            start_offset = stop_offset;
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
        let mut prev_position_idx = 0u64;
        VInt(self.term_infos[0].postings_start_offset).serialize_into_vec(buffer);
        for term_info in &self.term_infos {
            VInt(term_info.doc_freq as u64).serialize_into_vec(buffer);
            VInt(term_info.postings_stop_offset - term_info.postings_start_offset)
                .serialize_into_vec(buffer);
            VInt(term_info.positions_idx - prev_position_idx).serialize_into_vec(buffer);
            prev_position_idx = term_info.positions_idx;
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
            postings_start_offset: 17u64,
            postings_stop_offset: 45u64,
            positions_idx: 10u64,
        });
        term_info_writer.write(&TermInfo {
            doc_freq: 10u32,
            postings_start_offset: 45u64,
            postings_stop_offset: 450u64,
            positions_idx: 104u64,
        });
        term_info_writer.write(&TermInfo {
            doc_freq: 17u32,
            postings_start_offset: 450u64,
            postings_stop_offset: 462u64,
            positions_idx: 210u64,
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
                postings_start_offset: 17u64,
                postings_stop_offset: 45u64,
                positions_idx: 10u64
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

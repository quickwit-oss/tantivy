use crate::common::BinarySerializable;
use crate::directory::ReadOnlySource;
use crate::postings::compression::{compressed_block_size, COMPRESSION_BLOCK_SIZE};
use crate::schema::IndexRecordOption;
use crate::{DocId, TERMINATED};
use owned_read::OwnedRead;

pub struct SkipSerializer {
    buffer: Vec<u8>,
    prev_doc: DocId,
}

impl SkipSerializer {
    pub fn new() -> SkipSerializer {
        SkipSerializer {
            buffer: Vec::new(),
            prev_doc: 0u32,
        }
    }

    pub fn write_doc(&mut self, last_doc: DocId, doc_num_bits: u8) {
        assert!(
            last_doc > self.prev_doc,
            "write_doc(...) called with non-increasing doc ids. \
             Did you forget to call clear maybe?"
        );
        let delta_doc = last_doc - self.prev_doc;
        self.prev_doc = last_doc;
        delta_doc.serialize(&mut self.buffer).unwrap();
        self.buffer.push(doc_num_bits);
    }

    pub fn write_term_freq(&mut self, tf_num_bits: u8) {
        self.buffer.push(tf_num_bits);
    }

    pub fn write_total_term_freq(&mut self, tf_sum: u32) {
        tf_sum
            .serialize(&mut self.buffer)
            .expect("Should never fail");
    }

    pub fn data(&self) -> &[u8] {
        &self.buffer[..]
    }

    pub fn clear(&mut self) {
        self.prev_doc = 0u32;
        self.buffer.clear();
    }
}

pub(crate) struct SkipReader {
    last_doc_in_block: DocId,
    pub(crate) last_doc_in_previous_block: DocId,
    owned_read: OwnedRead,
    skip_info: IndexRecordOption,
    byte_offset: usize,
    remaining_docs: u32, // number of docs remaining, including the
    // documents in the current block.
    block_info: BlockInfo,

    position_offset: u64,
}

#[derive(Clone, Eq, PartialEq, Copy, Debug)]
pub(crate) enum BlockInfo {
    BitPacked {
        doc_num_bits: u8,
        tf_num_bits: u8,
        tf_sum: u32,
    },
    VInt(u32),
}

impl Default for BlockInfo {
    fn default() -> Self {
        BlockInfo::VInt(0)
    }
}

impl SkipReader {
    pub fn new(data: ReadOnlySource, doc_freq: u32, skip_info: IndexRecordOption) -> SkipReader {
        SkipReader {
            last_doc_in_block: 0u32,
            last_doc_in_previous_block: 0u32,
            owned_read: OwnedRead::new(data),
            skip_info,
            block_info: BlockInfo::default(),
            byte_offset: 0,
            remaining_docs: doc_freq,
            position_offset: 0u64,
        }
    }

    pub fn reset(&mut self, data: ReadOnlySource, doc_freq: u32) {
        self.last_doc_in_block = 0u32;
        self.last_doc_in_previous_block = 0u32;
        self.owned_read = OwnedRead::new(data);
        self.block_info = BlockInfo::default();
        self.byte_offset = 0;
        self.remaining_docs = doc_freq;
    }

    pub fn doc(&self) -> DocId {
        self.last_doc_in_block
    }

    pub fn position_offset(&self) -> u64 {
        self.position_offset
    }

    pub fn byte_offset(&self) -> usize {
        self.byte_offset
    }

    fn read_block_info(&mut self) {
        let doc_delta = u32::deserialize(&mut self.owned_read).expect("Skip data corrupted");
        self.last_doc_in_block += doc_delta as DocId;
        let doc_num_bits = self.owned_read.get(0);
        match self.skip_info {
            IndexRecordOption::Basic => {
                self.owned_read.advance(1);
                self.block_info = BlockInfo::BitPacked {
                    doc_num_bits,
                    tf_num_bits: 0,
                    tf_sum: 0,
                };
            }
            IndexRecordOption::WithFreqs => {
                let tf_num_bits = self.owned_read.get(1);
                self.block_info = BlockInfo::BitPacked {
                    doc_num_bits,
                    tf_num_bits,
                    tf_sum: 0,
                };
                self.owned_read.advance(2);
            }
            IndexRecordOption::WithFreqsAndPositions => {
                let tf_num_bits = self.owned_read.get(1);
                self.owned_read.advance(2);
                let tf_sum = u32::deserialize(&mut self.owned_read).expect("Failed reading tf_sum");
                self.block_info = BlockInfo::BitPacked {
                    doc_num_bits,
                    tf_num_bits,
                    tf_sum,
                };
            }
        }
    }

    pub fn block_info(&self) -> BlockInfo {
        self.block_info
    }

    /// Advance the skip reader to the block that may contain the target.
    ///
    /// If the target is larger than all documents, the skip_reader
    /// then advance to the last Variable In block.
    ///
    /// Returns true if the last block is reached.
    pub fn seek(&mut self, target: DocId) {
        while self.doc() < target {
            self.advance();
        }
    }

    pub fn advance(&mut self) -> bool {
        match self.block_info {
            BlockInfo::BitPacked {
                doc_num_bits,
                tf_num_bits,
                tf_sum,
            } => {
                self.remaining_docs -= COMPRESSION_BLOCK_SIZE as u32;
                self.byte_offset += compressed_block_size(doc_num_bits + tf_num_bits);
                self.position_offset += tf_sum as u64;
            }
            BlockInfo::VInt(num_vint_docs) => {
                self.remaining_docs -= num_vint_docs;
            }
        }
        self.last_doc_in_previous_block = self.last_doc_in_block;
        if self.remaining_docs >= COMPRESSION_BLOCK_SIZE as u32 {
            self.read_block_info();
            true
        } else {
            self.last_doc_in_block = TERMINATED;
            self.block_info = BlockInfo::VInt(self.remaining_docs);
            self.remaining_docs > 0
        }
    }
}

#[cfg(test)]
mod tests {

    use super::BlockInfo;
    use super::IndexRecordOption;
    use super::{SkipReader, SkipSerializer};
    use crate::directory::ReadOnlySource;
    use crate::postings::compression::COMPRESSION_BLOCK_SIZE;

    #[test]
    fn test_skip_with_freq() {
        let buf = {
            let mut skip_serializer = SkipSerializer::new();
            skip_serializer.write_doc(1u32, 2u8);
            skip_serializer.write_term_freq(3u8);
            skip_serializer.write_doc(5u32, 5u8);
            skip_serializer.write_term_freq(2u8);
            skip_serializer.data().to_owned()
        };
        let doc_freq = 3u32 + (COMPRESSION_BLOCK_SIZE * 2) as u32;
        let mut skip_reader = SkipReader::new(
            ReadOnlySource::new(buf),
            doc_freq,
            IndexRecordOption::WithFreqs,
        );
        assert!(skip_reader.advance());
        assert_eq!(skip_reader.doc(), 1u32);
        assert_eq!(
            skip_reader.block_info(),
            BlockInfo::BitPacked {
                doc_num_bits: 2u8,
                tf_num_bits: 3u8,
                tf_sum: 0
            }
        );
        assert!(skip_reader.advance());
        assert_eq!(skip_reader.doc(), 5u32);
        assert_eq!(
            skip_reader.block_info(),
            BlockInfo::BitPacked {
                doc_num_bits: 5u8,
                tf_num_bits: 2u8,
                tf_sum: 0
            }
        );
        assert!(skip_reader.advance());
        assert_eq!(skip_reader.block_info(), BlockInfo::VInt(3u32));
        assert!(!skip_reader.advance());
    }

    #[test]
    fn test_skip_no_freq() {
        let buf = {
            let mut skip_serializer = SkipSerializer::new();
            skip_serializer.write_doc(1u32, 2u8);
            skip_serializer.write_doc(5u32, 5u8);
            skip_serializer.data().to_owned()
        };
        let doc_freq = 3u32 + (COMPRESSION_BLOCK_SIZE * 2) as u32;
        let mut skip_reader = SkipReader::new(
            ReadOnlySource::from(buf),
            doc_freq,
            IndexRecordOption::Basic,
        );
        assert!(skip_reader.advance());
        assert_eq!(skip_reader.doc(), 1u32);
        assert_eq!(
            skip_reader.block_info(),
            BlockInfo::BitPacked {
                doc_num_bits: 2u8,
                tf_num_bits: 0,
                tf_sum: 0u32
            }
        );
        assert!(skip_reader.advance());
        assert_eq!(skip_reader.doc(), 5u32);
        assert_eq!(
            skip_reader.block_info(),
            BlockInfo::BitPacked {
                doc_num_bits: 5u8,
                tf_num_bits: 0,
                tf_sum: 0u32
            }
        );
        assert!(skip_reader.advance());
        assert_eq!(skip_reader.block_info(), BlockInfo::VInt(3u32));
        assert!(!skip_reader.advance());
    }

    #[test]
    fn test_skip_multiple_of_block_size() {
        let buf = {
            let mut skip_serializer = SkipSerializer::new();
            skip_serializer.write_doc(1u32, 2u8);
            skip_serializer.data().to_owned()
        };
        let doc_freq = COMPRESSION_BLOCK_SIZE as u32;
        let mut skip_reader = SkipReader::new(
            ReadOnlySource::from(buf),
            doc_freq,
            IndexRecordOption::Basic,
        );
        assert!(skip_reader.advance());
        assert_eq!(skip_reader.doc(), 1u32);
        assert_eq!(
            skip_reader.block_info(),
            BlockInfo::BitPacked {
                doc_num_bits: 2u8,
                tf_num_bits: 0,
                tf_sum: 0u32
            }
        );
        assert!(!skip_reader.advance());
    }
}

use std::convert::TryInto;

use crate::directory::OwnedBytes;
use crate::postings::compression::{compressed_block_size, COMPRESSION_BLOCK_SIZE};
use crate::query::BM25Weight;
use crate::schema::IndexRecordOption;
use crate::{DocId, Score, TERMINATED};

#[inline(always)]
fn encode_block_wand_max_tf(max_tf: u32) -> u8 {
    max_tf.min(u8::MAX as u32) as u8
}

#[inline(always)]
fn decode_block_wand_max_tf(max_tf_code: u8) -> u32 {
    if max_tf_code == u8::MAX {
        u32::MAX
    } else {
        max_tf_code as u32
    }
}

#[inline(always)]
fn read_u32(data: &[u8]) -> u32 {
    u32::from_le_bytes(data[..4].try_into().unwrap())
}

#[inline(always)]
fn write_u32(val: u32, buf: &mut Vec<u8>) {
    buf.extend_from_slice(&val.to_le_bytes());
}

pub struct SkipSerializer {
    buffer: Vec<u8>,
}

impl SkipSerializer {
    pub fn new() -> SkipSerializer {
        SkipSerializer { buffer: Vec::new() }
    }

    pub fn write_doc(&mut self, last_doc: DocId, doc_num_bits: u8) {
        write_u32(last_doc, &mut self.buffer);
        self.buffer.push(doc_num_bits);
    }

    pub fn write_term_freq(&mut self, tf_num_bits: u8) {
        self.buffer.push(tf_num_bits);
    }

    pub fn write_total_term_freq(&mut self, tf_sum: u32) {
        write_u32(tf_sum, &mut self.buffer);
    }

    pub fn write_blockwand_max(&mut self, fieldnorm_id: u8, term_freq: u32) {
        let block_wand_tf = encode_block_wand_max_tf(term_freq);
        self.buffer
            .extend_from_slice(&[fieldnorm_id, block_wand_tf]);
    }

    pub fn data(&self) -> &[u8] {
        &self.buffer[..]
    }

    pub fn clear(&mut self) {
        self.buffer.clear();
    }
}

#[derive(Clone)]
pub(crate) struct SkipReader {
    last_doc_in_block: DocId,
    pub(crate) last_doc_in_previous_block: DocId,
    owned_read: OwnedBytes,
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
        block_wand_fieldnorm_id: u8,
        block_wand_term_freq: u32,
    },
    VInt {
        num_docs: u32,
    },
}

impl Default for BlockInfo {
    fn default() -> Self {
        BlockInfo::VInt { num_docs: 0u32 }
    }
}

impl SkipReader {
    pub fn new(data: OwnedBytes, doc_freq: u32, skip_info: IndexRecordOption) -> SkipReader {
        let mut skip_reader = SkipReader {
            last_doc_in_block: if doc_freq >= COMPRESSION_BLOCK_SIZE as u32 {
                0
            } else {
                TERMINATED
            },
            last_doc_in_previous_block: 0u32,
            owned_read: data,
            skip_info,
            block_info: BlockInfo::VInt { num_docs: doc_freq },
            byte_offset: 0,
            remaining_docs: doc_freq,
            position_offset: 0u64,
        };
        if doc_freq >= COMPRESSION_BLOCK_SIZE as u32 {
            skip_reader.read_block_info();
        }
        skip_reader
    }

    pub fn reset(&mut self, data: OwnedBytes, doc_freq: u32) {
        self.last_doc_in_block = if doc_freq >= COMPRESSION_BLOCK_SIZE as u32 {
            0
        } else {
            TERMINATED
        };
        self.last_doc_in_previous_block = 0u32;
        self.owned_read = data;
        self.block_info = BlockInfo::VInt { num_docs: doc_freq };
        self.byte_offset = 0;
        self.remaining_docs = doc_freq;
        self.position_offset = 0u64;
        if doc_freq >= COMPRESSION_BLOCK_SIZE as u32 {
            self.read_block_info();
        }
    }

    // Returns the block max score for this block if available.
    //
    // The block max score is available for all full bitpacked block,
    // but no available for the last VInt encoded incomplete block.
    pub fn block_max_score(&self, bm25_weight: &BM25Weight) -> Option<Score> {
        match self.block_info {
            BlockInfo::BitPacked {
                block_wand_fieldnorm_id,
                block_wand_term_freq,
                ..
            } => Some(bm25_weight.score(block_wand_fieldnorm_id, block_wand_term_freq)),
            BlockInfo::VInt { .. } => None,
        }
    }

    pub(crate) fn last_doc_in_block(&self) -> DocId {
        self.last_doc_in_block
    }

    pub fn position_offset(&self) -> u64 {
        self.position_offset
    }

    #[inline(always)]
    pub fn byte_offset(&self) -> usize {
        self.byte_offset
    }

    fn read_block_info(&mut self) {
        let bytes = self.owned_read.as_slice();
        let advance_len: usize;
        self.last_doc_in_block = read_u32(bytes);
        let doc_num_bits = bytes[4];
        match self.skip_info {
            IndexRecordOption::Basic => {
                advance_len = 5;
                self.block_info = BlockInfo::BitPacked {
                    doc_num_bits,
                    tf_num_bits: 0,
                    tf_sum: 0,
                    block_wand_fieldnorm_id: 0,
                    block_wand_term_freq: 0,
                };
            }
            IndexRecordOption::WithFreqs => {
                let tf_num_bits = bytes[5];
                let block_wand_fieldnorm_id = bytes[6];
                let block_wand_term_freq = decode_block_wand_max_tf(bytes[7]);
                advance_len = 8;
                self.block_info = BlockInfo::BitPacked {
                    doc_num_bits,
                    tf_num_bits,
                    tf_sum: 0,
                    block_wand_fieldnorm_id,
                    block_wand_term_freq,
                };
            }
            IndexRecordOption::WithFreqsAndPositions => {
                let tf_num_bits = bytes[5];
                let tf_sum = read_u32(&bytes[6..10]);
                let block_wand_fieldnorm_id = bytes[10];
                let block_wand_term_freq = decode_block_wand_max_tf(bytes[11]);
                advance_len = 12;
                self.block_info = BlockInfo::BitPacked {
                    doc_num_bits,
                    tf_num_bits,
                    tf_sum,
                    block_wand_fieldnorm_id,
                    block_wand_term_freq,
                };
            }
        }
        self.owned_read.advance(advance_len);
    }

    pub fn block_info(&self) -> BlockInfo {
        self.block_info
    }

    /// Advance the skip reader to the block that may contain the target.
    ///
    /// If the target is larger than all documents, the skip_reader
    /// then advance to the last Variable In block.
    pub fn seek(&mut self, target: DocId) -> bool {
        if self.last_doc_in_block() >= target {
            return false;
        }
        loop {
            self.advance();
            if self.last_doc_in_block() >= target {
                return true;
            }
        }
    }

    pub fn advance(&mut self) {
        match self.block_info {
            BlockInfo::BitPacked {
                doc_num_bits,
                tf_num_bits,
                tf_sum,
                ..
            } => {
                self.remaining_docs -= COMPRESSION_BLOCK_SIZE as u32;
                self.byte_offset += compressed_block_size(doc_num_bits + tf_num_bits);
                self.position_offset += tf_sum as u64;
            }
            BlockInfo::VInt { num_docs } => {
                debug_assert_eq!(num_docs, self.remaining_docs);
                self.remaining_docs = 0;
                self.byte_offset = std::usize::MAX;
            }
        }
        self.last_doc_in_previous_block = self.last_doc_in_block;
        if self.remaining_docs >= COMPRESSION_BLOCK_SIZE as u32 {
            self.read_block_info();
        } else {
            self.last_doc_in_block = TERMINATED;
            self.block_info = BlockInfo::VInt {
                num_docs: self.remaining_docs,
            };
        }
    }
}

#[cfg(test)]
mod tests {

    use super::BlockInfo;
    use super::IndexRecordOption;
    use super::{SkipReader, SkipSerializer};
    use crate::directory::OwnedBytes;
    use crate::postings::compression::COMPRESSION_BLOCK_SIZE;

    #[test]
    fn test_encode_block_wand_max_tf() {
        for tf in 0..255 {
            assert_eq!(super::encode_block_wand_max_tf(tf), tf as u8);
        }
        for &tf in &[255, 256, 1_000_000, u32::MAX] {
            assert_eq!(super::encode_block_wand_max_tf(tf), 255);
        }
    }

    #[test]
    fn test_decode_block_wand_max_tf() {
        for tf in 0..255 {
            assert_eq!(super::decode_block_wand_max_tf(tf), tf as u32);
        }
        assert_eq!(super::decode_block_wand_max_tf(255), u32::MAX);
    }

    #[test]
    fn test_skip_with_freq() {
        let buf = {
            let mut skip_serializer = SkipSerializer::new();
            skip_serializer.write_doc(1u32, 2u8);
            skip_serializer.write_term_freq(3u8);
            skip_serializer.write_blockwand_max(13u8, 3u32);
            skip_serializer.write_doc(5u32, 5u8);
            skip_serializer.write_term_freq(2u8);
            skip_serializer.write_blockwand_max(8u8, 2u32);
            skip_serializer.data().to_owned()
        };
        let doc_freq = 3u32 + (COMPRESSION_BLOCK_SIZE * 2) as u32;
        let mut skip_reader =
            SkipReader::new(OwnedBytes::new(buf), doc_freq, IndexRecordOption::WithFreqs);
        assert_eq!(skip_reader.last_doc_in_block(), 1u32);
        assert_eq!(
            skip_reader.block_info,
            BlockInfo::BitPacked {
                doc_num_bits: 2u8,
                tf_num_bits: 3u8,
                tf_sum: 0,
                block_wand_fieldnorm_id: 13,
                block_wand_term_freq: 3
            }
        );
        skip_reader.advance();
        assert_eq!(skip_reader.last_doc_in_block(), 5u32);
        assert_eq!(
            skip_reader.block_info(),
            BlockInfo::BitPacked {
                doc_num_bits: 5u8,
                tf_num_bits: 2u8,
                tf_sum: 0,
                block_wand_fieldnorm_id: 8,
                block_wand_term_freq: 2
            }
        );
        skip_reader.advance();
        assert_eq!(skip_reader.block_info(), BlockInfo::VInt { num_docs: 3u32 });
        skip_reader.advance();
        assert_eq!(skip_reader.block_info(), BlockInfo::VInt { num_docs: 0u32 });
        skip_reader.advance();
        assert_eq!(skip_reader.block_info(), BlockInfo::VInt { num_docs: 0u32 });
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
        let mut skip_reader =
            SkipReader::new(OwnedBytes::new(buf), doc_freq, IndexRecordOption::Basic);
        assert_eq!(skip_reader.last_doc_in_block(), 1u32);
        assert_eq!(
            skip_reader.block_info(),
            BlockInfo::BitPacked {
                doc_num_bits: 2u8,
                tf_num_bits: 0,
                tf_sum: 0u32,
                block_wand_fieldnorm_id: 0,
                block_wand_term_freq: 0
            }
        );
        skip_reader.advance();
        assert_eq!(skip_reader.last_doc_in_block(), 5u32);
        assert_eq!(
            skip_reader.block_info(),
            BlockInfo::BitPacked {
                doc_num_bits: 5u8,
                tf_num_bits: 0,
                tf_sum: 0u32,
                block_wand_fieldnorm_id: 0,
                block_wand_term_freq: 0
            }
        );
        skip_reader.advance();
        assert_eq!(skip_reader.block_info(), BlockInfo::VInt { num_docs: 3u32 });
        skip_reader.advance();
        assert_eq!(skip_reader.block_info(), BlockInfo::VInt { num_docs: 0u32 });
        skip_reader.advance();
        assert_eq!(skip_reader.block_info(), BlockInfo::VInt { num_docs: 0u32 });
    }

    #[test]
    fn test_skip_multiple_of_block_size() {
        let buf = {
            let mut skip_serializer = SkipSerializer::new();
            skip_serializer.write_doc(1u32, 2u8);
            skip_serializer.data().to_owned()
        };
        let doc_freq = COMPRESSION_BLOCK_SIZE as u32;
        let mut skip_reader =
            SkipReader::new(OwnedBytes::new(buf), doc_freq, IndexRecordOption::Basic);
        assert_eq!(skip_reader.last_doc_in_block(), 1u32);
        assert_eq!(
            skip_reader.block_info(),
            BlockInfo::BitPacked {
                doc_num_bits: 2u8,
                tf_num_bits: 0,
                tf_sum: 0u32,
                block_wand_fieldnorm_id: 0,
                block_wand_term_freq: 0
            }
        );
        skip_reader.advance();
        assert_eq!(skip_reader.block_info(), BlockInfo::VInt { num_docs: 0u32 });
    }
}

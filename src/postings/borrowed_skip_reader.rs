use crate::postings::compression::{compressed_block_size, COMPRESSION_BLOCK_SIZE};
use crate::postings::skip::{decode_bitwidth, decode_block_wand_max_tf, read_u32};
use crate::postings::BlockInfo;
use crate::schema::IndexRecordOption;
use crate::{DocId, TERMINATED};

#[derive(Clone)]
pub(crate) struct BorrowedSkipReader<'a> {
    last_doc_in_block: DocId,
    pub(crate) last_doc_in_previous_block: DocId,
    owned_read: &'a [u8],
    skip_info: IndexRecordOption,
    byte_offset: usize,
    remaining_docs: u32, // number of docs remaining, including the
    // documents in the current block.
    block_info: BlockInfo,

    position_offset: u64,
}

impl<'a> BorrowedSkipReader<'a> {
    pub fn new(
        data: &'a [u8],
        doc_freq: u32,
        skip_info: IndexRecordOption,
    ) -> BorrowedSkipReader<'a> {
        let mut skip_reader = BorrowedSkipReader {
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

    pub(crate) fn last_doc_in_block(&self) -> DocId {
        self.last_doc_in_block
    }

    pub fn position_offset(&self) -> u64 {
        self.position_offset
    }

    #[inline]
    pub fn byte_offset(&self) -> usize {
        self.byte_offset
    }

    fn read_block_info(&mut self) {
        let bytes = self.owned_read;
        let advance_len: usize;
        self.last_doc_in_block = read_u32(bytes);
        let (doc_num_bits, strict_delta_encoded) = decode_bitwidth(bytes[4]);
        match self.skip_info {
            IndexRecordOption::Basic => {
                advance_len = 5;
                self.block_info = BlockInfo::BitPacked {
                    doc_num_bits,
                    strict_delta_encoded,
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
                    strict_delta_encoded,
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
                    strict_delta_encoded,
                    tf_num_bits,
                    tf_sum,
                    block_wand_fieldnorm_id,
                    block_wand_term_freq,
                };
            }
        }
        // self.owned_read.advance(advance_len);
        let (_, rest) = self.owned_read.split_at(advance_len);
        self.owned_read = rest;
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
                self.byte_offset = usize::MAX;
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

use std::io;

use common::VInt;

use crate::postings::block_segment_postings::{decode_bitpacked_block, decode_vint_block};
use crate::postings::borrowed_skip_reader::BorrowedSkipReader;
use crate::postings::compression::{BlockDecoder, COMPRESSION_BLOCK_SIZE};
use crate::postings::{BlockInfo, FreqReadingOption};
use crate::schema::IndexRecordOption;
use crate::{DocId, Score, TERMINATED};

/// `BorrowedBlockSegmentPostings` is a cursor iterating over blocks
/// of documents.
///
/// # Warning
///
/// While it is useful for some very specific high-performance
/// use cases, you should prefer using `SegmentPostings` for most usage.
#[derive(Clone)]
pub struct BorrowedBlockSegmentPostings<'a> {
    pub(crate) doc_decoder: BlockDecoder,
    block_loaded: bool,
    freq_decoder: BlockDecoder,
    freq_reading_option: FreqReadingOption,
    block_max_score_cache: Option<Score>,
    doc_freq: u32,
    data: &'a [u8],
    skip_reader: BorrowedSkipReader<'a>,
}

fn split_into_skips_and_postings(
    doc_freq: u32,
    mut bytes: &[u8],
) -> io::Result<(Option<&[u8]>, &[u8])> {
    if doc_freq < COMPRESSION_BLOCK_SIZE as u32 {
        return Ok((None, bytes));
    }
    let skip_len = VInt::deserialize_u64(&mut bytes)? as usize;
    let (skip_data, postings_data) = bytes.split_at(skip_len);
    Ok((Some(skip_data), postings_data))
}

impl<'a> BorrowedBlockSegmentPostings<'a> {
    /// Opens a `BorrowedBlockSegmentPostings`.
    /// `doc_freq` is the number of documents in the posting list.
    /// `record_option` represents the amount of data available according to the schema.
    /// `requested_option` is the amount of data requested by the user.
    /// If for instance, we do not request for term frequencies, this function will not decompress
    /// term frequency blocks.
    pub(crate) fn open(
        doc_freq: u32,
        bytes: &'a [u8],
        mut record_option: IndexRecordOption,
        requested_option: IndexRecordOption,
    ) -> io::Result<BorrowedBlockSegmentPostings<'a>> {
        let (skip_data_opt, postings_data) = split_into_skips_and_postings(doc_freq, bytes)?;
        let skip_reader = match skip_data_opt {
            Some(skip_data) => {
                let block_count = doc_freq as usize / COMPRESSION_BLOCK_SIZE;
                // 8 is the minimum size of a block with frequency (can be more if pos are stored
                // too)
                if skip_data.len() < 8 * block_count {
                    // the field might be encoded with frequency, but this term in particular isn't.
                    // This can happen for JSON field with term frequencies:
                    // - text terms are encoded with term freqs.
                    // - numerical terms are encoded without term freqs.
                    record_option = IndexRecordOption::Basic;
                }
                BorrowedSkipReader::new(skip_data, doc_freq, record_option)
            }
            None => BorrowedSkipReader::new(&[], doc_freq, record_option),
        };

        let freq_reading_option = match (record_option, requested_option) {
            (IndexRecordOption::Basic, _) => FreqReadingOption::NoFreq,
            (_, IndexRecordOption::Basic) => FreqReadingOption::SkipFreq,
            (_, _) => FreqReadingOption::ReadFreq,
        };

        let mut block_segment_postings = BorrowedBlockSegmentPostings {
            doc_decoder: BlockDecoder::with_val(TERMINATED),
            block_loaded: false,
            freq_decoder: BlockDecoder::with_val(1),
            freq_reading_option,
            block_max_score_cache: None,
            doc_freq,
            data: postings_data,
            skip_reader,
        };
        block_segment_postings.load_block();
        Ok(block_segment_postings)
    }

    /// Returns the overall number of documents in the block postings.
    /// It does not take in account whether documents are deleted or not.
    ///
    /// This `doc_freq` is simply the sum of the length of all of the blocks
    /// length, and it does not take in account deleted documents.
    pub fn doc_freq(&self) -> u32 {
        self.doc_freq
    }

    /// Returns a full block, regardless of whether the block is complete or incomplete (
    /// as it happens for the last block of the posting list).
    ///
    /// In the latter case, the block is guaranteed to be padded with the sentinel value:
    /// `TERMINATED`. The array is also guaranteed to be aligned on 16 bytes = 128 bits.
    ///
    /// This method is useful to run SSE2 linear search.
    #[inline]
    pub(crate) fn full_block(&self) -> &[DocId; COMPRESSION_BLOCK_SIZE] {
        debug_assert!(self.block_is_loaded());
        self.doc_decoder.full_output()
    }

    /// Return the document at index `idx` of the block.
    #[inline]
    pub fn doc(&self, idx: usize) -> u32 {
        self.doc_decoder.output(idx)
    }

    /// Return the array of `term freq` in the block.
    #[inline]
    pub fn freqs(&self) -> &[u32] {
        debug_assert!(self.block_is_loaded());
        self.freq_decoder.output_array()
    }

    /// Return the frequency at index `idx` of the block.
    #[inline]
    pub fn freq(&self, idx: usize) -> u32 {
        debug_assert!(self.block_is_loaded());
        self.freq_decoder.output(idx)
    }

    /// Position on a block that may contains `target_doc`.
    ///
    /// If all docs are smaller than target, the block loaded may be empty,
    /// or be the last an incomplete VInt block.
    pub fn seek(&mut self, target_doc: DocId) {
        self.shallow_seek(target_doc);
        self.load_block();
    }

    pub(crate) fn position_offset(&self) -> u64 {
        self.skip_reader.position_offset()
    }

    /// Dangerous API! This calls seek on the skip list,
    /// but does not `.load_block()` afterwards.
    ///
    /// `.load_block()` needs to be called manually afterwards.
    /// If all docs are smaller than target, the block loaded may be empty,
    /// or be the last an incomplete VInt block.
    pub(crate) fn shallow_seek(&mut self, target_doc: DocId) {
        if self.skip_reader.seek(target_doc) {
            self.block_max_score_cache = None;
            self.block_loaded = false;
        }
    }

    pub(crate) fn block_is_loaded(&self) -> bool {
        self.block_loaded
    }

    pub(crate) fn load_block(&mut self) {
        let offset = self.skip_reader.byte_offset();
        if self.block_is_loaded() {
            return;
        }
        match self.skip_reader.block_info() {
            BlockInfo::BitPacked {
                doc_num_bits,
                strict_delta_encoded,
                tf_num_bits,
                ..
            } => {
                decode_bitpacked_block(
                    &mut self.doc_decoder,
                    if let FreqReadingOption::ReadFreq = self.freq_reading_option {
                        Some(&mut self.freq_decoder)
                    } else {
                        None
                    },
                    &self.data[offset..],
                    self.skip_reader.last_doc_in_previous_block,
                    doc_num_bits,
                    tf_num_bits,
                    strict_delta_encoded,
                );
            }
            BlockInfo::VInt { num_docs } => {
                let data = {
                    if num_docs == 0 {
                        &[]
                    } else {
                        &self.data[offset..]
                    }
                };
                decode_vint_block(
                    &mut self.doc_decoder,
                    if let FreqReadingOption::ReadFreq = self.freq_reading_option {
                        Some(&mut self.freq_decoder)
                    } else {
                        None
                    },
                    data,
                    self.skip_reader.last_doc_in_previous_block,
                    num_docs as usize,
                );
            }
        }
        self.block_loaded = true;
    }

    /// Advance to the next block.
    pub fn advance(&mut self) {
        self.skip_reader.advance();
        self.block_loaded = false;
        self.block_max_score_cache = None;
        self.load_block();
    }
}

use crate::DocId;
use tantivy_fst::Streamer;
use crate::postings::{SkipReader, FreqReadingOption, USE_SKIP_INFO_LIMIT};
use owned_read::OwnedRead;
use crate::postings::compression::{BlockDecoder, COMPRESSION_BLOCK_SIZE, VIntDecoder, compressed_block_size, AlignedBuffer};
use crate::schema::IndexRecordOption;
use crate::common::{VInt, BinarySerializable};


fn split_into_skips_and_postings(
    doc_freq: u32,
    mut data: OwnedRead,
) -> (Option<OwnedRead>, OwnedRead) {
    if doc_freq >= USE_SKIP_INFO_LIMIT {
        let skip_len = VInt::deserialize(&mut data).expect("Data corrupted").0 as usize;
        let mut postings_data = data.clone();
        postings_data.advance(skip_len);
        data.clip(skip_len);
        (Some(data), postings_data)
    } else {
        (None, data)
    }
}

/// `BlockSegmentPostings` is a cursor iterating over blocks
/// of documents.
///
/// # Warning
///
/// While it is useful for some very specific high-performance
/// use cases, you should prefer using `SegmentPostings` for most usage.
pub struct BlockSegmentPostings {
    doc_decoder: BlockDecoder,
    freq_decoder: BlockDecoder,
    freq_reading_option: FreqReadingOption,

    doc_freq: usize,
    doc_offset: DocId,

    num_vint_docs: usize,

    remaining_data: OwnedRead,
    skip_reader: SkipReader,
}


#[derive(Debug, Eq, PartialEq)]
pub enum BlockSegmentPostingsSkipResult {
    Terminated,
    Success(u32), //< number of term freqs to skip
}

impl BlockSegmentPostings {
    pub(crate) fn from_data(
        doc_freq: u32,
        data: OwnedRead,
        record_option: IndexRecordOption,
        requested_option: IndexRecordOption,
    ) -> BlockSegmentPostings {
        let freq_reading_option = match (record_option, requested_option) {
            (IndexRecordOption::Basic, _) => FreqReadingOption::NoFreq,
            (_, IndexRecordOption::Basic) => FreqReadingOption::SkipFreq,
            (_, _) => FreqReadingOption::ReadFreq,
        };

        let (skip_data_opt, postings_data) = split_into_skips_and_postings(doc_freq, data);
        let skip_reader = match skip_data_opt {
            Some(skip_data) => SkipReader::new(skip_data, record_option),
            None => SkipReader::new(OwnedRead::new(&[][..]), record_option),
        };
        let doc_freq = doc_freq as usize;
        let num_vint_docs = doc_freq % COMPRESSION_BLOCK_SIZE;
        BlockSegmentPostings {
            num_vint_docs,
            doc_decoder: BlockDecoder::new(),
            freq_decoder: BlockDecoder::with_val(1),
            freq_reading_option,
            doc_offset: 0,
            doc_freq,
            remaining_data: postings_data,
            skip_reader,
        }
    }

    // Resets the block segment postings on another position
    // in the postings file.
    //
    // This is useful for enumerating through a list of terms,
    // and consuming the associated posting lists while avoiding
    // reallocating a `BlockSegmentPostings`.
    //
    // # Warning
    //
    // This does not reset the positions list.
    pub(crate) fn reset(&mut self, doc_freq: u32, postings_data: OwnedRead) {
        let (skip_data_opt, postings_data) = split_into_skips_and_postings(doc_freq, postings_data);
        let num_vint_docs = (doc_freq as usize) & (COMPRESSION_BLOCK_SIZE - 1);
        self.num_vint_docs = num_vint_docs;
        self.remaining_data = postings_data;
        if let Some(skip_data) = skip_data_opt {
            self.skip_reader.reset(skip_data);
        } else {
            self.skip_reader.reset(OwnedRead::new(&[][..]))
        }
        self.doc_offset = 0;
        self.doc_freq = doc_freq as usize;
    }

    /// Returns the document frequency associated to this block postings.
    ///
    /// This `doc_freq` is simply the sum of the length of all of the blocks
    /// length, and it does not take in account deleted documents.
    pub fn doc_freq(&self) -> usize {
        self.doc_freq
    }

    /// Returns the array of docs in the current block.
    ///
    /// Before the first call to `.advance()`, the block
    /// returned by `.docs()` is empty.
    #[inline]
    pub fn docs(&self) -> &[DocId] {
        self.doc_decoder.output_array()
    }

    pub(crate) fn docs_aligned(&self) -> (&AlignedBuffer, usize) {
        self.doc_decoder.output_aligned()
    }

    /// Return the document at index `idx` of the block.
    #[inline]
    pub fn doc(&self, idx: usize) -> u32 {
        self.doc_decoder.output(idx)
    }

    /// Return the array of `term freq` in the block.
    #[inline]
    pub fn freqs(&self) -> &[u32] {
        self.freq_decoder.output_array()
    }

    /// Return the frequency at index `idx` of the block.
    #[inline]
    pub fn freq(&self, idx: usize) -> u32 {
        self.freq_decoder.output(idx)
    }

    /// Returns the length of the current block.
    ///
    /// All blocks have a length of `NUM_DOCS_PER_BLOCK`,
    /// except the last block that may have a length
    /// of any number between 1 and `NUM_DOCS_PER_BLOCK - 1`
    #[inline]
    pub(crate) fn block_len(&self) -> usize {
        self.doc_decoder.output_len
    }

    /// position on a block that may contains `doc_id`.
    /// Always advance the current block.
    ///
    /// Returns true if a block that has an element greater or equal to the target is found.
    /// Returning true does not guarantee that the smallest element of the block is smaller
    /// than the target. It only guarantees that the last element is greater or equal.
    ///
    /// Returns false iff all of the document remaining are smaller than
    /// `doc_id`. In that case, all of these document are consumed.
    ///
    pub fn skip_to(&mut self, target_doc: DocId) -> BlockSegmentPostingsSkipResult {
        let mut skip_freqs = 0u32;
        while self.skip_reader.advance() {
            if self.skip_reader.doc() >= target_doc {
                // the last document of the current block is larger
                // than the target.
                //
                // We found our block!
                let num_bits = self.skip_reader.doc_num_bits();
                let num_consumed_bytes = self.doc_decoder.uncompress_block_sorted(
                    self.remaining_data.as_ref(),
                    self.doc_offset,
                    num_bits,
                );
                self.remaining_data.advance(num_consumed_bytes);
                let tf_num_bits = self.skip_reader.tf_num_bits();
                match self.freq_reading_option {
                    FreqReadingOption::NoFreq => {}
                    FreqReadingOption::SkipFreq => {
                        let num_bytes_to_skip = compressed_block_size(tf_num_bits);
                        self.remaining_data.advance(num_bytes_to_skip);
                    }
                    FreqReadingOption::ReadFreq => {
                        let num_consumed_bytes = self
                            .freq_decoder
                            .uncompress_block_unsorted(self.remaining_data.as_ref(), tf_num_bits);
                        self.remaining_data.advance(num_consumed_bytes);
                    }
                }
                self.doc_offset = self.skip_reader.doc();
                return BlockSegmentPostingsSkipResult::Success(skip_freqs);
            } else {
                skip_freqs += self.skip_reader.tf_sum();
                let advance_len = self.skip_reader.total_block_len();
                self.doc_offset = self.skip_reader.doc();
                self.remaining_data.advance(advance_len);
            }
        }

        // we are now on the last, incomplete, variable encoded block.
        if self.num_vint_docs > 0 {
            let num_compressed_bytes = self.doc_decoder.uncompress_vint_sorted(
                self.remaining_data.as_ref(),
                self.doc_offset,
                self.num_vint_docs,
            );
            self.remaining_data.advance(num_compressed_bytes);
            match self.freq_reading_option {
                FreqReadingOption::NoFreq | FreqReadingOption::SkipFreq => {}
                FreqReadingOption::ReadFreq => {
                    self.freq_decoder
                        .uncompress_vint_unsorted(self.remaining_data.as_ref(), self.num_vint_docs);
                }
            }
            self.num_vint_docs = 0;
            return self
                .docs()
                .last()
                .map(|last_doc| {
                    if *last_doc >= target_doc {
                        BlockSegmentPostingsSkipResult::Success(skip_freqs)
                    } else {
                        BlockSegmentPostingsSkipResult::Terminated
                    }
                })
                .unwrap_or(BlockSegmentPostingsSkipResult::Terminated);
        }
        BlockSegmentPostingsSkipResult::Terminated
    }

    /// Advance to the next block.
    ///
    /// Returns false iff there was no remaining blocks.
    pub fn advance(&mut self) -> bool {
        if self.skip_reader.advance() {
            let num_bits = self.skip_reader.doc_num_bits();
            let num_consumed_bytes = self.doc_decoder.uncompress_block_sorted(
                self.remaining_data.as_ref(),
                self.doc_offset,
                num_bits,
            );
            self.remaining_data.advance(num_consumed_bytes);
            let tf_num_bits = self.skip_reader.tf_num_bits();
            match self.freq_reading_option {
                FreqReadingOption::NoFreq => {}
                FreqReadingOption::SkipFreq => {
                    let num_bytes_to_skip = compressed_block_size(tf_num_bits);
                    self.remaining_data.advance(num_bytes_to_skip);
                }
                FreqReadingOption::ReadFreq => {
                    let num_consumed_bytes = self
                        .freq_decoder
                        .uncompress_block_unsorted(self.remaining_data.as_ref(), tf_num_bits);
                    self.remaining_data.advance(num_consumed_bytes);
                }
            }
            // it will be used as the next offset.
            self.doc_offset = self.doc_decoder.output(COMPRESSION_BLOCK_SIZE - 1);
            true
        } else if self.num_vint_docs > 0 {
            let num_compressed_bytes = self.doc_decoder.uncompress_vint_sorted(
                self.remaining_data.as_ref(),
                self.doc_offset,
                self.num_vint_docs,
            );
            self.remaining_data.advance(num_compressed_bytes);
            match self.freq_reading_option {
                FreqReadingOption::NoFreq | FreqReadingOption::SkipFreq => {}
                FreqReadingOption::ReadFreq => {
                    self.freq_decoder
                        .uncompress_vint_unsorted(self.remaining_data.as_ref(), self.num_vint_docs);
                }
            }
            self.num_vint_docs = 0;
            true
        } else {
            false
        }
    }

    /// Returns an empty segment postings object
    pub fn empty() -> BlockSegmentPostings {
        BlockSegmentPostings {
            num_vint_docs: 0,

            doc_decoder: BlockDecoder::new(),
            freq_decoder: BlockDecoder::with_val(1),
            freq_reading_option: FreqReadingOption::NoFreq,

            doc_offset: 0,
            doc_freq: 0,

            remaining_data: OwnedRead::new(vec![]),
            skip_reader: SkipReader::new(OwnedRead::new(vec![]), IndexRecordOption::Basic),
        }
    }
}

impl<'a> Streamer<'a> for BlockSegmentPostings {
    type Item = &'a [DocId];

    fn next(&'a mut self) -> Option<&'a [DocId]> {
        if self.advance() {
            Some(self.docs())
        } else {
            None
        }
    }
}

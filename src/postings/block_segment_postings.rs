use crate::common::{BinarySerializable, VInt};
use crate::postings::compression::{
    compressed_block_size, AlignedBuffer, BlockDecoder, VIntDecoder, COMPRESSION_BLOCK_SIZE,
};
use crate::postings::{FreqReadingOption, SkipReader, USE_SKIP_INFO_LIMIT};
use crate::schema::IndexRecordOption;
use crate::{DocId, TERMINATED};
use owned_read::OwnedRead;
use crate::directory::ReadOnlySource;

/// `BlockSegmentPostings` is a cursor iterating over blocks
/// of documents.
///
/// # Warning
///
/// While it is useful for some very specific high-performance
/// use cases, you should prefer using `SegmentPostings` for most usage.
pub struct BlockSegmentPostings {
    pub(crate) doc_decoder: BlockDecoder,
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

fn split_into_skips_and_postings(
    doc_freq: u32,
    data: ReadOnlySource,
) -> (Option<ReadOnlySource>, ReadOnlySource) {
    if doc_freq < USE_SKIP_INFO_LIMIT {
        return (None, data);
    }
    let mut data_byte_arr = data.as_slice();
    let skip_len = VInt::deserialize(&mut data_byte_arr).expect("Data corrupted").0 as usize;
    let vint_len = data.len() - data_byte_arr.len();
    let (skip_data, postings_data) = data.slice_from(vint_len).split(skip_len);
    (Some(skip_data), postings_data)
}

impl BlockSegmentPostings {
    pub(crate) fn from_data(
        doc_freq: u32,
        data: ReadOnlySource,
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
            None => SkipReader::new(ReadOnlySource::empty(), record_option),
        };
        let doc_freq = doc_freq as usize;
        let num_vint_docs = doc_freq % COMPRESSION_BLOCK_SIZE;
        let mut block_segment_postings = BlockSegmentPostings {
            num_vint_docs,
            doc_decoder: BlockDecoder::with_val(TERMINATED),
            freq_decoder: BlockDecoder::with_val(1),
            freq_reading_option,
            doc_offset: 0,
            doc_freq,
            remaining_data: OwnedRead::new(postings_data),
            skip_reader,
        };
        block_segment_postings.advance();
        block_segment_postings
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
    pub(crate) fn reset(&mut self, doc_freq: u32, postings_data: ReadOnlySource) {
        let (skip_data_opt, postings_data) = split_into_skips_and_postings(doc_freq, postings_data);
        let num_vint_docs = (doc_freq as usize) & (COMPRESSION_BLOCK_SIZE - 1);
        self.num_vint_docs = num_vint_docs;
        self.remaining_data = OwnedRead::new(postings_data);
        if let Some(skip_data) = skip_data_opt {
            self.skip_reader.reset(skip_data);
        } else {
            self.skip_reader.reset(ReadOnlySource::empty())
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

    pub(crate) fn docs_aligned(&self) -> &AlignedBuffer {
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
    pub fn block_len(&self) -> usize {
        self.doc_decoder.output_len
    }

    /// Position on a block that may contains `target_doc`.
    ///
    /// If the current block last element is greater or equal to `target_doc`, return true.
    ///
    /// Returns Success(num_term_freq_to_skip) if a block that has an element greater or equal to the target is found.
    /// Returning true does not guarantee that the smallest element of the block is smaller
    /// than the target. It only guarantees that the last element is greater or equal.
    ///
    /// Returns End iff all of the document remaining are smaller than
    /// `doc_id`. In that case, all of these document are consumed.
    ///
    pub fn seek(&mut self, target_doc: DocId) -> BlockSegmentPostingsSkipResult {
        let mut skip_freqs = 0u32;
        if self
            .doc_decoder
            .output_array()
            .last()
            .map(|&last_doc_in_block| last_doc_in_block >= target_doc)
            .unwrap_or(false)
        {
            return BlockSegmentPostingsSkipResult::Success(0u32);
        }
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

        self.doc_decoder.clear();

        if self.num_vint_docs == 0 {
            return BlockSegmentPostingsSkipResult::Terminated;
        }
        // we are now on the last, incomplete, variable encoded block.
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
        self.docs()
            .last()
            .map(|last_doc| {
                if *last_doc >= target_doc {
                    BlockSegmentPostingsSkipResult::Success(skip_freqs)
                } else {
                    BlockSegmentPostingsSkipResult::Terminated
                }
            })
            .unwrap_or(BlockSegmentPostingsSkipResult::Terminated)
    }

    fn read_bitpacked_block(&mut self) {
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
    }

    fn read_vint_block(&mut self, num_vint_docs: usize) {
        let num_compressed_bytes = self.doc_decoder.uncompress_vint_sorted(
            self.remaining_data.as_ref(),
            self.doc_offset,
            num_vint_docs,
        );
        self.remaining_data.advance(num_compressed_bytes);
        match self.freq_reading_option {
            FreqReadingOption::NoFreq | FreqReadingOption::SkipFreq => {}
            FreqReadingOption::ReadFreq => {
                self.freq_decoder
                    .uncompress_vint_unsorted(self.remaining_data.as_ref(), num_vint_docs);
            }
        }
    }

    /// Advance to the next block.
    ///
    /// Returns false iff there was no remaining blocks.
    pub fn advance(&mut self) -> bool {
        if self.skip_reader.advance() {
            self.read_bitpacked_block();
            return true;
        }
        self.doc_decoder.clear();
        if self.num_vint_docs == 0 {
            return false;
        }
        self.read_vint_block(self.num_vint_docs);
        self.num_vint_docs = 0;
        true
    }

    /// Returns an empty segment postings object
    pub fn empty() -> BlockSegmentPostings {
        BlockSegmentPostings {
            num_vint_docs: 0,

            doc_decoder: BlockDecoder::with_val(TERMINATED),
            freq_decoder: BlockDecoder::with_val(1),
            freq_reading_option: FreqReadingOption::NoFreq,

            doc_offset: 0,
            doc_freq: 0,

            remaining_data: OwnedRead::new(vec![]),
            skip_reader: SkipReader::new(ReadOnlySource::new(vec![]), IndexRecordOption::Basic),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::BlockSegmentPostings;
    use super::BlockSegmentPostingsSkipResult;
    use crate::common::HasLen;
    use crate::core::Index;
    use crate::docset::{DocSet, TERMINATED};
    use crate::postings::postings::Postings;
    use crate::postings::SegmentPostings;
    use crate::schema::IndexRecordOption;
    use crate::schema::Schema;
    use crate::schema::Term;
    use crate::schema::INDEXED;
    use crate::DocId;

    #[test]
    fn test_empty_segment_postings() {
        let mut postings = SegmentPostings::empty();
        assert_eq!(postings.advance(), TERMINATED);
        assert_eq!(postings.advance(), TERMINATED);
        assert_eq!(postings.len(), 0);
    }

    #[test]
    fn test_empty_postings_doc_returns_terminated() {
        let mut postings = SegmentPostings::empty();
        assert_eq!(postings.doc(), TERMINATED);
        assert_eq!(postings.advance(), TERMINATED);
    }

    #[test]
    fn test_empty_postings_doc_term_freq_returns_0() {
        let postings = SegmentPostings::empty();
        assert_eq!(postings.term_freq(), 1);
    }

    #[test]
    fn test_empty_block_segment_postings() {
        let mut postings = BlockSegmentPostings::empty();
        assert!(!postings.advance());
        assert_eq!(postings.doc_freq(), 0);
    }

    #[test]
    fn test_block_segment_postings() {
        let mut block_segments = build_block_postings(&(0..100_000).collect::<Vec<u32>>());
        let mut offset: u32 = 0u32;
        // checking that the `doc_freq` is correct
        assert_eq!(block_segments.doc_freq(), 100_000);
        loop {
            let block = block_segments.docs();
            for (i, doc) in block.iter().cloned().enumerate() {
                assert_eq!(offset + (i as u32), doc);
            }
            offset += block.len() as u32;
            if block_segments.advance() {
                break;
            }
        }
    }

    #[test]
    fn test_skip_right_at_new_block() {
        let mut doc_ids = (0..128).collect::<Vec<u32>>();
        doc_ids.push(129);
        doc_ids.push(130);
        {
            let block_segments = build_block_postings(&doc_ids);
            let mut docset = SegmentPostings::from_block_postings(block_segments, None);
            assert_eq!(docset.seek(128), 129);
            assert_eq!(docset.doc(), 129);
            assert_eq!(docset.advance(), 130);
            assert_eq!(docset.doc(), 130);
            assert_eq!(docset.advance(), TERMINATED);
        }
        {
            let block_segments = build_block_postings(&doc_ids);
            let mut docset = SegmentPostings::from_block_postings(block_segments, None);
            assert_eq!(docset.seek(129), 129);
            assert_eq!(docset.doc(), 129);
            assert_eq!(docset.advance(), 130);
            assert_eq!(docset.doc(), 130);
            assert_eq!(docset.advance(), TERMINATED);
        }
        {
            let block_segments = build_block_postings(&doc_ids);
            let mut docset = SegmentPostings::from_block_postings(block_segments, None);
            assert_eq!(docset.doc(), 0);
            assert_eq!(docset.seek(131), TERMINATED);
            assert_eq!(docset.doc(), TERMINATED);
        }
    }

    fn build_block_postings(docs: &[DocId]) -> BlockSegmentPostings {
        let mut schema_builder = Schema::builder();
        let int_field = schema_builder.add_u64_field("id", INDEXED);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut index_writer = index.writer_with_num_threads(1, 3_000_000).unwrap();
        let mut last_doc = 0u32;
        for &doc in docs {
            for _ in last_doc..doc {
                index_writer.add_document(doc!(int_field=>1u64));
            }
            index_writer.add_document(doc!(int_field=>0u64));
            last_doc = doc + 1;
        }
        index_writer.commit().unwrap();
        let searcher = index.reader().unwrap().searcher();
        let segment_reader = searcher.segment_reader(0);
        let inverted_index = segment_reader.inverted_index(int_field);
        let term = Term::from_field_u64(int_field, 0u64);
        let term_info = inverted_index.get_term_info(&term).unwrap();
        inverted_index.read_block_postings_from_terminfo(&term_info, IndexRecordOption::Basic)
    }

    #[test]
    fn test_block_segment_postings_skip() {
        for i in 0..4 {
            let mut block_postings = build_block_postings(&[3]);
            assert_eq!(
                block_postings.seek(i),
                BlockSegmentPostingsSkipResult::Success(0u32)
            );
            assert_eq!(
                block_postings.seek(i),
                BlockSegmentPostingsSkipResult::Success(0u32)
            );
        }
        let mut block_postings = build_block_postings(&[3]);
        assert_eq!(
            block_postings.seek(4u32),
            BlockSegmentPostingsSkipResult::Terminated
        );
    }

    #[test]
    fn test_block_segment_postings_skip2() {
        let mut docs = vec![0];
        for i in 0..1300 {
            docs.push((i * i / 100) + i);
        }
        let mut block_postings = build_block_postings(&docs[..]);
        for i in vec![0, 424, 10000] {
            assert_eq!(
                block_postings.seek(i),
                BlockSegmentPostingsSkipResult::Success(0u32)
            );
            let docs = block_postings.docs();
            assert!(docs[0] <= i);
            assert!(docs.last().cloned().unwrap_or(0u32) >= i);
        }
        assert_eq!(
            block_postings.seek(100_000),
            BlockSegmentPostingsSkipResult::Terminated
        );
        assert_eq!(
            block_postings.seek(101_000),
            BlockSegmentPostingsSkipResult::Terminated
        );
    }

    #[test]
    fn test_reset_block_segment_postings() {
        let mut schema_builder = Schema::builder();
        let int_field = schema_builder.add_u64_field("id", INDEXED);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut index_writer = index.writer_with_num_threads(1, 3_000_000).unwrap();
        // create two postings list, one containg even number,
        // the other containing odd numbers.
        for i in 0..6 {
            let doc = doc!(int_field=> (i % 2) as u64);
            index_writer.add_document(doc);
        }
        index_writer.commit().unwrap();
        let searcher = index.reader().unwrap().searcher();
        let segment_reader = searcher.segment_reader(0);

        let mut block_segments;
        {
            let term = Term::from_field_u64(int_field, 0u64);
            let inverted_index = segment_reader.inverted_index(int_field);
            let term_info = inverted_index.get_term_info(&term).unwrap();
            block_segments = inverted_index
                .read_block_postings_from_terminfo(&term_info, IndexRecordOption::Basic);
        }
        assert_eq!(block_segments.docs(), &[0, 2, 4]);
        {
            let term = Term::from_field_u64(int_field, 1u64);
            let inverted_index = segment_reader.inverted_index(int_field);
            let term_info = inverted_index.get_term_info(&term).unwrap();
            inverted_index.reset_block_postings_from_terminfo(&term_info, &mut block_segments);
        }
        assert!(block_segments.advance());
        assert_eq!(block_segments.docs(), &[1, 3, 5]);
    }
}

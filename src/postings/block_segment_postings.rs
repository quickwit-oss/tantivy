use crate::common::{BinarySerializable, VInt};
use crate::directory::ReadOnlySource;
use crate::postings::compression::{
    AlignedBuffer, BlockDecoder, VIntDecoder, COMPRESSION_BLOCK_SIZE,
};
use crate::postings::{BlockInfo, FreqReadingOption, SkipReader};
use crate::schema::IndexRecordOption;
use crate::{DocId, TERMINATED};

/// `BlockSegmentPostings` is a cursor iterating over blocks
/// of documents.
///
/// # Warning
///
/// While it is useful for some very specific high-performance
/// use cases, you should prefer using `SegmentPostings` for most usage.
pub struct BlockSegmentPostings {
    pub(crate) doc_decoder: BlockDecoder,
    loaded_offset: usize,
    freq_decoder: BlockDecoder,
    freq_reading_option: FreqReadingOption,

    doc_freq: usize,

    data: ReadOnlySource,
    skip_reader: SkipReader,
}

fn decode_bitpacked_block(
    doc_decoder: &mut BlockDecoder,
    freq_decoder_opt: Option<&mut BlockDecoder>,
    data: &[u8],
    doc_offset: DocId,
    doc_num_bits: u8,
    tf_num_bits: u8,
) {
    let num_consumed_bytes = doc_decoder.uncompress_block_sorted(data, doc_offset, doc_num_bits);
    if let Some(freq_decoder) = freq_decoder_opt {
        freq_decoder.uncompress_block_unsorted(&data[num_consumed_bytes..], tf_num_bits);
    }
}

fn decode_vint_block(
    doc_decoder: &mut BlockDecoder,
    freq_decoder_opt: Option<&mut BlockDecoder>,
    data: &[u8],
    doc_offset: DocId,
    num_vint_docs: usize,
) {
    let num_consumed_bytes = doc_decoder.uncompress_vint_sorted(data, doc_offset, num_vint_docs);
    if let Some(freq_decoder) = freq_decoder_opt {
        freq_decoder.uncompress_vint_unsorted(&data[num_consumed_bytes..], num_vint_docs);
    }
}

fn split_into_skips_and_postings(
    doc_freq: u32,
    data: ReadOnlySource,
) -> (Option<ReadOnlySource>, ReadOnlySource) {
    if doc_freq < COMPRESSION_BLOCK_SIZE as u32 {
        return (None, data);
    }
    let mut data_byte_arr = data.as_slice();
    let skip_len = VInt::deserialize(&mut data_byte_arr)
        .expect("Data corrupted")
        .0 as usize;
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
            Some(skip_data) => SkipReader::new(skip_data, doc_freq, record_option),
            None => SkipReader::new(ReadOnlySource::empty(), doc_freq, record_option),
        };

        let doc_freq = doc_freq as usize;
        let mut block_segment_postings = BlockSegmentPostings {
            doc_decoder: BlockDecoder::with_val(TERMINATED),
            loaded_offset: std::usize::MAX,
            freq_decoder: BlockDecoder::with_val(1),
            freq_reading_option,
            doc_freq,
            data: postings_data,
            skip_reader,
        };
        block_segment_postings.load_block();
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
        self.data = ReadOnlySource::new(postings_data);
        self.loaded_offset = std::usize::MAX;
        if let Some(skip_data) = skip_data_opt {
            self.skip_reader.reset(skip_data, doc_freq);
        } else {
            self.skip_reader.reset(ReadOnlySource::empty(), doc_freq);
        }
        self.doc_freq = doc_freq as usize;
        self.load_block();
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

    #[inline(always)]
    pub(crate) fn docs_aligned(&self) -> &AlignedBuffer {
        self.doc_decoder.output_aligned()
    }

    /// Return the document at index `idx` of the block.
    #[inline(always)]
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

    pub(crate) fn position_offset(&self) -> u64 {
        self.skip_reader.position_offset()
    }

    /// Position on a block that may contains `target_doc`.
    ///
    /// If all docs are smaller than target, the block loaded may be empty,
    /// or be the last an incomplete VInt block.
    pub fn seek(&mut self, target_doc: DocId) {
        self.skip_reader.seek(target_doc);
        self.load_block();
    }

    fn load_block(&mut self) {
        let offset = self.skip_reader.byte_offset();
        if self.loaded_offset == offset {
            return;
        }
        self.loaded_offset = offset;
        match self.skip_reader.block_info() {
            BlockInfo::BitPacked {
                doc_num_bits,
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
                    &self.data.as_slice()[offset..],
                    self.skip_reader.last_doc_in_previous_block,
                    doc_num_bits,
                    tf_num_bits,
                );
            }
            BlockInfo::VInt(num_vint_docs) => {
                self.doc_decoder.clear();
                if num_vint_docs == 0 {
                    return;
                }
                decode_vint_block(
                    &mut self.doc_decoder,
                    if let FreqReadingOption::ReadFreq = self.freq_reading_option {
                        Some(&mut self.freq_decoder)
                    } else {
                        None
                    },
                    &self.data.as_slice()[offset..],
                    self.skip_reader.last_doc_in_previous_block,
                    num_vint_docs as usize,
                );
            }
        }
    }

    /// Advance to the next block.
    ///
    /// Returns false iff there was no remaining blocks.
    pub fn advance(&mut self) {
        self.skip_reader.advance();
        self.load_block();
    }

    /// Returns an empty segment postings object
    pub fn empty() -> BlockSegmentPostings {
        BlockSegmentPostings {
            doc_decoder: BlockDecoder::with_val(TERMINATED),
            loaded_offset: std::usize::MAX,
            freq_decoder: BlockDecoder::with_val(1),
            freq_reading_option: FreqReadingOption::NoFreq,
            doc_freq: 0,
            data: ReadOnlySource::new(vec![]),
            skip_reader: SkipReader::new(ReadOnlySource::new(vec![]), 0, IndexRecordOption::Basic),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::BlockSegmentPostings;
    use crate::common::HasLen;
    use crate::core::Index;
    use crate::docset::{DocSet, TERMINATED};
    use crate::postings::compression::COMPRESSION_BLOCK_SIZE;
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
        postings.advance();
        assert!(postings.docs().is_empty());
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
            if block.is_empty() {
                break;
            }
            for (i, doc) in block.iter().cloned().enumerate() {
                assert_eq!(offset + (i as u32), doc);
            }
            offset += block.len() as u32;
            block_segments.advance();
        }
    }

    #[test]
    fn test_skip_right_at_new_block() {
        let mut doc_ids = (0..128).collect::<Vec<u32>>();
        // 128 is missing
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
    fn test_block_segment_postings_skip2() {
        let mut docs = vec![0];
        for i in 0..1300 {
            docs.push((i * i / 100) + i);
        }
        let mut block_postings = build_block_postings(&docs[..]);
        for i in vec![0, 424, 10000] {
            block_postings.seek(i);
            let docs = block_postings.docs();
            assert!(docs[0] <= i);
            assert!(docs.last().cloned().unwrap_or(0u32) >= i);
        }
        block_postings.seek(100_000);
        assert_eq!(block_postings.doc(COMPRESSION_BLOCK_SIZE - 1), TERMINATED);
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
        assert_eq!(block_segments.docs(), &[1, 3, 5]);
    }
}

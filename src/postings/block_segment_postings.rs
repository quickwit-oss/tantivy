use std::io;

use crate::common::{BinarySerializable, VInt};
use crate::directory::FileSlice;
use crate::directory::OwnedBytes;
use crate::fieldnorm::FieldNormReader;
use crate::postings::compression::{
    AlignedBuffer, BlockDecoder, VIntDecoder, COMPRESSION_BLOCK_SIZE,
};
use crate::postings::{BlockInfo, FreqReadingOption, SkipReader};
use crate::query::BM25Weight;
use crate::schema::IndexRecordOption;
use crate::{DocId, Score, TERMINATED};

fn max_score<I: Iterator<Item = Score>>(mut it: I) -> Option<Score> {
    if let Some(first) = it.next() {
        Some(it.fold(first, Score::max))
    } else {
        None
    }
}

/// `BlockSegmentPostings` is a cursor iterating over blocks
/// of documents.
///
/// # Warning
///
/// While it is useful for some very specific high-performance
/// use cases, you should prefer using `SegmentPostings` for most usage.
#[derive(Clone)]
pub struct BlockSegmentPostings {
    pub(crate) doc_decoder: BlockDecoder,
    loaded_offset: usize,
    freq_decoder: BlockDecoder,
    freq_reading_option: FreqReadingOption,
    block_max_score_cache: Option<Score>,

    doc_freq: u32,

    data: OwnedBytes,
    pub(crate) skip_reader: SkipReader,
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
    let num_consumed_bytes =
        doc_decoder.uncompress_vint_sorted(data, doc_offset, num_vint_docs, TERMINATED);
    if let Some(freq_decoder) = freq_decoder_opt {
        freq_decoder.uncompress_vint_unsorted(
            &data[num_consumed_bytes..],
            num_vint_docs,
            TERMINATED,
        );
    }
}

fn split_into_skips_and_postings(
    doc_freq: u32,
    mut bytes: OwnedBytes,
) -> (Option<OwnedBytes>, OwnedBytes) {
    if doc_freq < COMPRESSION_BLOCK_SIZE as u32 {
        return (None, bytes);
    }
    let skip_len = VInt::deserialize(&mut bytes).expect("Data corrupted").0 as usize;
    let (skip_data, postings_data) = bytes.split(skip_len);
    (Some(skip_data), postings_data)
}

impl BlockSegmentPostings {
    pub(crate) fn open(
        doc_freq: u32,
        data: FileSlice,
        record_option: IndexRecordOption,
        requested_option: IndexRecordOption,
    ) -> io::Result<BlockSegmentPostings> {
        let freq_reading_option = match (record_option, requested_option) {
            (IndexRecordOption::Basic, _) => FreqReadingOption::NoFreq,
            (_, IndexRecordOption::Basic) => FreqReadingOption::SkipFreq,
            (_, _) => FreqReadingOption::ReadFreq,
        };

        let (skip_data_opt, postings_data) =
            split_into_skips_and_postings(doc_freq, data.read_bytes()?);
        let skip_reader = match skip_data_opt {
            Some(skip_data) => SkipReader::new(skip_data, doc_freq, record_option),
            None => SkipReader::new(OwnedBytes::empty(), doc_freq, record_option),
        };

        let mut block_segment_postings = BlockSegmentPostings {
            doc_decoder: BlockDecoder::with_val(TERMINATED),
            loaded_offset: std::usize::MAX,
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

    /// Returns the block_max_score for the current block.
    /// It does not require the block to be loaded. For instance, it is ok to call this method
    /// after having called `.shallow_advance(..)`.
    ///
    /// See `TermScorer::block_max_score(..)` for more information.
    pub fn block_max_score(
        &mut self,
        fieldnorm_reader: &FieldNormReader,
        bm25_weight: &BM25Weight,
    ) -> Score {
        if let Some(score) = self.block_max_score_cache {
            return score;
        }
        if let Some(skip_reader_max_score) = self.skip_reader.block_max_score(bm25_weight) {
            // if we are on a full block, the skip reader should have the block max information
            // for us
            self.block_max_score_cache = Some(skip_reader_max_score);
            return skip_reader_max_score;
        }
        // this is the last block of the segment posting list.
        // If it is actually loaded, we can compute block max manually.
        if self.block_is_loaded() {
            let docs = self.doc_decoder.output_array().iter().cloned();
            let freqs = self.freq_decoder.output_array().iter().cloned();
            let bm25_scores = docs.zip(freqs).map(|(doc, term_freq)| {
                let fieldnorm_id = fieldnorm_reader.fieldnorm_id(doc);
                bm25_weight.score(fieldnorm_id, term_freq)
            });
            let block_max_score = max_score(bm25_scores).unwrap_or(0.0);
            self.block_max_score_cache = Some(block_max_score);
            return block_max_score;
        }
        // We do not have access to any good block max value. We return bm25_weight.max_score()
        // as it is a valid upperbound.
        //
        // We do not cache it however, so that it gets computed when once block is loaded.
        bm25_weight.max_score()
    }

    pub(crate) fn freq_reading_option(&self) -> FreqReadingOption {
        self.freq_reading_option
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
    pub(crate) fn reset(&mut self, doc_freq: u32, postings_data: OwnedBytes) {
        let (skip_data_opt, postings_data) = split_into_skips_and_postings(doc_freq, postings_data);
        self.data = postings_data;
        self.block_max_score_cache = None;
        self.loaded_offset = std::usize::MAX;
        if let Some(skip_data) = skip_data_opt {
            self.skip_reader.reset(skip_data, doc_freq);
        } else {
            self.skip_reader.reset(OwnedBytes::empty(), doc_freq);
        }
        self.doc_freq = doc_freq;
        self.load_block();
    }

    /// Returns the overall number of documents in the block postings.
    /// It does not take in account whether documents are deleted or not.
    ///
    /// This `doc_freq` is simply the sum of the length of all of the blocks
    /// length, and it does not take in account deleted documents.
    pub fn doc_freq(&self) -> u32 {
        self.doc_freq
    }

    /// Returns the array of docs in the current block.
    ///
    /// Before the first call to `.advance()`, the block
    /// returned by `.docs()` is empty.
    #[inline]
    pub fn docs(&self) -> &[DocId] {
        debug_assert!(self.block_is_loaded());
        self.doc_decoder.output_array()
    }

    /// Returns a full block, regardless of whetehr the block is complete or incomplete (
    /// as it happens for the last block of the posting list).
    ///
    /// In the latter case, the block is guaranteed to be padded with the sentinel value:
    /// `TERMINATED`. The array is also guaranteed to be aligned on 16 bytes = 128 bits.
    ///
    /// This method is useful to run SSE2 linear search.
    #[inline(always)]
    pub(crate) fn docs_aligned(&self) -> &AlignedBuffer {
        debug_assert!(self.block_is_loaded());
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
        debug_assert!(self.block_is_loaded());
        self.freq_decoder.output_array()
    }

    /// Return the frequency at index `idx` of the block.
    #[inline]
    pub fn freq(&self, idx: usize) -> u32 {
        debug_assert!(self.block_is_loaded());
        self.freq_decoder.output(idx)
    }

    /// Returns the length of the current block.
    ///
    /// All blocks have a length of `NUM_DOCS_PER_BLOCK`,
    /// except the last block that may have a length
    /// of any number between 1 and `NUM_DOCS_PER_BLOCK - 1`
    #[inline]
    pub fn block_len(&self) -> usize {
        debug_assert!(self.block_is_loaded());
        self.doc_decoder.output_len
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
        }
    }

    pub(crate) fn block_is_loaded(&self) -> bool {
        self.loaded_offset == self.skip_reader.byte_offset()
    }

    pub(crate) fn load_block(&mut self) {
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
            BlockInfo::VInt { num_docs } => {
                let data = {
                    if num_docs == 0 {
                        &[]
                    } else {
                        &self.data.as_slice()[offset..]
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
    }

    /// Advance to the next block.
    ///
    /// Returns false iff there was no remaining blocks.
    pub fn advance(&mut self) {
        self.skip_reader.advance();
        self.block_max_score_cache = None;
        self.load_block();
    }

    /// Returns an empty segment postings object
    pub fn empty() -> BlockSegmentPostings {
        BlockSegmentPostings {
            doc_decoder: BlockDecoder::with_val(TERMINATED),
            loaded_offset: 0,
            freq_decoder: BlockDecoder::with_val(1),
            freq_reading_option: FreqReadingOption::NoFreq,
            block_max_score_cache: None,
            doc_freq: 0,
            data: OwnedBytes::empty(),
            skip_reader: SkipReader::new(OwnedBytes::empty(), 0, IndexRecordOption::Basic),
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
        assert_eq!(postings.doc(), TERMINATED);
        assert_eq!(postings.advance(), TERMINATED);
        assert_eq!(postings.advance(), TERMINATED);
        assert_eq!(postings.doc_freq(), 0);
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
        assert!(postings.docs().is_empty());
        assert_eq!(postings.doc_freq(), 0);
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
        let mut index_writer = index.writer_for_tests().unwrap();
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
        let inverted_index = segment_reader.inverted_index(int_field).unwrap();
        let term = Term::from_field_u64(int_field, 0u64);
        let term_info = inverted_index.get_term_info(&term).unwrap().unwrap();
        inverted_index
            .read_block_postings_from_terminfo(&term_info, IndexRecordOption::Basic)
            .unwrap()
    }

    #[test]
    fn test_block_segment_postings_seek() {
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
    fn test_reset_block_segment_postings() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let int_field = schema_builder.add_u64_field("id", INDEXED);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut index_writer = index.writer_for_tests()?;
        // create two postings list, one containg even number,
        // the other containing odd numbers.
        for i in 0..6 {
            let doc = doc!(int_field=> (i % 2) as u64);
            index_writer.add_document(doc);
        }
        index_writer.commit()?;
        let searcher = index.reader()?.searcher();
        let segment_reader = searcher.segment_reader(0);

        let mut block_segments;
        {
            let term = Term::from_field_u64(int_field, 0u64);
            let inverted_index = segment_reader.inverted_index(int_field)?;
            let term_info = inverted_index.get_term_info(&term)?.unwrap();
            block_segments = inverted_index
                .read_block_postings_from_terminfo(&term_info, IndexRecordOption::Basic)?;
        }
        assert_eq!(block_segments.docs(), &[0, 2, 4]);
        {
            let term = Term::from_field_u64(int_field, 1u64);
            let inverted_index = segment_reader.inverted_index(int_field)?;
            let term_info = inverted_index.get_term_info(&term)?.unwrap();
            inverted_index.reset_block_postings_from_terminfo(&term_info, &mut block_segments)?;
        }
        assert_eq!(block_segments.docs(), &[1, 3, 5]);
        Ok(())
    }
}

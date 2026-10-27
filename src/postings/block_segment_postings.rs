use std::io;

use common::VInt;

use crate::directory::{FileSlice, OwnedBytes};
use crate::fieldnorm::FieldNormReader;
use crate::postings::compression::{BlockDecoder, VIntDecoder, COMPRESSION_BLOCK_SIZE};
use crate::postings::{BlockInfo, FreqReadingOption, ShortPostings, SkipReader};
use crate::query::Bm25Weight;
use crate::schema::IndexRecordOption;
use crate::{DocId, Score, TERMINATED};

fn max_score<I: Iterator<Item = Score>>(mut it: I) -> Option<Score> {
    it.next().map(|first| it.fold(first, Score::max))
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
    block_loaded: bool,
    freq_decoder: BlockDecoder,
    freq_reading_option: FreqReadingOption,
    record_option: IndexRecordOption,
    requested_option: IndexRecordOption,
    block_max_score_cache: Option<Score>,
    doc_freq: u32,
    data: OwnedBytes,
    skip_reader: SkipReader,
}

fn decode_bitpacked_block(
    doc_decoder: &mut BlockDecoder,
    freq_decoder_opt: Option<&mut BlockDecoder>,
    data: &[u8],
    doc_offset: DocId,
    doc_num_bits: u8,
    tf_num_bits: u8,
    strict_delta: bool,
) {
    let num_consumed_bytes =
        doc_decoder.uncompress_block_sorted(data, doc_offset, doc_num_bits, strict_delta);
    if let Some(freq_decoder) = freq_decoder_opt {
        freq_decoder.uncompress_block_unsorted(
            &data[num_consumed_bytes..],
            tf_num_bits,
            strict_delta,
        );
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
        // if it's a json term with freq, containing less than 256 docs, we can reach here thinking
        // we have a freq, despite not really having one.
        if data.len() > num_consumed_bytes {
            freq_decoder.uncompress_vint_unsorted(
                &data[num_consumed_bytes..],
                num_vint_docs,
                TERMINATED,
            );
        } else {
            freq_decoder.set_values(&[], 1);
        }
    }
}

fn split_into_skips_and_postings(
    doc_freq: u32,
    mut bytes: OwnedBytes,
) -> io::Result<(Option<OwnedBytes>, OwnedBytes)> {
    if doc_freq < COMPRESSION_BLOCK_SIZE as u32 {
        return Ok((None, bytes));
    }
    let skip_len = VInt::deserialize_u64(&mut bytes)? as usize;
    let (skip_data, postings_data) = bytes.split(skip_len);
    Ok((Some(skip_data), postings_data))
}

impl BlockSegmentPostings {
    /// Opens a `BlockSegmentPostings`.
    /// `doc_freq` is the number of documents in the posting list.
    /// `record_option` represents the amount of data available according to the schema.
    /// `requested_option` is the amount of data requested by the user.
    /// If for instance, we do not request for term frequencies, this function will not decompress
    /// term frequency blocks.
    pub(crate) fn open(
        doc_freq: u32,
        data: FileSlice,
        record_option: IndexRecordOption,
        requested_option: IndexRecordOption,
    ) -> io::Result<BlockSegmentPostings> {
        let mut block = Self::empty();
        block.record_option = record_option;
        block.requested_option = requested_option;
        block.reset(doc_freq, data.read_bytes()?)?;
        Ok(block)
    }

    /// Opens a single partial block from postings decoded by the term dictionary.
    pub(crate) fn from_short_postings(
        postings: &ShortPostings,
        record_option: IndexRecordOption,
        requested_option: IndexRecordOption,
    ) -> Self {
        let mut block = Self::empty();
        block.record_option = record_option;
        block.requested_option = requested_option;
        block.reset_short_postings(postings);
        block
    }

    pub(crate) fn reset_short_postings(&mut self, postings: &ShortPostings) {
        debug_assert!((1..=32).contains(&postings.doc_ids.len()));
        debug_assert!(
            postings.term_freqs.is_empty() || postings.doc_ids.len() == postings.term_freqs.len()
        );
        self.data = OwnedBytes::empty();
        self.doc_freq = postings.doc_ids.len() as u32;
        self.set_freq_reading_option(
            self.record_option.has_freq() && !postings.term_freqs.is_empty(),
        );
        self.skip_reader
            .reset(OwnedBytes::empty(), self.doc_freq, self.record_option);
        self.doc_decoder.set_values(&postings.doc_ids, TERMINATED);
        if let FreqReadingOption::ReadFreq = self.freq_reading_option {
            self.freq_decoder.set_values(&postings.term_freqs, 1);
        } else {
            self.freq_decoder.set_values(&[], 1);
        }
        self.block_max_score_cache = None;
        self.block_loaded = true;
    }

    /// Returns the block_max_score for the current block.
    /// It does not require the block to be loaded. For instance, it is ok to call this method
    /// after having called `.shallow_advance(..)`.
    ///
    /// See `TermScorer::block_max_score(..)` for more information.
    pub fn block_max_score(
        &mut self,
        fieldnorm_reader: &FieldNormReader,
        bm25_weight: &Bm25Weight,
    ) -> Score {
        if let Some(score) = self.block_max_score_cache {
            return score;
        }
        if let (FreqReadingOption::ReadFreq, Some(skip_reader_max_score)) = (
            self.freq_reading_option,
            self.skip_reader.block_max_score(bm25_weight),
        ) {
            // if we are on a full block, the skip reader should have the block max information
            // for us
            self.block_max_score_cache = Some(skip_reader_max_score);
            return skip_reader_max_score;
        }
        // this is the last block of the segment posting list.
        // If it is actually loaded, we can compute block max manually.
        if self.block_is_loaded() {
            let docs = self.doc_decoder.output_array().iter().cloned();
            let bm25_scores = docs.enumerate().map(|(i, doc)| {
                let fieldnorm_id = fieldnorm_reader.fieldnorm_id(doc);
                bm25_weight.score(fieldnorm_id, self.freq(i))
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

    // Selects frequency reading from the term's stored data and the requested option.
    fn set_freq_reading_option(&mut self, has_freqs: bool) {
        self.freq_reading_option = match (has_freqs, self.requested_option) {
            (false, _) => FreqReadingOption::NoFreq,
            (_, IndexRecordOption::Basic) => FreqReadingOption::SkipFreq,
            (_, _) => FreqReadingOption::ReadFreq,
        };
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
    pub(crate) fn reset(&mut self, doc_freq: u32, postings_data: OwnedBytes) -> io::Result<()> {
        let (skip_data_opt, postings_data) =
            split_into_skips_and_postings(doc_freq, postings_data)?;
        self.data = postings_data;
        self.block_max_score_cache = None;
        self.block_loaded = false;
        let mut record_option = self.record_option;
        let skip_data = skip_data_opt.unwrap_or_else(OwnedBytes::empty);
        // JSON numeric terms omit frequencies even when the field records them.
        if skip_data.len() < 8 * (doc_freq as usize / COMPRESSION_BLOCK_SIZE) {
            record_option = IndexRecordOption::Basic;
        }
        self.set_freq_reading_option(record_option.has_freq());
        self.skip_reader.reset(skip_data, doc_freq, record_option);
        self.freq_decoder.set_values(&[], 1);
        self.doc_freq = doc_freq;
        self.load_block();
        Ok(())
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

    /// Returns the length of the current block.
    ///
    /// Returns the decoded term-frequency buffer for the current block.
    #[inline]
    pub(crate) fn freq_output_array(&self) -> &[u32] {
        self.freq_decoder.output_array()
    }

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
    pub fn seek(&mut self, target_doc: DocId) -> usize {
        // Move to the block that might contain our document.
        self.seek_block(target_doc);
        self.load_block();

        // At this point we are on the block that might contain our document.
        let doc = self.doc_decoder.seek_within_block(target_doc);

        // The last block is not full and padded with TERMINATED,
        // so we are guaranteed to have at least one value (real or padding)
        // that is >= target_doc.
        debug_assert!(doc < COMPRESSION_BLOCK_SIZE);

        // `doc` is now the first element >= `target_doc`.
        // If all docs are smaller than target, the current block is incomplete and padded
        // with TERMINATED. After the search, the cursor points to the first TERMINATED.
        doc
    }

    /// Returns the number of documents with a doc id strictly smaller than `target`
    /// (i.e. the *rank* of `target` in this posting list).
    ///
    /// This jumps to the block that may contain `target` through the skip list, so no
    /// skipped block is decoded; a single block is then decoded to locate `target`
    /// within it. The cost is therefore `O(number_of_skip_list_entries)` plus one block
    /// decode, rather than `O(doc_freq)`.
    ///
    /// Like [`Self::seek`], the underlying cursor only ever moves forward. This method
    /// must be called with **non-decreasing** `target` values (galloping); calling it
    /// with a `target` smaller than a previous one yields an incorrect result. `target`
    /// must be a valid doc id (i.e. `target <= TERMINATED`), exactly as for `seek`.
    ///
    /// Edge cases: returns `0` when `target` is smaller than every doc id, and
    /// `doc_freq()` when `target` is larger than every doc id.
    pub fn rank(&mut self, target: DocId) -> u32 {
        if self.doc_freq == 0 {
            return 0;
        }
        // `within` = number of docs in the landed block with a doc id < target.
        let within = self.seek(target);
        // `remaining_docs` counts the landed block and everything after it, so the
        // difference is the number of docs in all blocks strictly before it.
        let docs_before_block = self.doc_freq - self.skip_reader.remaining_docs();
        docs_before_block + within as u32
    }

    pub(crate) fn position_offset(&self) -> u64 {
        self.skip_reader.position_offset()
    }

    /// Dangerous API! This calls seeks the next block on the skip list,
    /// but does not `.load_block()` afterwards.
    ///
    /// `.load_block()` needs to be called manually afterwards.
    /// If all docs are smaller than target, the block loaded may be empty,
    /// or be the last an incomplete VInt block.
    pub(crate) fn seek_block(&mut self, target_doc: DocId) {
        if self.skip_reader.seek(target_doc) {
            self.block_max_score_cache = None;
            self.block_loaded = false;
        }
    }

    #[inline]
    pub(crate) fn has_remaining_docs(&self) -> bool {
        self.skip_reader.has_remaining_docs()
    }

    pub(crate) fn block_is_loaded(&self) -> bool {
        self.block_loaded
    }

    pub(crate) fn load_block(&mut self) {
        if self.block_is_loaded() {
            return;
        }
        let offset = self.skip_reader.byte_offset();
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
                    &self.data.as_slice()[offset..],
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
        self.block_loaded = true;
    }

    /// Advance to the next block.
    pub fn advance(&mut self) {
        self.skip_reader.advance();
        self.block_loaded = false;
        self.block_max_score_cache = None;
        self.load_block();
    }

    /// Returns an empty segment postings object
    pub fn empty() -> BlockSegmentPostings {
        BlockSegmentPostings {
            doc_decoder: BlockDecoder::with_val(TERMINATED),
            block_loaded: true,
            freq_decoder: BlockDecoder::with_val(1),
            freq_reading_option: FreqReadingOption::NoFreq,
            record_option: IndexRecordOption::Basic,
            requested_option: IndexRecordOption::Basic,
            block_max_score_cache: None,
            doc_freq: 0,
            data: OwnedBytes::empty(),
            skip_reader: SkipReader::new(OwnedBytes::empty(), 0, IndexRecordOption::Basic),
        }
    }

    pub(crate) fn skip_reader(&self) -> &SkipReader {
        &self.skip_reader
    }
}

#[cfg(test)]
mod tests {
    use common::HasLen;

    use super::BlockSegmentPostings;
    use crate::docset::{DocSet, TERMINATED};
    use crate::index::Index;
    use crate::postings::compression::COMPRESSION_BLOCK_SIZE;
    use crate::postings::postings::Postings;
    use crate::postings::SegmentPostings;
    use crate::schema::{IndexRecordOption, Schema, Term, INDEXED};
    use crate::DocId;

    #[test]
    fn test_short_postings_seek_positions_and_score() {
        use std::sync::Arc;

        use crate::fieldnorm::FieldNormReader;
        use crate::positions::PositionReader;
        use crate::postings::ShortPostings;
        use crate::query::Bm25Weight;

        let short = Arc::new(ShortPostings {
            doc_ids: vec![1, 3, 8],
            term_freqs: vec![2, 1, 3],
            position_deltas: vec![0, 2, 5, 1, 2, 4],
        });
        let mut block = BlockSegmentPostings::from_short_postings(
            &short,
            IndexRecordOption::WithFreqsAndPositions,
            IndexRecordOption::WithFreqsAndPositions,
        );
        assert_eq!(block.doc_freq(), 3);
        assert_eq!(block.docs(), [1, 3, 8]);
        assert_eq!(block.rank(3), 1);
        assert_eq!(block.rank(9), 3);
        let norms = FieldNormReader::for_test(&[10; 9]);
        let weight = Bm25Weight::for_one_term(3, 9, 10.0);
        let expected = short
            .doc_ids
            .iter()
            .zip(&short.term_freqs)
            .map(|(&doc, &freq)| weight.score(norms.fieldnorm_id(doc), freq))
            .fold(0.0, f32::max);
        assert_eq!(block.block_max_score(&norms, &weight), expected);
        let mut without_freqs = BlockSegmentPostings::from_short_postings(
            &ShortPostings {
                doc_ids: short.doc_ids.clone(),
                ..Default::default()
            },
            IndexRecordOption::WithFreqs,
            IndexRecordOption::WithFreqs,
        );
        let expected = short
            .doc_ids
            .iter()
            .map(|&doc| weight.score(norms.fieldnorm_id(doc), 1))
            .fold(0.0, f32::max);
        assert!(expected > 0.0);
        assert_eq!(without_freqs.block_max_score(&norms, &weight), expected);
        assert!(without_freqs.freqs().is_empty());
        let mut postings = SegmentPostings::from_block_postings(
            block,
            Some(PositionReader::from_short_postings(short.clone())),
        );
        let mut positions = Vec::new();
        postings.positions(&mut positions);
        assert_eq!(positions, [0, 2]);
        assert_eq!(postings.seek(2), 3);
        postings.positions(&mut positions);
        assert_eq!(positions, [5]);
        let mut cloned = postings.clone();
        assert_eq!(cloned.advance(), 8);
        cloned.positions(&mut positions);
        assert_eq!(positions, [1, 3, 7]);
        assert_eq!(cloned.advance(), TERMINATED);
        assert_eq!(postings.doc(), 3);

        for (option, freq) in [(IndexRecordOption::Basic, 1), (IndexRecordOption::WithFreqs, 2)] {
            let mut block = BlockSegmentPostings::from_short_postings(
                &short,
                IndexRecordOption::WithFreqs,
                option,
            );
            assert_eq!(block.freq(0), freq);
            block.advance();
            assert!(block.docs().is_empty());
            assert_eq!(block.doc(0), TERMINATED);
            block.reset_short_postings(&short);
            assert_eq!(block.docs(), [1, 3, 8]);
            assert_eq!(block.seek(9), 3);
            assert_eq!(block.doc(3), TERMINATED);
        }
    }

    #[test]
    fn test_reset_between_short_and_external_postings() {
        use crate::directory::OwnedBytes;
        use crate::postings::serializer::PostingsSerializer;
        use crate::postings::ShortPostings;

        let short = ShortPostings {
            doc_ids: vec![2, 7],
            term_freqs: vec![3, 2],
            position_deltas: Vec::new(),
        };
        let mut block = BlockSegmentPostings::from_short_postings(
            &short,
            IndexRecordOption::WithFreqs,
            IndexRecordOption::WithFreqs,
        );
        let mut serializer = PostingsSerializer::new(0.0, IndexRecordOption::WithFreqs, None);
        serializer.new_term(260, true);
        for doc in 0..260 {
            serializer.write_doc(doc, 4);
        }
        let mut data = Vec::new();
        serializer.close_term(260, &mut data).unwrap();
        for _ in 0..2 {
            block.reset(260, OwnedBytes::new(data.clone())).unwrap();
            assert_eq!(block.doc_freq(), 260);
            let idx = block.seek(259);
            assert_eq!(block.doc(idx), 259);
            assert_eq!(block.freq(idx), 4);
            block.reset_short_postings(&short);
            assert_eq!(block.doc_freq(), 2);
            assert_eq!(block.docs(), [2, 7]);
            assert_eq!(block.freqs(), [3, 2]);
            assert_eq!(block.position_offset(), 0);
            assert_eq!(block.rank(8), 2);
        }
        let mut serializer = PostingsSerializer::new(0.0, IndexRecordOption::Basic, None);
        serializer.new_term(1, false);
        serializer.write_doc(9, 0);
        let mut data = Vec::new();
        serializer.close_term(1, &mut data).unwrap();
        block.reset(1, OwnedBytes::new(data)).unwrap();
        assert_eq!(block.docs(), [9]);
        assert!(block.freqs().is_empty());
        assert_eq!(block.freq(0), 1);
        block.reset_short_postings(&short);
        assert_eq!(block.freqs(), [3, 2]);
    }

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
    fn test_block_segment_postings() -> crate::Result<()> {
        let mut block_segments = build_block_postings(&(0..100_000).collect::<Vec<u32>>())?;
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
        Ok(())
    }

    #[test]
    fn test_skip_right_at_new_block() -> crate::Result<()> {
        let mut doc_ids = (0..128).collect::<Vec<u32>>();
        // 128 is missing
        doc_ids.push(129);
        doc_ids.push(130);
        {
            let block_segments = build_block_postings(&doc_ids)?;
            let mut docset = SegmentPostings::from_block_postings(block_segments, None);
            assert_eq!(docset.seek(128), 129);
            assert_eq!(docset.doc(), 129);
            assert_eq!(docset.advance(), 130);
            assert_eq!(docset.doc(), 130);
            assert_eq!(docset.advance(), TERMINATED);
        }
        {
            let block_segments = build_block_postings(&doc_ids).unwrap();
            let mut docset = SegmentPostings::from_block_postings(block_segments, None);
            assert_eq!(docset.seek(129), 129);
            assert_eq!(docset.doc(), 129);
            assert_eq!(docset.advance(), 130);
            assert_eq!(docset.doc(), 130);
            assert_eq!(docset.advance(), TERMINATED);
        }
        {
            let block_segments = build_block_postings(&doc_ids)?;
            let mut docset = SegmentPostings::from_block_postings(block_segments, None);
            assert_eq!(docset.doc(), 0);
            assert_eq!(docset.seek(131), TERMINATED);
            assert_eq!(docset.doc(), TERMINATED);
        }
        Ok(())
    }

    fn build_block_postings(docs: &[DocId]) -> crate::Result<BlockSegmentPostings> {
        let mut schema_builder = Schema::builder();
        let int_field = schema_builder.add_u64_field("id", INDEXED);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut index_writer = index.writer_for_tests()?;
        let mut last_doc = 0u32;
        for &doc in docs {
            for _ in last_doc..doc {
                index_writer.add_document(doc!(int_field=>1u64))?;
            }
            index_writer.add_document(doc!(int_field=>0u64))?;
            last_doc = doc + 1;
        }
        index_writer.commit()?;
        let searcher = index.reader()?.searcher();
        let segment_reader = searcher.segment_reader(0);
        let inverted_index = segment_reader.inverted_index(int_field).unwrap();
        let term = Term::from_field_u64(int_field, 0u64);
        let term_info = inverted_index.get_term_info(&term)?.unwrap();
        let block_postings = inverted_index
            .read_block_postings_from_terminfo(&term_info, IndexRecordOption::Basic)?;
        Ok(block_postings)
    }

    #[test]
    fn test_block_segment_postings_seek() -> crate::Result<()> {
        let mut docs = vec![0];
        for i in 0..1300 {
            docs.push((i * i / 100) + i);
        }
        let mut block_postings = build_block_postings(&docs[..])?;
        for i in &[0, 424, 10000] {
            block_postings.seek(*i);
            let docs = block_postings.docs();
            assert!(docs[0] <= *i);
            assert!(docs.last().cloned().unwrap_or(0u32) >= *i);
        }
        block_postings.seek(100_000);
        assert_eq!(block_postings.doc(COMPRESSION_BLOCK_SIZE - 1), TERMINATED);
        Ok(())
    }

    #[test]
    fn test_reset_block_segment_postings() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let int_field = schema_builder.add_u64_field("id", INDEXED);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut index_writer = index.writer_for_tests()?;
        // create two postings list, one containing even number,
        // the other containing odd numbers.
        for i in 0..6 {
            let doc = doc!(int_field=> (i % 2) as u64);
            index_writer.add_document(doc)?;
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

    #[test]
    fn test_block_segment_postings_rank() -> crate::Result<()> {
        // ~8 blocks worth of docs so the skip list is actually exercised.
        let docs: Vec<DocId> = (0..1000u32).map(|i| i * 3).collect();
        let mut block_postings = build_block_postings(&docs[..])?;
        let doc_freq = block_postings.doc_freq();

        // rank(target) must equal the number of docs strictly below target.
        // Targets are queried in non-decreasing order, as the API requires.
        // `target` values must be a valid doc id (<= TERMINATED) and non-decreasing.
        let targets = [
            0u32, 1, 2, 3, 4, 299, 300, 301, 1500, 2996, 2997, 3000, 10_000,
        ];
        for &target in &targets {
            let expected = docs.iter().filter(|&&d| d < target).count() as u32;
            assert_eq!(
                block_postings.rank(target),
                expected,
                "rank({target}) mismatch"
            );
        }

        // Edge cases: below the first doc -> 0, above the last doc -> doc_freq.
        let mut fresh = build_block_postings(&docs[..])?;
        assert_eq!(fresh.rank(0), 0);
        let mut fresh = build_block_postings(&docs[..])?;
        assert_eq!(fresh.rank(1_000_000), doc_freq);

        // Empty postings: rank is always 0.
        let mut empty = BlockSegmentPostings::empty();
        assert_eq!(empty.rank(42), 0);
        Ok(())
    }
}

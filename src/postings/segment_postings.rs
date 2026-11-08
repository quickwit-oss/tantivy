use common::{HasLen, TinySet};

use crate::docset::DocSet;
use crate::fastfield::AliveBitSet;
use crate::positions::PositionReader;
use crate::postings::bitset_fill::or_range_into_tinysets;
use crate::postings::compression::{bitset_base_doc, dense_block_size, COMPRESSION_BLOCK_SIZE};
use crate::postings::{BlockInfo, BlockSegmentPostings, Postings};
use crate::{DocId, TERMINATED};

/// `SegmentPostings` represents the inverted list or postings associated with
/// a term in a `Segment`.
///
/// As we iterate through the `SegmentPostings`, the frequencies are optionally decoded.
/// Positions on the other hand, are optionally entirely decoded upfront.
#[derive(Clone)]
pub struct SegmentPostings {
    pub(crate) block_cursor: BlockSegmentPostings,
    cur: usize,
    position_reader: Option<PositionReader>,
}

impl SegmentPostings {
    /// Returns an empty segment postings object
    pub fn empty() -> Self {
        SegmentPostings {
            block_cursor: BlockSegmentPostings::empty(),
            cur: 0,
            position_reader: None,
        }
    }

    /// Compute the number of non-deleted documents.
    ///
    /// This method will clone and scan through the posting lists.
    /// (this is a rather expensive operation).
    pub fn doc_freq_given_deletes(&self, alive_bitset: &AliveBitSet) -> u32 {
        let mut docset = self.clone();
        let mut doc_freq = 0;
        loop {
            let doc = docset.doc();
            if doc == TERMINATED {
                return doc_freq;
            }
            if alive_bitset.is_alive(doc) {
                doc_freq += 1u32;
            }
            docset.advance();
        }
    }

    /// Returns the overall number of documents in the block postings.
    /// It does not take in account whether documents are deleted or not.
    pub fn doc_freq(&self) -> u32 {
        self.block_cursor.doc_freq()
    }

    /// Creates a segment postings object with the given documents
    /// and no frequency encoded.
    ///
    /// This method is mostly useful for unit tests.
    ///
    /// It serializes the doc ids using tantivy's codec
    /// and returns a `SegmentPostings` object that embeds a
    /// buffer with the serialized data.
    #[cfg(test)]
    pub fn create_from_docs(docs: &[u32]) -> SegmentPostings {
        use crate::directory::FileSlice;
        use crate::postings::serializer::PostingsSerializer;
        use crate::schema::IndexRecordOption;
        let mut buffer = Vec::new();
        {
            let mut postings_serializer =
                PostingsSerializer::new(0.0, IndexRecordOption::Basic, None);
            postings_serializer.new_term(docs.len() as u32, false);
            for &doc in docs {
                postings_serializer.write_doc(doc, 1u32);
            }
            postings_serializer
                .close_term(docs.len() as u32, &mut buffer)
                .expect("In memory Serialization should never fail.");
        }
        let block_segment_postings = BlockSegmentPostings::open(
            docs.len() as u32,
            FileSlice::from(buffer),
            IndexRecordOption::Basic,
            IndexRecordOption::Basic,
        )
        .unwrap();
        SegmentPostings::from_block_postings(block_segment_postings, None)
    }

    /// Helper functions to create `SegmentPostings` for tests.
    #[cfg(test)]
    pub fn create_from_docs_and_tfs(
        doc_and_tfs: &[(u32, u32)],
        fieldnorms: Option<&[u32]>,
    ) -> SegmentPostings {
        use crate::directory::FileSlice;
        use crate::fieldnorm::FieldNormReader;
        use crate::postings::serializer::PostingsSerializer;
        use crate::schema::IndexRecordOption;
        use crate::Score;
        let mut buffer: Vec<u8> = Vec::new();
        let fieldnorm_reader = fieldnorms.map(FieldNormReader::for_test);
        let average_field_norm = fieldnorms
            .map(|fieldnorms| {
                if fieldnorms.is_empty() {
                    return 0.0;
                }
                let total_num_tokens: u64 = fieldnorms
                    .iter()
                    .map(|&fieldnorm| fieldnorm as u64)
                    .sum::<u64>();
                total_num_tokens as Score / fieldnorms.len() as Score
            })
            .unwrap_or(0.0);
        let mut postings_serializer = PostingsSerializer::new(
            average_field_norm,
            IndexRecordOption::WithFreqs,
            fieldnorm_reader,
        );
        postings_serializer.new_term(doc_and_tfs.len() as u32, true);
        for &(doc, tf) in doc_and_tfs {
            postings_serializer.write_doc(doc, tf);
        }
        postings_serializer
            .close_term(doc_and_tfs.len() as u32, &mut buffer)
            .unwrap();
        let block_segment_postings = BlockSegmentPostings::open(
            doc_and_tfs.len() as u32,
            FileSlice::from(buffer),
            IndexRecordOption::WithFreqs,
            IndexRecordOption::WithFreqs,
        )
        .unwrap();
        SegmentPostings::from_block_postings(block_segment_postings, None)
    }

    /// Reads a Segment postings from an &[u8]
    ///
    /// * `len` - number of document in the posting lists.
    /// * `data` - data array. The complete data is not necessarily used.
    /// * `freq_handler` - the freq handler is in charge of decoding frequencies and/or positions
    pub(crate) fn from_block_postings(
        segment_block_postings: BlockSegmentPostings,
        position_reader: Option<PositionReader>,
    ) -> SegmentPostings {
        SegmentPostings {
            block_cursor: segment_block_postings,
            cur: 0, // cursor within the block
            position_reader,
        }
    }
}

impl DocSet for SegmentPostings {
    // goes to the next element.
    // next needs to be called a first time to point to the correct element.
    #[inline]
    fn advance(&mut self) -> DocId {
        debug_assert!(self.block_cursor.block_is_loaded());
        if self.cur == COMPRESSION_BLOCK_SIZE - 1 {
            self.cur = 0;
            self.block_cursor.advance();
        } else {
            self.cur += 1;
        }
        self.doc()
    }

    #[inline]
    fn seek(&mut self, target: DocId) -> DocId {
        debug_assert!(self.doc() <= target);
        if self.doc() >= target {
            return self.doc();
        }

        // As an optimization, if the block is already loaded, we can
        // cheaply check the next doc.
        self.cur = (self.cur + 1).min(COMPRESSION_BLOCK_SIZE - 1);
        if self.doc() >= target {
            return self.doc();
        }

        // Delegate block-local search to BlockSegmentPostings::seek, which returns
        // the in-block index of the first doc >= target.
        self.cur = self.block_cursor.seek(target);
        let doc = self.doc();
        debug_assert!(doc >= target);
        doc
    }

    /// Return the current document's `DocId`.
    #[inline]
    fn doc(&self) -> DocId {
        self.block_cursor.doc(self.cur)
    }

    fn size_hint(&self) -> u32 {
        self.len() as u32
    }

    fn fill_bitset_window(&mut self, min_doc: DocId, mask: &mut [TinySet]) -> DocId {
        if mask.is_empty() {
            return self.doc();
        }
        if self.doc() < min_doc {
            self.seek(min_doc);
        }
        let horizon = min_doc.saturating_add(mask.len() as u32 * 64);
        loop {
            if !self.block_cursor.block_is_loaded() {
                if let Some(next) = self.try_or_unloaded_dense_block(min_doc, horizon, mask) {
                    if next == TERMINATED {
                        return TERMINATED;
                    }
                    continue;
                }
                self.block_cursor.load_block();
                self.cur = 0;
            }

            let doc = self.doc();
            if doc >= horizon {
                return doc;
            }

            match self.block_cursor.skip_reader().block_info() {
                BlockInfo::Dense { num_longs, .. } => {
                    let last = self.block_cursor.skip_reader().last_doc_in_block();
                    let base =
                        bitset_base_doc(self.block_cursor.skip_reader().last_doc_in_previous_block);
                    let to = last.saturating_add(1).min(horizon);
                    if to > doc {
                        let offset = self.block_cursor.skip_reader().byte_offset();
                        let nbytes = dense_block_size(num_longs);
                        let src = &self.block_cursor.postings_bytes()[offset..offset + nbytes];
                        or_range_into_tinysets(src, doc - base, mask, doc - min_doc, to - doc);
                    }
                    if last < horizon {
                        self.block_cursor.advance_skip_only();
                        self.cur = 0;
                        continue;
                    }
                    self.cur = self.block_cursor.seek_within_loaded_block(horizon);
                    return self.doc();
                }
                BlockInfo::BitPacked { .. } | BlockInfo::VInt { .. } => {
                    let docs = self.block_cursor.docs();
                    let len = self.block_cursor.block_len();
                    let mut i = self.cur;
                    while i < len {
                        let d = docs[i];
                        if d >= horizon {
                            self.cur = i;
                            return d;
                        }
                        let delta = d - min_doc;
                        mask[(delta / 64) as usize].insert_mut(delta % 64);
                        i += 1;
                    }
                    self.block_cursor.advance_skip_only();
                    self.cur = 0;
                }
            }
        }
    }
}

impl SegmentPostings {
    /// If the current (unloaded) block is dense and lies entirely inside
    /// `[min_doc, horizon)`, OR it into `mask` and skip decode. Returns
    /// `Some(TERMINATED)` when postings are exhausted, `Some(0)` when the
    /// block was consumed, `None` when the block must be decoded.
    fn try_or_unloaded_dense_block(
        &mut self,
        min_doc: DocId,
        horizon: DocId,
        mask: &mut [TinySet],
    ) -> Option<DocId> {
        let skip = self.block_cursor.skip_reader();
        if !skip.has_remaining_docs() {
            // Leave the decoder on a TERMINATED-padded empty block so
            // `doc()` matches the `TERMINATED` we return. Union refill
            // reads `doc()` after fill_bitset_window.
            self.block_cursor.load_block();
            self.cur = 0;
            return Some(TERMINATED);
        }
        let BlockInfo::Dense { num_longs, .. } = skip.block_info() else {
            return None;
        };
        let last = skip.last_doc_in_block();
        if last >= horizon {
            return None;
        }
        let base = bitset_base_doc(skip.last_doc_in_previous_block);
        let from = min_doc.max(base);
        let to = last.saturating_add(1);
        if to > from {
            let offset = skip.byte_offset();
            let nbytes = dense_block_size(num_longs);
            let src = &self.block_cursor.postings_bytes()[offset..offset + nbytes];
            or_range_into_tinysets(src, from - base, mask, from - min_doc, to - from);
        }
        self.block_cursor.advance_skip_only();
        self.cur = 0;
        Some(0)
    }
}

impl HasLen for SegmentPostings {
    fn len(&self) -> usize {
        self.block_cursor.doc_freq() as usize
    }
}

impl Postings for SegmentPostings {
    /// Returns the frequency associated with the current document.
    /// If the schema is set up so that no frequency have been encoded,
    /// this method should always return 1.
    ///
    /// # Panics
    ///
    /// Will panics if called without having called advance before.
    fn term_freq(&self) -> u32 {
        debug_assert!(
            // Here we do not use the len of `freqs()`
            // because it is actually ok to request for the freq of doc
            // even if no frequency were encoded for the field.
            //
            // In that case we hit the block just as if the frequency had been
            // decoded. The block is simply prefilled by the value 1.
            self.cur < COMPRESSION_BLOCK_SIZE,
            "Have you forgotten to call `.advance()` at least once before calling `.term_freq()`."
        );
        self.block_cursor.freq(self.cur)
    }

    fn append_positions_with_offset(&mut self, offset: u32, output: &mut Vec<u32>) {
        let term_freq = self.term_freq();
        let prev_len = output.len();
        if let Some(position_reader) = self.position_reader.as_mut() {
            debug_assert!(
                !self.block_cursor.freqs().is_empty(),
                "No positions available"
            );
            let read_offset = self.block_cursor.position_offset()
                + (self.block_cursor.freqs()[..self.cur]
                    .iter()
                    .cloned()
                    .sum::<u32>() as u64);
            // TODO: instead of zeroing the output, we could use MaybeUninit or similar.
            output.resize(prev_len + term_freq as usize, 0u32);
            position_reader.read(read_offset, &mut output[prev_len..]);
            let mut cum = offset;
            for output_mut in output[prev_len..].iter_mut() {
                cum += *output_mut;
                *output_mut = cum;
            }
        }
    }
}

#[cfg(test)]
mod tests {

    use common::HasLen;

    use super::SegmentPostings;
    use crate::docset::{DocSet, TERMINATED};
    use crate::fastfield::AliveBitSet;
    use crate::postings::postings::Postings;
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
    fn test_doc_freq() {
        let docs = SegmentPostings::create_from_docs(&[0, 2, 10]);
        assert_eq!(docs.doc_freq(), 3);
        let alive_bitset = AliveBitSet::for_test_from_deleted_docs(&[2], 12);
        assert_eq!(docs.doc_freq_given_deletes(&alive_bitset), 2);
        let all_deleted =
            AliveBitSet::for_test_from_deleted_docs(&[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11], 12);
        assert_eq!(docs.doc_freq_given_deletes(&all_deleted), 0);
    }

    fn collect_windows(docs: &[DocId]) -> Vec<(DocId, Vec<DocId>, DocId)> {
        let mut postings = SegmentPostings::create_from_docs(docs);
        let mut windows = Vec::new();
        let mut min_doc = postings.doc();
        while min_doc != TERMINATED {
            let mut mask = [common::TinySet::empty(); crate::docset::BLOCK_NUM_TINYBITSETS];
            let next = postings.fill_bitset_window(min_doc, &mut mask);
            let mut hits = Vec::new();
            for (i, tiny) in mask.iter().enumerate() {
                for bit in *tiny {
                    hits.push(min_doc + (i as u32) * 64 + bit);
                }
            }
            windows.push((min_doc, hits, next));
            min_doc = next;
        }
        windows
    }

    fn expected_windows(docs: &[DocId]) -> Vec<(DocId, Vec<DocId>, DocId)> {
        let window = crate::docset::BLOCK_WINDOW;
        let mut out = Vec::new();
        if docs.is_empty() {
            return out;
        }
        let mut min_doc = docs[0];
        loop {
            let horizon = min_doc.saturating_add(window);
            let hits: Vec<DocId> = docs
                .iter()
                .copied()
                .filter(|&d| d >= min_doc && d < horizon)
                .collect();
            let next = docs
                .iter()
                .copied()
                .find(|&d| d >= horizon)
                .unwrap_or(TERMINATED);
            out.push((min_doc, hits, next));
            if next == TERMINATED {
                break;
            }
            min_doc = next;
        }
        out
    }

    #[test]
    fn fill_bitset_window_dense_gapped() {
        // 90% dense with every-10th gap: forces dense bitset blocks.
        let docs: Vec<DocId> = (0..5_000u32).filter(|i| i % 10 != 0).collect();
        assert_eq!(collect_windows(&docs), expected_windows(&docs));
    }

    #[test]
    fn fill_bitset_window_sparse_for() {
        let docs: Vec<DocId> = (0..2_000u32).map(|i| i * 17).collect();
        assert_eq!(collect_windows(&docs), expected_windows(&docs));
    }

    #[test]
    fn fill_bitset_window_burst() {
        let docs: Vec<DocId> = (0..4_000u32).filter(|i| (i % 192) < 128).collect();
        assert_eq!(collect_windows(&docs), expected_windows(&docs));
    }

    #[test]
    fn fill_bitset_window_leaves_terminated() {
        let docs: Vec<DocId> = (0..5_000u32).filter(|i| i % 10 != 0).collect();
        let mut postings = SegmentPostings::create_from_docs(&docs);
        let mut min_doc = postings.doc();
        while min_doc != TERMINATED {
            let mut mask = [common::TinySet::empty(); 64];
            min_doc = postings.fill_bitset_window(min_doc, &mut mask);
        }
        assert_eq!(postings.doc(), TERMINATED);
    }
}

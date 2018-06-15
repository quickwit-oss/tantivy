use compression::{BlockDecoder, CompressedIntStream, VIntDecoder, COMPRESSION_BLOCK_SIZE};
use DocId;

use common::BitSet;
use common::CountingWriter;
use common::HasLen;
use compression::compressed_block_size;
use directory::{ReadOnlySource, SourceRead};
use docset::{DocSet, SkipResult};
use fst::Streamer;
use postings::serializer::PostingsSerializer;
use postings::FreqReadingOption;
use postings::Postings;

struct PositionComputer {
    // store the amount of position int
    // before reading positions.
    //
    // if none, position are already loaded in
    // the positions vec.
    position_to_skip: usize,
    positions_stream: CompressedIntStream,
}

impl PositionComputer {
    pub fn new(positions_stream: CompressedIntStream) -> PositionComputer {
        PositionComputer {
            position_to_skip: 0,
            positions_stream,
        }
    }

    pub fn add_skip(&mut self, num_skip: usize) {
        self.position_to_skip += num_skip;
    }

    // Positions can only be read once.
    pub fn positions_with_offset(&mut self, offset: u32, output: &mut [u32]) {
        self.positions_stream.skip(self.position_to_skip);
        self.position_to_skip = 0;
        self.positions_stream.read(output);
        let mut cum = offset;
        for output_mut in output.iter_mut() {
            cum += *output_mut;
            *output_mut = cum;
        }
    }
}

/// `SegmentPostings` represents the inverted list or postings associated to
/// a term in a `Segment`.
///
/// As we iterate through the `SegmentPostings`, the frequencies are optionally decoded.
/// Positions on the other hand, are optionally entirely decoded upfront.
pub struct SegmentPostings {
    block_cursor: BlockSegmentPostings,
    cur: usize,
    position_computer: Option<PositionComputer>,
}

impl SegmentPostings {
    /// Returns an empty segment postings object
    pub fn empty() -> Self {
        let empty_block_cursor = BlockSegmentPostings::empty();
        SegmentPostings {
            block_cursor: empty_block_cursor,
            cur: COMPRESSION_BLOCK_SIZE,
            position_computer: None,
        }
    }

    /// Creates a segment postings object with the given documents
    /// and no frequency encoded.
    ///
    /// This method is mostly useful for unit tests.
    ///
    /// It serializes the doc ids using tantivy's codec
    /// and returns a `SegmentPostings` object that embeds a
    /// buffer with the serialized data.
    pub fn create_from_docs(docs: &[u32]) -> SegmentPostings {
        let mut counting_writer = CountingWriter::wrap(Vec::new());
        {
            let mut postings_serializer = PostingsSerializer::new(&mut counting_writer, false);
            for &doc in docs {
                postings_serializer.write_doc(doc, 1u32).unwrap();
            }
            postings_serializer
                .close_term()
                .expect("In memory Serialization should never fail.");
        }
        let (buffer, _) = counting_writer
            .finish()
            .expect("Serializing in a buffer should never fail.");
        let data = ReadOnlySource::from(buffer);
        let block_segment_postings = BlockSegmentPostings::from_data(
            docs.len(),
            SourceRead::from(data),
            FreqReadingOption::NoFreq,
        );
        SegmentPostings::from_block_postings(block_segment_postings, None)
    }
}

impl SegmentPostings {
    /// Reads a Segment postings from an &[u8]
    ///
    /// * `len` - number of document in the posting lists.
    /// * `data` - data array. The complete data is not necessarily used.
    /// * `freq_handler` - the freq handler is in charge of decoding
    ///   frequencies and/or positions
    pub fn from_block_postings(
        segment_block_postings: BlockSegmentPostings,
        positions_stream_opt: Option<CompressedIntStream>,
    ) -> SegmentPostings {
        SegmentPostings {
            block_cursor: segment_block_postings,
            cur: COMPRESSION_BLOCK_SIZE, // cursor within the block
            position_computer: positions_stream_opt.map(PositionComputer::new),
        }
    }
}

fn exponential_search(target: u32, mut start: usize, arr: &[u32]) -> (usize, usize) {
    let end = arr.len();
    debug_assert!(target >= arr[start]);
    debug_assert!(target <= arr[end - 1]);
    let mut jump = 1;
    loop {
        let new = start + jump;
        if new >= end {
            return (start, end);
        }
        if arr[new] > target {
            return (start, new);
        }
        start = new;
        jump *= 2;
    }
}

impl DocSet for SegmentPostings {
    fn skip_next(&mut self, target: DocId) -> SkipResult {
        if !self.advance() {
            return SkipResult::End;
        }
        if self.doc() == target {
            return SkipResult::Reached;
        }

        // in the following, thanks to the call to advance above,
        // we know that the position is not loaded and we need
        // to skip every doc_freq we cross.

        // skip blocks until one that might contain the target
        loop {
            // check if we need to go to the next block
            let (current_doc, last_doc_in_block) = {
                let block_docs = self.block_cursor.docs();
                (block_docs[self.cur], block_docs[block_docs.len() - 1])
            };
            if target > last_doc_in_block {
                // we add skip for the current term independantly,
                // so that position_add_skip will decide if it should
                // just set itself to Some(0) or effectively
                // add the term freq.
                if self.position_computer.is_some() {
                    let freqs_skipped = &self.block_cursor.freqs()[self.cur..];
                    let sum_freq: u32 = freqs_skipped.iter().sum();
                    self.position_computer
                        .as_mut()
                        .unwrap()
                        .add_skip(sum_freq as usize);
                }
                if !self.block_cursor.advance() {
                    return SkipResult::End;
                }
                self.cur = 0;
            } else {
                if target < current_doc {
                    // We've passed the target after the first `advance` call
                    // or we're at the beginning of a block.
                    // Either way, we're on the first `DocId` greater than `target`
                    return SkipResult::OverStep;
                }
                break;
            }
        }

        // we're in the right block now, start with an exponential search
        let block_docs = self.block_cursor.docs();

        let (mut start, end) = exponential_search(target, self.cur, block_docs);

        start += block_docs[start..end]
            .binary_search(&target)
            .unwrap_or_else(|e| e);

        // `doc` is now the first element >= `target`
        let doc = block_docs[start];
        debug_assert!(doc >= target);

        if self.position_computer.is_some() {
            let freqs_skipped = &self.block_cursor.freqs()[self.cur..start];
            let sum_freqs: u32 = freqs_skipped.iter().sum();
            self.position_computer
                .as_mut()
                .unwrap()
                .add_skip(sum_freqs as usize);
        }

        self.cur = start;
        if doc == target {
            return SkipResult::Reached;
        } else {
            return SkipResult::OverStep;
        }
    }

    // goes to the next element.
    // next needs to be called a first time to point to the correct element.
    #[inline]
    fn advance(&mut self) -> bool {
        if self.position_computer.is_some() {
            let term_freq = self.term_freq() as usize;
            self.position_computer.as_mut().unwrap().add_skip(term_freq);
        }
        self.cur += 1;
        if self.cur >= self.block_cursor.block_len() {
            self.cur = 0;
            if !self.block_cursor.advance() {
                self.cur = COMPRESSION_BLOCK_SIZE;
                return false;
            }
        }
        true
    }

    fn size_hint(&self) -> u32 {
        self.len() as u32
    }

    /// Return the current document's `DocId`.
    #[inline]
    fn doc(&self) -> DocId {
        let docs = self.block_cursor.docs();
        debug_assert!(
            self.cur < docs.len(),
            "Have you forgotten to call `.advance()` at least once before calling .doc()."
        );
        docs[self.cur]
    }

    fn append_to_bitset(&mut self, bitset: &mut BitSet) {
        // finish the current block
        if self.advance() {
            for &doc in &self.block_cursor.docs()[self.cur..] {
                bitset.insert(doc);
            }
            // ... iterate through the remaining blocks.
            while self.block_cursor.advance() {
                for &doc in self.block_cursor.docs() {
                    bitset.insert(doc);
                }
            }
        }
    }
}

impl HasLen for SegmentPostings {
    fn len(&self) -> usize {
        self.block_cursor.doc_freq()
    }
}

impl Postings for SegmentPostings {
    fn term_freq(&self) -> u32 {
        self.block_cursor.freq(self.cur)
    }

    fn positions_with_offset(&mut self, offset: u32, output: &mut Vec<u32>) {
        if self.position_computer.is_some() {
            output.resize(self.term_freq() as usize, 0u32);
            self.position_computer
                .as_mut()
                .unwrap()
                .positions_with_offset(offset, &mut output[..])
        } else {
            output.clear();
        }
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
    num_bitpacked_blocks: usize,
    num_vint_docs: usize,
    remaining_data: SourceRead,
}

impl BlockSegmentPostings {
    pub(crate) fn from_data(
        doc_freq: usize,
        data: SourceRead,
        freq_reading_option: FreqReadingOption,
    ) -> BlockSegmentPostings {
        let num_bitpacked_blocks: usize = (doc_freq as usize) / COMPRESSION_BLOCK_SIZE;
        let num_vint_docs = (doc_freq as usize) - COMPRESSION_BLOCK_SIZE * num_bitpacked_blocks;
        BlockSegmentPostings {
            num_bitpacked_blocks,
            num_vint_docs,
            doc_decoder: BlockDecoder::new(),
            freq_decoder: BlockDecoder::with_val(1),
            freq_reading_option,
            remaining_data: data,
            doc_offset: 0,
            doc_freq,
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
    pub(crate) fn reset(&mut self, doc_freq: usize, postings_data: SourceRead) {
        let num_binpacked_blocks: usize = doc_freq / COMPRESSION_BLOCK_SIZE;
        let num_vint_docs = doc_freq & (COMPRESSION_BLOCK_SIZE - 1);
        self.num_bitpacked_blocks = num_binpacked_blocks;
        self.num_vint_docs = num_vint_docs;
        self.remaining_data = postings_data;
        self.doc_offset = 0;
        self.doc_freq = doc_freq;
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
    fn block_len(&self) -> usize {
        self.doc_decoder.output_len
    }

    /// Advance to the next block.
    ///
    /// Returns false iff there was no remaining blocks.
    pub fn advance(&mut self) -> bool {
        if self.num_bitpacked_blocks > 0 {
            let num_consumed_bytes = self.doc_decoder
                .uncompress_block_sorted(self.remaining_data.as_ref(), self.doc_offset);
            self.remaining_data.advance(num_consumed_bytes);
            match self.freq_reading_option {
                FreqReadingOption::NoFreq => {}
                FreqReadingOption::SkipFreq => {
                    let num_bytes_to_skip = compressed_block_size(self.remaining_data.as_ref()[0]);
                    self.remaining_data.advance(num_bytes_to_skip);
                }
                FreqReadingOption::ReadFreq => {
                    let num_consumed_bytes = self.freq_decoder
                        .uncompress_block_unsorted(self.remaining_data.as_ref());
                    self.remaining_data.advance(num_consumed_bytes);
                }
            }
            // it will be used as the next offset.
            self.doc_offset = self.doc_decoder.output(COMPRESSION_BLOCK_SIZE - 1);
            self.num_bitpacked_blocks -= 1;
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
            num_bitpacked_blocks: 0,
            num_vint_docs: 0,

            doc_decoder: BlockDecoder::new(),
            freq_decoder: BlockDecoder::with_val(1),
            freq_reading_option: FreqReadingOption::NoFreq,

            remaining_data: From::from(ReadOnlySource::empty()),
            doc_offset: 0,
            doc_freq: 0,
        }
    }
}

impl<'b> Streamer<'b> for BlockSegmentPostings {
    type Item = &'b [DocId];

    fn next(&'b mut self) -> Option<&'b [DocId]> {
        if self.advance() {
            Some(self.docs())
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {

    use super::BlockSegmentPostings;
    use super::SegmentPostings;
    use common::HasLen;
    use core::Index;
    use docset::DocSet;
    use fst::Streamer;
    use schema::IndexRecordOption;
    use schema::SchemaBuilder;
    use schema::Term;
    use schema::INT_INDEXED;

    #[test]
    fn test_empty_segment_postings() {
        let mut postings = SegmentPostings::empty();
        assert!(!postings.advance());
        assert!(!postings.advance());
        assert_eq!(postings.len(), 0);
    }

    #[test]
    fn test_empty_block_segment_postings() {
        let mut postings = BlockSegmentPostings::empty();
        assert!(!postings.advance());
        assert_eq!(postings.doc_freq(), 0);
    }

    #[test]
    fn test_block_segment_postings() {
        let mut schema_builder = SchemaBuilder::default();
        let int_field = schema_builder.add_u64_field("id", INT_INDEXED);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut index_writer = index.writer_with_num_threads(1, 40_000_000).unwrap();
        for _ in 0..100_000 {
            let doc = doc!(int_field=>0u64);
            index_writer.add_document(doc);
        }
        index_writer.commit().unwrap();
        index.load_searchers().unwrap();
        let searcher = index.searcher();
        let segment_reader = searcher.segment_reader(0);
        let inverted_index = segment_reader.inverted_index(int_field);
        let term = Term::from_field_u64(int_field, 0u64);
        let term_info = inverted_index.get_term_info(&term).unwrap();
        let mut block_segments =
            inverted_index.read_block_postings_from_terminfo(&term_info, IndexRecordOption::Basic);
        let mut offset: u32 = 0u32;
        // checking that the block before calling advance is empty
        assert!(block_segments.docs().is_empty());
        // checking that the `doc_freq` is correct
        assert_eq!(block_segments.doc_freq(), 100_000);
        while let Some(block) = block_segments.next() {
            for (i, doc) in block.iter().cloned().enumerate() {
                assert_eq!(offset + (i as u32), doc);
            }
            offset += block.len() as u32;
        }
    }

    #[test]
    fn test_reset_block_segment_postings() {
        let mut schema_builder = SchemaBuilder::default();
        let int_field = schema_builder.add_u64_field("id", INT_INDEXED);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut index_writer = index.writer_with_num_threads(1, 40_000_000).unwrap();
        // create two postings list, one containg even number,
        // the other containing odd numbers.
        for i in 0..6 {
            let doc = doc!(int_field=> (i % 2) as u64);
            index_writer.add_document(doc);
        }
        index_writer.commit().unwrap();
        index.load_searchers().unwrap();
        let searcher = index.searcher();
        let segment_reader = searcher.segment_reader(0);

        let mut block_segments;
        {
            let term = Term::from_field_u64(int_field, 0u64);
            let inverted_index = segment_reader.inverted_index(int_field);
            let term_info = inverted_index.get_term_info(&term).unwrap();
            block_segments = inverted_index
                .read_block_postings_from_terminfo(&term_info, IndexRecordOption::Basic);
        }
        assert!(block_segments.advance());
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

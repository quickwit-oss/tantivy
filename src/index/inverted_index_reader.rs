use std::io;

use common::json_path_writer::JSON_END_OF_PATH;
#[cfg(feature = "quickwit")]
use common::BitSet;
use common::{BinarySerializable, ByteCount};
#[cfg(feature = "quickwit")]
use futures_util::{FutureExt, StreamExt, TryStreamExt};
#[cfg(feature = "quickwit")]
use itertools::Itertools;
#[cfg(feature = "quickwit")]
use tantivy_fst::automaton::{AlwaysMatch, Automaton};

use crate::directory::FileSlice;
use crate::positions::PositionReader;
use crate::postings::{BlockSegmentPostings, SegmentPostings, TermInfo};
use crate::schema::{IndexRecordOption, Term, Type};
use crate::termdict::TermDictionary;

#[cfg(feature = "quickwit")]
// This is how many bytes we can hope to receive during a TTFB from S3 (~80MiB/s, 50ms).
const MERGE_HOLES_UNDER_BYTES: usize = (80 * 1024 * 1024 * 50) / 1000;

#[cfg(feature = "quickwit")]
#[derive(Clone, Copy)]
struct AutomatonUnion<'a, A>(&'a [A]);

#[cfg(feature = "quickwit")]
impl<A: Automaton> Automaton for AutomatonUnion<'_, A> {
    type State = Vec<A::State>;

    fn start(&self) -> Self::State {
        self.0.iter().map(Automaton::start).collect()
    }

    fn is_match(&self, state: &Self::State) -> bool {
        self.0
            .iter()
            .zip(state)
            .any(|(automaton, state)| automaton.is_match(state))
    }

    fn can_match(&self, state: &Self::State) -> bool {
        self.0
            .iter()
            .zip(state)
            .any(|(automaton, state)| automaton.can_match(state))
    }

    fn will_always_match(&self, state: &Self::State) -> bool {
        self.0
            .iter()
            .zip(state)
            .any(|(automaton, state)| automaton.will_always_match(state))
    }

    fn accept(&self, state: &Self::State, byte: u8) -> Self::State {
        self.0
            .iter()
            .zip(state)
            .map(|(automaton, state)| automaton.accept(state, byte))
            .collect()
    }
}

#[cfg(feature = "quickwit")]
fn send_coalesced_posting_ranges(
    posting_ranges: impl Iterator<Item = std::ops::Range<usize>>,
    sender: futures_channel::mpsc::UnboundedSender<std::ops::Range<usize>>,
) -> io::Result<()> {
    let merged_posting_ranges = posting_ranges.coalesce(|range1, range2| {
        if range1.end + MERGE_HOLES_UNDER_BYTES >= range2.start {
            Ok(range1.start..range2.end)
        } else {
            Err((range1, range2))
        }
    });
    for posting_range in merged_posting_ranges {
        sender
            .unbounded_send(posting_range)
            .map_err(|_| io::Error::other("failed to send posting range back"))?;
    }
    Ok(())
}

/// The inverted index reader is in charge of accessing
/// the inverted index associated with a specific field.
///
/// # Note
///
/// It is safe to delete the segment associated with
/// an `InvertedIndexReader`. As long as it is open,
/// the [`FileSlice`] it is relying on should
/// stay available.
///
/// `InvertedIndexReader` are created by calling
/// [`SegmentReader::inverted_index()`](crate::SegmentReader::inverted_index).
pub struct InvertedIndexReader {
    termdict: TermDictionary,
    postings_file_slice: FileSlice,
    positions_file_slice: FileSlice,
    record_option: IndexRecordOption,
    total_num_tokens: u64,
}

/// Object that records the amount of space used by a field in an inverted index.
pub(crate) struct InvertedIndexFieldSpace {
    pub field_name: String,
    pub field_type: Type,
    pub postings_size: ByteCount,
    pub positions_size: ByteCount,
    pub num_terms: u64,
}

/// Returns None if the term is not a valid JSON path.
fn extract_field_name_and_field_type_from_json_path(term: &[u8]) -> Option<(String, Type)> {
    let index = term.iter().position(|&byte| byte == JSON_END_OF_PATH)?;
    let field_type_code = term.get(index + 1).copied()?;
    let field_type = Type::from_code(field_type_code)?;
    // Let's flush the current field.
    let field_name = String::from_utf8_lossy(&term[..index]).to_string();
    Some((field_name, field_type))
}

impl InvertedIndexFieldSpace {
    fn record(&mut self, term_info: &TermInfo) {
        self.postings_size += ByteCount::from(term_info.posting_num_bytes() as u64);
        self.positions_size += ByteCount::from(term_info.positions_num_bytes() as u64);
        self.num_terms += 1;
    }
}

impl InvertedIndexReader {
    pub(crate) fn new(
        termdict: TermDictionary,
        postings_file_slice: FileSlice,
        positions_file_slice: FileSlice,
        record_option: IndexRecordOption,
    ) -> io::Result<InvertedIndexReader> {
        let (total_num_tokens_slice, postings_body) = postings_file_slice.split(8);
        let total_num_tokens = u64::deserialize(&mut total_num_tokens_slice.read_bytes()?)?;
        Ok(InvertedIndexReader {
            termdict,
            postings_file_slice: postings_body,
            positions_file_slice,
            record_option,
            total_num_tokens,
        })
    }

    /// Creates an empty `InvertedIndexReader` object, which
    /// contains no terms at all.
    pub fn empty(record_option: IndexRecordOption) -> InvertedIndexReader {
        InvertedIndexReader {
            termdict: TermDictionary::empty(),
            postings_file_slice: FileSlice::empty(),
            positions_file_slice: FileSlice::empty(),
            record_option,
            total_num_tokens: 0u64,
        }
    }

    /// Returns the term info associated with the term.
    pub fn get_term_info(&self, term: &Term) -> io::Result<Option<TermInfo>> {
        self.termdict.get(term.serialized_value_bytes())
    }

    /// Return the term dictionary datastructure.
    pub fn terms(&self) -> &TermDictionary {
        &self.termdict
    }

    /// Return the fields and types encoded in the dictionary in lexicographic order.
    /// Only valid on JSON fields.
    ///
    /// Notice: This requires a full scan and therefore **very expensive**.
    /// TODO: Move to sstable to use the index.
    pub(crate) fn list_encoded_json_fields(&self) -> io::Result<Vec<InvertedIndexFieldSpace>> {
        let mut stream = self.termdict.stream()?;
        let mut fields: Vec<InvertedIndexFieldSpace> = Vec::new();

        let mut current_field_opt: Option<InvertedIndexFieldSpace> = None;
        // Current field bytes, including the JSON_END_OF_PATH.
        let mut current_field_bytes: Vec<u8> = Vec::new();

        while let Some((term, term_info)) = stream.next() {
            if let Some(current_field) = &mut current_field_opt {
                if term.starts_with(&current_field_bytes) {
                    // We are still in the same field.
                    current_field.record(term_info);
                    continue;
                }
            }

            // This is a new field!
            // Let's flush the current field.
            fields.extend(current_field_opt.take());
            current_field_bytes.clear();

            // And create a new one.
            let Some((field_name, field_type)) =
                extract_field_name_and_field_type_from_json_path(term)
            else {
                error!(
                    "invalid term bytes encountered {term:?}. this only happens if the term \
                     dictionary is corrupted. please report"
                );
                continue;
            };
            let mut field_space = InvertedIndexFieldSpace {
                field_name,
                field_type,
                postings_size: ByteCount::default(),
                positions_size: ByteCount::default(),
                num_terms: 0u64,
            };
            field_space.record(term_info);

            // We include the json type and the json end of path to make sure the prefix check
            // is meaningful.
            current_field_bytes.extend_from_slice(&term[..field_space.field_name.len() + 2]);
            current_field_opt = Some(field_space);
        }

        // We need to flush the last field as well.
        fields.extend(current_field_opt.take());

        Ok(fields)
    }

    /// Resets the block segment to another position of the postings
    /// file.
    ///
    /// This is useful for enumerating through a list of terms,
    /// and consuming the associated posting lists while avoiding
    /// reallocating a [`BlockSegmentPostings`].
    ///
    /// # Warning
    ///
    /// This does not reset the positions list.
    pub fn reset_block_postings_from_terminfo(
        &self,
        term_info: &TermInfo,
        block_postings: &mut BlockSegmentPostings,
    ) -> io::Result<()> {
        let postings_slice = self
            .postings_file_slice
            .slice(term_info.postings_range.clone());
        let postings_bytes = postings_slice.read_bytes()?;
        block_postings.reset(term_info.doc_freq, postings_bytes)?;
        Ok(())
    }

    /// Returns a block postings given a `Term`.
    /// This method is for an advanced usage only.
    ///
    /// Most users should prefer using [`Self::read_postings()`] instead.
    pub fn read_block_postings(
        &self,
        term: &Term,
        option: IndexRecordOption,
    ) -> io::Result<Option<BlockSegmentPostings>> {
        self.get_term_info(term)?
            .map(move |term_info| self.read_block_postings_from_terminfo(&term_info, option))
            .transpose()
    }

    /// Returns a block postings given a `term_info`.
    /// This method is for an advanced usage only.
    ///
    /// Most users should prefer using [`Self::read_postings()`] instead.
    pub fn read_block_postings_from_terminfo(
        &self,
        term_info: &TermInfo,
        requested_option: IndexRecordOption,
    ) -> io::Result<BlockSegmentPostings> {
        let postings_data = self
            .postings_file_slice
            .slice(term_info.postings_range.clone());
        BlockSegmentPostings::open(
            term_info.doc_freq,
            postings_data,
            self.record_option,
            requested_option,
        )
    }

    /// Returns a posting object given a `term_info`.
    /// This method is for an advanced usage only.
    ///
    /// Most users should prefer using [`Self::read_postings()`] instead.
    pub fn read_postings_from_terminfo(
        &self,
        term_info: &TermInfo,
        option: IndexRecordOption,
    ) -> io::Result<SegmentPostings> {
        let option = option.downgrade(self.record_option);

        let block_postings = self.read_block_postings_from_terminfo(term_info, option)?;
        let position_reader = {
            if option.has_positions() {
                let positions_data = self
                    .positions_file_slice
                    .read_bytes_slice(term_info.positions_range.clone())?;
                let position_reader = PositionReader::open(positions_data)?;
                Some(position_reader)
            } else {
                None
            }
        };
        Ok(SegmentPostings::from_block_postings(
            block_postings,
            position_reader,
        ))
    }

    /// Returns the total number of tokens recorded for all documents
    /// (including deleted documents).
    pub fn total_num_tokens(&self) -> u64 {
        self.total_num_tokens
    }

    /// Returns the segment postings associated with the term, and with the given option,
    /// or `None` if the term has never been encountered and indexed.
    ///
    /// If the field was not indexed with the indexing options that cover
    /// the requested options, the returned [`SegmentPostings`] the method does not fail
    /// and returns a `SegmentPostings` with as much information as possible.
    ///
    /// For instance, requesting [`IndexRecordOption::WithFreqs`] for a
    /// [`TextOptions`](crate::schema::TextOptions) that does not index position
    /// will return a [`SegmentPostings`] with `DocId`s and frequencies.
    pub fn read_postings(
        &self,
        term: &Term,
        option: IndexRecordOption,
    ) -> io::Result<Option<SegmentPostings>> {
        self.get_term_info(term)?
            .map(move |term_info| self.read_postings_from_terminfo(&term_info, option))
            .transpose()
    }

    /// Returns the number of documents containing the term.
    pub fn doc_freq(&self, term: &Term) -> io::Result<u32> {
        Ok(self
            .get_term_info(term)?
            .map(|term_info| term_info.doc_freq)
            .unwrap_or(0u32))
    }
}

#[cfg(feature = "quickwit")]
impl InvertedIndexReader {
    pub(crate) async fn get_term_info_async(&self, term: &Term) -> io::Result<Option<TermInfo>> {
        self.termdict.get_async(term.serialized_value_bytes()).await
    }

    async fn get_term_range_async<'a, A: Automaton + 'a>(
        &'a self,
        terms: impl std::ops::RangeBounds<Term>,
        automaton: A,
        limit: Option<u64>,
        merge_holes_under_bytes: usize,
    ) -> io::Result<impl Iterator<Item = TermInfo> + 'a>
    where
        A::State: Clone,
    {
        use std::ops::Bound;
        let range_builder = self.termdict.search(automaton);
        let range_builder = match terms.start_bound() {
            Bound::Included(bound) => range_builder.ge(bound.serialized_value_bytes()),
            Bound::Excluded(bound) => range_builder.gt(bound.serialized_value_bytes()),
            Bound::Unbounded => range_builder,
        };
        let range_builder = match terms.end_bound() {
            Bound::Included(bound) => range_builder.le(bound.serialized_value_bytes()),
            Bound::Excluded(bound) => range_builder.lt(bound.serialized_value_bytes()),
            Bound::Unbounded => range_builder,
        };
        let range_builder = if let Some(limit) = limit {
            range_builder.limit(limit)
        } else {
            range_builder
        };

        let mut stream = range_builder
            .into_stream_async_merging_holes(merge_holes_under_bytes)
            .await?;

        let iter = std::iter::from_fn(move || stream.next().map(|(_k, v)| v.clone()));

        // limit on stream is only an optimization to load less data, the stream may still return
        // more than limit elements.
        let limit = limit.map(|limit| limit as usize).unwrap_or(usize::MAX);
        let iter = iter.take(limit);

        Ok(iter)
    }

    /// Warmup a block postings given a `Term`.
    /// This method is for an advanced usage only.
    ///
    /// returns a boolean, whether the term was found in the dictionary
    pub async fn warm_postings(&self, term: &Term, with_positions: bool) -> io::Result<bool> {
        let term_info_opt: Option<TermInfo> = self.get_term_info_async(term).await?;
        if let Some(term_info) = term_info_opt {
            let postings = self
                .postings_file_slice
                .read_bytes_slice_async(term_info.postings_range.clone());
            if with_positions {
                let positions = self
                    .positions_file_slice
                    .read_bytes_slice_async(term_info.positions_range.clone());
                futures_util::future::try_join(postings, positions).await?;
            } else {
                postings.await?;
            }
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Warmup a block postings given a range of `Term`s.
    /// This method is for an advanced usage only.
    ///
    /// returns a boolean, whether a term matching the range was found in the dictionary
    pub async fn warm_postings_range(
        &self,
        terms: impl std::ops::RangeBounds<Term>,
        limit: Option<u64>,
        with_positions: bool,
    ) -> io::Result<bool> {
        let mut term_info = self
            .get_term_range_async(terms, AlwaysMatch, limit, 0)
            .await?;

        let Some(first_terminfo) = term_info.next() else {
            // no key matches, nothing more to load
            return Ok(false);
        };

        let last_terminfo = term_info.last().unwrap_or_else(|| first_terminfo.clone());

        let postings_range = first_terminfo.postings_range.start..last_terminfo.postings_range.end;
        let positions_range =
            first_terminfo.positions_range.start..last_terminfo.positions_range.end;

        let postings = self
            .postings_file_slice
            .read_bytes_slice_async(postings_range);
        if with_positions {
            let positions = self
                .positions_file_slice
                .read_bytes_slice_async(positions_range);
            futures_util::future::try_join(postings, positions).await?;
        } else {
            postings.await?;
        }
        Ok(true)
    }

    async fn download_posting_ranges(
        &self,
        posting_ranges: futures_channel::mpsc::UnboundedReceiver<std::ops::Range<usize>>,
    ) -> io::Result<bool> {
        let slices_downloaded = posting_ranges
            .map(|posting_range| {
                self.postings_file_slice
                    .read_bytes_slice_async(posting_range)
                    .map(|result| result.map(|_slice| ()))
            })
            .buffer_unordered(5)
            .try_collect::<Vec<()>>()
            .await?;
        Ok(!slices_downloaded.is_empty())
    }

    /// Warmup a block postings given a range of `Term`s.
    /// This method is for an advanced usage only.
    ///
    /// returns a boolean, whether a term matching the range was found in the dictionary
    pub async fn warm_postings_automaton<
        A: Automaton + Clone + Send + 'static,
        E: FnOnce(Box<dyn FnOnce() -> io::Result<()> + Send>) -> F,
        F: std::future::Future<Output = io::Result<()>>,
    >(
        &self,
        automaton: A,
        // with_positions: bool, at the moment we have no use for it, and supporting it would add
        // complexity to the coalesce
        executor: E,
    ) -> io::Result<bool>
    where
        A::State: Clone,
    {
        // We build a first iterator to download everything. Simply calling the function already
        // download everything we need from the sstable, but doesn't start iterating over it.
        let _term_info_iter = self
            .get_term_range_async(.., automaton.clone(), None, MERGE_HOLES_UNDER_BYTES)
            .await?;

        let (sender, posting_ranges_to_load_stream) = futures_channel::mpsc::unbounded();
        let termdict = self.termdict.clone();
        let cpu_bound_task = move || {
            // then we build a 2nd iterator, this one with no holes, so we don't go through blocks
            // we can't match.
            // This makes the assumption there is a caching layer below us, which gives sync read
            // for free after the initial async access. This might not always be true, but is in
            // Quickwit.
            // We build things from this closure otherwise we get into lifetime issues that can only
            // be solved with self referential strucs. Returning an io::Result from here is a bit
            // more leaky abstraction-wise, but a lot better than the alternative
            let mut stream = termdict.search(automaton).into_stream()?;

            let posting_ranges =
                std::iter::from_fn(move || stream.next().map(|(_k, v)| v.postings_range.clone()));
            send_coalesced_posting_ranges(posting_ranges, sender)
        };
        let task_handle = executor(Box::new(cpu_bound_task));

        let (_, postings_found) = futures_util::future::try_join(
            task_handle,
            self.download_posting_ranges(posting_ranges_to_load_stream),
        )
        .await?;
        Ok(postings_found)
    }

    /// Warms and decodes the postings matching several automatons in one term dictionary scan.
    ///
    /// The returned bitsets are in the same order as `automatons`. A posting list matching several
    /// automatons is decoded only once. Matching automatons are identified from the dictionary
    /// traversal states, without evaluating each matching term again.
    pub async fn warm_postings_automatons<
        A: Automaton + Send + 'static,
        E: FnOnce(Box<dyn FnOnce() -> io::Result<()> + Send>) -> F,
        F: std::future::Future<Output = io::Result<()>>,
    >(
        &self,
        automatons: Vec<A>,
        max_doc: crate::DocId,
        executor: E,
    ) -> io::Result<Vec<BitSet>>
    where
        A::State: Clone,
    {
        if automatons.is_empty() {
            return Ok(Vec::new());
        }

        // Load only term dictionary blocks that can match at least one automaton before traversing
        // them synchronously on the executor.
        let automaton_union = AutomatonUnion(&automatons);
        let term_info_stream = self
            .termdict
            .search(automaton_union)
            .into_stream_async_merging_holes(MERGE_HOLES_UNDER_BYTES)
            .await?;
        drop(term_info_stream);

        let (posting_range_sender, posting_range_receiver) = futures_channel::mpsc::unbounded();
        let (downloads_done_sender, downloads_done_receiver) = std::sync::mpsc::channel();
        let (bitsets_sender, bitsets_receiver) = std::sync::mpsc::sync_channel(1);
        let termdict = self.termdict.clone();
        let postings_file_slice = self.postings_file_slice.clone();
        let record_option = self.record_option;
        let cpu_bound_task = move || {
            let automaton_union = AutomatonUnion(&automatons);
            let mut stream = termdict.search(automaton_union).into_stream()?;
            let mut matching_terms: Vec<(TermInfo, Vec<usize>)> = Vec::new();
            let posting_ranges = std::iter::from_fn(|| {
                if !stream.advance() {
                    return None;
                }
                let matching_automaton_ids: Vec<usize> = automatons
                    .iter()
                    .zip(stream.automaton_state())
                    .enumerate()
                    .filter_map(|(automaton_id, (automaton, state))| {
                        automaton.is_match(state).then_some(automaton_id)
                    })
                    .collect();
                let term_info = stream.value();
                matching_terms.push((term_info.clone(), matching_automaton_ids));
                Some(term_info.postings_range.clone())
            });
            send_coalesced_posting_ranges(posting_ranges, posting_range_sender)?;

            downloads_done_receiver
                .recv()
                .map_err(|_| io::Error::other("posting downloader stopped unexpectedly"))??;

            let mut bitsets: Vec<BitSet> = (0..automatons.len())
                .map(|_| BitSet::with_max_value(max_doc))
                .collect();
            for (term_info, matching_automaton_ids) in matching_terms {
                let postings_data = postings_file_slice.slice(term_info.postings_range.clone());
                let mut block_postings = BlockSegmentPostings::open(
                    term_info.doc_freq,
                    postings_data,
                    record_option,
                    IndexRecordOption::Basic,
                )?;
                loop {
                    let docs = block_postings.docs();
                    if docs.is_empty() {
                        break;
                    }
                    for &doc in docs {
                        for &automaton_id in &matching_automaton_ids {
                            bitsets[automaton_id].insert(doc);
                        }
                    }
                    block_postings.advance();
                }
            }
            bitsets_sender
                .send(bitsets)
                .map_err(|_| io::Error::other("failed to send automaton bitsets"))?;
            Ok(())
        };
        let task_handle = executor(Box::new(cpu_bound_task));

        let posting_downloader = async move {
            let result = self
                .download_posting_ranges(posting_range_receiver)
                .await
                .map(|_| ());
            let task_result = result
                .as_ref()
                .map(|_| ())
                .map_err(|error| io::Error::new(error.kind(), error.to_string()));
            let _ = downloads_done_sender.send(task_result);
            result
        };

        futures_util::future::try_join(task_handle, posting_downloader).await?;
        bitsets_receiver
            .recv()
            .map_err(|_| io::Error::other("automaton bitset task stopped unexpectedly"))
    }

    /// Warmup the block postings for all terms.
    /// This method is for an advanced usage only.
    ///
    /// If you know which terms to pre-load, prefer using [`Self::warm_postings`] or
    /// [`Self::warm_postings`] instead.
    pub async fn warm_postings_full(&self, with_positions: bool) -> io::Result<()> {
        self.postings_file_slice.read_bytes_async().await?;
        if with_positions {
            self.positions_file_slice.read_bytes_async().await?;
        }
        Ok(())
    }

    /// Returns the number of documents containing the term asynchronously.
    pub async fn doc_freq_async(&self, term: &Term) -> io::Result<u32> {
        Ok(self
            .get_term_info_async(term)
            .await?
            .map(|term_info| term_info.doc_freq)
            .unwrap_or(0u32))
    }
}

#[cfg(all(test, feature = "quickwit"))]
mod tests {
    use std::io;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use futures::channel::oneshot;
    use tantivy_fst::{Automaton, Regex};

    use super::{AutomatonUnion, MERGE_HOLES_UNDER_BYTES};
    use crate::schema::{Schema, STRING};
    use crate::{Index, IndexWriter};

    // Count byte transitions to detect a second evaluation of matching terms.
    struct CountingAutomaton {
        regex: Regex,
        transitions: Arc<AtomicUsize>,
    }

    impl Automaton for CountingAutomaton {
        type State = <Regex as Automaton>::State;

        fn start(&self) -> Self::State {
            self.regex.start()
        }

        fn is_match(&self, state: &Self::State) -> bool {
            self.regex.is_match(state)
        }

        fn can_match(&self, state: &Self::State) -> bool {
            self.regex.can_match(state)
        }

        fn will_always_match(&self, state: &Self::State) -> bool {
            self.regex.will_always_match(state)
        }

        fn accept(&self, state: &Self::State, byte: u8) -> Self::State {
            self.transitions.fetch_add(1, Ordering::Relaxed);
            self.regex.accept(state, byte)
        }
    }

    fn execute_on_thread(
        task: Box<dyn FnOnce() -> io::Result<()> + Send>,
    ) -> impl std::future::Future<Output = io::Result<()>> {
        let (sender, receiver) = oneshot::channel();
        std::thread::spawn(move || {
            let _ = sender.send(task());
        });
        async move {
            receiver
                .await
                .map_err(|_| io::Error::other("executor task panicked"))?
        }
    }

    #[test]
    fn test_warm_postings_automatons() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let field = schema_builder.add_text_field("field", STRING);
        let index = Index::create_in_ram(schema_builder.build());
        let mut writer: IndexWriter = index.writer_for_tests()?;
        writer.add_document(doc!(field => "apple", field => "banana"))?;
        writer.add_document(doc!(field => "apricot"))?;
        writer.add_document(doc!(field => "banana", field => "berry"))?;
        writer.add_document(doc!(field => "carrot"))?;
        writer.commit()?;

        let reader = index.reader()?;
        let searcher = reader.searcher();
        let segment_reader = searcher.segment_reader(0);
        let inverted_index = segment_reader.inverted_index(field)?;
        let transitions = Arc::new(AtomicUsize::new(0));
        let automatons: Vec<_> = ["b.*", "z.*", "a.*", ".*a.*", "b.*"]
            .into_iter()
            .map(|pattern| CountingAutomaton {
                regex: Regex::new(pattern).unwrap(),
                transitions: transitions.clone(),
            })
            .collect();

        // Count only block selection and dictionary traversal. Collecting hits in the warmup
        // must not add byte transitions beyond these same operations.
        let search = || inverted_index.termdict.search(AutomatonUnion(&automatons));
        drop(futures::executor::block_on(
            search().into_stream_async_merging_holes(MERGE_HOLES_UNDER_BYTES),
        )?);
        let mut stream = search().into_stream()?;
        while stream.advance() {}
        drop(stream);
        let traversal_transitions = transitions.swap(0, Ordering::Relaxed);
        assert!(traversal_transitions > 0);

        let bitsets = futures::executor::block_on(inverted_index.warm_postings_automatons(
            automatons,
            segment_reader.max_doc(),
            execute_on_thread,
        ))?;

        let docs: Vec<Vec<u32>> = bitsets
            .iter()
            .map(|bitset| {
                (0..bitset.max_value())
                    .filter(|&doc| bitset.contains(doc))
                    .collect()
            })
            .collect();
        assert_eq!(
            docs,
            vec![vec![0, 2], vec![], vec![0, 1], vec![0, 1, 2, 3], vec![0, 2]]
        );
        assert_eq!(transitions.load(Ordering::Relaxed), traversal_transitions);

        for patterns in [vec![], vec![Regex::new("z.*").unwrap()]] {
            let num_patterns = patterns.len();
            let bitsets = futures::executor::block_on(inverted_index.warm_postings_automatons(
                patterns,
                segment_reader.max_doc(),
                execute_on_thread,
            ))?;
            assert_eq!(bitsets.len(), num_patterns);
            assert!(bitsets.iter().all(|bitset| bitset.len() == 0));
        }
        Ok(())
    }
}

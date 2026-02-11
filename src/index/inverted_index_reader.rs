#[cfg(feature = "quickwit")]
use std::future::Future;
use std::io;
#[cfg(feature = "quickwit")]
use std::pin::Pin;

use common::json_path_writer::JSON_END_OF_PATH;
use common::{BinarySerializable, BitSet, ByteCount, OwnedBytes};
#[cfg(feature = "quickwit")]
use futures_util::{FutureExt, StreamExt, TryStreamExt};
#[cfg(feature = "quickwit")]
use itertools::Itertools;
#[cfg(feature = "quickwit")]
use tantivy_fst::automaton::{AlwaysMatch, Automaton};

use crate::codec::postings::RawPostingsData;
use crate::codec::standard::postings::{
    fill_bitset_from_raw_data, load_postings_from_raw_data, SegmentPostings,
};
use crate::directory::FileSlice;
use crate::docset::DocSet;
use crate::fieldnorm::FieldNormReader;
use crate::postings::{Postings, TermInfo};
use crate::query::term_query::TermScorer;
use crate::query::{box_scorer, Bm25Weight, PhraseScorer, Scorer};
use crate::schema::{IndexRecordOption, Term, Type};
use crate::termdict::TermDictionary;

/// Trait defining the contract for inverted index readers.
pub trait InvertedIndexReader: Send + Sync {
    /// Returns the term info associated with the term.
    fn get_term_info(&self, term: &Term) -> io::Result<Option<TermInfo>> {
        self.terms().get(term.serialized_value_bytes())
    }

    /// Return the term dictionary datastructure.
    fn terms(&self) -> &TermDictionary;

    /// Return the fields and types encoded in the dictionary in lexicographic order.
    /// Only valid on JSON fields.
    ///
    /// Notice: This requires a full scan and therefore **very expensive**.
    fn list_encoded_json_fields(&self) -> io::Result<Vec<InvertedIndexFieldSpace>>;

    /// Build a new term scorer.
    fn new_term_scorer(
        &self,
        term_info: &TermInfo,
        option: IndexRecordOption,
        fieldnorm_reader: FieldNormReader,
        similarity_weight: Bm25Weight,
    ) -> io::Result<Box<dyn Scorer>>;

    /// Returns a posting object given a `term_info`.
    /// This method is for an advanced usage only.
    ///
    /// Most users should prefer using [`Self::read_postings()`] instead.
    fn read_postings_from_terminfo(
        &self,
        term_info: &TermInfo,
        option: IndexRecordOption,
    ) -> io::Result<Box<dyn Postings>>;

    /// Returns the raw postings bytes and metadata for a term.
    fn read_raw_postings_data(
        &self,
        term_info: &TermInfo,
        option: IndexRecordOption,
    ) -> io::Result<RawPostingsData>;

    /// Fills a bitset with documents containing the term.
    ///
    /// Implementers can override this to avoid boxing postings.
    fn fill_bitset_for_term(
        &self,
        term_info: &TermInfo,
        option: IndexRecordOption,
        doc_bitset: &mut BitSet,
    ) -> io::Result<()> {
        let mut postings = self.read_postings_from_terminfo(term_info, option)?;
        postings.fill_bitset(doc_bitset);
        Ok(())
    }

    /// Builds a phrase scorer for the given term infos.
    fn new_phrase_scorer(
        &self,
        term_infos: &[(usize, TermInfo)],
        similarity_weight: Option<Bm25Weight>,
        fieldnorm_reader: FieldNormReader,
        slop: u32,
    ) -> io::Result<Box<dyn Scorer>>;

    /// Returns the total number of tokens recorded for all documents
    /// (including deleted documents).
    fn total_num_tokens(&self) -> u64;

    /// Returns the segment postings associated with the term, and with the given option,
    /// or `None` if the term has never been encountered and indexed.
    fn read_postings(
        &self,
        term: &Term,
        option: IndexRecordOption,
    ) -> io::Result<Option<Box<dyn Postings>>> {
        self.get_term_info(term)?
            .map(move |term_info| self.read_postings_from_terminfo(&term_info, option))
            .transpose()
    }

    /// Returns the number of documents containing the term.
    fn doc_freq(&self, term: &Term) -> io::Result<u32>;

    /// Returns the number of documents containing the term asynchronously.
    #[cfg(feature = "quickwit")]
    fn doc_freq_async<'a>(
        &'a self,
        term: &'a Term,
    ) -> Pin<Box<dyn Future<Output = io::Result<u32>> + Send + 'a>>;
}

/// Tantivy's default inverted index reader implementation.
///
/// The inverted index reader is in charge of accessing
/// the inverted index associated with a specific field.
///
/// # Note
///
/// It is safe to delete the segment associated with
/// an `InvertedIndexReader` implementation. As long as it is open,
/// the [`FileSlice`] it is relying on should
/// stay available.
///
/// `TantivyInvertedIndexReader` instances are created by calling
/// [`SegmentReader::inverted_index()`](crate::SegmentReader::inverted_index).
pub struct TantivyInvertedIndexReader {
    termdict: TermDictionary,
    postings_file_slice: FileSlice,
    positions_file_slice: FileSlice,
    record_option: IndexRecordOption,
    total_num_tokens: u64,
}

/// Object that records the amount of space used by a field in an inverted index.
pub struct InvertedIndexFieldSpace {
    /// Field name as encoded in the term dictionary.
    pub field_name: String,
    /// Value type for the encoded field.
    pub field_type: Type,
    /// Total bytes used by postings for this field.
    pub postings_size: ByteCount,
    /// Total bytes used by positions for this field.
    pub positions_size: ByteCount,
    /// Number of terms in the field.
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

impl TantivyInvertedIndexReader {
    pub(crate) fn read_raw_postings_data_inner(
        &self,
        term_info: &TermInfo,
        option: IndexRecordOption,
    ) -> io::Result<RawPostingsData> {
        let effective_option = option.downgrade(self.record_option);
        let postings_data = self
            .postings_file_slice
            .slice(term_info.postings_range.clone())
            .read_bytes()?;
        let positions_data: Option<OwnedBytes> = if effective_option.has_positions() {
            let positions_data = self
                .positions_file_slice
                .slice(term_info.positions_range.clone())
                .read_bytes()?;
            Some(positions_data)
        } else {
            None
        };
        Ok(RawPostingsData {
            postings_data,
            positions_data,
            record_option: self.record_option,
            effective_option,
        })
    }

    pub fn new(
        termdict: TermDictionary,
        postings_file_slice: FileSlice,
        positions_file_slice: FileSlice,
        record_option: IndexRecordOption,
    ) -> io::Result<TantivyInvertedIndexReader> {
        let (total_num_tokens_slice, postings_body) = postings_file_slice.split(8);
        let total_num_tokens = u64::deserialize(&mut total_num_tokens_slice.read_bytes()?)?;
        Ok(TantivyInvertedIndexReader {
            termdict,
            postings_file_slice: postings_body,
            positions_file_slice,
            record_option,
            total_num_tokens,
        })
    }

    /// Creates an empty `TantivyInvertedIndexReader` object, which
    /// contains no terms at all.
    pub fn empty(record_option: IndexRecordOption) -> TantivyInvertedIndexReader {
        TantivyInvertedIndexReader {
            termdict: TermDictionary::empty(),
            postings_file_slice: FileSlice::empty(),
            positions_file_slice: FileSlice::empty(),
            record_option,
            total_num_tokens: 0u64,
        }
    }

    fn load_segment_postings(
        &self,
        term_info: &TermInfo,
        option: IndexRecordOption,
    ) -> io::Result<SegmentPostings> {
        let postings_data = self.read_raw_postings_data_inner(term_info, option)?;
        load_postings_from_raw_data(term_info.doc_freq, postings_data)
    }
}

impl InvertedIndexReader for TantivyInvertedIndexReader {
    fn terms(&self) -> &TermDictionary {
        &self.termdict
    }

    fn list_encoded_json_fields(&self) -> io::Result<Vec<InvertedIndexFieldSpace>> {
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

    fn new_term_scorer(
        &self,
        term_info: &TermInfo,
        option: IndexRecordOption,
        fieldnorm_reader: FieldNormReader,
        similarity_weight: Bm25Weight,
    ) -> io::Result<Box<dyn Scorer>> {
        let postings = self.load_segment_postings(term_info, option)?;
        let term_scorer = TermScorer::new(postings, fieldnorm_reader, similarity_weight);
        Ok(box_scorer(term_scorer))
    }

    fn read_postings_from_terminfo(
        &self,
        term_info: &TermInfo,
        option: IndexRecordOption,
    ) -> io::Result<Box<dyn Postings>> {
        let postings = self.load_segment_postings(term_info, option)?;
        Ok(Box::new(postings))
    }

    fn read_raw_postings_data(
        &self,
        term_info: &TermInfo,
        option: IndexRecordOption,
    ) -> io::Result<RawPostingsData> {
        self.read_raw_postings_data_inner(term_info, option)
    }

    fn fill_bitset_for_term(
        &self,
        term_info: &TermInfo,
        option: IndexRecordOption,
        doc_bitset: &mut BitSet,
    ) -> io::Result<()> {
        let postings_data = self.read_raw_postings_data_inner(term_info, option)?;
        fill_bitset_from_raw_data(term_info.doc_freq, postings_data, doc_bitset)
    }

    fn new_phrase_scorer(
        &self,
        term_infos: &[(usize, TermInfo)],
        similarity_weight: Option<Bm25Weight>,
        fieldnorm_reader: FieldNormReader,
        slop: u32,
    ) -> io::Result<Box<dyn Scorer>> {
        let mut offset_and_term_postings: Vec<(usize, SegmentPostings)> =
            Vec::with_capacity(term_infos.len());
        for (offset, term_info) in term_infos {
            let postings =
                self.load_segment_postings(term_info, IndexRecordOption::WithFreqsAndPositions)?;
            offset_and_term_postings.push((*offset, postings));
        }
        let scorer = PhraseScorer::new(
            offset_and_term_postings,
            similarity_weight,
            fieldnorm_reader,
            slop,
        );
        Ok(box_scorer(scorer))
    }

    fn total_num_tokens(&self) -> u64 {
        self.total_num_tokens
    }

    fn read_postings(
        &self,
        term: &Term,
        option: IndexRecordOption,
    ) -> io::Result<Option<Box<dyn Postings>>> {
        self.get_term_info(term)?
            .map(move |term_info| self.read_postings_from_terminfo(&term_info, option))
            .transpose()
    }

    fn doc_freq(&self, term: &Term) -> io::Result<u32> {
        Ok(self
            .get_term_info(term)?
            .map(|term_info| term_info.doc_freq)
            .unwrap_or(0u32))
    }

    #[cfg(feature = "quickwit")]
    fn doc_freq_async<'a>(
        &'a self,
        term: &'a Term,
    ) -> Pin<Box<dyn Future<Output = io::Result<u32>> + Send + 'a>> {
        Box::pin(async move {
            Ok(self
                .get_term_info_async(term)
                .await?
                .map(|term_info| term_info.doc_freq)
                .unwrap_or(0u32))
        })
    }
}

#[cfg(feature = "quickwit")]
impl TantivyInvertedIndexReader {
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
        // merge holes under 4MiB, that's how many bytes we can hope to receive during a TTFB from
        // S3 (~80MiB/s, and 50ms latency)
        const MERGE_HOLES_UNDER_BYTES: usize = (80 * 1024 * 1024 * 50) / 1000;
        // we build a first iterator to download everything. Simply calling the function already
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

            // we could do without an iterator, but this allows us access to coalesce which simplify
            // things
            let posting_ranges_iter =
                std::iter::from_fn(move || stream.next().map(|(_k, v)| v.postings_range.clone()));

            let merged_posting_ranges_iter = posting_ranges_iter.coalesce(|range1, range2| {
                if range1.end + MERGE_HOLES_UNDER_BYTES >= range2.start {
                    Ok(range1.start..range2.end)
                } else {
                    Err((range1, range2))
                }
            });

            for posting_range in merged_posting_ranges_iter {
                if let Err(_) = sender.unbounded_send(posting_range) {
                    // this should happen only when search is cancelled
                    return Err(io::Error::other("failed to send posting range back"));
                }
            }
            Ok(())
        };
        let task_handle = executor(Box::new(cpu_bound_task));

        let posting_downloader = posting_ranges_to_load_stream
            .map(|posting_slice| {
                self.postings_file_slice
                    .read_bytes_slice_async(posting_slice)
                    .map(|result| result.map(|_slice| ()))
            })
            .buffer_unordered(5)
            .try_collect::<Vec<()>>();

        let (_, slices_downloaded) =
            futures_util::future::try_join(task_handle, posting_downloader).await?;

        Ok(!slices_downloaded.is_empty())
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
}

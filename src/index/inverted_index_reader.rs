use std::io;

use common::json_path_writer::JSON_END_OF_PATH;
use common::BinarySerializable;
use fnv::FnvHashSet;
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
    pub fn list_encoded_fields(&self) -> io::Result<Vec<(String, Type)>> {
        let mut stream = self.termdict.stream()?;
        let mut fields = Vec::new();
        let mut fields_set = FnvHashSet::default();
        while let Some((term, _term_info)) = stream.next() {
            if let Some(index) = term.iter().position(|&byte| byte == JSON_END_OF_PATH) {
                if !fields_set.contains(&term[..index + 2]) {
                    fields_set.insert(term[..index + 2].to_vec());
                    let typ = Type::from_code(term[index + 1]).unwrap();
                    fields.push((String::from_utf8_lossy(&term[..index]).to_string(), typ));
                }
            }
        }

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
        merge_holes_under: usize,
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
            .into_stream_async_merging_holes(merge_holes_under)
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
    pub async fn warm_postings_automaton<A: Automaton + Clone>(
        &self,
        automaton: A,
        // with_positions: bool, at the moment we have no use for it, and supporting it would add
        // complexity to the coalesce
    ) -> io::Result<bool>
    where
        A::State: Clone,
    {
        // merge holes under 4MiB, that's how many bytes we can hope to receive during a TTFB from
        // S3 (~80MiB/s, and 50ms latency)
        let merge_holes_under = (80 * 1024 * 1024 * 50) / 1000;
        // we build a first iterator to download everything. Simply calling the function already
        // loads everything, but doesn't start iterating over the sstable.
        let mut _term_info = self
            .get_term_range_async(.., automaton.clone(), None, merge_holes_under)
            .await?;
        // we build a 2nd iterator, this one with no holes, so we don't go through blocks we can't
        // match, and just download them to reduce our query count. This makes the assumption
        // there is a caching layer below, which might not always be true, but is in Quickwit.
        let term_info = self.get_term_range_async(.., automaton, None, 0).await?;

        let range_to_load = term_info
            .map(|term_info| term_info.postings_range)
            .coalesce(|range1, range2| {
                if range1.end + merge_holes_under >= range2.start {
                    Ok(range1.start..range2.end)
                } else {
                    Err((range1, range2))
                }
            });

        let slices_downloaded = futures_util::stream::iter(range_to_load)
            .map(|posting_slice| {
                self.postings_file_slice
                    .read_bytes_slice_async(posting_slice)
                    .map(|result| result.map(|_slice| ()))
            })
            .buffer_unordered(5)
            .try_collect::<Vec<()>>()
            .await?;

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

    /// Returns the number of documents containing the term asynchronously.
    pub async fn doc_freq_async(&self, term: &Term) -> io::Result<u32> {
        Ok(self
            .get_term_info_async(term)
            .await?
            .map(|term_info| term_info.doc_freq)
            .unwrap_or(0u32))
    }
}

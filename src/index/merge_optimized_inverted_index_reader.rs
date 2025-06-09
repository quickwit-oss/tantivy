use std::io;

use common::OwnedBytes;

use crate::directory::FileSlice;
use crate::positions::borrowed_position_reader::BorrowedPositionReader;
use crate::postings::borrowed_block_segment_postings::BorrowedBlockSegmentPostings;
use crate::postings::borrowed_segment_postings::BorrowedSegmentPostings;
use crate::postings::TermInfo;
use crate::schema::IndexRecordOption;
use crate::termdict::TermDictionary;

/// The inverted index reader is in charge of accessing
/// the inverted index associated with a specific field.
///
/// This is optimized for merging in that it full reads
/// the postings and positions files into memory when opened.
/// This eliminates all disk I/O to these files during merging.
///
/// NB:  This is a copy/paste from [`InvertedIndexReader`] and trimmed
/// down to only include the methods required by the merge process.
pub(crate) struct MergeOptimizedInvertedIndexReader {
    termdict: TermDictionary,
    postings_bytes: OwnedBytes,
    positions_bytes: OwnedBytes,
    record_option: IndexRecordOption,
}

impl MergeOptimizedInvertedIndexReader {
    pub(crate) fn new(
        termdict: TermDictionary,
        postings_file_slice: FileSlice,
        positions_file_slice: FileSlice,
        record_option: IndexRecordOption,
    ) -> io::Result<MergeOptimizedInvertedIndexReader> {
        let (_, postings_body) = postings_file_slice.split(8);
        Ok(MergeOptimizedInvertedIndexReader {
            termdict,
            postings_bytes: postings_body.read_bytes()?,
            positions_bytes: positions_file_slice.read_bytes()?,
            record_option,
        })
    }

    /// Creates an empty `InvertedIndexReader` object, which
    /// contains no terms at all.
    pub fn empty(record_option: IndexRecordOption) -> MergeOptimizedInvertedIndexReader {
        MergeOptimizedInvertedIndexReader {
            termdict: TermDictionary::empty(),
            postings_bytes: FileSlice::empty().read_bytes().unwrap(),
            positions_bytes: FileSlice::empty().read_bytes().unwrap(),
            record_option,
        }
    }

    /// Return the term dictionary datastructure.
    pub fn terms(&self) -> &TermDictionary {
        &self.termdict
    }

    /// Returns a block postings given a `term_info`.
    /// This method is for an advanced usage only.
    ///
    /// Most users should prefer using [`Self::read_postings()`] instead.
    pub fn read_block_postings_from_terminfo(
        &self,
        term_info: &TermInfo,
        requested_option: IndexRecordOption,
    ) -> io::Result<BorrowedBlockSegmentPostings> {
        let postings_data = &self.postings_bytes[term_info.postings_range.clone()];
        BorrowedBlockSegmentPostings::open(
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
    ) -> io::Result<BorrowedSegmentPostings> {
        let option = option.downgrade(self.record_option);

        let block_postings = self.read_block_postings_from_terminfo(term_info, option)?;
        let position_reader = {
            if option.has_positions() {
                let positions_data = &self.positions_bytes[term_info.positions_range.clone()];
                let position_reader = BorrowedPositionReader::open(positions_data)?;
                Some(position_reader)
            } else {
                None
            }
        };
        Ok(BorrowedSegmentPostings::from_block_postings(
            block_postings,
            position_reader,
        ))
    }
}

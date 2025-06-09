use std::io;

use crate::directory::{BufferedFileSlice, FileSlice};
use crate::positions::PositionReader;
use crate::postings::{BlockSegmentPostings, SegmentPostings, TermInfo};
use crate::schema::IndexRecordOption;
use crate::termdict::TermDictionary;

/// The inverted index reader is in charge of accessing
/// the inverted index associated with a specific field.
///
/// This is optimized for merging in that it uses a buffered reader
/// for the postings and positions files.
/// This eliminates most disk I/O to these files during merging, without
/// reading the entire file into memory at once.
///
/// NB:  This is a copy/paste from [`InvertedIndexReader`] and trimmed
/// down to only include the methods required by the merge process.
pub(crate) struct MergeOptimizedInvertedIndexReader {
    termdict: TermDictionary,
    postings_reader: BufferedFileSlice,
    positions_reader: BufferedFileSlice,
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
            postings_reader: BufferedFileSlice::new_with_default_buffer_size(postings_body),
            positions_reader: BufferedFileSlice::new_with_default_buffer_size(positions_file_slice),
            record_option,
        })
    }

    /// Creates an empty `InvertedIndexReader` object, which
    /// contains no terms at all.
    pub fn empty(record_option: IndexRecordOption) -> MergeOptimizedInvertedIndexReader {
        MergeOptimizedInvertedIndexReader {
            termdict: TermDictionary::empty(),
            postings_reader: BufferedFileSlice::empty(),
            positions_reader: BufferedFileSlice::empty(),
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
    ) -> io::Result<BlockSegmentPostings> {
        let postings_data = self.postings_reader.get_bytes(
            term_info.postings_range.start as u64..term_info.postings_range.end as u64,
        )?;
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
                let positions_data = self.positions_reader.get_bytes(
                    term_info.positions_range.start as u64..term_info.positions_range.end as u64,
                )?;
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
}

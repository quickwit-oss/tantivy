use core::fmt;
use std::ops::RangeInclusive;
use std::sync::Arc;

use fastfield_codecs::Column;

use crate::fastfield::MakeZero;
use crate::{DocId, DocSet, TERMINATED};

/// Helper to have a cursor over a vec of docids
struct VecCursor {
    docs: Vec<u32>,
    current_pos: usize,
}
impl VecCursor {
    fn new() -> Self {
        Self {
            docs: Vec::with_capacity(32),
            current_pos: 0,
        }
    }
    fn next(&mut self) -> Option<u32> {
        self.current_pos += 1;
        self.current()
    }
    #[inline]
    fn current(&self) -> Option<u32> {
        self.docs.get(self.current_pos).copied()
    }
    fn get_cleared_data(&mut self) -> &mut Vec<u32> {
        self.docs.clear();
        self.current_pos = 0;
        &mut self.docs
    }
    fn last_value(&self) -> Option<u32> {
        self.docs.iter().last().cloned()
    }
    fn is_empty(&self) -> bool {
        self.current().is_none()
    }
}

pub(crate) enum FastFieldCardinality<T: MakeZero> {
    SingleValue(Arc<dyn Column<T>>),
    MultiValue(MultiValuedFastFieldReader<T>),
}

impl<T: MakeZero + PartialOrd + Copy + fmt::Debug> FastFieldCardinality<T> {
    fn num_docs(&self) -> u32 {
        match self {
            FastFieldCardinality::SingleValue(single_value) => single_value.num_vals(),
            FastFieldCardinality::MultiValue(multi_value) => {
                multi_value.get_index_reader().num_docs()
            }
        }
    }
}

pub(crate) struct RangeDocSet<T: MakeZero> {
    /// The range filter on the values.
    value_range: RangeInclusive<T>,
    fast_field: FastFieldCardinality<T>,
    /// The next docid start range to fetch (inclusive).
    next_fetch_start: u32,
    /// Number of docs range checked in a batch.
    ///
    /// There are two patterns.
    /// - We do a full scan. => We can load large chunks. We don't know in advance if seek call
    /// will come, so we start with small chunks
    /// - We load docs, interspersed with seek calls. When there are big jumps in the seek, we
    /// should load small chunks. When the seeks are small, we can employ the same strategy as on a
    /// full scan.
    fetch_horizon: u32,
    /// Current batch of loaded docs.
    loaded_docs: VecCursor,
    last_seek_pos_opt: Option<u32>,
}

const DEFAULT_FETCH_HORIZON: u32 = 128;
impl<T: MakeZero + Send + PartialOrd + Copy + fmt::Debug> RangeDocSet<T> {
    pub(crate) fn new(value_range: RangeInclusive<T>, fast_field: FastFieldCardinality<T>) -> Self {
        let mut range_docset = Self {
            value_range,
            fast_field,
            loaded_docs: VecCursor::new(),
            next_fetch_start: 0,
            fetch_horizon: DEFAULT_FETCH_HORIZON,
            last_seek_pos_opt: None,
        };
        range_docset.reset_fetch_range();
        range_docset.fetch_block();
        range_docset
    }

    fn reset_fetch_range(&mut self) {
        self.fetch_horizon = DEFAULT_FETCH_HORIZON;
    }

    /// Returns true if more data could be fetched
    fn fetch_block(&mut self) {
        const MAX_HORIZON: u32 = 100_000;
        while self.loaded_docs.is_empty() {
            let finished_to_end = self.fetch_horizon(self.fetch_horizon);
            if finished_to_end {
                break;
            }
            // Fetch more data, increase horizon. Horizon only gets reset when doing a seek.
            self.fetch_horizon = (self.fetch_horizon * 2).min(MAX_HORIZON);
        }
    }

    /// check if the distance between the seek calls is large
    fn is_last_seek_distance_large(&self, new_seek: DocId) -> bool {
        if let Some(last_seek_pos) = self.last_seek_pos_opt {
            (new_seek - last_seek_pos) >= 128
        } else {
            true
        }
    }

    /// Fetches a block for docid range [next_fetch_start .. next_fetch_start + HORIZON]
    fn fetch_horizon(&mut self, horizon: u32) -> bool {
        let mut finished_to_end = false;

        let limit = self.fast_field.num_docs();
        let mut end = self.next_fetch_start + horizon;
        if end >= limit {
            end = limit;
            finished_to_end = true;
        }

        match &self.fast_field {
            FastFieldCardinality::MultiValue(multi) => {
                let last_value = self.loaded_docs.last_value();

                multi.get_docids_for_value_range(
                    self.value_range.clone(),
                    self.next_fetch_start..end,
                    self.loaded_docs.get_cleared_data(),
                );
                // In case of multivalues, we may have an overlap of the same docid between fetching
                // blocks
                if let Some(last_value) = last_value {
                    while self.loaded_docs.current() == Some(last_value) {
                        self.loaded_docs.next();
                    }
                }
            }
            FastFieldCardinality::SingleValue(single) => {
                single.get_docids_for_value_range(
                    self.value_range.clone(),
                    self.next_fetch_start..end,
                    self.loaded_docs.get_cleared_data(),
                );
            }
        }
        self.next_fetch_start = end;

        finished_to_end
    }
}

impl<T: MakeZero + Send + PartialOrd + Copy + fmt::Debug> DocSet for RangeDocSet<T> {
    #[inline]
    fn advance(&mut self) -> DocId {
        if let Some(docid) = self.loaded_docs.next() {
            docid
        } else {
            if self.next_fetch_start >= self.fast_field.num_docs() {
                return TERMINATED;
            }
            self.fetch_block();
            self.loaded_docs.current().unwrap_or(TERMINATED)
        }
    }

    #[inline]
    fn doc(&self) -> DocId {
        self.loaded_docs.current().unwrap_or(TERMINATED)
    }

    /// Advances the `DocSet` forward until reaching the target, or going to the
    /// lowest [`DocId`] greater than the target.
    ///
    /// If the end of the `DocSet` is reached, [`TERMINATED`] is returned.
    ///
    /// Calling `.seek(target)` on a terminated `DocSet` is legal. Implementation
    /// of `DocSet` should support it.
    ///
    /// Calling `seek(TERMINATED)` is also legal and is the normal way to consume a `DocSet`.
    fn seek(&mut self, target: DocId) -> DocId {
        if self.is_last_seek_distance_large(target) {
            self.reset_fetch_range();
        }
        if target > self.next_fetch_start {
            self.next_fetch_start = target;
        }
        let mut doc = self.doc();
        debug_assert!(doc <= target);
        while doc < target {
            doc = self.advance();
        }
        self.last_seek_pos_opt = Some(target);
        doc
    }

    fn size_hint(&self) -> u32 {
        0 // heuristic possible by checking number of hits when fetching a block
    }
}

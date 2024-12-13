use core::fmt::Debug;
use std::ops::RangeInclusive;

use columnar::Column;

use crate::{query::Scorer, DocId, DocSet, Score, COLLECT_BLOCK_BUFFER_LEN, TERMINATED};

/// Helper to have a cursor over a vec of docids
#[derive(Debug)]
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
    fn last_doc(&self) -> Option<u32> {
        self.docs.last().cloned()
    }
    fn is_empty(&self) -> bool {
        self.current().is_none()
    }
}

pub(crate) struct RangeDocSet<T> {
    /// The range filter on the values.
    value_range: RangeInclusive<T>,
    column: Column<T>,
    /// The next docid start range to fetch (inclusive).
    next_fetch_start: u32,
    /// Number of docs range checked in a batch.
    ///
    /// There are two patterns.
    /// - We do a full scan. => We can load large chunks. We don't know in advance if seek call
    ///   will come, so we start with small chunks
    /// - We load docs, interspersed with seek calls. When there are big jumps in the seek, we
    ///   should load small chunks. When the seeks are small, we can employ the same strategy as on
    ///   a full scan.
    fetch_horizon: u32,
    /// Current batch of loaded docs.
    loaded_docs: VecCursor,
    last_seek_pos_opt: Option<u32>,
}

const DEFAULT_FETCH_HORIZON: u32 = 128;
impl<T: Send + Sync + PartialOrd + Copy + Debug + 'static> RangeDocSet<T> {
    pub(crate) fn new(value_range: RangeInclusive<T>, column: Column<T>) -> Self {
        let mut range_docset = Self {
            value_range,
            column,
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

        let limit = self.column.num_docs();
        let mut end = self.next_fetch_start + horizon;
        if end >= limit {
            end = limit;
            finished_to_end = true;
        }

        let last_doc = self.loaded_docs.last_doc();
        let doc_buffer: &mut Vec<DocId> = self.loaded_docs.get_cleared_data();
        self.column.get_docids_for_value_range(
            self.value_range.clone(),
            self.next_fetch_start..end,
            doc_buffer,
        );
        if let Some(last_doc) = last_doc {
            while self.loaded_docs.current() == Some(last_doc) {
                self.loaded_docs.next();
            }
        }
        self.next_fetch_start = end;

        finished_to_end
    }

    pub fn fetch_doc_id_first_value(&self, doc_id: DocId) -> Option<T> {
        if doc_id != TERMINATED {
            let mut block: Vec<Option<T>> = vec![None; 1];
            self.column.first_vals(&vec![doc_id], block.as_mut_slice());
            return *block.first().unwrap_or(&None);
        }
        None
    }
}

impl<T: Send + Sync + PartialOrd + Copy + Debug + 'static> DocSet for RangeDocSet<T> {
    #[inline]
    fn advance(&mut self) -> DocId {
        if let Some(docid) = self.loaded_docs.next() {
            return docid;
        }
        if self.next_fetch_start >= self.column.num_docs() {
            return TERMINATED;
        }
        self.fetch_block();
        self.loaded_docs.current().unwrap_or(TERMINATED)
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
        self.column.num_docs()
    }
}

pub struct FastFieldRangeScorer<T> {
    docset: RangeDocSet<T>,
    score: Score,
    scoring_callback: Option<fn(Option<T>, Option<T>, Score) -> Score>,
    base_scoring_value: Option<T>,
}

impl<T> FastFieldRangeScorer<T> {
    pub fn new(
        docset: RangeDocSet<T>,
        score: Score,
        scoring_callback: Option<fn(Option<T>, Option<T>, Score) -> Score>,
        base_scoring_value: Option<T>,
    ) -> FastFieldRangeScorer<T> {
        FastFieldRangeScorer {
            docset,
            score,
            scoring_callback,
            base_scoring_value,
        }
    }
}

impl<T> From<RangeDocSet<T>> for FastFieldRangeScorer<T> {
    fn from(docset: RangeDocSet<T>) -> Self {
        FastFieldRangeScorer::new(docset, 1.0, None, None)
    }
}

impl<T: Send + Sync + PartialOrd + Copy + Debug + 'static> DocSet for FastFieldRangeScorer<T> {
    fn advance(&mut self) -> DocId {
        self.docset.advance()
    }

    fn seek(&mut self, target: DocId) -> DocId {
        self.docset.seek(target)
    }

    fn fill_buffer(&mut self, buffer: &mut [DocId; COLLECT_BLOCK_BUFFER_LEN]) -> usize {
        self.docset.fill_buffer(buffer)
    }

    fn doc(&self) -> DocId {
        self.docset.doc()
    }

    fn size_hint(&self) -> u32 {
        self.docset.size_hint()
    }
}

impl<T: Send + Sync + PartialOrd + Copy + Debug + 'static> Scorer for FastFieldRangeScorer<T> {
    fn score(&mut self) -> Score {
        match self.scoring_callback {
            Some(scoring_callback) => scoring_callback(
                self.docset.fetch_doc_id_first_value(self.doc()),
                self.base_scoring_value,
                self.score,
            ),
            None => self.score,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Bound;

    use crate::collector::Count;
    use crate::directory::RamDirectory;
    use crate::query::RangeQuery;
    use crate::{schema, IndexBuilder, TantivyDocument, Term};

    #[test]
    fn range_query_fast_optional_field_minimum() {
        let mut schema_builder = schema::SchemaBuilder::new();
        let id_field = schema_builder.add_text_field("id", schema::STRING);
        let score_field = schema_builder.add_u64_field("score", schema::FAST | schema::INDEXED);

        let dir = RamDirectory::default();
        let index = IndexBuilder::new()
            .schema(schema_builder.build())
            .open_or_create(dir)
            .unwrap();

        {
            let mut writer = index.writer(15_000_000).unwrap();

            let count = 1000;
            for i in 0..count {
                let mut doc = TantivyDocument::new();
                doc.add_text(id_field, format!("doc{i}"));

                let nb_scores = i % 2; // 0 or 1 scores
                for _ in 0..nb_scores {
                    doc.add_u64(score_field, 80);
                }

                writer.add_document(doc).unwrap();
            }
            writer.commit().unwrap();
        }

        let reader = index.reader().unwrap();
        let searcher = reader.searcher();

        let query = RangeQuery::new(
            Bound::Included(Term::from_field_u64(score_field, 70)),
            Bound::Unbounded,
            None,
            None,
        );

        let count = searcher.search(&query, &Count).unwrap();
        assert_eq!(count, 500);
    }
}

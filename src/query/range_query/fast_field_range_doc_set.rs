use core::fmt::Debug;
use std::cell::RefCell;
use std::ops::RangeInclusive;

use columnar::Column;

use crate::{DocId, DocSet, COLLECT_BLOCK_BUFFER_LEN, TERMINATED};

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

#[derive(Debug)]
struct RangeDocSetInner {
    /// The next docid start range to fetch (inclusive).
    next_fetch_start: u32,
    /// Number of docs range checked in a batch.
    fetch_horizon: u32,
    /// Current batch of loaded docs.
    loaded_docs: VecCursor,
    last_seek_pos_opt: Option<u32>,
}

pub(crate) struct RangeDocSet<T> {
    /// The range filter on the values.
    value_range: RangeInclusive<T>,
    column: Column<T>,
    /// Inner fields that need mutable access even in immutable methods
    inner: RefCell<RangeDocSetInner>,
}

const DEFAULT_FETCH_HORIZON: u32 = 128;

impl<T: Send + Sync + PartialOrd + Copy + Debug + 'static> RangeDocSet<T> {
    pub(crate) fn new(value_range: RangeInclusive<T>, column: Column<T>) -> Self {
        Self {
            value_range,
            column,
            inner: RefCell::new(RangeDocSetInner {
                next_fetch_start: 0,
                fetch_horizon: DEFAULT_FETCH_HORIZON,
                loaded_docs: VecCursor::new(),
                last_seek_pos_opt: None,
            }),
        }
    }

    fn reset_fetch_range(&self, inner: &mut RangeDocSetInner) {
        inner.fetch_horizon = DEFAULT_FETCH_HORIZON;
    }

    /// Returns true if more data could be fetched
    fn fetch_block(&self, inner: &mut RangeDocSetInner) {
        const MAX_HORIZON: u32 = 100_000;
        while inner.loaded_docs.is_empty() {
            let finished_to_end = self.fetch_horizon(inner, inner.fetch_horizon);
            if finished_to_end {
                break;
            }
            // Fetch more data, increase horizon. Horizon only gets reset when doing a seek.
            inner.fetch_horizon = (inner.fetch_horizon * 2).min(MAX_HORIZON);
        }
    }

    /// check if the distance between the seek calls is large
    fn is_last_seek_distance_large(&self, inner: &RangeDocSetInner, new_seek: DocId) -> bool {
        if let Some(last_seek_pos) = inner.last_seek_pos_opt {
            (new_seek - last_seek_pos) >= 128
        } else {
            true
        }
    }

    /// Fetches a block for docid range [next_fetch_start .. next_fetch_start + HORIZON]
    fn fetch_horizon(&self, inner: &mut RangeDocSetInner, horizon: u32) -> bool {
        let mut finished_to_end = false;

        let limit = self.column.num_docs();
        let mut end = inner.next_fetch_start + horizon;
        if end >= limit {
            end = limit;
            finished_to_end = true;
        }

        let last_doc = inner.loaded_docs.last_doc();
        let doc_buffer: &mut Vec<DocId> = inner.loaded_docs.get_cleared_data();
        self.column.get_docids_for_value_range(
            self.value_range.clone(),
            inner.next_fetch_start..end,
            doc_buffer,
        );
        if let Some(last_doc) = last_doc {
            while inner.loaded_docs.current() == Some(last_doc) {
                inner.loaded_docs.next();
            }
        }
        inner.next_fetch_start = end;

        finished_to_end
    }
}

impl<T: Send + Sync + PartialOrd + Copy + Debug + 'static> DocSet for RangeDocSet<T> {
    #[inline]
    fn advance(&mut self) -> DocId {
        let mut inner = self.inner.borrow_mut();
        if inner.next_fetch_start == 0 {
            // Lazy loading of the first block of docs.
            self.fetch_block(&mut inner);
        }

        if let Some(docid) = inner.loaded_docs.next() {
            return docid;
        }
        if inner.next_fetch_start >= self.column.num_docs() {
            return TERMINATED;
        }
        self.fetch_block(&mut inner);
        inner.loaded_docs.current().unwrap_or(TERMINATED)
    }

    #[inline]
    fn doc(&self) -> DocId {
        let mut inner = self.inner.borrow_mut();
        if inner.next_fetch_start == 0 {
            // Lazy loading of the first block of docs.
            self.fetch_block(&mut inner);
        }
        inner.loaded_docs.current().unwrap_or(TERMINATED)
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
        let mut inner = self.inner.borrow_mut();

        if target == TERMINATED {
            // Clear the loaded docs to ensure other methods work correctly.
            inner.loaded_docs.get_cleared_data();
            inner.next_fetch_start = target;
            return TERMINATED;
        }
        if inner.next_fetch_start == 0 {
            inner.next_fetch_start = target;
            // Lazy loading of the first block of docs.
            self.fetch_block(&mut inner);
        }

        if self.is_last_seek_distance_large(&inner, target) {
            self.reset_fetch_range(&mut inner);
        }
        if target > inner.next_fetch_start {
            inner.next_fetch_start = target;
        }

        let mut doc = inner.loaded_docs.current().unwrap_or(TERMINATED);
        while doc < target {
            // Copy from advance()
            if let Some(docid) = inner.loaded_docs.next() {
                doc = docid;
            } else if inner.next_fetch_start >= self.column.num_docs() {
                doc = TERMINATED;
            } else {
                self.fetch_block(&mut inner);
                doc = inner.loaded_docs.current().unwrap_or(TERMINATED);
            }
        }
        inner.last_seek_pos_opt = Some(target);
        doc
    }

    fn size_hint(&self) -> u32 {
        self.column.num_docs()
    }

    /// Returns a best-effort hint of the
    /// cost to drive the docset.
    fn cost(&self) -> u64 {
        // Advancing the docset is relatively expensive since it scans the column.
        // Keep cost relative to a term query driver; use num_docs as baseline.
        self.column.num_docs() as u64
    }

    fn is_range(&self) -> bool {
        true
    }

    fn fill_buffer(&mut self, buffer: &mut [DocId; COLLECT_BLOCK_BUFFER_LEN]) -> usize {
        let mut inner = self.inner.borrow_mut();
        if inner.next_fetch_start == 0 {
            // Lazy loading of the first block of docs.
            self.fetch_block(&mut inner);
        }
        let mut doc = inner.loaded_docs.current().unwrap_or(TERMINATED);
        if doc == TERMINATED {
            return 0;
        }
        for (i, buffer_val) in buffer.iter_mut().enumerate() {
            *buffer_val = doc;
            if let Some(next_doc) = inner.loaded_docs.next() {
                doc = next_doc;
            } else if inner.next_fetch_start >= self.column.num_docs() {
                return i + 1;
            } else {
                self.fetch_block(&mut inner);
                doc = inner.loaded_docs.current().unwrap_or(TERMINATED);
                if doc == TERMINATED {
                    return i + 1;
                }
            }
        }
        buffer.len()
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
        );

        let count = searcher.search(&query, &Count).unwrap();
        assert_eq!(count, 500);
    }
}

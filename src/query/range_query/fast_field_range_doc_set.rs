use core::fmt::Debug;
use std::ops::RangeInclusive;

use columnar::Column;

use crate::docset::SeekDangerResult;
use crate::{DocId, DocSet, TERMINATED};

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
    /// Rolling confidence that `seek_danger` targets are clustered: each small hop builds it up to a
    /// cap, each large hop erodes it. Once it clears [`MIN_RUN_TO_SCAN`] the seeks are dense enough
    /// that one forward-scanned block serves many targets more cheaply than a point lookup each.
    /// Unlike a cumulative distance sum it has no periodic reset, so a sustained dense run keeps
    /// scanning while a few isolated jumps only nick it (see [`DocSet::seek_danger`]).
    seek_cluster_run: u32,
}

const DEFAULT_FETCH_HORIZON: u32 = 128;
impl<T: Send + Sync + PartialOrd + Copy + Debug + 'static> RangeDocSet<T> {
    pub(crate) fn new(value_range: RangeInclusive<T>, column: Column<T>) -> Self {
        if *value_range.start() > column.max_value() || *value_range.end() < column.min_value() {
            return Self {
                value_range,
                column,
                loaded_docs: VecCursor::new(),
                next_fetch_start: TERMINATED,
                fetch_horizon: DEFAULT_FETCH_HORIZON,
                last_seek_pos_opt: None,
                seek_cluster_run: 0,
            };
        }

        let mut range_docset = Self {
            value_range,
            column,
            loaded_docs: VecCursor::new(),
            next_fetch_start: 0,
            fetch_horizon: DEFAULT_FETCH_HORIZON,
            last_seek_pos_opt: None,
            seek_cluster_run: 0,
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
        if self.next_fetch_start >= self.column.num_docs() {
            return;
        }
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

        let num_docs = self.column.num_docs();
        let mut fetch_end = self.next_fetch_start + horizon;
        if fetch_end >= num_docs {
            fetch_end = num_docs;
            finished_to_end = true;
        }

        let last_doc = self.loaded_docs.last_doc();
        let doc_buffer: &mut Vec<DocId> = self.loaded_docs.get_cleared_data();

        // TODO: for very sparse columns (e.g. 0.1%), we could load the values in the column and translate them back to
        // docids, instead of starting at the docids. That way we should be able to extend fetch_end for cheap.
        self.column.get_docids_for_value_range(
            self.value_range.clone(),
            self.next_fetch_start..fetch_end,
            doc_buffer,
        );
        if let Some(last_doc) = last_doc {
            while self.loaded_docs.current() == Some(last_doc) {
                self.loaded_docs.next();
            }
        }
        self.next_fetch_start = fetch_end;

        finished_to_end
    }
}

impl<T: Send + Sync + PartialOrd + Copy + Debug + 'static> DocSet for RangeDocSet<T> {
    #[inline]
    fn advance(&mut self) -> DocId {
        if let Some(docid) = self.loaded_docs.next() {
            return docid;
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
    #[inline(never)]
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

    /// `seek_danger` only needs to answer whether `target` itself matches, so it does a cheap
    /// point lookup on the column instead of scanning forward to materialize the next match (the
    /// expensive part of a regular `seek`).
    fn seek_danger(&mut self, target: DocId) -> SeekDangerResult {
        // Covers `target == TERMINATED` and any target past the last doc: no match is possible.
        if target >= self.column.num_docs() {
            return SeekDangerResult::SeekLowerBound(TERMINATED);
        }

        let distance_to_last_seek = self
            .last_seek_pos_opt
            .map(|last_seek_pos| target.saturating_sub(last_seek_pos))
            .unwrap_or(u32::MAX);
        // The point lookup is more expensive than scanning forward, so once the seeks look
        // clustered we switch to scanning forward instead of doing a point lookup per target.
        //
        // We can't look into the future to see if the next seek is also small, but if we change
        // the API in tantivy to operate on blocks of DocIds or have access to the callers docset, we can
        // do a better job here.
        //
        // A fetch_block via seek on 128 elements is as expensive as ~24 point lookups. 
        const SMALL_HOP: u32 = 4;
        const MIN_RUN_TO_SCAN: u32 = 16;
        const LARGE_HOP_PENALTY: u32 = 6;
        // Cap keeps the counter reactive
        const MAX_RUN: u32 = MIN_RUN_TO_SCAN + 4 * LARGE_HOP_PENALTY;
        if distance_to_last_seek <= SMALL_HOP {
            self.seek_cluster_run = (self.seek_cluster_run + 1).min(MAX_RUN);
        } else {
            // Large hops erode 4× faster than small hops build, so we keep scanning only while
            // seeks stay ~80%+ clustered.
            self.seek_cluster_run = self.seek_cluster_run.saturating_sub(LARGE_HOP_PENALTY);
        }
        if self.seek_cluster_run >= MIN_RUN_TO_SCAN {
            // Start each clustered scan from a modest horizon (fetch_block grows it if a block comes
            // up empty) rather than inheriting a large one from an earlier full scan.
            self.fetch_horizon = DEFAULT_FETCH_HORIZON;
            let mut doc = self.doc();
            if doc < target {
                doc = self.seek(target);
            }
            return if doc == target {
                SeekDangerResult::Found
            } else {
                SeekDangerResult::SeekLowerBound(doc)
            };
        }

        self.last_seek_pos_opt = Some(target);

        // If the target is already in the loaded docs, we can just return Found without doing a
        // point lookup. This also leaves the cursor positioned on `target`, so the docset stays in
        // a valid state and a following `advance()` resumes the scan right after it.
        if self
            .loaded_docs
            .last_doc()
            .map(|doc| doc >= target)
            .unwrap_or(false)
        {
            // A previously scanned block still covers the target, let's artificially extend the run
            // to the cap so it keeps the same hysteresis headroom as a naturally clustered run.
            self.seek_cluster_run = MAX_RUN;
            self.fetch_horizon = DEFAULT_FETCH_HORIZON;

            // iterate through the loaded docs to find the target or the next doc after it
            while let Some(doc) = self.loaded_docs.current() {
                if doc == target {
                    return SeekDangerResult::Found;
                } else if doc > target {
                    return SeekDangerResult::SeekLowerBound(doc);
                }
                self.loaded_docs.next();
            }
        }

        let is_match = self
            .column
            .values_for_doc(target)
            .any(|value| self.value_range.contains(&value));
        if is_match {
            // Leave the docset in a valid state positioned on `target`, so `doc()` returns it and a
            // following `advance()` resumes the scan right after it.
            self.loaded_docs.get_cleared_data().push(target);
            self.next_fetch_start = target + 1;
            SeekDangerResult::Found
        } else {
            // `target` is not in the docset. The next match is strictly greater than `target`, so
            // `target + 1` is a valid lower bound. We may leave the docset in an invalid state.
            SeekDangerResult::SeekLowerBound(target + 1)
        }
    }

    fn size_hint(&self) -> u32 {
        // TODO: Implement a better size hint
        self.column.num_docs() / 10
    }

    /// Returns a best-effort hint of the
    /// cost to drive the docset.
    fn cost(&self) -> u64 {
        // Advancing the docset is pretty expensive since it scans the whole column, there is no
        // index currently (will change with an kd-tree)
        // Since we use SIMD to scan the fast field range query we lower the cost a little bit,
        // assuming that we hit 10% of the docs like in size_hint.
        //
        // If we would return a cost higher than num_docs, we would never choose ff range query as
        // the driver in a DocSet, when intersecting a term query with a fast field. But
        // it's the faster choice when the term query has a lot of docids and the range
        // query has not.
        //
        // Ideally this would take the fast field codec into account
        (self.column.num_docs() as f64 * 0.8) as u64
    }
}

#[cfg(test)]
mod tests {
    use std::ops::{Bound, RangeInclusive};

    use columnar::Column;

    use super::RangeDocSet;
    use crate::collector::Count;
    use crate::directory::RamDirectory;
    use crate::docset::{SeekDangerResult, TERMINATED};
    use crate::query::RangeQuery;
    use crate::{schema, DocSet, Index, IndexBuilder, TantivyDocument, Term};

    /// Builds a single-segment index where doc `i` carries `values_for_doc(i)` in a u64 fast
    /// field, then returns its column so we can drive a `RangeDocSet` directly.
    fn build_u64_column(
        num_docs: usize,
        values_for_doc: impl Fn(usize) -> Vec<u64>,
    ) -> Column<u64> {
        let mut schema_builder = schema::SchemaBuilder::new();
        let value_field = schema_builder.add_u64_field("value", schema::FAST);
        let index = Index::create_in_ram(schema_builder.build());
        {
            let mut writer = index.writer_for_tests().unwrap();
            for i in 0..num_docs {
                let mut doc = TantivyDocument::new();
                for v in values_for_doc(i) {
                    doc.add_u64(value_field, v);
                }
                writer.add_document(doc).unwrap();
            }
            writer.commit().unwrap();
        }
        let searcher = index.reader().unwrap().searcher();
        assert_eq!(searcher.segment_readers().len(), 1);
        searcher
            .segment_reader(0)
            .fast_fields()
            .u64("value")
            .unwrap()
    }

    fn range_docset(
        value_range: RangeInclusive<u64>,
        num_docs: usize,
        values_for_doc: impl Fn(usize) -> Vec<u64>,
    ) -> RangeDocSet<u64> {
        RangeDocSet::new(value_range, build_u64_column(num_docs, values_for_doc))
    }

    #[test]
    fn seek_danger_found_leaves_valid_state() {
        // Even docs match the range, odd docs do not.
        let mut docset = range_docset(0..=0, 100, |i| vec![(i % 2) as u64]);

        // Matching target: `Found`, and the docset is positioned exactly on it.
        assert_eq!(docset.seek_danger(10), SeekDangerResult::Found);
        assert_eq!(docset.doc(), 10);
        // A following advance resumes the scan right after the found doc.
        assert_eq!(docset.advance(), 12);
        assert_eq!(docset.doc(), 12);
    }

    #[test]
    fn seek_danger_miss_returns_lower_bound() {
        let mut docset = range_docset(0..=0, 100, |i| vec![(i % 2) as u64]);

        // Odd target does not match: lower bound is strictly greater than the target and never
        // skips past the next real match (here doc 12, the first even doc after 11).
        match docset.seek_danger(11) {
            SeekDangerResult::SeekLowerBound(lower_bound) => {
                assert!(lower_bound > 11);
                assert!(lower_bound <= 12);
            }
            SeekDangerResult::Found => panic!("11 should not match"),
        }
        // After a miss we may be in an invalid state; another seek_danger recovers it.
        assert_eq!(docset.seek_danger(12), SeekDangerResult::Found);
        assert_eq!(docset.doc(), 12);
    }

    #[test]
    fn seek_danger_terminated_and_out_of_bounds() {
        let mut docset = range_docset(0..=0, 10, |i| vec![(i % 2) as u64]);
        assert_eq!(
            docset.seek_danger(TERMINATED),
            SeekDangerResult::SeekLowerBound(TERMINATED)
        );
        // A target past the last doc has no possible match either.
        assert_eq!(
            docset.seek_danger(10),
            SeekDangerResult::SeekLowerBound(TERMINATED)
        );
    }

    #[test]
    fn seek_danger_multivalued() {
        // Doc `i` holds values [i, i+1]; the range {5} matches docs 4 and 5.
        let mut docset = range_docset(5..=5, 20, |i| vec![i as u64, i as u64 + 1]);

        assert_eq!(docset.seek_danger(4), SeekDangerResult::Found);
        assert_eq!(docset.doc(), 4);
        assert_eq!(docset.advance(), 5);
        // No further match after doc 5.
        assert_eq!(docset.advance(), TERMINATED);
    }

    #[test]
    fn seek_danger_matches_seek() {
        // Cross-check seek_danger against the true next match for every target, on a column with a
        // few sparse matches.
        let matches = [3u32, 7, 50, 51, 99];
        let num_docs = 100;
        let values_for_doc = |i: usize| {
            vec![if matches.contains(&(i as u32)) {
                1u64
            } else {
                0u64
            }]
        };

        for target in 0..num_docs as u32 {
            // The first matching doc greater than or equal to `target`, i.e. what `seek` returns.
            let expected = matches
                .iter()
                .copied()
                .find(|&m| m >= target)
                .unwrap_or(TERMINATED);

            let mut danger = range_docset(1..=1, num_docs, values_for_doc);
            match danger.seek_danger(target) {
                SeekDangerResult::Found => {
                    assert_eq!(expected, target, "target {target} reported Found");
                    assert_eq!(danger.doc(), target);
                }
                SeekDangerResult::SeekLowerBound(lower_bound) => {
                    assert_ne!(expected, target, "target {target} should have been Found");
                    assert!(lower_bound > target);
                    // The lower bound must never skip past the true next match.
                    assert!(lower_bound <= expected);
                }
            }
        }
    }

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

    #[test]
    fn range_query_no_overlap_optimization() {
        let mut schema_builder = schema::SchemaBuilder::new();
        let id_field = schema_builder.add_text_field("id", schema::STRING);
        let value_field = schema_builder.add_u64_field("value", schema::FAST | schema::INDEXED);

        let dir = RamDirectory::default();
        let index = IndexBuilder::new()
            .schema(schema_builder.build())
            .open_or_create(dir)
            .unwrap();

        {
            let mut writer = index.writer(15_000_000).unwrap();

            // Add documents with values in the range [10, 20]
            for i in 0..100 {
                let mut doc = TantivyDocument::new();
                doc.add_text(id_field, format!("doc{i}"));
                doc.add_u64(value_field, 10 + (i % 11) as u64); // values in range 10-20

                writer.add_document(doc).unwrap();
            }
            writer.commit().unwrap();
        }

        let reader = index.reader().unwrap();
        let searcher = reader.searcher();

        // Test a range query [100, 200] that has no overlap with data range [10, 20]
        let query = RangeQuery::new(
            Bound::Included(Term::from_field_u64(value_field, 100)),
            Bound::Included(Term::from_field_u64(value_field, 200)),
        );

        let count = searcher.search(&query, &Count).unwrap();
        assert_eq!(count, 0); // should return 0 results since there's no overlap

        // Test another non-overlapping range: [0, 5] while data range is [10, 20]
        let query2 = RangeQuery::new(
            Bound::Included(Term::from_field_u64(value_field, 0)),
            Bound::Included(Term::from_field_u64(value_field, 5)),
        );

        let count2 = searcher.search(&query2, &Count).unwrap();
        assert_eq!(count2, 0); // should return 0 results since there's no overlap
    }
}

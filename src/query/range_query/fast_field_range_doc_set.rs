use core::fmt::Debug;
use std::net::Ipv6Addr;
use std::ops::RangeInclusive;

use columnar::Column;

/// Maps supported column values into an ordered integer space for range-width estimation.
pub(crate) trait RangeDocSetValue:
    Send + Sync + PartialOrd + Copy + Debug + 'static
{
    fn to_u128(self) -> u128;
}

impl RangeDocSetValue for u64 {
    fn to_u128(self) -> u128 {
        self as u128
    }
}

impl RangeDocSetValue for Ipv6Addr {
    fn to_u128(self) -> u128 {
        u128::from_be_bytes(self.octets())
    }
}

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
    fn is_consumed(&self) -> bool {
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
    /// Upper bound for `fetch_horizon`. This can be changed while the docset is running.
    /// Must remain greater than zero.
    max_fetch_horizon: u32,
    /// Current batch of loaded docs.
    loaded_docs: VecCursor,
    last_seek_pos_opt: Option<u32>,
    /// Rolling confidence that `seek_danger` targets are clustered: each small hop builds it up to
    /// a cap, each large hop erodes it. Once it clears `MIN_RUN_TO_SCAN` the seeks are dense
    /// enough that one forward-scanned block serves many targets more cheaply than a point
    /// lookup each. Unlike a cumulative distance sum it has no periodic reset, so a sustained
    /// dense run keeps scanning while a few isolated jumps only nick it (see
    /// [`DocSet::seek_danger`]).
    seek_cluster_run: u32,
}

const DEFAULT_FETCH_HORIZON: u32 = 128;
const DEFAULT_MAX_FETCH_HORIZON: u32 = 100_000;
const RANGE_DOCSET_COST_PER_HIT: f64 = 0.5;

impl<T: RangeDocSetValue> RangeDocSet<T> {
    pub(crate) fn new(value_range: RangeInclusive<T>, column: Column<T>) -> Self {
        if *value_range.start() > column.max_value() || *value_range.end() < column.min_value() {
            return Self {
                value_range,
                column,
                loaded_docs: VecCursor::new(),
                next_fetch_start: TERMINATED,
                fetch_horizon: DEFAULT_FETCH_HORIZON,
                max_fetch_horizon: DEFAULT_MAX_FETCH_HORIZON,
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
            max_fetch_horizon: DEFAULT_MAX_FETCH_HORIZON,
            last_seek_pos_opt: None,
            seek_cluster_run: 0,
        };
        range_docset.reset_fetch_range();
        range_docset.fetch_block();
        range_docset
    }

    /// Estimates matching values by assuming values are uniformly distributed between the
    /// column's minimum and maximum.
    fn estimated_num_hits(&self) -> f64 {
        if self.column.num_values() == 0 {
            return 0.0;
        }

        let column_min = self.column.min_value().to_u128();
        let column_max = self.column.max_value().to_u128();
        let range_start = (*self.value_range.start()).to_u128().max(column_min);
        let range_end = (*self.value_range.end()).to_u128().min(column_max);
        if range_start > range_end {
            return 0.0;
        }

        let column_width = (column_max - column_min) as f64 + 1.0;
        let range_width = (range_end - range_start) as f64 + 1.0;
        self.column.num_values() as f64 * range_width / column_width
    }

    fn set_fetch_horizon(&mut self, fetch_horizon: u32) {
        self.fetch_horizon = fetch_horizon.min(self.max_fetch_horizon);
    }

    fn reset_fetch_range(&mut self) {
        self.set_fetch_horizon(DEFAULT_FETCH_HORIZON);
    }

    /// Returns true if more data could be fetched
    fn fetch_block(&mut self) {
        if self.next_fetch_start >= self.column.num_docs() {
            return;
        }
        while self.loaded_docs.is_consumed() {
            let finished_to_end = self.do_fetch_horizon();
            if finished_to_end {
                break;
            }
            // Fetch more data, increase horizon. Horizon only gets reset when doing a seek.
            self.set_fetch_horizon(self.fetch_horizon.saturating_mul(2));
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
    fn do_fetch_horizon(&mut self) -> bool {
        let horizon = self.fetch_horizon;
        let mut finished_to_end = false;

        let num_docs = self.column.num_docs();
        let mut fetch_end = self.next_fetch_start.saturating_add(horizon);
        if fetch_end >= num_docs {
            fetch_end = num_docs;
            finished_to_end = true;
        }

        let last_doc = self.loaded_docs.last_doc();
        let doc_buffer: &mut Vec<DocId> = self.loaded_docs.get_cleared_data();

        // TODO: for very sparse columns (e.g. 0.1%), we could load the values in the column and
        // translate them back to docids, instead of starting at the docids. That way we
        // should be able to extend fetch_end for cheap.
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

    /// Specialized `fetch_block` for seek_danger. Unlike the regular fetch_block, it does not
    /// double the horizon until it finds a hit.
    fn fetch_block_seek_danger(&mut self, target: DocId) -> SeekDangerResult {
        self.next_fetch_start = self.next_fetch_start.max(target);

        while self.loaded_docs.current().is_some_and(|doc| doc < target) {
            self.loaded_docs.next();
        }

        // We want to limit the number of docs we fetch, so we don't scan the whole column if the
        // target is far away.
        // There are exceptions to this.
        // 1. If we intersect term "a" that has a lot of docs with a range query that has few hits,
        //    we want to scan more, so we don't get called repeatedly with small incremental
        //    targets.
        // 2. If the range column is mostly empty, checking is relatively cheap, so we can scan
        //    more.
        if self.loaded_docs.is_consumed() {
            self.do_fetch_horizon();
        }

        match self.loaded_docs.current() {
            Some(doc) if doc == target => SeekDangerResult::Found,
            Some(doc) => SeekDangerResult::SeekLowerBound(doc),
            None if self.next_fetch_start >= self.column.num_docs() => {
                SeekDangerResult::SeekLowerBound(TERMINATED)
            }
            None => {
                // Since the caller will use this a new lowerbound this messes up our
                // last_seek_pos_opt, distance computation so we artificially set it
                // to the returned lower bound, which is the next_fetch_start.
                self.last_seek_pos_opt = Some(self.next_fetch_start);
                SeekDangerResult::SeekLowerBound(self.next_fetch_start)
            }
        }
    }
}

impl<T: RangeDocSetValue> DocSet for RangeDocSet<T> {
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
        self.next_fetch_start = self.next_fetch_start.max(target);
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

        // A scan miss can return an actual matching doc as its lower bound. The intersection then
        // calls us again with that doc. It is already loaded, and this self-generated hop says
        // nothing about the density of the driving docset.
        if self.loaded_docs.current() == Some(target) {
            self.last_seek_pos_opt = Some(target);
            return SeekDangerResult::Found;
        }

        let distance_to_last_seek = self
            .last_seek_pos_opt
            .map(|last_seek_pos| target.saturating_sub(last_seek_pos))
            .unwrap_or(u32::MAX);
        // The point lookup is more expensive than scanning forward, so once the seeks look
        // clustered we switch to scanning forward instead of doing a point lookup per target.
        //
        // We can't look into the future to see if the next seek is also small, but if we change
        // the API in tantivy to operate on blocks of DocIds or have access to the callers docset,
        // we can do a better job here.
        //
        // A fetch_block over 128 documents costs about as much as ~24 point lookups, putting the
        // break-even spacing near five documents.
        //
        // These values below are empirically evaluated on the bool_queries_with_range benchmark and
        // a real dataset.
        const SCORE_DELTA_BY_HOP: [i8; 7] = [1, 1, 1, 0, -2, -4, -6];
        const MIN_RUN_TO_SCAN: u32 = 16;
        // Cap the so sustained large hops switch back to point lookups.
        const MAX_RUN: u32 = 32 * MIN_RUN_TO_SCAN;
        let hop_bucket = distance_to_last_seek
            .saturating_sub(1)
            .min(SCORE_DELTA_BY_HOP.len() as u32 - 1) as usize;
        let score_delta = SCORE_DELTA_BY_HOP[hop_bucket] as i32;
        self.seek_cluster_run = self
            .seek_cluster_run
            .saturating_add_signed(score_delta)
            .min(MAX_RUN);
        if self.seek_cluster_run == MIN_RUN_TO_SCAN {
            // Start a newly detected dense run with a bounded speculative scan. Productive blocks
            // grow the horizon in `fetch_block_seek_danger`.
            self.reset_fetch_range();
        }
        self.last_seek_pos_opt = Some(target);
        if self.seek_cluster_run >= MIN_RUN_TO_SCAN {
            return self.fetch_block_seek_danger(target);
        }

        // If the target is already in the loaded docs, we can just return Found without doing a
        // point lookup. This also leaves the cursor positioned on `target`, so the docset stays in
        // a valid state and a following `advance()` resumes the scan right after it.
        if self
            .loaded_docs
            .last_doc()
            .map(|doc| doc >= target)
            .unwrap_or(false)
        {
            // Iterate through the loaded docs to find the target or the next doc after it.
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
        // We have two cost:
        // * a fixed cost of scanning the fast field, which is 10% of the column size
        // * another fixed cost of start scanning the column, which is 100
        // * a cost for each hit, we assume a uniform distribution of the values in the column
        //
        100 + (self.column.num_values() as u64 / 10)
            + (self.estimated_num_hits() * RANGE_DOCSET_COST_PER_HIT) as u64
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
    fn cost_estimates_hits_from_range_and_column_bounds() {
        let column = build_u64_column(1_000, |i| vec![(i % 100) as u64]);

        assert_eq!(RangeDocSet::new(20..=39, column.clone()).cost(), 100);
        assert_eq!(RangeDocSet::new(50..=50, column.clone()).cost(), 5);
        assert_eq!(RangeDocSet::new(0..=99, column.clone()).cost(), 500);
        assert_eq!(RangeDocSet::new(90..=200, column.clone()).cost(), 50);
        assert_eq!(RangeDocSet::new(200..=300, column).cost(), 0);
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
    fn seek_danger_keeps_point_lookups_for_selective_targets() {
        // Only every tenth document matches. Widely spaced candidates should stay on the point
        // lookup path: its miss lower bound is target + 1 rather than the next actual match.
        let mut docset = range_docset(1..=1, 5_000, |i| vec![(i % 10 == 0) as u64]);

        for target in [1_001, 2_001, 3_001, 4_001] {
            assert_eq!(
                docset.seek_danger(target),
                SeekDangerResult::SeekLowerBound(target + 1)
            );
        }
        assert_eq!(docset.seek_cluster_run, 0);
    }

    #[test]
    fn seek_danger_scan_miss_does_not_expand_past_one_horizon() {
        // The range only matches an early prefix. A locally dense burst much later can trigger scan
        // mode, but that speculative scan must not expand through the entire empty suffix.
        let mut docset = range_docset(1..=1, 5_000, |i| vec![(i < 100) as u64]);
        for target in 500..516 {
            assert_eq!(
                docset.seek_danger(target),
                SeekDangerResult::SeekLowerBound(target + 1)
            );
        }

        // This call reaches MIN_RUN_TO_SCAN. Only [516, 644) is inspected; 644 is a valid lower
        // bound because that entire horizon was proven empty.
        assert_eq!(
            docset.seek_danger(516),
            SeekDangerResult::SeekLowerBound(644)
        );
        assert_eq!(docset.next_fetch_start, 644);
        assert_eq!(docset.fetch_horizon, 128);

        // The empty first horizon rejects the scan-mode transition and returns to point lookups.
        assert_eq!(
            docset.seek_danger(644),
            SeekDangerResult::SeekLowerBound(645)
        );
    }

    #[test]
    fn seek_danger_scan_returns_a_real_match_after_exhausted_block() {
        let mut docset = range_docset(1..=1, 1_000, |i| vec![(i % 10 == 0) as u64]);
        while docset.loaded_docs.current().is_some() {
            docset.loaded_docs.next();
        }
        assert_eq!(docset.next_fetch_start, 128);

        // Enter scan mode in the known-empty tail of the exhausted block. The bounded seek must
        // continue with the next block and return a real match with a valid cursor.
        docset.last_seek_pos_opt = Some(124);
        docset.seek_cluster_run = 15;
        assert_eq!(
            docset.seek_danger(125),
            SeekDangerResult::SeekLowerBound(130)
        );
        assert_eq!(docset.doc(), 130);
        assert_eq!(docset.seek_danger(130), SeekDangerResult::Found);
    }

    #[test]
    fn seek_danger_dense_scan_keeps_growing_fetch_horizon() {
        // Every tenth document matches, while the supplied candidates are dense. After enough
        // adjacent candidates, seek_danger should switch from target + 1 point-lookup lower bounds
        // to a scan returning the next real match.
        let mut docset = range_docset(1..=1, 5_000, |i| vec![(i % 10 == 0) as u64]);
        for target in 1_000..1_016 {
            let _ = docset.seek_danger(target);
        }
        let first_scanned_match = match docset.seek_danger(1_016) {
            SeekDangerResult::SeekLowerBound(doc) => doc,
            SeekDangerResult::Found => panic!("1016 does not match"),
        };
        assert_eq!(first_scanned_match, 1_020);

        // The lower bound is generated by our own scan. Confirming it must neither look it up nor
        // penalize the density score for the ten-document hop.
        let score_before_confirmation = docset.seek_cluster_run;
        assert_eq!(
            docset.seek_danger(first_scanned_match),
            SeekDangerResult::Found
        );
        assert_eq!(docset.seek_cluster_run, score_before_confirmation);

        // Consume enough scan results to cross a block boundary. The horizon should grow from 128
        // to 256 and then 512; resetting it on every seek_danger call would leave it at 256.
        let mut current = first_scanned_match;
        while current <= 1_150 {
            let mut candidate = current + 1;
            loop {
                match docset.seek_danger(candidate) {
                    SeekDangerResult::Found => {
                        current = candidate;
                        break;
                    }
                    SeekDangerResult::SeekLowerBound(lower_bound) => {
                        candidate = lower_bound;
                    }
                }
            }
        }
        assert!(docset.fetch_horizon >= 512);
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

//! Lazy gallop strategy for `FastFieldTermSetQuery` on sorted segments.
//!
//! `TermSetGallopDocSet` is a single-cursor DocSet that gallops the sorted
//! column on demand inside `advance()` and `seek()`. For each pruned term,
//! two `gallop_search_sorted` calls produce a half-open `[start, end)`
//! range of DocIds whose value equals the term; the cursor consumes the
//! range, then `find_next_match` walks to the next term and gallops again.
//!
//! Lazy because: when this scorer is ANDed with a more selective DocSet
//! (another `TermSetQuery` filter, a `TermQuery`, a TopK driver), the
//! intersection driver calls `seek(target)` and skips most ranges. The
//! eager precomputation that materialized all K ranges at construction
//! time produced ranges the consumer never reaches; this version scales
//! work with ranges actually visited.
//!
//! `seek` short-circuits when `target` falls inside the current range
//! (cheap in-range path); otherwise it reads the column at `target` and
//! uses `gallop_partition_point` to skip past intervening terms in
//! O(log K) without galloping their column ranges.
//!
//! The shrinking-window cursor (`non_null_start`) advances monotonically
//! through DocId space because in a sorted column, larger terms map to
//! higher DocIds (and symmetrically for DESC after pre-rotating the term
//! list to descending order). Each per-term search is bounded above by
//! the previous term's `end`, so the planner's claimed cost of
//! `O(K · log(N/K))` is realistic, not `O(K · log N)`.

use columnar::{Cardinality, Column};

use crate::query::range_query::sorted_internals::{
    binary_search_null_boundary, gallop_partition_point, gallop_search_sorted,
};
use crate::{DocId, DocSet, Order, TERMINATED};

/// A `DocSet` that gallops a sorted column on demand for each term in a
/// pre-rotated, ascending-iteration-order list. See module-level docs.
pub(crate) struct TermSetGallopDocSet {
    column: Column<u64>,
    /// Pre-rotated to iteration order: ascending for `Order::Asc`, descending
    /// for `Order::Desc`. Rotating once in `new()` lets `seek` use a single
    /// `gallop_partition_point` call regardless of sort direction.
    terms: Vec<u64>,
    sort_order: Order,
    /// Lower bound (inclusive) of the unconsumed column window. Advances
    /// monotonically as terms are consumed.
    non_null_start: DocId,
    /// Upper bound (exclusive). For ASC this is `n`; for DESC on Optional
    /// columns this is the NULL boundary. Fixed at construction.
    non_null_end: DocId,
    next_term_idx: usize,
    current_range_end: DocId,
    current: DocId,
}

impl TermSetGallopDocSet {
    pub(crate) fn new(
        column: Column<u64>,
        sort_order: Order,
        mut sorted_terms: Vec<u64>,
        cardinality: Cardinality,
    ) -> Self {
        let n = column.num_docs();

        // NULL-boundary for Optional columns (NULLs cluster at start in ASC,
        // end in DESC). Full has no NULLs. Multivalued is filtered out by
        // the planner's gallop gate.
        let (non_null_start, non_null_end) = match cardinality {
            Cardinality::Full => (0u32, n),
            Cardinality::Optional => match sort_order {
                Order::Asc => (binary_search_null_boundary(&column, 0, n, Order::Asc), n),
                Order::Desc => (0, binary_search_null_boundary(&column, 0, n, Order::Desc)),
            },
            Cardinality::Multivalued => unreachable!("planner filters out Multivalued"),
        };

        // Pre-rotate so that ascending iteration of `terms` corresponds to
        // ascending-DocId iteration in the column, in either sort direction.
        let terms = match sort_order {
            Order::Asc => sorted_terms,
            Order::Desc => {
                sorted_terms.reverse();
                sorted_terms
            }
        };

        let mut s = Self {
            column,
            terms,
            sort_order,
            non_null_start,
            non_null_end,
            next_term_idx: 0,
            current_range_end: 0,
            current: TERMINATED,
        };

        // Seed the cursor: advance past empty/missing terms to the first
        // matching DocId, or set current=TERMINATED if none exist.
        s.find_next_match();
        s
    }

    /// Advance `next_term_idx` until a non-empty range is found. Sets
    /// `current`, `current_range_end`, and `non_null_start` accordingly.
    /// Returns the new `current` (which may be `TERMINATED`).
    fn find_next_match(&mut self) -> DocId {
        while self.next_term_idx < self.terms.len() && self.non_null_start < self.non_null_end {
            let t = self.terms[self.next_term_idx];
            let start = gallop_search_sorted(
                &self.column,
                self.non_null_start,
                self.non_null_end,
                t,
                self.sort_order,
                false,
            );
            let end = gallop_search_sorted(
                &self.column,
                self.non_null_start,
                self.non_null_end,
                t,
                self.sort_order,
                true,
            );
            self.next_term_idx += 1;
            if start < end {
                self.current = start;
                self.current_range_end = end;
                self.non_null_start = end;
                return self.current;
            }
            // Term absent from the remaining window: advance the lower bound
            // so the next term's gallop doesn't re-scan the eliminated prefix.
            self.non_null_start = end;
        }
        self.current = TERMINATED;
        TERMINATED
    }
}

impl DocSet for TermSetGallopDocSet {
    fn advance(&mut self) -> DocId {
        if self.current == TERMINATED {
            return TERMINATED;
        }
        let next = self.current + 1;
        if next < self.current_range_end {
            self.current = next;
            return next;
        }
        // Past the current range: gallop the next term.
        self.non_null_start = self.current_range_end;
        self.find_next_match()
    }

    fn seek(&mut self, target: DocId) -> DocId {
        debug_assert!(target >= self.current || self.current == TERMINATED);
        if self.current == TERMINATED {
            return TERMINATED;
        }
        if target >= self.non_null_end {
            self.current = TERMINATED;
            return TERMINATED;
        }
        // Cheap in-range path: target falls inside the current range we've
        // already galloped. No column or term-list lookup.
        if target < self.current_range_end {
            self.current = target;
            return target;
        }
        // Skip path: read the column at `target`, then partition-point the
        // term list. For ASC the term list is ascending and the column value
        // at `target` is `v`; terms with `t < v` have ranges entirely before
        // `target` (already past us), so the partition point is the first
        // `t` with `t >= v`. Symmetric for DESC (`t > v` means already past).
        let v = self
            .column
            .first(target)
            .expect("seek into non-NULL window must find a value");
        let next_idx = match self.sort_order {
            Order::Asc => gallop_partition_point(&self.terms, self.next_term_idx, |&t| t < v),
            Order::Desc => gallop_partition_point(&self.terms, self.next_term_idx, |&t| t > v),
        };
        if next_idx >= self.terms.len() {
            self.current = TERMINATED;
            return TERMINATED;
        }
        self.next_term_idx = next_idx;
        // Clamping `non_null_start` by `target` is load-bearing: when
        // `terms[next_idx] == v`, the term's range may start before
        // `target`, but the seek contract requires the returned DocId be
        // `>= target`. Starting the gallop at `target` ensures that.
        self.non_null_start = target;
        self.find_next_match()
    }

    fn doc(&self) -> DocId {
        self.current
    }

    fn size_hint(&self) -> u32 {
        // Deliberate overestimate: the lazy DocSet does not know exact match
        // count without doing the work it exists to defer. Reporting the
        // unconsumed window size biases the intersection driver toward
        // seeking us, which is the regime this strategy is built for.
        self.non_null_end.saturating_sub(self.non_null_start)
    }

    fn cost(&self) -> u64 {
        self.size_hint() as u64
    }
}

#[cfg(test)]
mod gallop_tests {
    use columnar::Cardinality;

    use super::*;
    use crate::collector::DocSetCollector;
    use crate::query::FastFieldTermSetQuery;
    use crate::schema::{NumericOptions, SchemaBuilder};
    use crate::{Index, IndexSettings, IndexSortByField, ReloadPolicy, SegmentReader, Term};

    fn build_sorted_index(order: Order, values: &[u64]) -> (Index, crate::schema::Field, String) {
        let mut sb = SchemaBuilder::new();
        let field = sb.add_u64_field("fk", NumericOptions::default().set_fast().set_indexed());
        let schema = sb.build();
        let index = Index::builder()
            .schema(schema)
            .settings(IndexSettings {
                sort_by_field: Some(IndexSortByField {
                    field: "fk".to_string(),
                    order,
                }),
                ..Default::default()
            })
            .create_in_ram()
            .unwrap();
        let mut writer = index.writer_for_tests().unwrap();
        for &v in values {
            writer.add_document(doc!(field => v)).unwrap();
        }
        writer.commit().unwrap();
        (index, field, "fk".to_string())
    }

    fn build_optional_sorted_index(
        order: Order,
        values: &[Option<u64>],
    ) -> (Index, crate::schema::Field, String) {
        let mut sb = SchemaBuilder::new();
        // First field forces every doc to exist regardless of whether `fk`
        // is set; otherwise a None-only doc would not be added.
        let label = sb.add_text_field("label", crate::schema::STRING);
        let field = sb.add_u64_field("fk", NumericOptions::default().set_fast().set_indexed());
        let schema = sb.build();
        let index = Index::builder()
            .schema(schema)
            .settings(IndexSettings {
                sort_by_field: Some(IndexSortByField {
                    field: "fk".to_string(),
                    order,
                }),
                ..Default::default()
            })
            .create_in_ram()
            .unwrap();
        let mut writer = index.writer_for_tests().unwrap();
        for v in values {
            match v {
                Some(x) => writer
                    .add_document(doc!(label => "x", field => *x))
                    .unwrap(),
                None => writer.add_document(doc!(label => "x")).unwrap(),
            };
        }
        writer.commit().unwrap();
        (index, field, "fk".to_string())
    }

    fn open_segment_and_column(index: &Index, field_name: &str) -> (SegmentReader, Column<u64>) {
        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()
            .unwrap();
        let searcher = reader.searcher();
        let segment = searcher.segment_reader(0).clone();
        let (column, _) = segment
            .fast_fields()
            .u64_lenient_for_type(None, field_name)
            .unwrap()
            .unwrap();
        (segment, column)
    }

    /// Drive a `TermSetGallopDocSet` to exhaustion via `advance()` and
    /// collect every emitted DocId.
    fn collect_via_docset(
        column: Column<u64>,
        order: Order,
        sorted_terms: &[u64],
        cardinality: Cardinality,
    ) -> Vec<DocId> {
        let mut ds = TermSetGallopDocSet::new(column, order, sorted_terms.to_vec(), cardinality);
        let mut docs = Vec::new();
        while ds.doc() != TERMINATED {
            docs.push(ds.doc());
            ds.advance();
        }
        docs
    }

    fn collect_docs_via_query(
        index: &Index,
        field: crate::schema::Field,
        terms: &[u64],
    ) -> Vec<DocId> {
        let q = FastFieldTermSetQuery::new(terms.iter().map(|v| Term::from_field_u64(field, *v)));
        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()
            .unwrap();
        let searcher = reader.searcher();
        let result = searcher.search(&q, &DocSetCollector).unwrap();
        let mut docs: Vec<DocId> = result.into_iter().map(|addr| addr.doc_id).collect();
        docs.sort_unstable();
        docs
    }

    /// 20-doc fixture from design.md §2.1.
    /// Term set {3,7,13,99} on column [1,1,1,3,3,5,5,5,5,5,5,7,7,9,9,11,13,13,13,17]
    /// pruned to {3,7,13} → DocIds {3,4, 11,12, 16,17,18}.
    #[test]
    fn gallop_design_doc_fixture_asc() {
        let values: Vec<u64> = vec![
            1, 1, 1, 3, 3, 5, 5, 5, 5, 5, 5, 7, 7, 9, 9, 11, 13, 13, 13, 17,
        ];
        let (index, field, name) = build_sorted_index(Order::Asc, &values);
        let (_segment, column) = open_segment_and_column(&index, &name);
        let cardinality = column.get_cardinality();

        // Drive `TermSetGallopDocSet` directly. Term 99 is outside [1, 17] so
        // the planner would prune it; here we pre-prune to mirror that and
        // feed only {3, 7, 13} sorted ASC.
        let docs = collect_via_docset(column, Order::Asc, &[3, 7, 13], cardinality);
        assert_eq!(docs, vec![3, 4, 11, 12, 16, 17, 18]);

        // End-to-end: also pass the unpruned term set through the actual
        // query pipeline to verify dispatch + min/max pruning agree.
        let docs_from_query = collect_docs_via_query(&index, field, &[3, 7, 13, 99]);
        assert_eq!(docs_from_query, vec![3, 4, 11, 12, 16, 17, 18]);
    }

    /// Symmetric DESC fixture: same logical set, segment built DESC. Ground
    /// truth from `DocSetCollector` (independent of strategy) cross-checked
    /// against the unsorted-index path.
    #[test]
    fn gallop_design_doc_fixture_desc_matches_query_pipeline() {
        let values: Vec<u64> = vec![
            1, 1, 1, 3, 3, 5, 5, 5, 5, 5, 5, 7, 7, 9, 9, 11, 13, 13, 13, 17,
        ];
        let (index, field, _name) = build_sorted_index(Order::Desc, &values);
        let docs = collect_docs_via_query(&index, field, &[3, 7, 13, 99]);
        assert_eq!(docs.len(), 7);

        // Cross-check by also running an unsorted index with the same data
        // (linear strategy) — the matched DocIds will differ (sort permutes
        // the assignment) but the *count* must agree.
        let mut sb = SchemaBuilder::new();
        let f2 = sb.add_u64_field("fk", NumericOptions::default().set_fast().set_indexed());
        let schema = sb.build();
        let unsorted = Index::create_in_ram(schema);
        let mut writer = unsorted.writer_for_tests().unwrap();
        for &v in &values {
            writer.add_document(doc!(f2 => v)).unwrap();
        }
        writer.commit().unwrap();
        let docs_unsorted = collect_docs_via_query(&unsorted, f2, &[3, 7, 13, 99]);
        assert_eq!(docs.len(), docs_unsorted.len());
    }

    /// `K = 1`: gallop degenerates to two `gallop_search_sorted` calls
    /// and one emitted range.
    #[test]
    fn gallop_single_term() {
        let values: Vec<u64> = (0..32).map(|i| i / 4).collect(); // 8 distinct values, 4 docs each
        let (index, _field, name) = build_sorted_index(Order::Asc, &values);
        let (_segment, column) = open_segment_and_column(&index, &name);
        let cardinality = column.get_cardinality();
        let docs = collect_via_docset(column, Order::Asc, &[3], cardinality);
        assert_eq!(docs, vec![12, 13, 14, 15]);
    }

    /// All terms outside [min, max]: empty result. The planner would prune
    /// these before constructing the DocSet, but the DocSet itself handles
    /// the case via each per-term `start >= end` check, leaving `current`
    /// at `TERMINATED`.
    #[test]
    fn gallop_all_terms_outside_range_returns_empty() {
        let values: Vec<u64> = (0..16).collect();
        let (index, _field, name) = build_sorted_index(Order::Asc, &values);
        let (_segment, column) = open_segment_and_column(&index, &name);
        let cardinality = column.get_cardinality();
        let docs = collect_via_docset(column, Order::Asc, &[100, 200, 300], cardinality);
        assert!(docs.is_empty());
    }

    /// Optional column with NULLs at the start (ASC). The NULL prefix must
    /// be skipped and absent from the output.
    #[test]
    fn gallop_optional_with_nulls_asc() {
        // 5 NULLs, then [1,1,1,3,3,5,5,5,5,5,5,7,7,9,9,11,13,13,13,17].
        let values: Vec<Option<u64>> = vec![
            None,
            None,
            None,
            None,
            None,
            Some(1),
            Some(1),
            Some(1),
            Some(3),
            Some(3),
            Some(5),
            Some(5),
            Some(5),
            Some(5),
            Some(5),
            Some(5),
            Some(7),
            Some(7),
            Some(9),
            Some(9),
            Some(11),
            Some(13),
            Some(13),
            Some(13),
            Some(17),
        ];
        let (index, _field, name) = build_optional_sorted_index(Order::Asc, &values);
        let (_segment, column) = open_segment_and_column(&index, &name);
        let cardinality = column.get_cardinality();
        assert!(matches!(
            cardinality,
            Cardinality::Full | Cardinality::Optional
        ));

        let docs = collect_via_docset(column, Order::Asc, &[3, 7, 13], cardinality);
        // No NULL-region doc may appear; count must equal ground truth.
        assert!(!docs.iter().any(|&d| d < 5));
        let truth = values
            .iter()
            .filter(|v| matches!(v, Some(3) | Some(7) | Some(13)))
            .count();
        assert_eq!(docs.len(), truth);

        // Cross-check via the actual query pipeline (which also dispatches
        // through `select_strategy` → `TermSetGallopDocSet`).
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        let field = searcher.schema().get_field(&name).unwrap();
        let docs2 = collect_docs_via_query(&index, field, &[3, 7, 13, 99]);
        assert_eq!(docs2.len(), truth);
    }

    // ----- Lazy-path tests: exercise paths the eager impl couldn't -----

    /// `seek` skips intermediate terms via `gallop_partition_point` without
    /// galloping their column ranges. Column has 8 distinct values × 4 docs
    /// each (32 docs); term set covers all 8 values.
    #[test]
    fn seek_skips_intermediate_terms_via_partition_point() {
        let values: Vec<u64> = (0..32).map(|i| i / 4).collect();
        let (index, _field, name) = build_sorted_index(Order::Asc, &values);
        let (_segment, column) = open_segment_and_column(&index, &name);
        let cardinality = column.get_cardinality();
        let mut ds = TermSetGallopDocSet::new(column, Order::Asc, (0u64..8).collect(), cardinality);
        // Seed lands on term 0's range [0, 4).
        assert_eq!(ds.doc(), 0);
        // seek(20) is in term 5's range [20, 24). The partition-point skip
        // skips terms 1..5 without galloping their column ranges.
        assert_eq!(ds.seek(20), 20);
        // Advance through the rest of term 5's range.
        assert_eq!(ds.advance(), 21);
        assert_eq!(ds.advance(), 22);
        assert_eq!(ds.advance(), 23);
        // Crossing into term 6's range [24, 28).
        assert_eq!(ds.advance(), 24);
        assert_eq!(ds.advance(), 25);
    }

    /// `seek` to a target inside the current already-galloped range hits
    /// the cheap in-range path (no column read, no term-list lookup).
    /// `seek` past `current_range_end` then triggers the column-read path.
    #[test]
    fn seek_inside_current_range_returns_target() {
        // 16 docs: 0..7 = 0, 8..11 = 5, 12..15 = 6. Term set {5} occupies [8, 12).
        let mut values: Vec<u64> = vec![0; 8];
        values.extend(std::iter::repeat_n(5u64, 4));
        values.extend(std::iter::repeat_n(6u64, 4));
        let (index, _field, name) = build_sorted_index(Order::Asc, &values);
        let (_segment, column) = open_segment_and_column(&index, &name);
        let cardinality = column.get_cardinality();
        let mut ds = TermSetGallopDocSet::new(column, Order::Asc, vec![5], cardinality);
        assert_eq!(ds.doc(), 8);
        // Cheap in-range path.
        assert_eq!(ds.seek(10), 10);
        assert_eq!(ds.seek(11), 11);
        // seek past current_range_end with no further term in the set: TERMINATED.
        assert_eq!(ds.seek(12), TERMINATED);
    }

    /// `seek` to a target whose column value isn't in the term set must
    /// land on the next term's range (not on `target`).
    #[test]
    fn seek_to_non_matching_value_lands_on_next_term() {
        let values: Vec<u64> = (0..32).map(|i| i / 4).collect(); // 8 distinct × 4
        let (index, _field, name) = build_sorted_index(Order::Asc, &values);
        let (_segment, column) = open_segment_and_column(&index, &name);
        let cardinality = column.get_cardinality();
        let mut ds = TermSetGallopDocSet::new(column, Order::Asc, vec![1, 5], cardinality);
        // Seed lands on term 1's range [4, 8).
        assert_eq!(ds.doc(), 4);
        // seek(12): column[12] = 3 (not in {1, 5}). partition-point skips
        // past term 1, lands on term 5 (next ≥ 3), term 5's range is [20, 24).
        assert_eq!(ds.seek(12), 20);
    }

    /// `seek` past `non_null_end` terminates immediately.
    #[test]
    fn seek_past_non_null_end_terminates() {
        let values: Vec<u64> = (0..16).map(|i| i / 4).collect(); // 4 distinct × 4
        let (index, _field, name) = build_sorted_index(Order::Asc, &values);
        let (_segment, column) = open_segment_and_column(&index, &name);
        let cardinality = column.get_cardinality();
        let mut ds = TermSetGallopDocSet::new(column, Order::Asc, vec![3], cardinality);
        // Seed lands on term 3's range [12, 16).
        assert_eq!(ds.doc(), 12);
        // seek(100) is past the column's 16 docs.
        assert_eq!(ds.seek(100), TERMINATED);
    }
}

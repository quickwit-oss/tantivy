use std::cmp::Ordering;

use columnar::{Cardinality, ColumnValues, RowId};

use crate::aggregation::value_source::ValueSource;
use crate::DocId;

/// Buffers the values associated with a block of documents loaded from a [`ValueSource`].
///
/// Regardless of their original types, values are loaded in their `u64` representation using the
/// associated monotonic mapping.
#[derive(Debug, Default, Clone)]
pub(crate) struct ColumnBlockAccessor {
    /// Values loaded for the latest document block, in monotonic `u64` representation.
    val_cache: Vec<u64>,
    /// Document ID corresponding to each value in `val_cache` for a non-full source.
    /// For full sources, this is likely to be empty.
    ///
    /// A document can occur more than once for a multivalued source. For a full source this buffer
    /// is ignored because `val_cache` is aligned directly with the requested document block.
    docid_cache: Vec<DocId>,
    /// Scratch buffer used to identify documents for which a missing value must be inserted.
    missing_docids_cache: Vec<DocId>,
    /// Scratch buffer available to sources for translating document IDs into value row IDs.
    row_id_cache: Vec<RowId>,
    /// Cardinality here is describes the relationship with the loaded doc_id_cache and val_cache.
    ///
    /// For physical columns, we typically set the full column cardinality,
    /// even though the loaded block might have exactly one value per value.
    ///
    /// Use [`Self::has_one_value_per_doc`] rather than this field to detect that.
    cardinality: Cardinality,
}

impl ColumnBlockAccessor {
    #[inline]
    pub(crate) fn fetch_block(&mut self, docs: &[DocId], source: &dyn ValueSource) {
        self.cardinality = source.load_block(
            docs,
            &mut self.val_cache,
            &mut self.docid_cache,
            &mut self.row_id_cache,
        );
        debug_assert!(
            !self.cardinality.is_full() || self.val_cache.len() == docs.len(),
            "a Full source must return exactly one value per input doc"
        );
    }

    /// Fetches a block from a column known to be full (hence we pass the ColumnValue Object
    /// directly).
    ///
    /// docs needs to be strictly increasing.
    #[inline]
    pub(crate) fn fetch_full_column_block(
        &mut self,
        docs: &[DocId],
        column_values: &dyn ColumnValues<u64>,
    ) {
        super::load_full_column_values(docs, column_values, &mut self.val_cache);
        self.cardinality = Cardinality::Full;
    }

    /// Fetches a block and appends `missing_opt` for documents without a value.
    #[inline]
    pub(crate) fn fetch_block_with_missing(
        &mut self,
        docs: &[DocId],
        source: &dyn ValueSource,
        missing_opt: Option<u64>,
    ) {
        self.fetch_block_with_missing_ordered(docs, source, missing_opt, false)
    }

    /// Fetches a block and adds `missing_opt` for documents without a value. When `ordered` is
    /// true, the missing entries are inserted in document order instead of appended as a second
    /// run.
    #[inline]
    pub(crate) fn fetch_block_with_missing_ordered(
        &mut self,
        docs: &[DocId],
        source: &dyn ValueSource,
        missing_opt: Option<u64>,
        ordered: bool,
    ) {
        self.fetch_block(docs, source);
        let cardinality = self.cardinality;
        // no missing values
        if cardinality.is_full() {
            return;
        }
        let Some(missing) = missing_opt else {
            return;
        };

        // We can compare docid_cache length with docs to find missing docs.
        // For multi value columns we can't rely on the length and always need to scan.
        let is_multivalue = cardinality.is_multivalue();
        if !is_multivalue && docs.len() == self.docid_cache.len() {
            return;
        }

        if ordered && !is_multivalue {
            // Rewrite backwards so unread compact values are not overwritten.
            let mut remaining_hits = self.docid_cache.len();
            self.val_cache.resize(docs.len(), missing);
            for (target_idx, &doc) in docs.iter().enumerate().rev() {
                let value = if remaining_hits > 0 && self.docid_cache[remaining_hits - 1] == doc {
                    remaining_hits -= 1;
                    self.val_cache[remaining_hits]
                } else {
                    missing
                };
                self.val_cache[target_idx] = value;
            }
            debug_assert_eq!(remaining_hits, 0);
            self.docid_cache.clear();
            self.docid_cache.extend_from_slice(docs);
            return;
        }

        find_missing_docs(docs, &self.docid_cache, &mut self.missing_docids_cache);

        if !ordered {
            self.val_cache.resize(
                self.val_cache.len() + self.missing_docids_cache.len(),
                missing,
            );
            self.docid_cache
                .extend_from_slice(&self.missing_docids_cache);
            return;
        }

        for &doc in &self.missing_docids_cache {
            let pos = self.docid_cache.partition_point(|&hit| hit < doc);
            // TODO insert by back to avoid shifting the same elements
            // self.missing_docids_cache.len() times
            self.docid_cache.insert(pos, doc);
            self.val_cache.insert(pos, missing);
        }
    }

    /// Like `fetch_block_with_missing`, but deduplicates (doc_id, value) pairs
    /// so that each unique value per document is returned only once.
    ///
    /// This is necessary for correct document counting in aggregations,
    /// where multi-valued fields can produce duplicate entries that inflate counts.
    #[inline]
    pub(crate) fn fetch_block_with_missing_unique_per_doc(
        &mut self,
        docs: &[DocId],
        source: &dyn ValueSource,
        missing: Option<u64>,
        ordered: bool,
    ) {
        self.fetch_block_with_missing_ordered(docs, source, missing, ordered);
        if self.cardinality.is_multivalue() {
            self.dedup_docid_val_pairs();
        }
    }

    /// Removes duplicate (doc_id, value) pairs from the caches.
    ///
    /// After `fetch_block`, entries are sorted by doc_id, but values within
    /// the same doc may not be sorted (e.g. `(0,1), (0,2), (0,1)`).
    /// We group consecutive entries by doc_id, sort values within each group
    /// if it has more than 2 elements, then deduplicate adjacent pairs.
    ///
    /// Skips entirely if no doc_id appears more than once in the block.
    fn dedup_docid_val_pairs(&mut self) {
        if self.docid_cache.len() <= 1 {
            return;
        }

        // Quick check: if no consecutive doc_ids are equal, no dedup needed.
        let has_multivalue = self.docid_cache.windows(2).any(|w| w[0] == w[1]);
        if !has_multivalue {
            return;
        }

        // Sort values within each doc_id group so duplicates become adjacent.
        let mut start = 0;
        while start < self.docid_cache.len() {
            let doc = self.docid_cache[start];
            let mut end = start + 1;
            while end < self.docid_cache.len() && self.docid_cache[end] == doc {
                end += 1;
            }
            if end - start > 2 {
                self.val_cache[start..end].sort();
            }
            start = end;
        }

        // Now duplicates are adjacent — deduplicate in place.
        let mut write = 0;
        for read in 1..self.docid_cache.len() {
            if self.docid_cache[read] != self.docid_cache[write]
                || self.val_cache[read] != self.val_cache[write]
            {
                write += 1;
                if write != read {
                    self.docid_cache[write] = self.docid_cache[read];
                    self.val_cache[write] = self.val_cache[read];
                }
            }
        }
        let new_len = write + 1;
        self.docid_cache.truncate(new_len);
        self.val_cache.truncate(new_len);
    }

    /// Returns the values fetched by the last `fetch_block*` call.
    #[inline]
    pub(crate) fn values(&self) -> &[u64] {
        &self.val_cache
    }

    /// Returns the document IDs corresponding to [`Self::values`] for a non-full column.
    #[inline]
    pub(crate) fn docids(&self) -> &[DocId] {
        &self.docid_cache
    }

    /// Returns whether the last fetched block contains exactly one aligned value per input doc.
    #[inline]
    pub(crate) fn has_one_value_per_doc(&self, docs: &[DocId]) -> bool {
        self.val_cache.len() == docs.len()
            && (self.cardinality.is_full() || self.docid_cache == docs)
    }

    #[inline]
    pub(crate) fn is_multivalued(&self) -> bool {
        self.cardinality.is_multivalue()
    }

    #[inline]
    pub(crate) fn iter_vals(&self) -> impl ExactSizeIterator<Item = u64> + '_ {
        self.val_cache.iter().cloned()
    }

    #[inline]
    /// Returns an iterator over the docids and values
    /// The passed in `docs` slice needs to be the same slice that was passed to `fetch_block` or
    /// `fetch_block_with_missing`.
    ///
    /// The docs are used if the source is full (each doc has exactly one value); otherwise the
    /// internal docid vec is used and may contain duplicate docs.
    pub(crate) fn iter_docid_vals<'a>(
        &'a self,
        docs: &'a [DocId],
    ) -> impl Iterator<Item = (DocId, u64)> + 'a {
        if self.cardinality.is_full() {
            docs.iter().cloned().zip(self.val_cache.iter().cloned())
        } else {
            self.docid_cache
                .iter()
                .cloned()
                .zip(self.val_cache.iter().cloned())
        }
    }
}

/// Given two sorted lists of docids `docs` and `hits`, hits is a subset of `docs`.
/// Write in the output Vec all of the docs that are not in `hits`.
///
/// If output contains elements when called they will be cleared as a preliminary step.
///
/// TODO optimize me. It could work with run length and branch-free.
fn find_missing_docs(docs: &[u32], hits: &[u32], output: &mut Vec<u32>) {
    output.clear();
    let mut docs_iter = docs.iter().copied();
    let mut hits_iter = hits.iter().copied();

    let mut doc: Option<u32> = docs_iter.next();
    let mut hit: Option<u32> = hits_iter.next();

    while let (Some(current_doc), Some(current_hit)) = (doc, hit) {
        match current_doc.cmp(&current_hit) {
            Ordering::Less => {
                output.push(current_doc);
                doc = docs_iter.next();
            }
            Ordering::Equal => {
                doc = docs_iter.next();
                hit = hits_iter.next();
            }
            Ordering::Greater => {
                hit = hits_iter.next();
            }
        }
    }

    output.extend(doc);
    output.extend(docs_iter);
}

#[cfg(test)]
#[allow(clippy::field_reassign_with_default)]
mod tests {
    use std::sync::Arc;

    use columnar::{Column, ColumnType, MonotonicallyMappableToU64};

    use super::*;

    #[derive(Debug)]
    struct TestValueSource {
        cardinality: Cardinality,
        entries: Vec<(DocId, u64)>,
    }

    impl ValueSource for TestValueSource {
        fn column_type(&self) -> ColumnType {
            ColumnType::U64
        }

        fn load_block(
            &self,
            docs: &[DocId],
            values: &mut Vec<u64>,
            docids: &mut Vec<DocId>,
            row_ids: &mut Vec<RowId>,
        ) -> Cardinality {
            values.clear();
            docids.clear();
            row_ids.clear();
            if self.cardinality.is_full() {
                assert_eq!(self.entries.len(), docs.len());
                values.extend(self.entries.iter().map(|(_, value)| *value));
            } else {
                docids.extend(self.entries.iter().map(|(doc, _)| *doc));
                values.extend(self.entries.iter().map(|(_, value)| *value));
            }
            self.cardinality
        }
    }

    #[test]
    fn test_fetch_block_accepts_trait_object() {
        let docs = [2, 4, 8];
        let source = TestValueSource {
            cardinality: Cardinality::Full,
            entries: vec![(2, 20), (4, 40), (8, 80)],
        };
        let dyn_source: &dyn ValueSource = &source;
        let mut accessor = ColumnBlockAccessor::default();

        accessor.fetch_block(&docs, dyn_source);

        assert!(accessor.has_one_value_per_doc(&docs));
        assert_eq!(
            accessor.iter_docid_vals(&docs).collect::<Vec<_>>(),
            [(2, 20), (4, 40), (8, 80)]
        );
    }

    fn full_column(vals: &[u64]) -> Column<u64> {
        use columnar::column_index::ColumnIndex;
        use columnar::column_values::{
            serialize_and_load_u64_based_column_values, ALL_U64_CODEC_TYPES,
        };
        Column {
            index: ColumnIndex::Full,
            values: serialize_and_load_u64_based_column_values::<u64>(&vals, &ALL_U64_CODEC_TYPES),
        }
    }

    #[test]
    fn test_as_column_distinguishes_the_two_kinds() {
        let column: Arc<dyn ValueSource> = Arc::new((full_column(&[5, 6, 7]), ColumnType::U64));
        assert!(column.as_column().is_some());
        assert_eq!(column.bounds(), Some((5, 7)));

        let computed: Arc<dyn ValueSource> = Arc::new(TestValueSource {
            cardinality: Cardinality::Full,
            entries: vec![(0, 1)],
        });
        assert!(computed.as_column().is_none());
        // No global view of a computed source, so no bounds and no bounds-driven fast paths.
        assert_eq!(computed.bounds(), None);
    }

    #[test]
    fn test_fetch_source_block_with_missing_on_a_computed_source() {
        // A computed source reports what it produced: docs 0 and 2 have no value, so they are
        // absent from `docids` and the source is `Optional`.
        let docs = [0, 1, 2, 3];
        let computed: Arc<dyn ValueSource> = Arc::new(TestValueSource {
            cardinality: Cardinality::Optional,
            entries: vec![(1, 11), (3, 33)],
        });
        let mut accessor = ColumnBlockAccessor::default();

        accessor.fetch_block(&docs, &*computed);
        assert!(!accessor.has_one_value_per_doc(&docs));
        assert_eq!(
            accessor.iter_docid_vals(&docs).collect::<Vec<_>>(),
            [(1, 11), (3, 33)]
        );

        accessor.fetch_block_with_missing(&docs, &*computed, Some(99));
        let mut pairs = accessor.iter_docid_vals(&docs).collect::<Vec<_>>();
        pairs.sort_unstable();
        assert_eq!(pairs, [(0, 99), (1, 11), (2, 99), (3, 33)]);
    }

    #[test]
    fn test_find_missing_docs() {
        let docs: Vec<u32> = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        let hits: Vec<u32> = vec![2, 4, 6, 8, 10];

        let mut missing_docs: Vec<u32> = Vec::new();
        find_missing_docs(&docs, &hits, &mut missing_docs);
        assert_eq!(missing_docs, [1, 3, 5, 7, 9]);
    }

    #[test]
    fn test_find_missing_docs_empty() {
        let docs: Vec<u32> = Vec::new();
        let hits: Vec<u32> = vec![2, 4, 6, 8, 10];
        let mut missing_docs: Vec<u32> = Vec::new();
        find_missing_docs(&docs, &hits, &mut missing_docs);
        assert_eq!(missing_docs, [0u32; 0]);
    }

    #[test]
    fn test_find_missing_docs_all_missing() {
        let docs: &[u32] = &[1, 2, 3, 4, 5];
        let hits: &[u32] = &[];
        let mut missing_docs: Vec<u32> = vec![10];
        find_missing_docs(&docs, &hits, &mut missing_docs);
        assert_eq!(&missing_docs, &[1u32, 2, 3, 4, 5]);
    }

    #[test]
    fn test_source_neutral_full_block_alignment() {
        let docs = [2, 4, 8];
        let source = TestValueSource {
            cardinality: Cardinality::Full,
            entries: vec![(2, 20), (4, 40), (8, 80)],
        };
        let mut accessor = ColumnBlockAccessor::default();
        accessor.fetch_block(&docs, &source);
        assert!(accessor.has_one_value_per_doc(&docs));
        assert_eq!(
            accessor.iter_docid_vals(&docs).collect::<Vec<_>>(),
            [(2, 20), (4, 40), (8, 80)]
        );
    }

    #[test]
    fn test_source_neutral_optional_block_with_missing() {
        let docs = [0, 1, 2, 4];
        let source = TestValueSource {
            cardinality: Cardinality::Optional,
            entries: vec![(1, 10), (4, 40)],
        };
        let mut accessor = ColumnBlockAccessor::default();

        accessor.fetch_block_with_missing_ordered(&docs, &source, Some(99), true);

        assert!(accessor.has_one_value_per_doc(&docs));
        assert_eq!(
            accessor.iter_docid_vals(&docs).collect::<Vec<_>>(),
            [(0, 99), (1, 10), (2, 99), (4, 40)]
        );
    }

    #[test]
    fn test_source_neutral_multivalue_block_deduplication() {
        let docs = [0, 1];
        let source = TestValueSource {
            cardinality: Cardinality::Multivalued,
            entries: vec![(0, 3), (0, 1), (0, 3), (1, 5), (1, 5)],
        };
        let mut accessor = ColumnBlockAccessor::default();

        accessor.fetch_block_with_missing_unique_per_doc(&docs, &source, None, false);

        assert!(!accessor.has_one_value_per_doc(&docs));
        assert_eq!(
            accessor.iter_docid_vals(&docs).collect::<Vec<_>>(),
            [(0, 1), (0, 3), (1, 5)]
        );
    }

    #[test]
    fn test_fetch_block_with_missing_ordered() {
        use columnar::column_index::{ColumnIndex, OptionalIndex};
        use columnar::column_values::{
            serialize_and_load_u64_based_column_values, ALL_U64_CODEC_TYPES,
        };

        let vals = [10u64, 40, 70];
        let values =
            serialize_and_load_u64_based_column_values::<u64>(&&vals[..], &ALL_U64_CODEC_TYPES);
        let column = Column {
            index: ColumnIndex::Optional(OptionalIndex::for_test(9, &[1, 4, 7])),
            values,
        };
        let docs = [0, 1, 2, 4, 7, 8];
        let mut accessor = ColumnBlockAccessor::default();

        accessor.fetch_block_with_missing_ordered(
            &docs,
            &(&column, ColumnType::U64),
            Some(99),
            true,
        );

        assert_eq!(
            accessor.iter_vals().collect::<Vec<_>>(),
            [99, 10, 99, 40, 70, 99]
        );
        assert_eq!(
            accessor.iter_docid_vals(&docs).collect::<Vec<_>>(),
            [(0, 99), (1, 10), (2, 99), (4, 40), (7, 70), (8, 99)]
        );
    }

    #[test]
    fn test_dedup_docid_val_pairs_consecutive() {
        let mut accessor = ColumnBlockAccessor::default();
        accessor.docid_cache = vec![0, 0, 2, 3];
        accessor.val_cache = vec![10, 10, 10, 10];
        accessor.dedup_docid_val_pairs();
        assert_eq!(accessor.docid_cache, [0, 2, 3]);
        assert_eq!(accessor.val_cache, [10, 10, 10]);
    }

    #[test]
    fn test_dedup_docid_val_pairs_non_consecutive() {
        // (0,1), (0,2), (0,1) — duplicate value not adjacent
        let mut accessor = ColumnBlockAccessor::default();
        accessor.docid_cache = vec![0, 0, 0];
        accessor.val_cache = vec![1, 2, 1];
        accessor.dedup_docid_val_pairs();
        assert_eq!(accessor.docid_cache, [0, 0]);
        assert_eq!(accessor.val_cache, [1, 2]);
    }

    #[test]
    fn test_dedup_docid_val_pairs_multi_doc() {
        // doc 0: values [3, 1, 3], doc 1: values [5, 5]
        let mut accessor = ColumnBlockAccessor::default();
        accessor.docid_cache = vec![0, 0, 0, 1, 1];
        accessor.val_cache = vec![3, 1, 3, 5, 5];
        accessor.dedup_docid_val_pairs();
        assert_eq!(accessor.docid_cache, [0, 0, 1]);
        assert_eq!(accessor.val_cache, [1, 3, 5]);
    }

    #[test]
    fn test_dedup_docid_val_pairs_no_duplicates() {
        let mut accessor = ColumnBlockAccessor::default();
        accessor.docid_cache = vec![0, 0, 1];
        accessor.val_cache = vec![1, 2, 3];
        accessor.dedup_docid_val_pairs();
        assert_eq!(accessor.docid_cache, [0, 0, 1]);
        assert_eq!(accessor.val_cache, [1, 2, 3]);
    }

    #[test]
    fn test_dedup_docid_val_pairs_single_element() {
        let mut accessor = ColumnBlockAccessor::default();
        accessor.docid_cache = vec![0];
        accessor.val_cache = vec![1];
        accessor.dedup_docid_val_pairs();
        assert_eq!(accessor.docid_cache, [0]);
        assert_eq!(accessor.val_cache, [1]);
    }

    #[test]
    fn test_fetch_block_contiguous_and_gather_match() {
        use columnar::column_index::ColumnIndex;
        use columnar::column_values::{
            serialize_and_load_u64_based_column_values, ALL_U64_CODEC_TYPES,
        };

        let vals: Vec<u64> = (0..200u64).map(|i| i * 7 + 3).collect();
        let values =
            serialize_and_load_u64_based_column_values::<u64>(&&vals[..], &ALL_U64_CODEC_TYPES);
        let column = Column {
            index: ColumnIndex::Full,
            values,
        };

        let check = |accessor: &mut ColumnBlockAccessor, docs: &[u32]| {
            accessor.fetch_block(docs, &(&column, ColumnType::U64));
            let got: Vec<(u32, u64)> = accessor.iter_docid_vals(docs).collect();
            let expected: Vec<(u32, u64)> = docs.iter().map(|&d| (d, vals[d as usize])).collect();
            assert_eq!(got, expected);
        };

        let mut accessor = ColumnBlockAccessor::default();
        // Contiguous block -> get_range fast path.
        check(&mut accessor, &(10..74).collect::<Vec<u32>>());
        // Non-contiguous block -> get_vals gather path.
        check(&mut accessor, &[0, 5, 9, 100, 199]);
        // Single doc and full span.
        check(&mut accessor, &[42]);
        check(&mut accessor, &(0..200).collect::<Vec<u32>>());
    }
}

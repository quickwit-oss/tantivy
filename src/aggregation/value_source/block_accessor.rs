use std::cmp::Ordering;

use columnar::{Cardinality, RowId};

use crate::aggregation::value_source::ValueSource;
use crate::DocId;

/// Reusable buffers to load the values associated with a block of documents from a
/// [`ValueSource`].
///
/// Regardless of their original types, values are loaded in their `u64` representation using the
/// associated monotonic mapping.
///
/// The accessor holds no queryable state of its own: each `fetch_*` method returns a view of what
/// it loaded, borrowing the accessor. This makes it impossible to read the result of a previous
/// fetch, or to interpret the buffers with a different `docs` slice than the one they were loaded
/// for.
#[derive(Debug, Default, Clone)]
pub(crate) struct ColumnBlockAccessor {
    /// Values loaded for the latest document block, in monotonic `u64` representation.
    val_cache: Vec<u64>,
    /// Document ID corresponding to each value in `val_cache` for a non-full source.
    ///
    /// Left stale by full sources: it must only be exposed for non-full blocks.
    docid_cache: Vec<DocId>,
    /// Scratch buffer used to identify documents for which a missing value must be inserted.
    missing_docids_cache: Vec<DocId>,
    /// Scratch buffer available to sources for translating document IDs into value row IDs.
    row_id_cache: Vec<RowId>,
}

impl ColumnBlockAccessor {
    /// Loads the raw block from the source, without any missing value handling or deduplication.
    #[inline]
    fn load_block<S: ValueSource + ?Sized>(
        &mut self,
        docs: &[DocId],
        source: &mut S,
    ) -> Cardinality {
        let cardinality = source.load_block(
            docs,
            &mut self.val_cache,
            &mut self.docid_cache,
            &mut self.row_id_cache,
        );
        debug_assert!(
            !cardinality.is_full() || self.val_cache.len() == docs.len(),
            "a Full source must return exactly one value per input doc"
        );
        cardinality
    }

    /// Fetches the values of a block, for consumers that do not care about which document a value
    /// belongs to (e.g. metric aggregations).
    ///
    /// `missing_opt` is appended once for every document without a value. The returned values
    /// are not in document order.
    ///
    /// Duplicate values within a document are kept.
    #[inline]
    pub(crate) fn fetch_values<S: ValueSource + ?Sized>(
        &mut self,
        docs: &[DocId],
        source: &mut S,
        missing_opt: Option<u64>,
    ) -> &[u64] {
        let cardinality = self.load_block(docs, source);
        let Some(missing) = missing_opt else {
            return &self.val_cache;
        };
        // We only need the number of documents without a value, not their IDs.
        // This relies on the `ValueSource` contract: `docid_cache` only contains docs from `docs`,
        // grouped by doc.
        let num_docs_with_values = match cardinality {
            Cardinality::Full => return &self.val_cache,
            Cardinality::Optional => self.docid_cache.len(),
            Cardinality::Multivalued => count_distinct_grouped(&self.docid_cache),
        };
        let num_missing = docs.len() - num_docs_with_values;
        self.val_cache
            .resize(self.val_cache.len() + num_missing, missing);
        &self.val_cache
    }

    /// Fetches the (doc, value) pairs of a block, adding `missing_opt` for documents without a
    /// value, and deduplicating (doc, value) pairs so that each unique value per document is
    /// returned only once.
    ///
    /// Deduplication is necessary for correct document counting in bucket aggregations, where
    /// multi-valued fields can produce duplicate entries that inflate counts.
    ///
    /// When `ordered` is true, the missing entries are inserted in document order, so that the
    /// returned docids are sorted. Otherwise, they are appended as a second run.
    #[inline]
    pub(crate) fn fetch_unique_per_doc<'acc, 'docs>(
        &'acc mut self,
        docs: &'docs [DocId],
        source: &mut dyn ValueSource,
        missing_opt: Option<u64>,
        ordered: bool,
    ) -> DocValueBlock<'acc, 'docs> {
        let cardinality = self.load_block(docs, source);
        if cardinality.is_full() {
            return DocValueBlock {
                values: &self.val_cache,
                docids: BlockDocIds::Input(docs),
                multivalued: false,
                one_value_per_doc: true,
            };
        }
        // Optional sources cannot repeat documents.
        let mut multivalued = cardinality.is_multivalue()
            && self.docid_cache.windows(2).any(|pair| pair[0] == pair[1]);
        let docids_sorted = match missing_opt {
            Some(missing) => self.add_missing(docs, cardinality, missing, ordered),
            None => true,
        };
        if multivalued {
            multivalued = self.dedup_docid_val_pairs();
        }
        // `docid_cache` is a duplicate-free subset of `docs` (when not multivalued), so having
        // the same length means it contains every doc. If it is also sorted, it is equal to
        // `docs`.
        let one_value_per_doc = !multivalued && docids_sorted && self.val_cache.len() == docs.len();
        DocValueBlock {
            values: &self.val_cache,
            docids: BlockDocIds::Loaded(&self.docid_cache),
            multivalued,
            one_value_per_doc,
        }
    }

    /// Adds `missing` for the documents of `docs` absent from `docid_cache`.
    ///
    /// Precondition: the block was loaded from a non-full source.
    ///
    /// Returns whether `docid_cache` is still sorted afterwards.
    fn add_missing(
        &mut self,
        docs: &[DocId],
        cardinality: Cardinality,
        missing: u64,
        ordered: bool,
    ) -> bool {
        debug_assert!(!cardinality.is_full());
        // We can compare docid_cache length with docs to find missing docs.
        // For multi value columns we can't rely on the length and always need to scan.
        let is_multivalue = cardinality.is_multivalue();
        if !is_multivalue && docs.len() == self.docid_cache.len() {
            return true;
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
            return true;
        }

        find_missing_docs(docs, &self.docid_cache, &mut self.missing_docids_cache);

        if !ordered {
            let docids_sorted = match (self.docid_cache.last(), self.missing_docids_cache.first()) {
                (Some(last_hit), Some(first_missing)) => last_hit < first_missing,
                _ => true,
            };
            self.val_cache.resize(
                self.val_cache.len() + self.missing_docids_cache.len(),
                missing,
            );
            self.docid_cache
                .extend_from_slice(&self.missing_docids_cache);
            return docids_sorted;
        }

        for &doc in &self.missing_docids_cache {
            let pos = self.docid_cache.partition_point(|&hit| hit < doc);
            // TODO insert by back to avoid shifting the same elements
            // self.missing_docids_cache.len() times
            self.docid_cache.insert(pos, doc);
            self.val_cache.insert(pos, missing);
        }
        true
    }

    /// Sorts values within each document when needed for deduplication.
    /// Entries must already be grouped by document. Groups of at most two values do not
    /// need sorting for deduplication, so only larger groups are sorted.
    fn sort_values_per_doc_for_dedup(&mut self) {
        let mut start = 0;
        while start < self.docid_cache.len() {
            let doc = self.docid_cache[start];
            let mut end = start + 1;
            while end < self.docid_cache.len() && self.docid_cache[end] == doc {
                end += 1;
            }
            if end - start > 2 {
                self.val_cache[start..end].sort_unstable();
            }
            start = end;
        }
    }

    /// Removes duplicate (doc_id, value) pairs from the caches.
    ///
    /// Entries must be grouped by doc_id, but values within the same doc may not be sorted
    /// (e.g. `(0,1), (0,2), (0,1)`).
    /// We sort values within each group if it has more than 2 elements, then deduplicate
    /// adjacent pairs.
    ///
    /// Returns whether any document still has multiple values after deduplication.
    fn dedup_docid_val_pairs(&mut self) -> bool {
        if self.docid_cache.is_empty() {
            return false;
        }

        self.sort_values_per_doc_for_dedup();

        // Now duplicates are adjacent — deduplicate in place.
        let mut write = 0;
        let mut multivalued = false;
        for read in 1..self.docid_cache.len() {
            if self.docid_cache[read] != self.docid_cache[write]
                || self.val_cache[read] != self.val_cache[write]
            {
                // write has not been incremented yet, so
                // self.docid_cache[write] is the last written value.
                multivalued |= self.docid_cache[read] == self.docid_cache[write];
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
        multivalued
    }
}

/// Where the document IDs of a [`DocValueBlock`] come from.
#[derive(Debug, Clone, Copy)]
enum BlockDocIds<'acc, 'docs> {
    /// Full source: values are aligned with the requested docs.
    Input(&'docs [DocId]),
    /// Non-full source: document IDs were loaded alongside the values.
    Loaded(&'acc [DocId]),
}

/// (doc, value) pairs returned by [`ColumnBlockAccessor::fetch_unique_per_doc`].
///
/// Entries are grouped by document, and each (doc, value) pair appears at most once.
#[derive(Debug, Clone)]
pub(crate) struct DocValueBlock<'acc, 'docs> {
    values: &'acc [u64],
    docids: BlockDocIds<'acc, 'docs>,
    /// Whether any document has multiple (distinct) values.
    multivalued: bool,
    /// Whether `values` contains exactly one value per requested doc, in the order of the
    /// requested docs.
    one_value_per_doc: bool,
}

impl<'acc> DocValueBlock<'acc, '_> {
    /// Returning the same doc value block without the doc ids lifetime.
    #[inline]
    pub(crate) fn try_drop_docs_lifetime(self) -> Option<DocValueBlock<'acc, 'static>> {
        let BlockDocIds::Loaded(items) = self.docids else {
            return None;
        };
        let docids: BlockDocIds<'acc, 'static> = BlockDocIds::Loaded(items);
        Some(DocValueBlock {
            values: self.values,
            docids,
            multivalued: self.multivalued,
            one_value_per_doc: self.one_value_per_doc,
        })
    }

    #[inline]
    pub(crate) fn values(&self) -> &'acc [u64] {
        self.values
    }

    /// Returns the document ID of each value. A document can occur more than once.
    #[inline]
    pub(crate) fn docids(&self) -> &[DocId] {
        match self.docids {
            BlockDocIds::Input(docids) => docids,
            BlockDocIds::Loaded(docids) => docids,
        }
    }

    #[inline]
    pub(crate) fn iter_docid_vals(&self) -> impl Iterator<Item = (DocId, u64)> + '_ {
        self.docids()
            .iter()
            .copied()
            .zip(self.values.iter().copied())
    }

    /// Whether any document has multiple values in the block.
    #[inline]
    pub(crate) fn is_multivalued(&self) -> bool {
        self.multivalued
    }

    /// Whether the block contains exactly one value per requested doc, aligned with the
    /// requested docs.
    #[inline]
    pub(crate) fn has_one_value_per_doc(&self) -> bool {
        self.one_value_per_doc
    }
}

/// Counts the distinct values of a slice in which equal values are contiguous.
fn count_distinct_grouped(docids: &[DocId]) -> usize {
    let Some(&first) = docids.first() else {
        return 0;
    };
    let mut count = 1;
    let mut previous = first;
    for &doc in &docids[1..] {
        if doc != previous {
            count += 1;
            previous = doc;
        }
    }
    count
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

    use columnar::{Column, ColumnType};

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
            &mut self,
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
        let mut source = TestValueSource {
            cardinality: Cardinality::Full,
            entries: vec![(2, 20), (4, 40), (8, 80)],
        };
        let dyn_source: &mut dyn ValueSource = &mut source;
        let mut accessor = ColumnBlockAccessor::default();

        let block = accessor.fetch_unique_per_doc(&docs, dyn_source, None, false);

        assert!(block.has_one_value_per_doc());
        assert_eq!(
            block.iter_docid_vals().collect::<Vec<_>>(),
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
    fn test_is_multivalued_checks_loaded_values() {
        let docs = [0, 1, 2];
        let mut accessor = ColumnBlockAccessor::default();
        for (entries, expected) in [
            (vec![], false),
            (vec![(0, 12)], false),
            (vec![(0, 12), (1, 15), (2, 25)], false),
            (vec![(0, 12), (2, 25)], false),
            (vec![(0, 12), (0, 15)], true),
            (vec![(0, 12), (2, 25), (2, 28)], true),
        ] {
            let mut source = TestValueSource {
                cardinality: Cardinality::Multivalued,
                entries,
            };
            let block = accessor.fetch_unique_per_doc(&docs, &mut source, None, false);
            assert_eq!(block.is_multivalued(), expected);
            let block = accessor.fetch_unique_per_doc(&docs, &mut source, Some(99), true);
            assert_eq!(block.is_multivalued(), expected);
        }

        // Duplicate values are removed before computing the flag.
        let mut source = TestValueSource {
            cardinality: Cardinality::Multivalued,
            entries: vec![(0, 12), (0, 12)],
        };
        let block = accessor.fetch_unique_per_doc(&docs, &mut source, None, false);
        assert!(!block.is_multivalued());
    }

    #[test]
    fn test_full_block_does_not_expose_stale_docids() {
        let mut accessor = ColumnBlockAccessor::default();
        let mut source = TestValueSource {
            cardinality: Cardinality::Multivalued,
            entries: vec![(0, 12), (0, 15)],
        };
        let block = accessor.fetch_unique_per_doc(&[0], &mut source, None, false);
        assert!(block.is_multivalued());

        let mut column = (full_column(&[25]), ColumnType::U64);
        let block = accessor.fetch_unique_per_doc(&[0], &mut column, None, false);
        assert!(!block.is_multivalued());
        assert_eq!(block.docids(), &[0]);
        assert_eq!(block.iter_docid_vals().collect::<Vec<_>>(), [(0, 25)]);
        assert!(block.try_drop_docs_lifetime().is_none());
    }

    #[test]
    fn test_as_column_distinguishes_the_two_kinds() {
        let column: Box<dyn ValueSource> = Box::new((full_column(&[5, 6, 7]), ColumnType::U64));
        assert!(column.as_column().is_some());
        assert_eq!(column.bounds(), Some((5, 7)));

        let computed: Box<dyn ValueSource> = Box::new(TestValueSource {
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
        let mut computed: Box<dyn ValueSource> = Box::new(TestValueSource {
            cardinality: Cardinality::Optional,
            entries: vec![(1, 11), (3, 33)],
        });
        let mut accessor = ColumnBlockAccessor::default();

        let block = accessor.fetch_unique_per_doc(&docs, &mut *computed, None, false);
        assert!(!block.has_one_value_per_doc());
        assert_eq!(
            block.iter_docid_vals().collect::<Vec<_>>(),
            [(1, 11), (3, 33)]
        );

        let block = accessor.fetch_unique_per_doc(&docs, &mut *computed, Some(99), false);
        let mut pairs = block.iter_docid_vals().collect::<Vec<_>>();
        pairs.sort_unstable();
        assert_eq!(pairs, [(0, 99), (1, 11), (2, 99), (3, 33)]);

        let mut values = accessor
            .fetch_values(&docs, &mut *computed, Some(99))
            .to_vec();
        values.sort_unstable();
        assert_eq!(values, [11, 33, 99, 99]);
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
        find_missing_docs(docs, hits, &mut missing_docs);
        assert_eq!(&missing_docs, &[1u32, 2, 3, 4, 5]);
    }

    #[test]
    fn test_source_neutral_full_block_alignment() {
        let docs = [2, 4, 8];
        let mut source = TestValueSource {
            cardinality: Cardinality::Full,
            entries: vec![(2, 20), (4, 40), (8, 80)],
        };
        let mut accessor = ColumnBlockAccessor::default();
        let block = accessor.fetch_unique_per_doc(&docs, &mut source, None, false);
        assert!(block.has_one_value_per_doc());
        assert_eq!(
            block.iter_docid_vals().collect::<Vec<_>>(),
            [(2, 20), (4, 40), (8, 80)]
        );
    }

    #[test]
    fn test_source_neutral_optional_block_with_missing() {
        let docs = [0, 1, 2, 4];
        let mut source = TestValueSource {
            cardinality: Cardinality::Optional,
            entries: vec![(1, 10), (4, 40)],
        };
        let mut accessor = ColumnBlockAccessor::default();

        let block = accessor.fetch_unique_per_doc(&docs, &mut source, Some(99), true);

        assert!(block.has_one_value_per_doc());
        assert_eq!(
            block.iter_docid_vals().collect::<Vec<_>>(),
            [(0, 99), (1, 10), (2, 99), (4, 40)]
        );
    }

    #[test]
    fn test_source_neutral_multivalue_block_deduplication() {
        let docs = [0, 1];
        let mut source = TestValueSource {
            cardinality: Cardinality::Multivalued,
            entries: vec![(0, 3), (0, 1), (0, 3), (1, 5), (1, 5)],
        };
        let mut accessor = ColumnBlockAccessor::default();

        let block = accessor.fetch_unique_per_doc(&docs, &mut source, None, false);

        assert!(!block.has_one_value_per_doc());
        assert_eq!(
            block.iter_docid_vals().collect::<Vec<_>>(),
            [(0, 1), (0, 3), (1, 5)]
        );
    }

    #[test]
    fn test_fetch_unique_per_doc_with_missing_ordered() {
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

        let block =
            accessor.fetch_unique_per_doc(&docs, &mut (&column, ColumnType::U64), Some(99), true);

        assert_eq!(block.values(), [99, 10, 99, 40, 70, 99]);
        assert_eq!(
            block.iter_docid_vals().collect::<Vec<_>>(),
            [(0, 99), (1, 10), (2, 99), (4, 40), (7, 70), (8, 99)]
        );
    }

    #[test]
    fn test_sort_values_per_doc_for_dedup() {
        let mut accessor = ColumnBlockAccessor::default();
        accessor.docid_cache = vec![0, 0, 1, 1, 1, 2, 2, 2, 2];
        accessor.val_cache = vec![3, 1, 4, 2, 4, 9, 5, 7, 5];

        accessor.sort_values_per_doc_for_dedup();

        assert_eq!(accessor.docid_cache, [0, 0, 1, 1, 1, 2, 2, 2, 2]);
        assert_eq!(accessor.val_cache, [3, 1, 2, 4, 4, 5, 5, 7, 9]);
    }

    #[test]
    fn test_dedup_docid_val_pairs_consecutive() {
        let mut accessor = ColumnBlockAccessor::default();
        accessor.docid_cache = vec![0, 0, 2, 3];
        accessor.val_cache = vec![10, 10, 10, 10];
        assert!(!accessor.dedup_docid_val_pairs());
        assert_eq!(accessor.docid_cache, [0, 2, 3]);
        assert_eq!(accessor.val_cache, [10, 10, 10]);
    }

    #[test]
    fn test_dedup_docid_val_pairs_non_consecutive() {
        // (0,1), (0,2), (0,1) — duplicate value not adjacent
        let mut accessor = ColumnBlockAccessor::default();
        accessor.docid_cache = vec![0, 0, 0];
        accessor.val_cache = vec![1, 2, 1];
        assert!(accessor.dedup_docid_val_pairs());
        assert_eq!(accessor.docid_cache, [0, 0]);
        assert_eq!(accessor.val_cache, [1, 2]);
    }

    #[test]
    fn test_dedup_docid_val_pairs_multi_doc() {
        // doc 0: values [3, 1, 3], doc 1: values [5, 5]
        let mut accessor = ColumnBlockAccessor::default();
        accessor.docid_cache = vec![0, 0, 0, 1, 1];
        accessor.val_cache = vec![3, 1, 3, 5, 5];
        assert!(accessor.dedup_docid_val_pairs());
        assert_eq!(accessor.docid_cache, [0, 0, 1]);
        assert_eq!(accessor.val_cache, [1, 3, 5]);
    }

    #[test]
    fn test_dedup_docid_val_pairs_no_duplicates() {
        let mut accessor = ColumnBlockAccessor::default();
        accessor.docid_cache = vec![0, 0, 1];
        accessor.val_cache = vec![1, 2, 3];
        assert!(accessor.dedup_docid_val_pairs());
        assert_eq!(accessor.docid_cache, [0, 0, 1]);
        assert_eq!(accessor.val_cache, [1, 2, 3]);
    }

    #[test]
    fn test_dedup_docid_val_pairs_single_element() {
        let mut accessor = ColumnBlockAccessor::default();
        accessor.docid_cache = vec![0];
        accessor.val_cache = vec![1];
        assert!(!accessor.dedup_docid_val_pairs());
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
            let block =
                accessor.fetch_unique_per_doc(docs, &mut (&column, ColumnType::U64), None, false);
            let got: Vec<(u32, u64)> = block.iter_docid_vals().collect();
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

    #[test]
    fn test_fetch_values_appends_missing() {
        let docs = [0, 1, 2, 3];
        let mut accessor = ColumnBlockAccessor::default();
        for (cardinality, entries, expected_with_missing) in [
            (
                Cardinality::Optional,
                vec![(1, 11), (3, 33)],
                vec![11, 33, 99, 99],
            ),
            (Cardinality::Optional, vec![], vec![99, 99, 99, 99]),
            (
                Cardinality::Multivalued,
                vec![(1, 11), (1, 11), (1, 12), (3, 33)],
                vec![11, 11, 12, 33, 99, 99],
            ),
            (
                Cardinality::Multivalued,
                vec![(0, 1), (1, 2), (2, 3), (3, 4), (3, 5)],
                vec![1, 2, 3, 4, 5],
            ),
            (
                Cardinality::Full,
                vec![(0, 1), (1, 2), (2, 3), (3, 4)],
                vec![1, 2, 3, 4],
            ),
        ] {
            let mut source = TestValueSource {
                cardinality,
                entries: entries.clone(),
            };
            let values_without_missing: Vec<u64> =
                entries.iter().map(|(_doc, value)| *value).collect();
            assert_eq!(
                accessor.fetch_values(&docs, &mut source, None),
                &values_without_missing[..]
            );
            assert_eq!(
                accessor.fetch_values(&docs, &mut source, Some(99)),
                &expected_with_missing[..]
            );
        }
    }

    #[test]
    fn test_has_one_value_per_doc_requires_alignment() {
        let mut accessor = ColumnBlockAccessor::default();
        // Unordered missing values appended after the hits break the alignment with `docs`...
        let mut source = TestValueSource {
            cardinality: Cardinality::Optional,
            entries: vec![(1, 11)],
        };
        let block = accessor.fetch_unique_per_doc(&[0, 1], &mut source, Some(99), false);
        assert_eq!(
            block.iter_docid_vals().collect::<Vec<_>>(),
            [(1, 11), (0, 99)]
        );
        assert!(!block.has_one_value_per_doc());
        // ... unless all of them come after the hits.
        let block = accessor.fetch_unique_per_doc(&[1, 2], &mut source, Some(99), false);
        assert_eq!(
            block.iter_docid_vals().collect::<Vec<_>>(),
            [(1, 11), (2, 99)]
        );
        assert!(block.has_one_value_per_doc());

        // Multivalued source with one value per doc, after deduplication.
        let mut source = TestValueSource {
            cardinality: Cardinality::Multivalued,
            entries: vec![(0, 1), (0, 1), (1, 2)],
        };
        let block = accessor.fetch_unique_per_doc(&[0, 1], &mut source, None, false);
        assert!(block.has_one_value_per_doc());
        let block = accessor.fetch_unique_per_doc(&[0, 1, 2], &mut source, None, false);
        assert!(!block.has_one_value_per_doc());
        let block = accessor.fetch_unique_per_doc(&[0, 1, 2], &mut source, Some(99), true);
        assert!(block.has_one_value_per_doc());
        assert_eq!(block.values(), [1, 2, 99]);
    }

    #[test]
    fn test_count_distinct_grouped() {
        assert_eq!(count_distinct_grouped(&[]), 0);
        assert_eq!(count_distinct_grouped(&[3]), 1);
        assert_eq!(count_distinct_grouped(&[3, 3, 3]), 1);
        assert_eq!(count_distinct_grouped(&[1, 3, 3, 4, 7, 7]), 4);
    }
}

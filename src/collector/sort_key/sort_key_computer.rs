use std::cmp::Ordering;

use crate::collector::sort_key::shared_threshold::SharedThresholdArcOpt;
use crate::collector::sort_key::{Comparator, NaturalComparator};
use crate::collector::sort_key_top_collector::TopBySortKeySegmentCollector;
use crate::collector::{default_collect_segment_impl, TopNComputer};
use crate::schema::Schema;
use crate::{DocId, Result, Score, SegmentOrdinal, SegmentReader};

/// A `SegmentSortKeyComputer` makes it possible to modify the default score
/// for a given document belonging to a specific segment.
///
/// It is the segment local version of the [`SortKeyComputer`].
pub trait SegmentSortKeyComputer: 'static {
    /// The final score being emitted.
    type SortKey: 'static + Send + Sync + Clone;

    /// Sort key used by at the segment level by the `SegmentSortKeyComputer`.
    ///
    /// It is typically small like a `u64`, and is meant to be converted
    /// to the final score at the end of the collection of the segment.
    type SegmentSortKey: 'static + Clone + Send + Sync + Clone;

    /// Comparator type.
    type SegmentComparator: Comparator<Self::SegmentSortKey> + 'static;

    /// Returns the segment sort key comparator.
    fn segment_comparator(&self) -> Self::SegmentComparator {
        Self::SegmentComparator::default()
    }

    /// Computes the sort key for the given document and score.
    fn segment_sort_key(&mut self, doc: DocId, score: Score) -> Self::SegmentSortKey;

    /// Computes the sort key and pushes the document in a TopN Computer.
    ///
    /// When using a tuple as the sorting key, the sort key is evaluated in a lazy manner.
    #[inline(always)]
    fn compute_sort_key_and_collect<C: Comparator<Self::SegmentSortKey>>(
        &mut self,
        doc: DocId,
        score: Score,
        top_n_computer: &mut TopNComputer<Self::SegmentSortKey, DocId, C>,
    ) {
        let sort_key = self.segment_sort_key(doc, score);
        top_n_computer.push(sort_key, doc);
    }

    /// A SegmentSortKeyComputer maps to a SegmentSortKey, but it can also decide on
    /// its ordering.
    ///
    /// This method must be consistent with the `SortKey` ordering.
    #[inline(always)]
    fn compare_segment_sort_key(
        &self,
        left: &Self::SegmentSortKey,
        right: &Self::SegmentSortKey,
    ) -> Ordering {
        self.segment_comparator().compare(left, right)
    }

    /// Implementing this method makes it possible to avoid computing
    /// a sort_key entirely if we can assess that it won't pass a threshold
    /// with a partial computation.
    ///
    /// This is currently used for lexicographic sorting.
    fn accept_sort_key_lazy(
        &mut self,
        doc_id: DocId,
        score: Score,
        threshold: &Self::SegmentSortKey,
    ) -> Option<(Ordering, Self::SegmentSortKey)> {
        let sort_key = self.segment_sort_key(doc_id, score);
        let cmp = self.compare_segment_sort_key(&sort_key, threshold);
        if cmp == Ordering::Less {
            None
        } else {
            Some((cmp, sort_key))
        }
    }

    /// Convert a segment level sort key into the global sort key.
    fn convert_segment_sort_key(&self, sort_key: Self::SegmentSortKey) -> Self::SortKey;

    /// Indicates whether the sort key computer can take advantage of the BMW (Block Max WAND)
    /// optimization over the BM25 score.
    ///
    /// If `true`, the collector will use `weight.for_each_pruning` to skip blocks of documents
    /// that cannot possibly be competitive. If `false`, it falls back to a standard
    /// document-by-document collection.
    ///
    /// Wrappers around other `SegmentSortKeyComputer`s should typically forward this method
    /// to their inner computer, or return `false` if they break the monotonic relationship
    /// with the BM25 score.
    fn supports_bm25_pruning(&self) -> bool;

    /// Extracts the BM25 score pruning threshold from the `SegmentSortKey` threshold, if
    /// applicable.
    ///
    /// This method is only called if `supports_bm25_pruning()` returns `true` and the Top-K queue
    /// has established a competitive threshold.
    ///
    /// The returned `Score` is used as the pruning threshold in
    /// `weight.for_each_pruning(threshold)`. Any blocks of documents with a maximum BM25 score
    /// strictly less than the returned threshold will be skipped entirely by the query
    /// execution.
    ///
    /// In compound sort keys (like Tuples), if secondary tie-breaking components are used,
    /// this method must return a relaxed threshold (e.g., using `Score::next_down()`) so that
    /// the secondary components have a chance to resolve the tie.
    ///
    /// Returning `None` disables pruning for the current execution.
    fn bm25_pruning_threshold(
        &self,
        threshold: &Self::SegmentSortKey,
        segment_ord: crate::SegmentOrdinal,
        threshold_ord: crate::SegmentOrdinal,
    ) -> Option<Score>;
}

/// `SortKeyComputer` defines the sort key to be used by a TopK Collector.
///
/// The `SortKeyComputer` itself does not make much of the computation itself.
/// Instead, it helps constructing `Self::Child` instances that will compute
/// the sort key at a segment scale.
pub trait SortKeyComputer: Sync {
    /// The sort key type.
    type SortKey: 'static + Send + Sync + Clone + std::fmt::Debug;
    /// Type of the associated [`SegmentSortKeyComputer`].
    type Child: SegmentSortKeyComputer<SortKey = Self::SortKey>;
    /// Comparator type.
    type Comparator: Comparator<Self::SortKey>
        + Comparator<<Self::Child as SegmentSortKeyComputer>::SegmentSortKey>
        + 'static;

    /// Checks whether the schema is compatible with the sort key computer.
    fn check_schema(&self, _schema: &Schema) -> crate::Result<()> {
        Ok(())
    }

    /// Returns the sort key comparator.
    fn comparator(&self) -> Self::Comparator {
        Self::Comparator::default()
    }

    /// Indicates whether the sort key actually uses the similarity score (by default BM25).
    /// If set to false, the similary score might not be computed (as an optimization),
    /// and the score fed in the segment sort key computer could take any value.
    fn requires_scoring(&self) -> bool {
        false
    }

    /// Returns a shared threshold that can be used to synchronize the worst score across multiple
    /// threads or segments.
    ///
    /// This is used to prune documents early during search when multiple segments are searched in
    /// parallel.
    /// By default, returns `None`.
    fn shared_threshold(
        &self,
    ) -> SharedThresholdArcOpt<
        <<Self as SortKeyComputer>::Child as SegmentSortKeyComputer>::SegmentSortKey,
    > {
        None
    }

    /// Computes the top-k results for a segment.
    ///
    /// The default implementation provides an optimized execution path that leverages Block Max
    /// WAND (BMW) pruning if the underlying `Comparator` signals support via
    /// `supports_bm25_pruning()`. Otherwise, it falls back to a standard document-by-document
    /// collection.
    fn collect_segment_top_k(
        &self,
        weight: &dyn crate::query::Weight,
        reader: &crate::SegmentReader,
        segment_collector: &mut TopBySortKeySegmentCollector<Self::Child, Self::Comparator>,
    ) -> crate::Result<()> {
        let with_scoring = self.requires_scoring();

        if !with_scoring
            || !segment_collector
                .segment_sort_key_computer
                .supports_bm25_pruning()
        {
            default_collect_segment_impl(segment_collector, weight, reader, with_scoring)?;
            return Ok(());
        }

        let top_n = &mut segment_collector.topn_computer;

        let initial_threshold = top_n.shared_threshold.as_ref().and_then(|s| s.load());

        if let Some(threshold) = initial_threshold {
            top_n.set_threshold(threshold);
        }

        let segment_ord = top_n.segment_ord;
        let segment_sort_key_computer = &mut segment_collector.segment_sort_key_computer;

        let get_pruning_threshold = |top_n: &TopNComputer<_, _, _>, comp: &Self::Child| -> Score {
            if let Some((threshold_key, threshold_ord)) = &top_n.threshold {
                comp.bm25_pruning_threshold(threshold_key, segment_ord, *threshold_ord)
                    .unwrap_or(Score::MIN)
            } else {
                Score::MIN
            }
        };

        let initial_pruning_threshold = get_pruning_threshold(top_n, segment_sort_key_computer);

        if let Some(alive_bitset) = reader.alive_bitset() {
            weight.for_each_pruning(initial_pruning_threshold, reader, &mut |doc, score| {
                if alive_bitset.is_deleted(doc) {
                    return get_pruning_threshold(top_n, segment_sort_key_computer);
                }
                segment_sort_key_computer.compute_sort_key_and_collect(doc, score, top_n);
                get_pruning_threshold(top_n, segment_sort_key_computer)
            })?;
        } else {
            weight.for_each_pruning(initial_pruning_threshold, reader, &mut |doc, score| {
                segment_sort_key_computer.compute_sort_key_and_collect(doc, score, top_n);
                get_pruning_threshold(top_n, segment_sort_key_computer)
            })?;
        }

        Ok(())
    }

    /// Builds a child sort key computer for a specific segment.
    fn segment_sort_key_computer(&self, segment_reader: &SegmentReader) -> Result<Self::Child>;
}

impl<HeadSortKeyComputer, TailSortKeyComputer> SortKeyComputer
    for (HeadSortKeyComputer, TailSortKeyComputer)
where
    HeadSortKeyComputer: SortKeyComputer,
    TailSortKeyComputer: SortKeyComputer,
{
    type SortKey = (HeadSortKeyComputer::SortKey, TailSortKeyComputer::SortKey);
    type Child = (HeadSortKeyComputer::Child, TailSortKeyComputer::Child);

    type Comparator = (
        HeadSortKeyComputer::Comparator,
        TailSortKeyComputer::Comparator,
    );

    fn comparator(&self) -> Self::Comparator {
        (self.0.comparator(), self.1.comparator())
    }

    fn segment_sort_key_computer(&self, segment_reader: &SegmentReader) -> Result<Self::Child> {
        Ok((
            self.0.segment_sort_key_computer(segment_reader)?,
            self.1.segment_sort_key_computer(segment_reader)?,
        ))
    }

    /// Checks whether the schema is compatible with the sort key computer.
    fn check_schema(&self, schema: &Schema) -> crate::Result<()> {
        self.0.check_schema(schema)?;
        self.1.check_schema(schema)?;
        Ok(())
    }

    /// Indicates whether the sort key actually uses the similarity score (by default BM25).
    /// If set to false, the similary score might not be computed (as an optimization),
    /// and the score fed in the segment sort key computer could take any value.
    fn requires_scoring(&self) -> bool {
        self.0.requires_scoring() || self.1.requires_scoring()
    }
}

impl<HeadSegmentSortKeyComputer, TailSegmentSortKeyComputer> SegmentSortKeyComputer
    for (HeadSegmentSortKeyComputer, TailSegmentSortKeyComputer)
where
    HeadSegmentSortKeyComputer: SegmentSortKeyComputer,
    TailSegmentSortKeyComputer: SegmentSortKeyComputer,
{
    type SortKey = (
        HeadSegmentSortKeyComputer::SortKey,
        TailSegmentSortKeyComputer::SortKey,
    );
    type SegmentSortKey = (
        HeadSegmentSortKeyComputer::SegmentSortKey,
        TailSegmentSortKeyComputer::SegmentSortKey,
    );

    type SegmentComparator = (
        HeadSegmentSortKeyComputer::SegmentComparator,
        TailSegmentSortKeyComputer::SegmentComparator,
    );

    /// A SegmentSortKeyComputer maps to a SegmentSortKey, but it can also decide on
    /// its ordering.
    ///
    /// By default, it uses the natural ordering.
    #[inline]
    fn compare_segment_sort_key(
        &self,
        left: &Self::SegmentSortKey,
        right: &Self::SegmentSortKey,
    ) -> Ordering {
        self.0
            .compare_segment_sort_key(&left.0, &right.0)
            .then_with(|| self.1.compare_segment_sort_key(&left.1, &right.1))
    }

    #[inline(always)]
    fn compute_sort_key_and_collect<C: Comparator<Self::SegmentSortKey>>(
        &mut self,
        doc: DocId,
        score: Score,
        top_n_computer: &mut TopNComputer<Self::SegmentSortKey, DocId, C>,
    ) {
        let sort_key: Self::SegmentSortKey;
        if let Some((threshold, _ord)) = &top_n_computer.threshold {
            if let Some((_cmp, lazy_sort_key)) = self.accept_sort_key_lazy(doc, score, threshold) {
                sort_key = lazy_sort_key;
            } else {
                return;
            }
        } else {
            sort_key = self.segment_sort_key(doc, score);
        };
        top_n_computer.append_doc(doc, sort_key);
    }

    #[inline(always)]
    fn segment_sort_key(&mut self, doc: DocId, score: Score) -> Self::SegmentSortKey {
        let head_sort_key = self.0.segment_sort_key(doc, score);
        let tail_sort_key = self.1.segment_sort_key(doc, score);
        (head_sort_key, tail_sort_key)
    }

    fn accept_sort_key_lazy(
        &mut self,
        doc_id: DocId,
        score: Score,
        threshold: &Self::SegmentSortKey,
    ) -> Option<(Ordering, Self::SegmentSortKey)> {
        let (head_threshold, tail_threshold) = threshold;
        let (head_cmp, head_sort_key) =
            self.0.accept_sort_key_lazy(doc_id, score, head_threshold)?;
        if head_cmp == Ordering::Equal {
            let (tail_cmp, tail_sort_key) =
                self.1.accept_sort_key_lazy(doc_id, score, tail_threshold)?;
            Some((tail_cmp, (head_sort_key, tail_sort_key)))
        } else {
            let tail_sort_key = self.1.segment_sort_key(doc_id, score);
            Some((head_cmp, (head_sort_key, tail_sort_key)))
        }
    }

    fn convert_segment_sort_key(&self, sort_key: Self::SegmentSortKey) -> Self::SortKey {
        let (head_sort_key, tail_sort_key) = sort_key;
        (
            self.0.convert_segment_sort_key(head_sort_key),
            self.1.convert_segment_sort_key(tail_sort_key),
        )
    }

    fn supports_bm25_pruning(&self) -> bool {
        self.0.supports_bm25_pruning()
    }

    fn bm25_pruning_threshold(
        &self,
        threshold: &Self::SegmentSortKey,
        segment_ord: SegmentOrdinal,
        _threshold_ord: SegmentOrdinal,
    ) -> Option<Score> {
        let (head_threshold, _tail_threshold) = threshold;
        // Since we have a tail, we must always evaluate documents where Head matches exactly,
        // because the Tail could still win. We simulate this by passing `SegmentOrdinal::MAX`
        // as the `threshold_ord` (so segment_ord < threshold_ord is always true for the Head).
        self.0
            .bm25_pruning_threshold(head_threshold, segment_ord, SegmentOrdinal::MAX)
    }
}

/// This struct is used as an adapter to take a sort key computer and map its score to another
/// new sort key.
pub struct MappedSegmentSortKeyComputer<T, PreviousSortKey, NewSortKey> {
    sort_key_computer: T,
    map: fn(PreviousSortKey) -> NewSortKey,
}

impl<T, PreviousScore, NewScore> SegmentSortKeyComputer
    for MappedSegmentSortKeyComputer<T, PreviousScore, NewScore>
where
    T: SegmentSortKeyComputer<SortKey = PreviousScore>,
    PreviousScore: 'static + Clone + Send + Sync,
    NewScore: 'static + Clone + Send + Sync,
{
    type SortKey = NewScore;
    type SegmentSortKey = T::SegmentSortKey;
    type SegmentComparator = T::SegmentComparator;

    fn segment_sort_key(&mut self, doc: DocId, score: Score) -> Self::SegmentSortKey {
        self.sort_key_computer.segment_sort_key(doc, score)
    }

    fn accept_sort_key_lazy(
        &mut self,
        doc_id: DocId,
        score: Score,
        threshold: &Self::SegmentSortKey,
    ) -> Option<(Ordering, Self::SegmentSortKey)> {
        self.sort_key_computer
            .accept_sort_key_lazy(doc_id, score, threshold)
    }

    #[inline(always)]
    fn compute_sort_key_and_collect<C: Comparator<Self::SegmentSortKey>>(
        &mut self,
        doc: DocId,
        score: Score,
        top_n_computer: &mut TopNComputer<Self::SegmentSortKey, DocId, C>,
    ) {
        self.sort_key_computer
            .compute_sort_key_and_collect(doc, score, top_n_computer);
    }

    fn convert_segment_sort_key(&self, segment_sort_key: Self::SegmentSortKey) -> Self::SortKey {
        (self.map)(
            self.sort_key_computer
                .convert_segment_sort_key(segment_sort_key),
        )
    }

    fn supports_bm25_pruning(&self) -> bool {
        self.sort_key_computer.supports_bm25_pruning()
    }

    fn bm25_pruning_threshold(
        &self,
        threshold: &Self::SegmentSortKey,
        segment_ord: SegmentOrdinal,
        threshold_ord: SegmentOrdinal,
    ) -> Option<Score> {
        self.sort_key_computer
            .bm25_pruning_threshold(threshold, segment_ord, threshold_ord)
    }
}

// We then re-use our (head, tail) implement and our mapper by seeing mapping any tuple (a, b, c,
// ...) as the chain (a, (b, (c, ...)))

impl<SortKeyComputer1, SortKeyComputer2, SortKeyComputer3> SortKeyComputer
    for (SortKeyComputer1, SortKeyComputer2, SortKeyComputer3)
where
    SortKeyComputer1: SortKeyComputer,
    SortKeyComputer2: SortKeyComputer,
    SortKeyComputer3: SortKeyComputer,
{
    type SortKey = (
        SortKeyComputer1::SortKey,
        SortKeyComputer2::SortKey,
        SortKeyComputer3::SortKey,
    );
    type Child = MappedSegmentSortKeyComputer<
        <(SortKeyComputer1, (SortKeyComputer2, SortKeyComputer3)) as SortKeyComputer>::Child,
        (
            SortKeyComputer1::SortKey,
            (SortKeyComputer2::SortKey, SortKeyComputer3::SortKey),
        ),
        Self::SortKey,
    >;

    type Comparator = (
        SortKeyComputer1::Comparator,
        SortKeyComputer2::Comparator,
        SortKeyComputer3::Comparator,
    );

    fn comparator(&self) -> Self::Comparator {
        (
            self.0.comparator(),
            self.1.comparator(),
            self.2.comparator(),
        )
    }

    fn segment_sort_key_computer(&self, segment_reader: &SegmentReader) -> Result<Self::Child> {
        let sort_key_computer1 = self.0.segment_sort_key_computer(segment_reader)?;
        let sort_key_computer2 = self.1.segment_sort_key_computer(segment_reader)?;
        let sort_key_computer3 = self.2.segment_sort_key_computer(segment_reader)?;
        let map = |(sort_key1, (sort_key2, sort_key3))| (sort_key1, sort_key2, sort_key3);
        Ok(MappedSegmentSortKeyComputer {
            sort_key_computer: (sort_key_computer1, (sort_key_computer2, sort_key_computer3)),
            map,
        })
    }

    fn check_schema(&self, schema: &Schema) -> crate::Result<()> {
        self.0.check_schema(schema)?;
        self.1.check_schema(schema)?;
        self.2.check_schema(schema)?;
        Ok(())
    }

    fn requires_scoring(&self) -> bool {
        self.0.requires_scoring() || self.1.requires_scoring() || self.2.requires_scoring()
    }
}

impl<SortKeyComputer1, SortKeyComputer2, SortKeyComputer3, SortKeyComputer4> SortKeyComputer
    for (
        SortKeyComputer1,
        SortKeyComputer2,
        SortKeyComputer3,
        SortKeyComputer4,
    )
where
    SortKeyComputer1: SortKeyComputer,
    SortKeyComputer2: SortKeyComputer,
    SortKeyComputer3: SortKeyComputer,
    SortKeyComputer4: SortKeyComputer,
{
    type Child = MappedSegmentSortKeyComputer<
        <(
            SortKeyComputer1,
            (SortKeyComputer2, (SortKeyComputer3, SortKeyComputer4)),
        ) as SortKeyComputer>::Child,
        (
            SortKeyComputer1::SortKey,
            (
                SortKeyComputer2::SortKey,
                (SortKeyComputer3::SortKey, SortKeyComputer4::SortKey),
            ),
        ),
        Self::SortKey,
    >;
    type SortKey = (
        SortKeyComputer1::SortKey,
        SortKeyComputer2::SortKey,
        SortKeyComputer3::SortKey,
        SortKeyComputer4::SortKey,
    );
    type Comparator = (
        SortKeyComputer1::Comparator,
        SortKeyComputer2::Comparator,
        SortKeyComputer3::Comparator,
        SortKeyComputer4::Comparator,
    );

    fn comparator(&self) -> Self::Comparator {
        (
            self.0.comparator(),
            self.1.comparator(),
            self.2.comparator(),
            self.3.comparator(),
        )
    }

    fn segment_sort_key_computer(&self, segment_reader: &SegmentReader) -> Result<Self::Child> {
        let sort_key_computer1 = self.0.segment_sort_key_computer(segment_reader)?;
        let sort_key_computer2 = self.1.segment_sort_key_computer(segment_reader)?;
        let sort_key_computer3 = self.2.segment_sort_key_computer(segment_reader)?;
        let sort_key_computer4 = self.3.segment_sort_key_computer(segment_reader)?;
        Ok(MappedSegmentSortKeyComputer {
            sort_key_computer: (
                sort_key_computer1,
                (sort_key_computer2, (sort_key_computer3, sort_key_computer4)),
            ),
            map: |(sort_key1, (sort_key2, (sort_key3, sort_key4)))| {
                (sort_key1, sort_key2, sort_key3, sort_key4)
            },
        })
    }

    fn check_schema(&self, schema: &Schema) -> crate::Result<()> {
        self.0.check_schema(schema)?;
        self.1.check_schema(schema)?;
        self.2.check_schema(schema)?;
        self.3.check_schema(schema)?;
        Ok(())
    }

    fn requires_scoring(&self) -> bool {
        self.0.requires_scoring()
            || self.1.requires_scoring()
            || self.2.requires_scoring()
            || self.3.requires_scoring()
    }
}

impl<SortKeyComputer1, SortKeyComputer2, SortKeyComputer3, SortKeyComputer4, SortKeyComputer5>
    SortKeyComputer
    for (
        SortKeyComputer1,
        SortKeyComputer2,
        SortKeyComputer3,
        SortKeyComputer4,
        SortKeyComputer5,
    )
where
    SortKeyComputer1: SortKeyComputer,
    SortKeyComputer2: SortKeyComputer,
    SortKeyComputer3: SortKeyComputer,
    SortKeyComputer4: SortKeyComputer,
    SortKeyComputer5: SortKeyComputer,
{
    type Child = MappedSegmentSortKeyComputer<
        <(
            SortKeyComputer1,
            (
                SortKeyComputer2,
                (SortKeyComputer3, (SortKeyComputer4, SortKeyComputer5)),
            ),
        ) as SortKeyComputer>::Child,
        (
            SortKeyComputer1::SortKey,
            (
                SortKeyComputer2::SortKey,
                (
                    SortKeyComputer3::SortKey,
                    (SortKeyComputer4::SortKey, SortKeyComputer5::SortKey),
                ),
            ),
        ),
        Self::SortKey,
    >;

    type SortKey = (
        SortKeyComputer1::SortKey,
        SortKeyComputer2::SortKey,
        SortKeyComputer3::SortKey,
        SortKeyComputer4::SortKey,
        SortKeyComputer5::SortKey,
    );
    type Comparator = (
        SortKeyComputer1::Comparator,
        SortKeyComputer2::Comparator,
        SortKeyComputer3::Comparator,
        SortKeyComputer4::Comparator,
        SortKeyComputer5::Comparator,
    );

    fn comparator(&self) -> Self::Comparator {
        (
            self.0.comparator(),
            self.1.comparator(),
            self.2.comparator(),
            self.3.comparator(),
            self.4.comparator(),
        )
    }

    fn segment_sort_key_computer(&self, segment_reader: &SegmentReader) -> Result<Self::Child> {
        let sort_key_computer1 = self.0.segment_sort_key_computer(segment_reader)?;
        let sort_key_computer2 = self.1.segment_sort_key_computer(segment_reader)?;
        let sort_key_computer3 = self.2.segment_sort_key_computer(segment_reader)?;
        let sort_key_computer4 = self.3.segment_sort_key_computer(segment_reader)?;
        let sort_key_computer5 = self.4.segment_sort_key_computer(segment_reader)?;
        Ok(MappedSegmentSortKeyComputer {
            sort_key_computer: (
                sort_key_computer1,
                (
                    sort_key_computer2,
                    (sort_key_computer3, (sort_key_computer4, sort_key_computer5)),
                ),
            ),
            map: |(sort_key1, (sort_key2, (sort_key3, (sort_key4, sort_key5))))| {
                (sort_key1, sort_key2, sort_key3, sort_key4, sort_key5)
            },
        })
    }

    fn check_schema(&self, schema: &Schema) -> crate::Result<()> {
        self.0.check_schema(schema)?;
        self.1.check_schema(schema)?;
        self.2.check_schema(schema)?;
        self.3.check_schema(schema)?;
        self.4.check_schema(schema)?;
        Ok(())
    }

    fn requires_scoring(&self) -> bool {
        self.0.requires_scoring()
            || self.1.requires_scoring()
            || self.2.requires_scoring()
            || self.3.requires_scoring()
            || self.4.requires_scoring()
    }
}

impl<F, SegmentF, TSortKey> SortKeyComputer for F
where
    F: 'static + Send + Sync + Fn(&SegmentReader) -> SegmentF,
    SegmentF: 'static + FnMut(DocId) -> TSortKey,
    TSortKey: 'static + PartialOrd + Clone + Send + Sync + std::fmt::Debug,
{
    type SortKey = TSortKey;
    type Child = SegmentF;
    type Comparator = NaturalComparator;

    fn segment_sort_key_computer(&self, segment_reader: &SegmentReader) -> Result<Self::Child> {
        Ok((self)(segment_reader))
    }
}

impl<F, TSortKey> SegmentSortKeyComputer for F
where
    F: 'static + FnMut(DocId) -> TSortKey,
    TSortKey: 'static + PartialOrd + Clone + Send + Sync,
{
    type SortKey = TSortKey;
    type SegmentSortKey = TSortKey;
    type SegmentComparator = NaturalComparator;

    fn segment_sort_key(&mut self, doc: DocId, _score: Score) -> TSortKey {
        (self)(doc)
    }

    /// Convert a segment level score into the global level score.
    fn convert_segment_sort_key(&self, sort_key: Self::SegmentSortKey) -> Self::SortKey {
        sort_key
    }

    fn supports_bm25_pruning(&self) -> bool {
        false
    }
    fn bm25_pruning_threshold(
        &self,
        _threshold: &Self::SegmentSortKey,
        _segment_ord: crate::SegmentOrdinal,
        _threshold_ord: crate::SegmentOrdinal,
    ) -> Option<crate::Score> {
        None
    }
}

#[cfg(test)]
mod tests {
    use std::cmp::Ordering;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
    use std::sync::Arc;

    use crate::collector::{SegmentSortKeyComputer, SortKeyComputer};
    use crate::schema::Schema;
    use crate::{DocId, Index, Order, SegmentReader};

    fn build_test_index() -> Index {
        let schema = Schema::builder().build();
        let index = Index::create_in_ram(schema);
        let mut index_writer = index.writer_for_tests().unwrap();
        index_writer
            .add_document(crate::TantivyDocument::default())
            .unwrap();
        index_writer.commit().unwrap();
        index
    }

    #[test]
    fn test_lazy_score_computer() {
        let score_computer_primary = |_segment_reader: &SegmentReader| |_doc: DocId| 200u32;
        let call_count = Arc::new(AtomicUsize::new(0));
        let call_count_clone = call_count.clone();
        let score_computer_secondary = move |_segment_reader: &SegmentReader| {
            let call_count_new_clone = call_count_clone.clone();
            move |_doc: DocId| {
                call_count_new_clone.fetch_add(1, AtomicOrdering::SeqCst);
                "b"
            }
        };
        let lazy_score_computer = (score_computer_primary, score_computer_secondary);
        let index = build_test_index();
        let searcher = index.reader().unwrap().searcher();
        let mut segment_sort_key_computer = lazy_score_computer
            .segment_sort_key_computer(searcher.segment_reader(0))
            .unwrap();
        let expected_sort_key = (200, "b");
        {
            let sort_key_opt =
                segment_sort_key_computer.accept_sort_key_lazy(0u32, 1f32, &(100u32, "a"));
            assert_eq!(sort_key_opt, Some((Ordering::Greater, expected_sort_key)));
            assert_eq!(call_count.load(AtomicOrdering::SeqCst), 1);
        }
        {
            let sort_key_opt =
                segment_sort_key_computer.accept_sort_key_lazy(0u32, 1f32, &(100u32, "c"));
            assert_eq!(sort_key_opt, Some((Ordering::Greater, expected_sort_key)));
            assert_eq!(call_count.load(AtomicOrdering::SeqCst), 2);
        }
        {
            let sort_key_opt =
                segment_sort_key_computer.accept_sort_key_lazy(0u32, 1f32, &(200u32, "a"));
            assert_eq!(sort_key_opt, Some((Ordering::Greater, expected_sort_key)));
            assert_eq!(call_count.load(AtomicOrdering::SeqCst), 3);
        }
        {
            let sort_key_opt =
                segment_sort_key_computer.accept_sort_key_lazy(0u32, 1f32, &(200u32, "c"));
            assert!(sort_key_opt.is_none());
            assert_eq!(call_count.load(AtomicOrdering::SeqCst), 4);
        }
        {
            let sort_key_opt =
                segment_sort_key_computer.accept_sort_key_lazy(0u32, 1f32, &(300u32, "a"));
            assert_eq!(sort_key_opt, None);
            assert_eq!(call_count.load(AtomicOrdering::SeqCst), 4);
        }
        {
            let sort_key_opt =
                segment_sort_key_computer.accept_sort_key_lazy(0u32, 1f32, &(300u32, "c"));
            assert_eq!(sort_key_opt, None);
            assert_eq!(call_count.load(AtomicOrdering::SeqCst), 4);
        }
        {
            let sort_key_opt =
                segment_sort_key_computer.accept_sort_key_lazy(0u32, 1f32, &expected_sort_key);
            assert_eq!(sort_key_opt, Some((Ordering::Equal, expected_sort_key)));
            assert_eq!(call_count.load(AtomicOrdering::SeqCst), 5);
        }
    }

    #[test]
    fn test_lazy_score_computer_dynamic_ordering() {
        let score_computer_primary = |_segment_reader: &SegmentReader| |_doc: DocId| 200u32;
        let call_count = Arc::new(AtomicUsize::new(0));
        let call_count_clone = call_count.clone();
        let score_computer_secondary = move |_segment_reader: &SegmentReader| {
            let call_count_new_clone = call_count_clone.clone();
            move |_doc: DocId| {
                call_count_new_clone.fetch_add(1, AtomicOrdering::SeqCst);
                2u32
            }
        };
        let lazy_score_computer = (
            (score_computer_primary, Order::Desc),
            (score_computer_secondary, Order::Asc),
        );
        let index = build_test_index();
        let searcher = index.reader().unwrap().searcher();
        let mut segment_sort_key_computer = lazy_score_computer
            .segment_sort_key_computer(searcher.segment_reader(0))
            .unwrap();
        let expected_sort_key = (200, 2u32);

        {
            let sort_key_opt =
                segment_sort_key_computer.accept_sort_key_lazy(0u32, 1f32, &(100u32, 1u32));
            assert_eq!(sort_key_opt, Some((Ordering::Greater, expected_sort_key)));
            assert_eq!(call_count.load(AtomicOrdering::SeqCst), 1);
        }
        {
            let sort_key_opt =
                segment_sort_key_computer.accept_sort_key_lazy(0u32, 1f32, &(100u32, 3u32));
            assert_eq!(sort_key_opt, Some((Ordering::Greater, expected_sort_key)));
            assert_eq!(call_count.load(AtomicOrdering::SeqCst), 2);
        }
        {
            let sort_key_opt =
                segment_sort_key_computer.accept_sort_key_lazy(0u32, 1f32, &(200u32, 1u32));
            assert!(sort_key_opt.is_none());
            assert_eq!(call_count.load(AtomicOrdering::SeqCst), 3);
        }
        {
            let sort_key_opt =
                segment_sort_key_computer.accept_sort_key_lazy(0u32, 1f32, &(200u32, 3u32));
            assert_eq!(sort_key_opt, Some((Ordering::Greater, expected_sort_key)));
            assert_eq!(call_count.load(AtomicOrdering::SeqCst), 4);
        }
        {
            let sort_key_opt =
                segment_sort_key_computer.accept_sort_key_lazy(0u32, 1f32, &(300u32, 1u32));
            assert_eq!(sort_key_opt, None);
            assert_eq!(call_count.load(AtomicOrdering::SeqCst), 4);
        }
        {
            let sort_key_opt =
                segment_sort_key_computer.accept_sort_key_lazy(0u32, 1f32, &(300u32, 3u32));
            assert_eq!(sort_key_opt, None);
            assert_eq!(call_count.load(AtomicOrdering::SeqCst), 4);
        }
        {
            let sort_key_opt =
                segment_sort_key_computer.accept_sort_key_lazy(0u32, 1f32, &expected_sort_key);
            assert_eq!(sort_key_opt, Some((Ordering::Equal, expected_sort_key)));
            assert_eq!(call_count.load(AtomicOrdering::SeqCst), 5);
        }
        assert_eq!(
            segment_sort_key_computer.convert_segment_sort_key(expected_sort_key),
            (200u32, 2u32)
        );
    }
}

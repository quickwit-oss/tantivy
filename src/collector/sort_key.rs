use std::cmp::Ordering;

use crate::{DocId, Result, Score, SegmentReader};

/// A `SegmentSortKeyComputer` makes it possible to modify the default score
/// for a given document belonging to a specific segment.
///
/// It is the segment local version of the [`SortKeyComputer`].
pub trait SegmentSortKeyComputer: 'static {
    /// The final score being emitted.
    type SortKey: 'static + PartialOrd + Send + Sync + Clone;

    /// Sort key used by at the segment level by the `SegmentSortKeyComputer`.
    ///
    /// It is typically small like a `u64`, and is meant to be converted
    /// to the final score at the end of the collection of the segment.
    type SegmentSortKey: 'static + PartialOrd + Clone + Send + Sync + Clone;

    /// Computes the sort key for the given document and score.
    fn sort_key(&mut self, doc: DocId, score: Score) -> Self::SegmentSortKey;

    /// Returns true if the `SegmentSortKeyComputer` is a good candidate for the lazy evaluation
    /// optimization. See [`SegmentSortKeyComputer::accept_score_lazy`].
    fn is_lazy() -> bool {
        false
    }

    /// Implementing this method makes it possible to avoid computing
    /// a sort_key entirely if we can assess that it won't pass a threshold
    /// with a partial computation.
    ///
    /// This is currently used for lexicographic sorting.
    ///
    /// If REVERSE_ORDER is false (resp. true),
    /// - we return None if the score is below the threshold (resp. above to the threshold)
    /// - we return Some(ordering, score) if the score is above or equal to the threshold (resp.
    ///   below or equal to)
    fn accept_sort_key_lazy<const REVERSE_ORDER: bool>(
        &mut self,
        doc_id: DocId,
        score: Score,
        threshold: &Self::SegmentSortKey,
    ) -> Option<(std::cmp::Ordering, Self::SegmentSortKey)> {
        let excluded_ordering = if REVERSE_ORDER {
            Ordering::Greater
        } else {
            Ordering::Less
        };
        let sort_key = self.sort_key(doc_id, score);
        let cmp = sort_key.partial_cmp(threshold).unwrap_or(excluded_ordering);
        if cmp == excluded_ordering {
            return None;
        } else {
            return Some((cmp, sort_key));
        }
    }

    /// Convert a segment level sort key into the global sort key.
    fn convert_segment_sort_key(&self, sort_key: Self::SegmentSortKey) -> Self::SortKey;
}

/// `SortKeyComputer` defines the sort key to be used by a TopK Collector.
///
/// The `SortKeyComputer` itself does not make much of the computation itself.
/// Instead, it helps constructing `Self::Child` instances that will compute
/// the sort key at a segment scale.
pub trait SortKeyComputer: Sync {
    /// The sort key type.
    type SortKey: 'static + Send + Sync + PartialOrd + Clone;
    /// Type of the associated [`SegmentSortKeyComputer`].
    type Child: SegmentSortKeyComputer<SortKey = Self::SortKey>;

    /// Indicates whether the sort key actually uses the similarity score (by default BM25).
    /// If set to false, the similary score might not be computed (as an optimization),
    /// and the score fed in the segment sort key computer could take any value.
    fn requires_scoring(&self) -> bool {
        false
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
    type SortKey = (
        <HeadSortKeyComputer::Child as SegmentSortKeyComputer>::SortKey,
        <TailSortKeyComputer::Child as SegmentSortKeyComputer>::SortKey,
    );
    type Child = (HeadSortKeyComputer::Child, TailSortKeyComputer::Child);

    fn segment_sort_key_computer(&self, segment_reader: &SegmentReader) -> Result<Self::Child> {
        Ok((
            self.0.segment_sort_key_computer(segment_reader)?,
            self.1.segment_sort_key_computer(segment_reader)?,
        ))
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

    fn sort_key(&mut self, doc: DocId, score: Score) -> Self::SegmentSortKey {
        let head_sort_key = self.0.sort_key(doc, score);
        let tail_sort_key = self.1.sort_key(doc, score);
        (head_sort_key, tail_sort_key)
    }

    fn accept_sort_key_lazy<const REVERSE_ORDER: bool>(
        &mut self,
        doc_id: DocId,
        score: Score,
        threshold: &Self::SegmentSortKey,
    ) -> Option<(Ordering, Self::SegmentSortKey)> {
        let (head_threshold, tail_threshold) = threshold;
        let (head_cmp, head_sort_key) =
            self.0
                .accept_sort_key_lazy::<REVERSE_ORDER>(doc_id, score, head_threshold)?;
        if head_cmp == Ordering::Equal {
            let (tail_cmp, tail_sort_key) =
                self.1
                    .accept_sort_key_lazy::<REVERSE_ORDER>(doc_id, score, tail_threshold)?;
            Some((tail_cmp, (head_sort_key, tail_sort_key)))
        } else {
            let tail_sort_key = self.1.sort_key(doc_id, score);
            Some((head_cmp, (head_sort_key, tail_sort_key)))
        }
    }

    fn is_lazy() -> bool {
        true
    }

    fn convert_segment_sort_key(&self, sort_key: Self::SegmentSortKey) -> Self::SortKey {
        let (head_sort_key, tail_sort_key) = sort_key;
        (
            self.0.convert_segment_sort_key(head_sort_key),
            self.1.convert_segment_sort_key(tail_sort_key),
        )
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
    PreviousScore: 'static + Clone + Send + Sync + PartialOrd,
    NewScore: 'static + Clone + Send + Sync + PartialOrd,
{
    type SortKey = NewScore;
    type SegmentSortKey = T::SegmentSortKey;

    fn sort_key(&mut self, doc: DocId, score: Score) -> Self::SegmentSortKey {
        self.sort_key_computer.sort_key(doc, score)
    }

    fn accept_sort_key_lazy<const REVERSE_ORDER: bool>(
        &mut self,
        doc_id: DocId,
        score: Score,
        threshold: &Self::SegmentSortKey,
    ) -> Option<(std::cmp::Ordering, Self::SegmentSortKey)> {
        self.sort_key_computer
            .accept_sort_key_lazy::<REVERSE_ORDER>(doc_id, score, threshold)
    }

    fn is_lazy() -> bool {
        T::is_lazy()
    }

    fn convert_segment_sort_key(&self, segment_sort_key: Self::SegmentSortKey) -> Self::SortKey {
        (self.map)(
            self.sort_key_computer
                .convert_segment_sort_key(segment_sort_key),
        )
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
    type Child = MappedSegmentSortKeyComputer<
        <(SortKeyComputer1, (SortKeyComputer2, SortKeyComputer3)) as SortKeyComputer>::Child,
        (
            SortKeyComputer1::SortKey,
            (SortKeyComputer2::SortKey, SortKeyComputer3::SortKey),
        ),
        Self::SortKey,
    >;
    type SortKey = (
        SortKeyComputer1::SortKey,
        SortKeyComputer2::SortKey,
        SortKeyComputer3::SortKey,
    );

    fn segment_sort_key_computer(&self, segment_reader: &SegmentReader) -> Result<Self::Child> {
        let sort_key_computer1 = self.0.segment_sort_key_computer(segment_reader)?;
        let sort_key_computer2 = self.1.segment_sort_key_computer(segment_reader)?;
        let sort_key_computer3 = self.2.segment_sort_key_computer(segment_reader)?;
        Ok(MappedSegmentSortKeyComputer {
            sort_key_computer: (sort_key_computer1, (sort_key_computer2, sort_key_computer3)),
            map: |(sort_key1, (sort_key2, sort_key3))| (sort_key1, sort_key2, sort_key3),
        })
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
}

impl<F, TSegmentSortKeyComputer> SortKeyComputer for F
where
    F: 'static + Send + Sync + Fn(&SegmentReader) -> TSegmentSortKeyComputer,
    TSegmentSortKeyComputer: SegmentSortKeyComputer,
{
    type SortKey = TSegmentSortKeyComputer::SortKey;
    type Child = TSegmentSortKeyComputer;

    fn segment_sort_key_computer(&self, segment_reader: &SegmentReader) -> Result<Self::Child> {
        Ok((self)(segment_reader))
    }
}

impl<F, TSortKey> SegmentSortKeyComputer for F
where
    F: 'static + FnMut(DocId, Score) -> TSortKey,
    TSortKey: 'static + PartialOrd + Clone + Send + Sync,
{
    type SortKey = TSortKey;
    type SegmentSortKey = TSortKey;

    fn sort_key(&mut self, doc: DocId, score: Score) -> TSortKey {
        (self)(doc, score)
    }

    /// Convert a segment level score into the global level score.
    fn convert_segment_sort_key(&self, sort_key: Self::SegmentSortKey) -> Self::SortKey {
        sort_key
    }
}

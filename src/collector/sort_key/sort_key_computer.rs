use std::cmp::Ordering;

use columnar::ValueRange;

use crate::collector::sort_key::{Comparator, NaturalComparator};
use crate::collector::sort_key_top_collector::TopBySortKeySegmentCollector;
use crate::collector::top_score_collector::push_assuming_capacity;
use crate::collector::{
    default_collect_segment_impl, ComparableDoc, SegmentCollector as _, TopNComputer,
};
use crate::schema::Schema;
use crate::{DocAddress, DocId, Result, Score, SegmentReader};

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
    type SegmentComparator: Comparator<Self::SegmentSortKey> + Clone + 'static;

    /// Buffer type used for scratch space.
    type Buffer: Default + Send + Sync + 'static;

    /// Returns the segment sort key comparator.
    fn segment_comparator(&self) -> Self::SegmentComparator {
        Self::SegmentComparator::default()
    }

    /// Computes the sort key for the given document and score.
    fn segment_sort_key(&mut self, doc: DocId, score: Score) -> Self::SegmentSortKey;

    /// Computes the sort keys for a batch of documents.
    ///
    /// The computed sort keys and document IDs are pushed into the `output` vector.
    /// The `buffer` is used for scratch space.
    fn segment_sort_keys(
        &mut self,
        input_docs: &[DocId],
        output: &mut Vec<ComparableDoc<Self::SegmentSortKey, DocId>>,
        buffer: &mut Self::Buffer,
        filter: ValueRange<Self::SegmentSortKey>,
    );

    /// Computes the sort key and pushes the document in a TopN Computer.
    ///
    /// When using a tuple as the sorting key, the sort key is evaluated in a lazy manner.
    #[inline(always)]
    fn compute_sort_key_and_collect<C: Comparator<Self::SegmentSortKey>>(
        &mut self,
        doc: DocId,
        score: Score,
        top_n_computer: &mut TopNComputer<Self::SegmentSortKey, DocId, C, Self::Buffer>,
    ) {
        let sort_key = self.segment_sort_key(doc, score);
        top_n_computer.push(sort_key, doc);
    }

    fn compute_sort_keys_and_collect<C: Comparator<Self::SegmentSortKey>>(
        &mut self,
        docs: &[DocId],
        top_n_computer: &mut TopNComputer<Self::SegmentSortKey, DocId, C, Self::Buffer>,
    ) {
        // The capacity of a TopNComputer is larger than 2*n + COLLECT_BLOCK_BUFFER_LEN, so we
        // should always be able to `reserve` space for the entire block.
        top_n_computer.reserve(docs.len());

        let comparator = self.segment_comparator();
        let value_range = if let Some(threshold) = &top_n_computer.threshold {
            comparator.threshold_to_valuerange(threshold.clone())
        } else {
            ValueRange::All
        };

        let (buffer, scratch) = top_n_computer.buffer_and_scratch();
        self.segment_sort_keys(
            docs,
            buffer,
            scratch,
            value_range,
        );


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

    /// Similar to `accept_sort_key_lazy`, but pushes results directly into the given buffer. Does
    /// not support scoring.
    ///
    /// The buffer must have at least enough capacity for `docs` matches, or this method will
    /// panic.
    fn accept_sort_key_block_lazy(
        &mut self,
        docs: &[DocId],
        threshold: &Self::SegmentSortKey,
        output: &mut Vec<ComparableDoc<Self::SegmentSortKey, DocId>>,
    ) {
        let comparator = self.segment_comparator();
        for &doc in docs {
            let sort_key = self.segment_sort_key(doc, 0.0);
            let cmp = comparator.compare(&sort_key, threshold);
            if cmp != Ordering::Less {
                push_assuming_capacity(ComparableDoc { sort_key, doc }, output);
            }
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

    /// Sorting by score has a overriding implementation for BM25 scores, using Block-WAND.
    fn collect_segment_top_k(
        &self,
        k: usize,
        weight: &dyn crate::query::Weight,
        reader: &crate::SegmentReader,
        segment_ord: u32,
    ) -> crate::Result<Vec<(Self::SortKey, DocAddress)>> {
        let with_scoring = self.requires_scoring();
        let segment_sort_key_computer = self.segment_sort_key_computer(reader)?;
        let topn_computer = TopNComputer::new_with_comparator(k, self.comparator());
        let mut segment_top_key_collector = TopBySortKeySegmentCollector {
            topn_computer,
            segment_ord,
            segment_sort_key_computer,
        };
        default_collect_segment_impl(&mut segment_top_key_collector, weight, reader, with_scoring)?;
        Ok(segment_top_key_collector.harvest())
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
    type Child =
        ChainSegmentSortKeyComputer<HeadSortKeyComputer::Child, TailSortKeyComputer::Child>;

    type Comparator = (
        HeadSortKeyComputer::Comparator,
        TailSortKeyComputer::Comparator,
    );

    fn comparator(&self) -> Self::Comparator {
        (self.0.comparator(), self.1.comparator())
    }

    fn segment_sort_key_computer(&self, segment_reader: &SegmentReader) -> Result<Self::Child> {
        Ok(ChainSegmentSortKeyComputer {
            head: self.0.segment_sort_key_computer(segment_reader)?,
            tail: self.1.segment_sort_key_computer(segment_reader)?,
        })
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

pub struct ChainSegmentSortKeyComputer<Head, Tail>
where
    Head: SegmentSortKeyComputer,
    Tail: SegmentSortKeyComputer,
{
    head: Head,
    tail: Tail,
}

pub struct ChainBuffer<HeadBuffer, TailBuffer, HeadKey, TailKey> {
    pub head: HeadBuffer,
    pub tail: TailBuffer,
    pub head_output: Vec<ComparableDoc<HeadKey, DocId>>,
    pub tail_output: Vec<ComparableDoc<TailKey, DocId>>,
    pub tail_input_docs: Vec<DocId>,
}

impl<HeadBuffer: Default, TailBuffer: Default, HeadKey, TailKey> Default
    for ChainBuffer<HeadBuffer, TailBuffer, HeadKey, TailKey>
{
    fn default() -> Self {
        ChainBuffer {
            head: HeadBuffer::default(),
            tail: TailBuffer::default(),
            head_output: Vec::new(),
            tail_output: Vec::new(),
            tail_input_docs: Vec::new(),
        }
    }
}

impl<Head, Tail> ChainSegmentSortKeyComputer<Head, Tail>
where
    Head: SegmentSortKeyComputer,
    Tail: SegmentSortKeyComputer,
{
    fn accept_sort_key_lazy(
        &mut self,
        doc_id: DocId,
        score: Score,
        threshold: &<Self as SegmentSortKeyComputer>::SegmentSortKey,
    ) -> Option<(Ordering, <Self as SegmentSortKeyComputer>::SegmentSortKey)> {
        let (head_threshold, tail_threshold) = threshold;
        let head_sort_key = self.head.segment_sort_key(doc_id, score);
        let head_cmp = self
            .head
            .compare_segment_sort_key(&head_sort_key, head_threshold);
        if head_cmp == Ordering::Less {
            None
        } else if head_cmp == Ordering::Equal {
            let tail_sort_key = self.tail.segment_sort_key(doc_id, score);
            let tail_cmp = self
                .tail
                .compare_segment_sort_key(&tail_sort_key, tail_threshold);
            if tail_cmp == Ordering::Less {
                None
            } else {
                Some((tail_cmp, (head_sort_key, tail_sort_key)))
            }
        } else {
            let tail_sort_key = self.tail.segment_sort_key(doc_id, score);
            Some((head_cmp, (head_sort_key, tail_sort_key)))
        }
    }
}

impl<Head, Tail> SegmentSortKeyComputer for ChainSegmentSortKeyComputer<Head, Tail>
where
    Head: SegmentSortKeyComputer,
    Tail: SegmentSortKeyComputer,
{
    type SortKey = (Head::SortKey, Tail::SortKey);
    type SegmentSortKey = (Head::SegmentSortKey, Tail::SegmentSortKey);

    type SegmentComparator = (Head::SegmentComparator, Tail::SegmentComparator);

    type Buffer =
        ChainBuffer<Head::Buffer, Tail::Buffer, Head::SegmentSortKey, Tail::SegmentSortKey>;

    fn segment_comparator(&self) -> Self::SegmentComparator {
        (
            self.head.segment_comparator(),
            self.tail.segment_comparator(),
        )
    }

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
        self.head
            .compare_segment_sort_key(&left.0, &right.0)
            .then_with(|| self.tail.compare_segment_sort_key(&left.1, &right.1))
    }

    fn segment_sort_keys(
        &mut self,
        _input_docs: &[DocId],
        _output: &mut Vec<ComparableDoc<Self::SegmentSortKey, DocId>>,
        _buffer: &mut Self::Buffer,
        _filter: ValueRange<Self::SegmentSortKey>,
    ) {
        unimplemented!("The head and the tail are accessed independently.");
    }

    #[inline(always)]
    fn compute_sort_key_and_collect<C: Comparator<Self::SegmentSortKey>>(
        &mut self,
        doc: DocId,
        score: Score,
        top_n_computer: &mut TopNComputer<Self::SegmentSortKey, DocId, C, Self::Buffer>,
    ) {
        let sort_key: Self::SegmentSortKey;
        if let Some(threshold) = &top_n_computer.threshold {
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

    fn compute_sort_keys_and_collect<C: Comparator<Self::SegmentSortKey>>(
        &mut self,
        docs: &[DocId],
        top_n_computer: &mut TopNComputer<Self::SegmentSortKey, DocId, C, Self::Buffer>,
    ) {
        // The capacity of a TopNComputer is larger than 2*n + COLLECT_BLOCK_BUFFER_LEN, so we
        // should always be able to `reserve` space for the entire block.
        top_n_computer.reserve(docs.len());

        let mut scratch = std::mem::take(&mut top_n_computer.scratch);

        if let Some(threshold) = &top_n_computer.threshold {
            let (head_threshold, tail_threshold) = threshold.clone();
            let head_cmp = self.head.segment_comparator();
            let tail_cmp = self.tail.segment_comparator();
            let head_filter = head_cmp.threshold_to_valuerange(head_threshold.clone());

            scratch.head_output.clear();
            self.head.segment_sort_keys(
                docs,
                &mut scratch.head_output,
                &mut scratch.head,
                head_filter,
            );

            if !scratch.head_output.is_empty() {
                scratch.tail_output.clear();
                scratch.tail_input_docs.clear();
                for cd in &scratch.head_output {
                    scratch.tail_input_docs.push(cd.doc);
                }

                self.tail.segment_sort_keys(
                    &scratch.tail_input_docs,
                    &mut scratch.tail_output,
                    &mut scratch.tail,
                    ValueRange::All,
                );

                for (head_doc, tail_doc) in scratch
                    .head_output
                    .drain(..)
                    .zip(scratch.tail_output.drain(..))
                {
                    debug_assert_eq!(head_doc.doc, tail_doc.doc);
                    let doc = head_doc.doc;
                    let head_key = head_doc.sort_key;
                    let tail_key = tail_doc.sort_key;

                    let head_ord = head_cmp.compare(&head_key, &head_threshold);
                    let ord = if head_ord == Ordering::Equal {
                        tail_cmp.compare(&tail_key, &tail_threshold)
                    } else {
                        head_ord
                    };
                    if ord == Ordering::Greater {
                        push_assuming_capacity(
                            ComparableDoc {
                                sort_key: (head_key, tail_key),
                                doc,
                            },
                            top_n_computer.buffer(),
                        );
                    }
                }
            }
        } else {
            // Eagerly push, without a threshold to compare to.
            scratch.head_output.clear();
            self.head.segment_sort_keys(
                docs,
                &mut scratch.head_output,
                &mut scratch.head,
                ValueRange::All,
            );

            scratch.tail_output.clear();
            scratch.tail_input_docs.clear();
            for cd in &scratch.head_output {
                scratch.tail_input_docs.push(cd.doc);
            }

            self.tail.segment_sort_keys(
                &scratch.tail_input_docs,
                &mut scratch.tail_output,
                &mut scratch.tail,
                ValueRange::All,
            );

            for (head_doc, tail_doc) in scratch
                .head_output
                .drain(..)
                .zip(scratch.tail_output.drain(..))
            {
                debug_assert_eq!(head_doc.doc, tail_doc.doc);
                let doc = head_doc.doc;
                let head_key = head_doc.sort_key;
                let tail_key = tail_doc.sort_key;

                // We validated at the top of the method that we have capacity.
                push_assuming_capacity(
                    ComparableDoc {
                        sort_key: (head_key, tail_key),
                        doc,
                    },
                    top_n_computer.buffer(),
                );
            }
        }
        top_n_computer.scratch = scratch;
    }

    #[inline(always)]
    fn segment_sort_key(&mut self, doc: DocId, score: Score) -> Self::SegmentSortKey {
        let head_sort_key = self.head.segment_sort_key(doc, score);
        let tail_sort_key = self.tail.segment_sort_key(doc, score);
        (head_sort_key, tail_sort_key)
    }

    fn convert_segment_sort_key(&self, sort_key: Self::SegmentSortKey) -> Self::SortKey {
        let (head_sort_key, tail_sort_key) = sort_key;
        (
            self.head.convert_segment_sort_key(head_sort_key),
            self.tail.convert_segment_sort_key(tail_sort_key),
        )
    }
}

/// This struct is used as an adapter to take a sort key computer and map its score to another
/// new sort key.
pub struct MappedSegmentSortKeyComputer<T: SegmentSortKeyComputer, NewSortKey> {
    sort_key_computer: T,
    map: fn(T::SortKey) -> NewSortKey,
}

impl<T, PreviousScore, NewScore> SegmentSortKeyComputer
    for MappedSegmentSortKeyComputer<T, NewScore>
where
    T: SegmentSortKeyComputer<SortKey = PreviousScore>,
    PreviousScore: 'static + Clone + Send + Sync,
    NewScore: 'static + Clone + Send + Sync,
{
    type SortKey = NewScore;
    type SegmentSortKey = T::SegmentSortKey;
    type SegmentComparator = T::SegmentComparator;
    type Buffer = T::Buffer;

    fn segment_comparator(&self) -> Self::SegmentComparator {
        self.sort_key_computer.segment_comparator()
    }

    fn segment_sort_key(&mut self, doc: DocId, score: Score) -> Self::SegmentSortKey {
        self.sort_key_computer.segment_sort_key(doc, score)
    }

    fn segment_sort_keys(
        &mut self,
        input_docs: &[DocId],
        output: &mut Vec<ComparableDoc<Self::SegmentSortKey, DocId>>,
        buffer: &mut Self::Buffer,
        filter: ValueRange<Self::SegmentSortKey>,
    ) {
        self.sort_key_computer
            .segment_sort_keys(input_docs, output, buffer, filter)
    }

    #[inline(always)]
    fn compute_sort_key_and_collect<C: Comparator<Self::SegmentSortKey>>(
        &mut self,
        doc: DocId,
        score: Score,
        top_n_computer: &mut TopNComputer<Self::SegmentSortKey, DocId, C, Self::Buffer>,
    ) {
        self.sort_key_computer
            .compute_sort_key_and_collect(doc, score, top_n_computer);
    }

    fn compute_sort_keys_and_collect<C: Comparator<Self::SegmentSortKey>>(
        &mut self,
        docs: &[DocId],
        top_n_computer: &mut TopNComputer<Self::SegmentSortKey, DocId, C, Self::Buffer>,
    ) {
        self.sort_key_computer
            .compute_sort_keys_and_collect(docs, top_n_computer);
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
    type SortKey = (
        SortKeyComputer1::SortKey,
        SortKeyComputer2::SortKey,
        SortKeyComputer3::SortKey,
    );
    type Child = MappedSegmentSortKeyComputer<
        <(SortKeyComputer1, (SortKeyComputer2, SortKeyComputer3)) as SortKeyComputer>::Child,
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
            sort_key_computer: ChainSegmentSortKeyComputer {
                head: sort_key_computer1,
                tail: ChainSegmentSortKeyComputer {
                    head: sort_key_computer2,
                    tail: sort_key_computer3,
                },
            },
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

    fn segment_sort_key_computer(&self, segment_reader: &SegmentReader) -> Result<Self::Child> {
        let sort_key_computer1 = self.0.segment_sort_key_computer(segment_reader)?;
        let sort_key_computer2 = self.1.segment_sort_key_computer(segment_reader)?;
        let sort_key_computer3 = self.2.segment_sort_key_computer(segment_reader)?;
        let sort_key_computer4 = self.3.segment_sort_key_computer(segment_reader)?;
        Ok(MappedSegmentSortKeyComputer {
            sort_key_computer: ChainSegmentSortKeyComputer {
                head: sort_key_computer1,
                tail: ChainSegmentSortKeyComputer {
                    head: sort_key_computer2,
                    tail: ChainSegmentSortKeyComputer {
                        head: sort_key_computer3,
                        tail: sort_key_computer4,
                    },
                },
            },
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

use std::marker::PhantomData;

pub struct FuncSegmentSortKeyComputer<F, TSortKey> {
    func: F,
    _phantom: PhantomData<TSortKey>,
}



impl<F, SegmentF, TSortKey> SortKeyComputer for F
where
    F: 'static + Send + Sync + Fn(&SegmentReader) -> SegmentF,
    SegmentF: 'static + FnMut(DocId) -> TSortKey,
    TSortKey: 'static + PartialOrd + Clone + Send + Sync + std::fmt::Debug,
{
    type SortKey = TSortKey;
    type Child = FuncSegmentSortKeyComputer<SegmentF, TSortKey>;
    type Comparator = NaturalComparator;

    fn segment_sort_key_computer(&self, segment_reader: &SegmentReader) -> Result<Self::Child> {
        Ok(FuncSegmentSortKeyComputer {
            func: (self)(segment_reader),
            _phantom: PhantomData,
        })
    }
}

impl<F, TSortKey> SegmentSortKeyComputer for FuncSegmentSortKeyComputer<F, TSortKey>
where
    F: 'static + FnMut(DocId) -> TSortKey,
    TSortKey: 'static + PartialOrd + Clone + Send + Sync,
{
    type SortKey = TSortKey;
    type SegmentSortKey = TSortKey;
    type SegmentComparator = NaturalComparator;
    type Buffer = ();

    fn segment_sort_key(&mut self, doc: DocId, _score: Score) -> TSortKey {
        (self.func)(doc)
    }

    fn segment_sort_keys(
        &mut self,
        input_docs: &[DocId],
        output: &mut Vec<ComparableDoc<Self::SegmentSortKey, DocId>>,
        _buffer: &mut Self::Buffer,
        _filter: ValueRange<Self::SegmentSortKey>,
    ) {
        for &doc in input_docs {
            output.push(ComparableDoc {
                sort_key: (self.func)(doc),
                doc,
            });
        }
    }

    /// Convert a segment level score into the global level score.
    fn convert_segment_sort_key(&self, sort_key: Self::SegmentSortKey) -> Self::SortKey {
        sort_key
    }
}

pub(crate) fn range_contains_none(range: &ValueRange<Option<u64>>) -> bool {
    match range {
        ValueRange::All => true,
        ValueRange::Inclusive(r) => r.contains(&None),
        ValueRange::GreaterThan(threshold, match_nulls) => *match_nulls || (None > *threshold),
        ValueRange::LessThan(threshold, match_nulls) => *match_nulls || (None < *threshold),
    }
}

pub(crate) fn convert_optional_u64_range_to_u64_range(
    range: ValueRange<Option<u64>>,
) -> ValueRange<u64> {
    if range_contains_none(&range) {
        return ValueRange::All;
    }
    match range {
        ValueRange::Inclusive(r) => {
            let start = r.start().unwrap_or(0);
            let end = r.end().unwrap_or(u64::MAX);
            ValueRange::Inclusive(start..=end)
        }
        ValueRange::GreaterThan(Some(val), _match_nulls) => ValueRange::GreaterThan(val, false),
        ValueRange::GreaterThan(None, _match_nulls) => ValueRange::Inclusive(u64::MIN..=u64::MAX),
        ValueRange::LessThan(None, _match_nulls) => ValueRange::Inclusive(1..=0),
        _ => ValueRange::All,
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

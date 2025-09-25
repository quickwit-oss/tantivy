use std::cmp::Ordering;
use std::marker::PhantomData;
use std::ops::Deref;
use std::sync::Arc;

use columnar::{Column, ColumnType, ColumnValues, DynamicColumn, MonotonicallyMappableToU64};

use crate::collector::{Collector, ComparableDoc, SegmentCollector};
use crate::fastfield::{FastFieldNotAvailableError, FastValue};
use crate::schema::OwnedValue;
use crate::{DateTime, DocAddress, DocId, Order, Score, SegmentOrdinal, SegmentReader};

struct LazyTopNComputer<C, const REVERSE_ORDER: bool = true>
where C: TopNCompare
{
    comparator: C,
    /// The buffer reverses sort order to get top-semantics instead of bottom-semantics
    buffer: Vec<ComparableDoc<C::Accepted, DocId, REVERSE_ORDER>>,
    top_n: usize,
    pub(crate) threshold: Option<ComparableDoc<C::Accepted, DocId, REVERSE_ORDER>>,
}

impl<C, const R: bool> LazyTopNComputer<C, R>
where C: TopNCompare
{
    /// Create a new `LazyTopNComputer`.
    /// Internally it will allocate a buffer of size `2 * top_n`.
    pub fn new(comparator: C, top_n: usize) -> Self {
        let vec_cap = top_n.max(1) * 2;
        LazyTopNComputer {
            comparator,
            buffer: Vec::with_capacity(vec_cap),
            top_n,
            threshold: None,
        }
    }

    /// Push a new document to the top n.
    /// If the document is below the current threshold, it will be ignored.
    #[inline]
    pub fn push(&mut self, score: Score, doc: DocId) {
        let feature = if let Some(last_median) = self.threshold.as_ref() {
            let Some(accepted) =
                self.comparator
                    .accept(&last_median.feature, last_median.doc, score, doc)
            else {
                return;
            };
            accepted
        } else {
            self.comparator.get(score, doc)
        };

        let comparable_doc = ComparableDoc { doc, feature };

        if self.buffer.len() == self.buffer.capacity() {
            self.truncate_top_n();
        }

        // This is faster since it avoids the buffer resizing to be inlined from vec.push()
        // (this is in the hot path)
        // TODO: Replace with `push_within_capacity` when it's stabilized
        let uninit = self.buffer.spare_capacity_mut();
        // This cannot panic, because we truncate_median will at least remove one element, since
        // the min capacity is 2.
        uninit[0].write(comparable_doc);
        // This is safe because it would panic in the line above
        unsafe {
            self.buffer.set_len(self.buffer.len() + 1);
        }
    }

    #[inline(never)]
    fn truncate_top_n(&mut self) {
        // Use select_nth_unstable to find the top nth score
        let (_, median_el, _) = self.buffer.select_nth_unstable(self.top_n);

        let median = median_el.clone();
        // Remove all elements below the top_n
        self.buffer.truncate(self.top_n);

        self.threshold = Some(median);
    }

    /// Returns the top n elements in sorted order.
    pub fn into_sorted_vec(mut self) -> Vec<ComparableDoc<C::Accepted, DocId, R>> {
        if self.buffer.len() > self.top_n {
            self.truncate_top_n();
        }
        self.buffer.sort_unstable();
        self.buffer
    }
}

/// A `Feature` can be thought of as a column of data which can be used to order documents.
///
/// `Feature` implementations are provided for sorting by document `Score` (`ScoreFeature`), or
/// by a fast field (`FieldFeature`).
pub trait Feature: Sync + Send + 'static {
    /// The output type of the feature, which is the type that will be returned to the user.
    type Output: Clone + Sync + Send + 'static;
    /// The segment output type of the feature, which is the type that will be used for
    /// comparisons within a segment.
    type SegmentOutput: Clone + PartialOrd + Sync + Send + 'static;

    /// True if this Feature is, or is derived from a bm25 Score.
    fn is_score(&self) -> bool;

    /// Open a FeatureColumn for this Feature.
    fn open(&self, segment_reader: &SegmentReader) -> crate::Result<FeatureColumn>;

    /// Get the value for this Feature from its associated FeatureColumn at the given DocId.
    fn get(
        &self,
        column: &FeatureColumn,
        order: Order,
        doc: DocId,
        score: Score,
    ) -> Self::SegmentOutput;

    /// Decode SegmentOutputs into Outputs.
    fn decode(
        &self,
        column: &FeatureColumn,
        order: Order,
        // TODO: This would ideally be lazy, but that is tricky to make dyn-safe:
        // `Box<dyn Iterator>` implicitly requires that the Iterator is static, which defeats the
        // purpose.
        segment_output: Vec<Self::SegmentOutput>,
    ) -> Vec<Self::Output>;

    /// Return an Ordering for the given comparison.
    ///
    /// NOTE: We don't require a `PartialOrd` bound on the output type in order to make it possible
    /// to use a boxed type like `OwnedValue` without giving it a `PartialOrd` implementation which
    /// might be unsafe (i.e.: panicing) in other positions.
    fn compare(&self, a: &Self::Output, b: &Self::Output) -> Option<Ordering>;
}

struct ErasedFeature<F: Feature>(F);

/// A (partial) implementation of PartialOrd for OwnedValue.
///
/// Intended for use within columns of homogenous types, and so will panic for OwnedValues with
/// mismatched types. The one exception is Null, for which we do define all comparisons.
fn owned_value_partial_cmp(a: &OwnedValue, b: &OwnedValue) -> Option<std::cmp::Ordering> {
    match (a, b) {
        (OwnedValue::Null, OwnedValue::Null) => None,
        (OwnedValue::Null, _) => Some(std::cmp::Ordering::Less),
        (_, OwnedValue::Null) => Some(std::cmp::Ordering::Greater),
        (OwnedValue::Str(a), OwnedValue::Str(b)) => a.partial_cmp(b),
        (OwnedValue::PreTokStr(a), OwnedValue::PreTokStr(b)) => a.partial_cmp(b),
        (OwnedValue::U64(a), OwnedValue::U64(b)) => a.partial_cmp(b),
        (OwnedValue::I64(a), OwnedValue::I64(b)) => a.partial_cmp(b),
        (OwnedValue::F64(a), OwnedValue::F64(b)) => a.partial_cmp(b),
        (OwnedValue::Bool(a), OwnedValue::Bool(b)) => a.partial_cmp(b),
        (OwnedValue::Date(a), OwnedValue::Date(b)) => a.partial_cmp(b),
        (OwnedValue::Facet(a), OwnedValue::Facet(b)) => a.partial_cmp(b),
        (OwnedValue::Bytes(a), OwnedValue::Bytes(b)) => a.partial_cmp(b),
        (OwnedValue::IpAddr(a), OwnedValue::IpAddr(b)) => a.partial_cmp(b),
        x => panic!("Unsupported comparison: {x:?}"),
    }
}

impl Feature for Arc<dyn Feature<Output = OwnedValue, SegmentOutput = Option<u64>>> {
    type Output = OwnedValue;
    type SegmentOutput = Option<u64>;

    fn is_score(&self) -> bool {
        self.deref().is_score()
    }

    /// Open a FeatureColumn for this Feature.
    fn open(&self, segment_reader: &SegmentReader) -> crate::Result<FeatureColumn> {
        self.deref().open(segment_reader)
    }

    /// Get the value for this Feature from its associated FeatureColumn at the given DocId.
    fn get(
        &self,
        column: &FeatureColumn,
        order: Order,
        doc: DocId,
        score: Score,
    ) -> Self::SegmentOutput {
        self.deref().get(column, order, doc, score)
    }

    /// Decode SegmentOutputs into Outputs.
    fn decode(
        &self,
        column: &FeatureColumn,
        order: Order,
        segment_output: Vec<Self::SegmentOutput>,
    ) -> Vec<Self::Output> {
        self.deref().decode(column, order, segment_output)
    }

    fn compare(&self, a: &Self::Output, b: &Self::Output) -> Option<Ordering> {
        owned_value_partial_cmp(a, b)
    }
}

/// A `Feature` that sorts by the document's score.
#[derive(Clone)]
pub struct ScoreFeature;

impl ScoreFeature {
    /// Erase the type of the feature, and return it as a boxed trait object.
    pub fn erased(self) -> Arc<dyn Feature<Output = OwnedValue, SegmentOutput = Option<u64>>> {
        Arc::new(ErasedFeature(self))
    }
}

impl Feature for ScoreFeature {
    type Output = Score;
    type SegmentOutput = Score;

    fn is_score(&self) -> bool {
        true
    }

    fn open(&self, _segment_reader: &SegmentReader) -> crate::Result<FeatureColumn> {
        Ok(FeatureColumn::Score)
    }

    fn get(
        &self,
        _column: &FeatureColumn,
        order: Order,
        _doc: DocId,
        score: Score,
    ) -> Self::SegmentOutput {
        if order.is_asc() {
            -score
        } else {
            score
        }
    }

    fn decode(
        &self,
        _column: &FeatureColumn,
        order: Order,
        segment_output: Vec<Self::SegmentOutput>,
    ) -> Vec<Self::Output> {
        if order.is_asc() {
            segment_output.into_iter().map(|score| -score).collect()
        } else {
            segment_output
        }
    }

    fn compare(&self, a: &Self::Output, b: &Self::Output) -> Option<Ordering> {
        a.partial_cmp(b)
    }
}

impl Feature for ErasedFeature<ScoreFeature> {
    type Output = OwnedValue;
    type SegmentOutput = Option<u64>;

    fn is_score(&self) -> bool {
        true
    }

    /// Open a FeatureColumn for this Feature.
    fn open(&self, segment_reader: &SegmentReader) -> crate::Result<FeatureColumn> {
        self.0.open(segment_reader)
    }

    /// Get the value for this Feature from its associated FeatureColumn at the given DocId.
    fn get(
        &self,
        column: &FeatureColumn,
        order: Order,
        doc: DocId,
        score: Score,
    ) -> Self::SegmentOutput {
        Some((self.0.get(column, order, doc, score) as f64).to_u64())
    }

    /// Decode SegmentOutputs into Outputs.
    fn decode(
        &self,
        _column: &FeatureColumn,
        order: Order,
        segment_output: Vec<Self::SegmentOutput>,
    ) -> Vec<Self::Output> {
        if order.is_asc() {
            segment_output
                .into_iter()
                .map(|v| match v {
                    Some(v) => OwnedValue::F64(-(f64::from_u64(v))),
                    None => OwnedValue::Null,
                })
                .collect()
        } else {
            segment_output
                .into_iter()
                .map(|v| match v {
                    Some(v) => OwnedValue::F64(f64::from_u64(v)),
                    None => OwnedValue::Null,
                })
                .collect()
        }
    }

    fn compare(&self, a: &Self::Output, b: &Self::Output) -> Option<Ordering> {
        owned_value_partial_cmp(a, b)
    }
}

/// A `Feature` that sorts by a fast field.
#[derive(Clone)]
pub struct FieldFeature<T>
where T: Clone + PartialOrd + Sync + Send + 'static
{
    field: String,
    score: Option<Score>,
    _output_type: PhantomData<T>,
}

impl FieldFeature<String> {
    /// Creates a new `FieldFeature` for a string fast field.
    pub fn string(field: impl AsRef<str>) -> Self {
        Self {
            field: field.as_ref().to_owned(),
            score: None,
            _output_type: PhantomData,
        }
    }

    /// Erase the type of the feature, and return it as a boxed trait object.
    pub fn erased(self) -> Arc<dyn Feature<Output = OwnedValue, SegmentOutput = Option<u64>>> {
        Arc::new(ErasedFeature(self))
    }
}

impl Feature for FieldFeature<String> {
    type Output = (Option<String>, Option<Score>);
    type SegmentOutput = (u64, Option<Score>);

    fn is_score(&self) -> bool {
        self.score.is_some()
    }

    fn open(&self, segment_reader: &SegmentReader) -> crate::Result<FeatureColumn> {
        // We interpret this field as u64, regardless of its type, that way,
        // we avoid needless conversion. Regardless of the fast field type, the
        // mapping is monotonic, so it is sufficient to compute our top-K docs.
        //
        // The conversion will then happen only on the top-K docs for each segment.
        let sort_column_opt = segment_reader.fast_fields().u64_lenient(&self.field)?;
        let (sort_column, _sort_column_type) =
            sort_column_opt.ok_or_else(|| FastFieldNotAvailableError {
                field_name: self.field.to_owned(),
            })?;

        let dynamic_column = segment_reader
            .fast_fields()
            .dynamic_column_handle(&self.field, ColumnType::Str)?
            .ok_or_else(|| FastFieldNotAvailableError {
                field_name: self.field.to_owned(),
            })?
            .open()?;
        let default_value = u64::MAX;
        Ok(FeatureColumn::String(
            dynamic_column,
            sort_column.first_or_default_col(default_value),
        ))
    }

    fn get(
        &self,
        column: &FeatureColumn,
        order: Order,
        doc: DocId,
        score: Score,
    ) -> Self::SegmentOutput {
        let FeatureColumn::String(_, sort_column) = column else {
            panic!("Field column type does not match field definition type.");
        };
        let value = sort_column.get_val(doc);
        if order.is_desc() {
            (value, Some(score))
        } else {
            (u64::MAX - value, Some(score))
        }
    }

    fn decode(
        &self,
        column: &FeatureColumn,
        order: Order,
        segment_output: Vec<Self::SegmentOutput>,
    ) -> Vec<Self::Output> {
        let FeatureColumn::String(DynamicColumn::Str(ff), _) = column else {
            panic!("Unexpected column type.");
        };

        // In the presence of a compound sort, the ordinals will not already be in their declared
        // order. Sort them for use with `sorted_ords_to_term_cb`.
        let mut ordinals: Vec<_> = if order.is_asc() {
            segment_output
                .into_iter()
                .map(|(term_ord, score)| (u64::MAX - term_ord, score))
                .enumerate()
                .collect()
        } else {
            segment_output.into_iter().rev().enumerate().collect()
        };
        ordinals.sort_unstable_by_key(|(_, (ord, _))| *ord);

        // Handle trailing nulls.
        let end_idx = if matches!(ordinals.last(), Some((_, (u64::MAX, _)))) {
            ordinals.partition_point(|(_, (ord, _))| *ord < u64::MAX)
        } else {
            ordinals.len()
        };

        // Collect terms.
        let mut terms = Vec::with_capacity(ordinals.len());
        let result = ff.dictionary().sorted_ords_to_term_cb(
            ordinals[0..end_idx].iter().map(|(_, (ord, _))| *ord),
            |term| {
                terms.push(
                    std::str::from_utf8(term)
                        .expect("Failed to decode term as unicode")
                        .to_owned(),
                );
                Ok(())
            },
        );
        assert!(
            result.expect("Failed to read terms from term dictionary"),
            "Not all terms were matched in segment."
        );

        // Rearrange back to row order.
        let mut result = Vec::with_capacity(terms.len());
        result.resize_with(ordinals.len(), || (None, None));
        for ((idx, (_, score)), term) in ordinals.into_iter().zip(terms.into_iter()) {
            result[idx] = (Some(term), score);
        }

        if order.is_desc() {
            result.reverse()
        }

        result
    }

    fn compare(&self, a: &Self::Output, b: &Self::Output) -> Option<Ordering> {
        a.partial_cmp(b)
    }
}

impl Feature for ErasedFeature<FieldFeature<String>> {
    type Output = OwnedValue;
    type SegmentOutput = Option<u64>;

    fn is_score(&self) -> bool {
        self.0.is_score()
    }

    /// Open a FeatureColumn for this Feature.
    fn open(&self, segment_reader: &SegmentReader) -> crate::Result<FeatureColumn> {
        self.0.open(segment_reader)
    }

    /// Get the value for this Feature from its associated FeatureColumn at the given DocId.
    fn get(
        &self,
        column: &FeatureColumn,
        order: Order,
        doc: DocId,
        score: Score,
    ) -> Self::SegmentOutput {
        Some(self.0.get(column, order, doc, score).0)
    }

    /// Decode SegmentOutputs into Outputs.
    fn decode(
        &self,
        column: &FeatureColumn,
        order: Order,
        segment_output: Vec<Self::SegmentOutput>,
    ) -> Vec<Self::Output> {
        self.0
            .decode(
                column,
                order,
                segment_output.into_iter().map(|s| (s.expect("An erased String feature never produces None."), None)).collect(),
            )
            .into_iter()
            .map(|(s, _)| match s {
                Some(s) => OwnedValue::Str(s),
                None => OwnedValue::Null,
            })
            .collect()
    }

    fn compare(&self, a: &Self::Output, b: &Self::Output) -> Option<Ordering> {
        owned_value_partial_cmp(a, b)
    }
}

impl FieldFeature<u64> {
    /// Creates a new `FieldFeature` for a u64 fast field.
    pub fn u64(field: impl AsRef<str>) -> Self {
        Self {
            field: field.as_ref().to_owned(),
            score: None,
            _output_type: PhantomData,
        }
    }
}

impl FieldFeature<i64> {
    /// Creates a new `FieldFeature` for a i64 fast field.
    pub fn i64(field: impl AsRef<str>) -> Self {
        Self {
            field: field.as_ref().to_owned(),
            score: None,
            _output_type: PhantomData,
        }
    }
}

impl FieldFeature<f64> {
    /// Creates a new `FieldFeature` for a f64 fast field.
    pub fn f64(field: impl AsRef<str>) -> Self {
        Self {
            field: field.as_ref().to_owned(),
            score: None,
            _output_type: PhantomData,
        }
    }
}

impl FieldFeature<bool> {
    /// Creates a new `FieldFeature` for a bool fast field.
    pub fn bool(field: impl AsRef<str>) -> Self {
        Self {
            field: field.as_ref().to_owned(),
            score: None,
            _output_type: PhantomData,
        }
    }
}

impl FieldFeature<DateTime> {
    /// Creates a new `FieldFeature` for a datetime fast field.
    pub fn datetime(field: impl AsRef<str>) -> Self {
        Self {
            field: field.as_ref().to_owned(),
            score: None,
            _output_type: PhantomData,
        }
    }
}

impl<F: FastValue> FieldFeature<F> {
    /// Erase the type of the feature, and return it as a boxed trait object.
    pub fn erased(self) -> Arc<dyn Feature<Output = OwnedValue, SegmentOutput = Option<u64>>> {
        Arc::new(ErasedFeature(self))
    }
}

impl<F: FastValue> Feature for FieldFeature<F> {
    type Output = (Option<F>, Option<Score>);
    type SegmentOutput = (Option<u64>, Option<Score>);

    fn is_score(&self) -> bool {
        self.score.is_some()
    }

    fn open(&self, segment_reader: &SegmentReader) -> crate::Result<FeatureColumn> {
        // We interpret this field as u64, regardless of its type, that way,
        // we avoid needless conversion. Regardless of the fast field type, the
        // mapping is monotonic, so it is sufficient to compute our top-K docs.
        //
        // The conversion will then happen only on the top-K docs for each segment.
        let sort_column_opt = segment_reader.fast_fields().u64_lenient(&self.field)?;
        let (sort_column, _sort_column_type) =
            sort_column_opt.ok_or_else(|| FastFieldNotAvailableError {
                field_name: self.field.to_owned(),
            })?;
        Ok(FeatureColumn::Numeric(sort_column))
    }

    fn get(
        &self,
        column: &FeatureColumn,
        order: Order,
        doc: DocId,
        score: Score,
    ) -> Self::SegmentOutput {
        let FeatureColumn::Numeric(sort_column) = column else {
            panic!("Field column type does not match field definition type.");
        };
        let value = sort_column.first(doc);
        if order.is_desc() {
            (value, Some(score))
        } else {
            (value.map(|v| u64::MAX - v), Some(score))
        }
    }

    fn decode(
        &self,
        _column: &FeatureColumn,
        order: Order,
        segment_output: Vec<Self::SegmentOutput>,
    ) -> Vec<Self::Output> {
        if order.is_desc() {
            segment_output
                .into_iter()
                .map(|(v, s)| (v.map(F::from_u64), s))
                .collect()
        } else {
            segment_output
                .into_iter()
                .map(|(v, s)| (v.map(|v| F::from_u64(u64::MAX - v)), s))
                .collect()
        }
    }

    fn compare(&self, a: &Self::Output, b: &Self::Output) -> Option<Ordering> {
        a.partial_cmp(b)
    }
}

impl<F: FastValue> Feature for ErasedFeature<FieldFeature<F>> {
    type Output = OwnedValue;
    type SegmentOutput = Option<u64>;

    fn is_score(&self) -> bool {
        self.0.is_score()
    }

    /// Open a FeatureColumn for this Feature.
    fn open(&self, segment_reader: &SegmentReader) -> crate::Result<FeatureColumn> {
        self.0.open(segment_reader)
    }

    /// Get the value for this Feature from its associated FeatureColumn at the given DocId.
    fn get(
        &self,
        column: &FeatureColumn,
        order: Order,
        doc: DocId,
        score: Score,
    ) -> Self::SegmentOutput {
        self.0.get(column, order, doc, score).0
    }

    /// Decode SegmentOutputs into Outputs.
    fn decode(
        &self,
        _column: &FeatureColumn,
        order: Order,
        segment_output: Vec<Self::SegmentOutput>,
    ) -> Vec<Self::Output> {
        if order.is_desc() {
            segment_output
                .into_iter()
                .map(|v| match v {
                    Some(v) => OwnedValue::U64(v),
                    None => OwnedValue::Null,
                })
                .collect()
        } else {
            segment_output
                .into_iter()
                .map(|v| match v {
                    Some(v) => OwnedValue::U64(u64::MAX - v),
                    None => OwnedValue::Null,
                })
                .collect()
        }
    }

    fn compare(&self, a: &Self::Output, b: &Self::Output) -> Option<Ordering> {
        owned_value_partial_cmp(a, b)
    }
}

pub enum FeatureColumn {
    Score,
    Numeric(Column<u64>),
    String(DynamicColumn, Arc<dyn ColumnValues<u64>>),
}

pub trait TopOrderable: Sync + Send + 'static {
    type Output: Clone + Sync + Send + 'static;
    type SegmentOutput: Clone + PartialOrd + Sync + Send + 'static;
    type SegmentComparator: TopNCompare<Accepted = Self::SegmentOutput>;

    /// True if scores are required for any of the FeatureColumns.
    fn requires_scoring(&self) -> bool;

    /// Returns a per-segment TopNCompare instance.
    fn segment_comparator(
        &self,
        segment_reader: &SegmentReader,
    ) -> crate::Result<Self::SegmentComparator>;

    /// For each column, open a FeatureColumn.
    fn feature_columns(
        &self,
        segment_reader: &SegmentReader,
    ) -> impl Iterator<Item = crate::Result<(FeatureColumn, Order)>>;

    /// Decode per-segment batches of SegmentOutputs into a single batch of the Output type.
    fn decode(
        &self,
        features: &Vec<(FeatureColumn, Order)>,
        segment_output: Vec<(Self::SegmentOutput, DocAddress)>,
    ) -> Vec<(Self::Output, DocAddress)>;

    /// Compare the Output types, falling back to the DocAddress if necessary.
    fn compare(&self, a: &(Self::Output, DocAddress), b: &(Self::Output, DocAddress)) -> bool;
}

pub struct TopOrderableSegmentCollector<O: TopOrderable> {
    segment_ord: SegmentOrdinal,
    topn_computer: LazyTopNComputer<O::SegmentComparator>,
    orderable: Arc<O>,
    features: Vec<(FeatureColumn, Order)>,
}

impl<O: TopOrderable> SegmentCollector for TopOrderableSegmentCollector<O> {
    type Fruit = Vec<(O::Output, DocAddress)>;

    #[inline]
    fn collect(&mut self, doc: DocId, score: Score) {
        self.topn_computer.push(score, doc);
    }

    fn harvest(self) -> Vec<(O::Output, DocAddress)> {
        let segment_ord = self.segment_ord;
        // TODO: Switch to unsorted, a-la https://github.com/quickwit-oss/tantivy/pull/2646
        let harvested = self
            .topn_computer
            .into_sorted_vec()
            .into_iter()
            .map(|comparable_doc| {
                (
                    comparable_doc.feature,
                    DocAddress {
                        segment_ord,
                        doc_id: comparable_doc.doc,
                    },
                )
            })
            .collect();
        self.orderable.decode(&self.features, harvested)
    }
}

pub(crate) struct TopOrderableCollector<O: TopOrderable> {
    orderable: Arc<O>,
    limit: usize,
    offset: usize,
}

impl<O: TopOrderable> TopOrderableCollector<O> {
    pub(crate) fn new(orderable: O, limit: usize, offset: usize) -> TopOrderableCollector<O> {
        Self {
            orderable: Arc::new(orderable),
            limit,
            offset,
        }
    }
}

impl<O: TopOrderable> Collector for TopOrderableCollector<O> {
    type Fruit = Vec<(O::Output, DocAddress)>;

    type Child = TopOrderableSegmentCollector<O>;

    fn for_segment(
        &self,
        segment_ord: SegmentOrdinal,
        segment_reader: &SegmentReader,
    ) -> crate::Result<Self::Child> {
        Ok(TopOrderableSegmentCollector {
            segment_ord,
            topn_computer: LazyTopNComputer::new(
                self.orderable.segment_comparator(segment_reader)?,
                self.limit + self.offset,
            ),
            orderable: self.orderable.clone(),
            features: self
                .orderable
                .feature_columns(segment_reader)
                .collect::<crate::Result<Vec<_>>>()?,
        })
    }

    fn requires_scoring(&self) -> bool {
        true
    }

    fn merge_fruits(&self, segment_fruits: Vec<Self::Fruit>) -> crate::Result<Self::Fruit> {
        let merged = itertools::kmerge_by(
            segment_fruits,
            |a: &(O::Output, DocAddress), b: &(O::Output, DocAddress)| self.orderable.compare(a, b),
        )
        .collect::<Vec<_>>();

        Ok(merged
            .into_iter()
            .skip(self.offset)
            .take(self.limit)
            .collect())
    }
}

/// A recursive macro to collect the remaining values using the `get` method.
///
/// This macro is called by `accept_features!` when a feature returns
/// `Acceptance::Greater`. It recursively calls `get` on the rest of the
/// features in the tuple and returns a new tuple with the results.
macro_rules! get_remaining {
    // Base case for 1 collected value.
    (
        $doc_id:expr,
        $score:expr,
        (),
        $collected:expr
    ) => {
        ( $collected , )
    };

    // Base case for 0 or N collected values.
    (
        $doc_id:expr,
        $score:expr,
        ()
        $(, $collected:expr )*
    ) => {
        ( $( $collected ),* )
    };

    // Recursive step: Get the value for the current feature, then recurse
    // with the rest of the features and the new value added to the accumulator.
    (
        $doc_id:expr,
        $score:expr,
        ( ($idx:expr, $threshold:expr, $feature:expr, $column:expr, $order:expr) $(, $tail:tt )* )
        $(, $collected:expr )*
    ) => {
        paste::paste! {
            let [<value_ $idx>] = $feature.get($column, $order.clone(), $doc_id, $score);
        }
        get_remaining! {
            $doc_id,
            $score,
            ( $( $tail ),* )
            $(, $collected )*,
            paste::paste! {
                [<value_ $idx>]
            }
        }
    };
}

/// The main recursive macro to iterate over features and thresholds.
///
/// This macro takes a features tuple, and local variables `doc_id` and `score`.
/// It iterates through the features tuple, calling the `accept` method on each
/// feature. The recursion continues as long as `Acceptance::Equal` is returned.
/// If `Acceptance::Greater` is returned, it calls the `get_remaining!` macro
/// to collect the rest of the values.
macro_rules! accept_features {
    // Entry point for the macro. It sets up the initial accumulator for the collected values.
    (
        $features:tt,
        $doc_id:expr,
        $score:expr
    ) => {{
        accept_features! {
            @loop
            $features,
            $doc_id,
            $score
        }
    }};

    // Base case for 1 collected value.
    (
        @loop
        (),
        $doc_id:expr,
        $score:expr,
        $collected:expr
    ) => {
        ( $collected , )
    };

    // Base case for 0 or N collected values.
    (
        @loop
        (),
        $doc_id:expr,
        $score:expr
        $(, $collected:expr )*
    ) => {
        ( $( $collected ),* )
    };

    // Recursive Step: Handle the next item in the tuple.
    (
        @loop
        ( ($idx:expr, $threshold:expr, $feature:expr, $column:expr, $order:expr) $(, $tail_features:tt )* ),
        $doc_id:expr,
        $score:expr
        $(, $collected:expr )*
    ) => {
        paste::paste! {
            let [<value_ $idx>] = $feature.get($column, $order.clone(), $doc_id, $score);
            match [<value_ $idx>].partial_cmp(&$threshold) {
                Some(Ordering::Equal) | None => {
                    // Continue comparing.
                },
                Some(Ordering::Greater) => {
                    // This value is greater than the threshold. Call `get_remaining!`
                    // to immediately collect the rest of the values, and return them.
                    return Some({
                        get_remaining! {
                            $doc_id,
                            $score,
                            ( $( $tail_features ),* )
                            $(, $collected )*,
                            [<value_ $idx>]
                        }
                    })
                },
                Some(Ordering::Less) => return None,
            };

            // Recurse to the next item, adding the new value to the accumulator.
            accept_features! {
                @loop
                ( $( $tail_features ),* ),
                $doc_id,
                $score
                $(, $collected )*,
                [<value_ $idx>]
            }
        }
    };
}

macro_rules! impl_top_orderable {
    ( $( ($T:ident, $idx:tt) ),+ ) => {
        impl<$($T: Feature + Clone),+> TopOrderable for ( $(($T, Order)),+ ,) {
            type Output = ( $($T::Output),+ ,);
            type SegmentOutput = ( $($T::SegmentOutput),+ ,);
            type SegmentComparator = ( $( ($T, FeatureColumn, Order) ),+ ,);

            fn requires_scoring(&self) -> bool {
                // Returns true if any of the features are the score.
                false $(|| self.$idx.0.is_score())*
            }

            fn segment_comparator(
                &self,
                segment_reader: &SegmentReader,
            ) -> crate::Result<Self::SegmentComparator> {
                Ok(($(
                    (self.$idx.0.clone(), self.$idx.0.open(segment_reader)?, self.$idx.1.clone())
                ),+,))
            }

            fn feature_columns(
                &self,
                segment_reader: &SegmentReader,
            ) -> impl Iterator<Item = crate::Result<(FeatureColumn, Order)>> {
                // Collects all feature columns from the tuple elements.
                [
                    $(
                        self.$idx.0.open(segment_reader).map(|fc| (fc, self.$idx.1.clone()))
                    ),+
                ]
                .into_iter()
            }

            fn decode(
                &self,
                features: &Vec<(FeatureColumn, Order)>,
                segment_output: Vec<(Self::SegmentOutput, DocAddress)>,
            ) -> Vec<(Self::Output, DocAddress)> {
                // Decode each feature's values separately.
                $(
                    paste::paste! {
                        let mut [<decoded_values_ $idx>] = self.$idx.0.decode(
                            &features[$idx].0,
                            features[$idx].1.clone(),
                            segment_output.iter().map(|(v, _)| v.$idx.clone()).collect(),
                        ).into_iter();
                    }
                )*

                // Zip the decoded values and doc addresses back together.
                let mut result = Vec::with_capacity(segment_output.len());
                for (_, doc_address) in segment_output {
                    let output_tuple = (
                        $(
                            paste::paste! {
                                [<decoded_values_ $idx>].next().unwrap()
                            }
                        ),+
                        ,
                    );
                    result.push((output_tuple, doc_address));
                }
                result
            }

            fn compare(&self, a: &(Self::Output, DocAddress), b: &(Self::Output, DocAddress)) -> bool {
                // Perform lexicographical comparison on the tuple elements.
                $(
                    if self.$idx.1.is_asc() {
                        match self.$idx.0.compare(&(a.0).$idx, &(b.0).$idx) {
                            Some(Ordering::Less) => return true,
                            Some(Ordering::Greater) => return false,
                            Some(Ordering::Equal) | None => {} // Fall through
                        }
                    } else {
                        match self.$idx.0.compare(&(a.0).$idx, &(b.0).$idx) {
                            Some(Ordering::Less) => return false,
                            Some(Ordering::Greater) => return true,
                            Some(Ordering::Equal) | None => {} // Fall through
                        }
                    }
                )*

                // Tie-breaker: DocAddress is always compared ascending.
                a.1 < b.1
            }
        }

        impl<$($T: Feature + Clone),+> TopNCompare for ( $( ($T, FeatureColumn, Order) ),+ ,) {
            type Accepted = ( $($T::SegmentOutput),+ ,);

            fn accept(
                &self,
                threshold_values: &Self::Accepted,
                threshold_doc_id: DocId,
                score: Score,
                doc_id: DocId,
            ) -> Option<Self::Accepted> {
                // Iterate over a tuple of tuples containing a threshold value, feature, column,
                // and order.
                let result: Self::Accepted = accept_features!(
                    ( $( ($idx, threshold_values.$idx, self.$idx.0, &self.$idx.1, self.$idx.2) ),+ ),
                    doc_id,
                    score
                );

                if threshold_doc_id < doc_id {
                    Some(result)
                } else {
                    None
                }
            }

            fn get(&self, score: Score, doc: DocId) -> Self::Accepted {
                (
                    $(
                        self.$idx.0.get(&self.$idx.1, self.$idx.2.clone(), doc, score)
                    ),+
                    ,
                )
            }
        }
    };
}

impl_top_orderable! { (F1, 0) }
impl_top_orderable! { (F1, 0), (F2, 1) }
impl_top_orderable! { (F1, 0), (F2, 1), (F3, 2) }

pub trait TopNCompare {
    // TODO: Remove the Clone bound.
    type Accepted: Clone + PartialOrd;

    /// Given the current threshold of accepted values and a candidate doc_id/score, compare the
    /// candidate value to the threshold, and convert the candidate to Accepted if it is
    /// greater-than the threshold.
    fn accept(
        &self,
        threshold_value: &Self::Accepted,
        threshold_doc_id: DocId,
        score: Score,
        doc_id: DocId,
    ) -> Option<Self::Accepted>;

    /// Get an Accepted value for the given Score and DocId.
    fn get(&self, score: Score, doc_id: DocId) -> Self::Accepted;
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use proptest::prelude::*;

    use super::{FieldFeature, ScoreFeature};
    use crate::collector::top_collector::ComparableDoc;
    use crate::collector::{DocSetCollector, TopDocs};
    use crate::indexer::NoMergePolicy;
    use crate::query::{AllQuery, QueryParser};
    use crate::schema::{Schema, FAST, TEXT};
    use crate::{DocAddress, Document, Index, Order, Score, Searcher};

    fn make_index() -> crate::Result<Index> {
        let mut schema_builder = Schema::builder();
        let id = schema_builder.add_u64_field("id", FAST);
        let city = schema_builder.add_text_field("city", TEXT | FAST);
        let catchphrase = schema_builder.add_text_field("catchphrase", TEXT);
        let altitude = schema_builder.add_f64_field("altitude", FAST);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);

        fn create_segment(index: &Index, docs: Vec<impl Document>) -> crate::Result<()> {
            let mut index_writer = index.writer_for_tests()?;
            index_writer.set_merge_policy(Box::new(NoMergePolicy));
            for doc in docs {
                index_writer.add_document(doc)?;
            }
            index_writer.commit()?;
            Ok(())
        }

        create_segment(
            &index,
            vec![
                doc!(
                    id => 0_u64,
                    city => "austin",
                    catchphrase => "Hills, Barbeque, Glow",
                    altitude => 149.0,
                ),
                doc!(
                    id => 1_u64,
                    city => "greenville",
                    catchphrase => "Grow, Glow, Glow",
                    altitude => 27.0,
                ),
            ],
        )?;
        create_segment(
            &index,
            vec![doc!(
                id => 2_u64,
                city => "tokyo",
                catchphrase => "Glow, Glow, Glow",
                altitude => 40.0,
            )],
        )?;
        create_segment(
            &index,
            vec![doc!(
                id => 3_u64,
                catchphrase => "No, No, No",
                altitude => 0.0,
            )],
        )?;
        Ok(index)
    }

    // NOTE: You cannot determine the SegmentIds that will be generated for Segments
    // ahead of time, so DocAddresses must be mapped back to a unique id for each Searcher.
    fn id_mapping(searcher: &Searcher) -> HashMap<DocAddress, u64> {
        searcher
            .search(&AllQuery, &DocSetCollector)
            .unwrap()
            .into_iter()
            .map(|doc_address| {
                let column = searcher.segment_readers()[doc_address.segment_ord as usize]
                    .fast_fields()
                    .u64("id")
                    .unwrap();
                (doc_address, column.first(doc_address.doc_id).unwrap())
            })
            .collect()
    }

    #[test]
    fn test_order_by_string() -> crate::Result<()> {
        let index = make_index()?;

        fn assert_query(
            index: &Index,
            order: Order,
            limit: usize,
            offset: usize,
            expected: Vec<(Option<String>, u64)>,
        ) -> crate::Result<()> {
            let searcher = index.reader()?.searcher();
            let ids = id_mapping(&searcher);

            // Try as primitive.
            let top_collector = TopDocs::with_limit(limit)
                .and_offset(offset)
                .order_by(((FieldFeature::string("city"), order.clone()),));
            let actual = searcher
                .search(&AllQuery, &top_collector)?
                .into_iter()
                .map(|((s,), doc)| (s, ids[&doc]))
                .collect::<Vec<_>>();
            assert_eq!(actual, expected);

            // Try as erased.
            let top_collector = TopDocs::with_limit(limit)
                .and_offset(offset)
                .order_by(((FieldFeature::string("city").erased(), order),));
            let actual = searcher
                .search(&AllQuery, &top_collector)?
                .into_iter()
                .map(|(_, doc)| ids[&doc])
                .collect::<Vec<_>>();
            assert_eq!(
                actual,
                expected.iter().map(|(_, doc)| *doc).collect::<Vec<u64>>()
            );

            Ok(())
        }

        assert_query(
            &index,
            Order::Asc,
            4,
            0,
            vec![
                (None, 3),
                (Some("austin".to_owned()), 0),
                (Some("greenville".to_owned()), 1),
                (Some("tokyo".to_owned()), 2),
            ],
        )?;

        assert_query(&index, Order::Asc, 1, 0, vec![(None, 3)])?;

        assert_query(
            &index,
            Order::Asc,
            2,
            1,
            vec![
                (Some("austin".to_owned()), 0),
                (Some("greenville".to_owned()), 1),
            ],
        )?;

        assert_query(
            &index,
            Order::Desc,
            4,
            0,
            vec![
                (Some("tokyo".to_owned()), 2),
                (Some("greenville".to_owned()), 1),
                (Some("austin".to_owned()), 0),
                (None, 3),
            ],
        )?;

        assert_query(
            &index,
            Order::Desc,
            2,
            1,
            vec![
                (Some("greenville".to_owned()), 1),
                (Some("austin".to_owned()), 0),
            ],
        )?;

        assert_query(
            &index,
            Order::Desc,
            1,
            0,
            vec![(Some("tokyo".to_owned()), 2)],
        )?;

        Ok(())
    }

    #[test]
    fn test_order_by_f64() -> crate::Result<()> {
        let index = make_index()?;

        fn assert_query(
            index: &Index,
            order: Order,
            expected: Vec<(Option<f64>, u64)>,
        ) -> crate::Result<()> {
            let searcher = index.reader()?.searcher();
            let ids = id_mapping(&searcher);

            // Try as primitive.
            let top_collector =
                TopDocs::with_limit(3).order_by(((FieldFeature::f64("altitude"), order.clone()),));
            let actual = searcher
                .search(&AllQuery, &top_collector)?
                .into_iter()
                .map(|((f,), doc)| (f, ids[&doc]))
                .collect::<Vec<_>>();
            assert_eq!(actual, expected);

            // And as erased.
            let top_collector =
                TopDocs::with_limit(3).order_by(((FieldFeature::f64("altitude").erased(), order),));
            let actual = searcher
                .search(&AllQuery, &top_collector)?
                .into_iter()
                .map(|(_, doc)| ids[&doc])
                .collect::<Vec<_>>();
            assert_eq!(
                actual,
                expected.iter().map(|(_, id)| *id).collect::<Vec<u64>>()
            );

            Ok(())
        }

        assert_query(
            &index,
            Order::Asc,
            vec![(Some(0.0), 3), (Some(27.0), 1), (Some(40.0), 2)],
        )?;

        assert_query(
            &index,
            Order::Desc,
            vec![(Some(149.0), 0), (Some(40.0), 2), (Some(27.0), 1)],
        )?;

        Ok(())
    }

    #[test]
    fn test_order_by_score() -> crate::Result<()> {
        let index = make_index()?;

        fn query(index: &Index, order: Order) -> crate::Result<Vec<((Score,), u64)>> {
            let searcher = index.reader()?.searcher();
            let ids = id_mapping(&searcher);

            let top_collector = TopDocs::with_limit(4).order_by(((ScoreFeature, order),));
            let field = index.schema().get_field("catchphrase").unwrap();
            let query_parser = QueryParser::for_index(&index, vec![field]);
            let text_query = query_parser.parse_query("glow")?;

            Ok(searcher
                .search(&text_query, &top_collector)?
                .into_iter()
                .map(|(score, doc)| (score, ids[&doc]))
                .collect())
        }

        assert_eq!(
            &query(&index, Order::Asc)?,
            &[((0.35667497,), 0), ((0.4904281,), 1), ((0.5604893,), 2),]
        );

        assert_eq!(
            &query(&index, Order::Desc)?,
            &[((0.5604893,), 2), ((0.4904281,), 1), ((0.35667497,), 0),]
        );
        Ok(())
    }

    #[test]
    fn test_order_by_score_then_string() -> crate::Result<()> {
        let index = make_index()?;

        fn query(
            index: &Index,
            score_order: Order,
            city_order: Order,
        ) -> crate::Result<Vec<((Score, Option<String>), u64)>> {
            let searcher = index.reader()?.searcher();
            let ids = id_mapping(&searcher);

            let top_collector = TopDocs::with_limit(4).order_by((
                (ScoreFeature, score_order),
                (FieldFeature::string("city"), city_order),
            ));
            Ok(searcher
                .search(&AllQuery, &top_collector)?
                .into_iter()
                .map(|(f, doc)| (f, ids[&doc]))
                .collect())
        }

        assert_eq!(
            &query(&index, Order::Asc, Order::Asc)?,
            &[
                ((1.0, None), 3),
                ((1.0, Some("austin".to_owned())), 0),
                ((1.0, Some("greenville".to_owned())), 1),
                ((1.0, Some("tokyo".to_owned())), 2),
            ]
        );

        assert_eq!(
            &query(&index, Order::Asc, Order::Desc)?,
            &[
                ((1.0, Some("tokyo".to_owned())), 2),
                ((1.0, Some("greenville".to_owned())), 1),
                ((1.0, Some("austin".to_owned())), 0),
                ((1.0, None), 3),
            ]
        );
        Ok(())
    }

    proptest! {
        #[test]
        fn test_order_by_string_prop(
          order in prop_oneof!(Just(Order::Desc), Just(Order::Asc)),
          limit in 1..64_usize,
          offset in 0..64_usize,
          segments_terms in
            proptest::collection::vec(
                proptest::collection::vec(0..32_u8, 1..32_usize),
                0..8_usize,
            )
        ) {
            let mut schema_builder = Schema::builder();
            let city = schema_builder.add_text_field("city", TEXT | FAST);
            let schema = schema_builder.build();
            let index = Index::create_in_ram(schema);
            let mut index_writer = index.writer_for_tests()?;

            // A Vec<Vec<u8>>, where the outer Vec represents segments, and the inner Vec
            // represents terms.
            for segment_terms in segments_terms.into_iter() {
                for term in segment_terms.into_iter() {
                    let term = format!("{term:0>3}");
                    index_writer.add_document(doc!(
                        city => term,
                    ))?;
                }
                index_writer.commit()?;
            }

            let searcher = index.reader()?.searcher();
            let top_n_results = searcher.search(&AllQuery, &TopDocs::with_limit(limit)
                .and_offset(offset)
                .order_by(
                    ((FieldFeature::string("city"), order.clone()),)
                ))?;
            let all_results = searcher.search(&AllQuery, &DocSetCollector)?.into_iter().map(|doc_address| {
                // Get the term for this address.
                let column = searcher.segment_readers()[doc_address.segment_ord as usize].fast_fields().str("city").unwrap().unwrap();
                let value = column.term_ords(doc_address.doc_id).next().map(|term_ord| {
                    let mut city = Vec::new();
                    column.dictionary().ord_to_term(term_ord, &mut city).unwrap();
                    String::try_from(city).unwrap()
                });
                (value, doc_address)
            });

            // Using the TopDocs collector should always be equivalent to sorting, skipping the
            // offset, and then taking the limit.
            let sorted_docs: Vec<_> = if order.is_desc() {
                let mut comparable_docs: Vec<ComparableDoc<_, _, true>> =
                    all_results.into_iter().map(|(feature, doc)| ComparableDoc { feature, doc}).collect();
                comparable_docs.sort();
                comparable_docs.into_iter().map(|cd| ((cd.feature,), cd.doc)).collect()
            } else {
                let mut comparable_docs: Vec<ComparableDoc<_, _, false>> =
                    all_results.into_iter().map(|(feature, doc)| ComparableDoc { feature, doc}).collect();
                comparable_docs.sort();
                comparable_docs.into_iter().map(|cd| ((cd.feature,), cd.doc)).collect()
            };
            let expected_docs = sorted_docs.into_iter().skip(offset).take(limit).collect::<Vec<_>>();
            prop_assert_eq!(
                expected_docs,
                top_n_results
            );
        }
    }
}

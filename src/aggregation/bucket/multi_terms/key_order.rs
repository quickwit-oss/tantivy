//! Map column values to bucket sort values and retain the first page of bucket keys.

use std::cmp::Ordering;

use columnar::{ColumnType, MonotonicallyMappableToU64};

use super::collector::{DocumentValues, MultiTermsBuckets, SourceValue};
use super::map::ArrayHeapMap;
use super::sources::{BucketKeyMapping, BucketSource, DateInterval, KeyPage, SourceAfterKey};
use super::{calendar_interval, MultiTermsAggReqData, TuplePacking};
use crate::aggregation::bucket::term_agg::{AggregationMapKey, Bucket};
use crate::aggregation::bucket::{CalendarInterval, Order};
use crate::aggregation::intermediate_agg_result::CompositeIntermediateKey;
use crate::aggregation::segment_agg_result::BucketIdProvider;
use crate::aggregation::{BucketId, ColumnBlockAccessor};
use crate::{DocId, TantivyError};

/// A bucket value encoded to sort in the requested order.
/// Each grouping rule (called a source in the composite API) contributes a value to the bucket key.
/// For example, grouping prices into intervals of 10 maps price 23 to bucket value 20.
/// This stores the bucket index 2 with sort encoding applied. Resolving the key recovers 20.
///
/// Values sort by column position, then encoded value. Positions 0 and 255 place missing values
/// first or last. Columns occupy 1..=254. Descending values are bit-flipped.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Default, Hash)]
pub(super) struct BucketSortValue {
    column_position: u8,
    encoded_value: u64,
}

impl BucketSortValue {
    fn encode_value(value: u64, column_index: usize, order: Order) -> Self {
        Self {
            column_position: column_index as u8 + 1,
            encoded_value: match order {
                Order::Asc => value,
                Order::Desc => !value,
            },
        }
    }

    fn missing(source: &BucketSource) -> Self {
        Self {
            column_position: if source.missing_first() { 0 } else { u8::MAX },
            encoded_value: 0,
        }
    }
}

pub(super) const MAX_SOURCES: usize = 16;
impl<const S: usize> AggregationMapKey for [BucketSortValue; S] {}

/// Packs bucket sort values into an array used as a bucket key.
/// For example, grouping by product and price interval produces an array of two encoded values
/// representing a key such as `{product: "shoe", price_band: 20}`.
#[derive(Clone, Debug)]
pub(super) struct BucketSortValueArrayPacking<const S: usize>;

impl<const S: usize> TuplePacking for BucketSortValueArrayPacking<S> {
    type Value = BucketSortValue;
    type Key = [BucketSortValue; S];

    fn new_key(&self) -> Self::Key {
        [BucketSortValue::default(); S]
    }

    fn clear_key(&self, _key: &mut Self::Key) {
        // Each grouping rule's value is overwritten before the key prefix is compared or collected.
    }

    fn push(&self, key: &mut Self::Key, field_idx: usize, value: Self::Value) {
        key[field_idx] = value;
    }

    fn unpack_value(&self, key: &Self::Key, field_idx: usize) -> Self::Value {
        key[field_idx]
    }
}

/// Buckets for the first `size` keys in the requested sort order.
#[derive(Debug)]
pub(super) struct KeyPageBuckets<const S: usize> {
    map: ArrayHeapMap<BucketSortValue, Bucket, S>,
    size: usize,
}

impl<const S: usize> MultiTermsBuckets<[BucketSortValue; S]> for KeyPageBuckets<S> {
    type ChildBucketId = BucketId;
    const SORTED_BY_KEY: bool = false;

    fn new(
        request: &MultiTermsAggReqData,
        _key_limit: u64,
        _bucket_id_provider: &mut BucketIdProvider,
    ) -> Self {
        let page = request.key_page.as_ref().expect("key page configured");
        Self {
            map: ArrayHeapMap::default(),
            size: page.size,
        }
    }

    fn memory_consumption(&self) -> usize {
        self.map.memory_consumption()
    }

    fn into_vec(self) -> Vec<([BucketSortValue; S], Bucket)> {
        self.map.into_iter().collect()
    }

    fn count_tuple(
        &mut self,
        key: &[BucketSortValue; S],
        bucket_id_provider: &mut BucketIdProvider,
    ) -> Option<BucketId> {
        let bucket = self.map.get_or_insert_with(key, self.size, || Bucket {
            count: 0,
            bucket_id: bucket_id_provider.next_bucket_id(),
        })?;
        bucket.count += 1;
        Some(bucket.bucket_id)
    }
}

/// Holds document IDs and the bucket values produced by one grouping rule.
/// For example, with price interval 10, document 7's prices `[12, 15]` both map to bucket 10,
/// and document 8's price `[25]` maps to bucket 20. After deduplication, `document_ids` is `[7, 8]`
/// and `sort_values` holds the encoded bucket values for `[10, 20]`.
#[derive(Debug, Default)]
pub(super) struct DocumentBucketValues {
    /// Document IDs paired with bucket sort values for sorting and deduplication.
    document_values: Vec<(DocId, BucketSortValue)>,
    /// Document IDs corresponding to the output sort values.
    /// Left empty when a single column supplies one value per input document.
    document_ids: Vec<DocId>,
    /// Sort values returned for the requested documents, deduplicated per document.
    sort_values: Vec<BucketSortValue>,
}

impl SourceValue for BucketSortValue {
    const ENCODES_KEY_ORDER: bool = true;
    type Buffer = DocumentBucketValues;

    fn buffer_memory_consumption(buffer: &DocumentBucketValues) -> usize {
        buffer.document_values.capacity() * std::mem::size_of::<(DocId, Self)>()
            + buffer.document_ids.capacity() * std::mem::size_of::<DocId>()
            + buffer.sort_values.capacity() * std::mem::size_of::<Self>()
    }

    fn fetch_field<'a>(
        req: &MultiTermsAggReqData,
        field: usize,
        docs: &[DocId],
        buffer: &'a mut DocumentBucketValues,
        block: &'a mut ColumnBlockAccessor,
    ) -> crate::Result<DocumentValues<'a, Self>> {
        let source = &req.key_page.as_ref().expect("key page configured").sources[field];
        buffer.document_values.clear();
        buffer.document_ids.clear();
        buffer.sort_values.clear();
        for (column_index, column) in source.columns.iter().enumerate() {
            block.fetch_block_with_missing(docs, &mut (&column.column, column.column_type), None);
            // A single column with one value per document yields one sort value per document.
            if source.columns.len() == 1 && block.has_one_value_per_doc(docs) {
                for &value in block.values() {
                    buffer
                        .sort_values
                        .push(map_bucket_sort_value(source, column_index, value)?);
                }
                return Ok(DocumentValues {
                    docids: None,
                    values: &buffer.sort_values,
                    multivalued: false,
                });
            }
            for (doc, value) in block.iter_docid_vals(docs) {
                buffer
                    .document_values
                    .push((doc, map_bucket_sort_value(source, column_index, value)?));
            }
        }
        // Histogram mapping can give different column values the same bucket sort value,
        // including values from different physical columns.
        buffer.document_values.sort_unstable();
        buffer.document_values.dedup();
        let mut document_values = buffer.document_values.iter().peekable();
        let mut multivalued = false;
        for &doc in docs {
            let start = buffer.sort_values.len();
            while let Some(&(_, value)) =
                document_values.next_if(|(value_doc, _)| *value_doc == doc)
            {
                buffer.document_ids.push(doc);
                buffer.sort_values.push(value);
            }
            if start == buffer.sort_values.len() && source.missing_bucket {
                buffer.document_ids.push(doc);
                buffer.sort_values.push(Self::missing(source));
            }
            multivalued |= buffer.sort_values.len() - start > 1;
        }
        Ok(DocumentValues {
            docids: Some(&buffer.document_ids),
            values: &buffer.sort_values,
            multivalued,
        })
    }

    fn missing_value(_req: &MultiTermsAggReqData, _field: usize, _doc: DocId) -> Option<Self> {
        // Missing buckets are inserted only after checking all physical columns above.
        None
    }

    fn decode(self, order: Order) -> Option<(usize, u64)> {
        if self.column_position == 0 || self.column_position == u8::MAX {
            return None;
        }
        let value = match order {
            Order::Asc => self.encoded_value,
            Order::Desc => !self.encoded_value,
        };
        Some((self.column_position as usize - 1, value))
    }
}

impl KeyPage {
    /// Whether this key prefix can follow the pagination key. Complete keys must be greater.
    pub(super) fn prefix_can_follow_after<P: TuplePacking>(
        &self,
        packing: &P,
        key: &P::Key,
        prefix_len: usize,
    ) -> bool {
        for (field, source) in self.sources.iter().take(prefix_len).enumerate() {
            let value = packing.unpack_value(key, field).decode(source.order);
            match (&source.after, value) {
                (SourceAfterKey::None, _) => return true,
                (SourceAfterKey::Missing, None) => continue,
                (SourceAfterKey::Missing, Some(_)) => return source.missing_first(),
                (SourceAfterKey::Value { .. }, None) => return !source.missing_first(),
                (SourceAfterKey::Value { column, key }, Some((column_index, value))) => {
                    if matches!(source.mapping, BucketKeyMapping::Terms) {
                        match column_index.cmp(column) {
                            Ordering::Less => return false,
                            Ordering::Greater => return true,
                            Ordering::Equal => {}
                        }
                    }
                    match key.compare_column_value(value) {
                        Ordering::Less => return source.order == Order::Desc,
                        Ordering::Greater => return source.order == Order::Asc,
                        Ordering::Equal => {}
                    }
                }
            }
        }
        prefix_len < self.sources.len()
    }

    pub(super) fn resolve_key<P: TuplePacking>(
        &self,
        packing: &P,
        key: &P::Key,
    ) -> crate::Result<Vec<CompositeIntermediateKey>> {
        self.sources
            .iter()
            .enumerate()
            .map(|(field, source)| {
                let Some((column_index, value)) =
                    packing.unpack_value(key, field).decode(source.order)
                else {
                    return Ok(CompositeIntermediateKey::Null);
                };
                match source.mapping {
                    BucketKeyMapping::Terms => {
                        let column = &source.columns[column_index];
                        if column.column_type == ColumnType::DateTime {
                            return Ok(CompositeIntermediateKey::DateTime(i64::from_u64(value)));
                        }
                        let value = super::resolve_column_value(
                            value,
                            &column.column_type,
                            &column.str_dict_column,
                            &column.column,
                        )?;
                        Ok(match value {
                            super::IntermediateKey::Str(v) => CompositeIntermediateKey::Str(v),
                            super::IntermediateKey::Bool(v) => CompositeIntermediateKey::Bool(v),
                            super::IntermediateKey::IpAddr(v) => {
                                CompositeIntermediateKey::IpAddr(v)
                            }
                            super::IntermediateKey::U64(v) => CompositeIntermediateKey::U64(v),
                            super::IntermediateKey::I64(v) => CompositeIntermediateKey::I64(v),
                            super::IntermediateKey::F64(v) => CompositeIntermediateKey::F64(v),
                        })
                    }
                    BucketKeyMapping::Histogram { interval } => Ok(CompositeIntermediateKey::F64(
                        i64::from_u64(value) as f64 * interval,
                    )),
                    BucketKeyMapping::DateHistogram { .. } => {
                        Ok(CompositeIntermediateKey::DateTime(i64::from_u64(value)))
                    }
                }
            })
            .collect()
    }
}

/// Maps a column value to a bucket sort value before deduplication.
#[inline(always)]
fn map_bucket_sort_value(
    source: &BucketSource,
    column_index: usize,
    value: u64,
) -> crate::Result<BucketSortValue> {
    let bucket = match source.mapping {
        BucketKeyMapping::Terms => {
            return Ok(BucketSortValue::encode_value(
                value,
                column_index,
                source.order,
            ));
        }
        BucketKeyMapping::Histogram { interval } => {
            let value = match source.columns[column_index].column_type {
                ColumnType::U64 => value as f64,
                ColumnType::I64 => i64::from_u64(value) as f64,
                ColumnType::DateTime => i64::from_u64(value) as f64 / 1_000_000.,
                ColumnType::F64 => f64::from_u64(value),
                _ => unreachable!("unexpected histogram column type"),
            };
            (value / interval).floor() as i64
        }
        BucketKeyMapping::DateHistogram { interval } => {
            let value = i64::from_u64(value);
            match interval {
                DateInterval::FixedNanoseconds(interval) => value
                    .div_euclid(interval)
                    .checked_mul(interval)
                    .ok_or_else(|| {
                        TantivyError::InvalidArgument(
                            "date histogram bucket is outside the supported timestamp range"
                                .to_string(),
                        )
                    })?,
                DateInterval::Calendar(CalendarInterval::Year) => {
                    calendar_interval::try_year_bucket(value)?
                }
                DateInterval::Calendar(CalendarInterval::Month) => {
                    calendar_interval::try_month_bucket(value)?
                }
                DateInterval::Calendar(CalendarInterval::Week) => {
                    calendar_interval::week_bucket(value)
                }
            }
        }
    };
    Ok(BucketSortValue::encode_value(
        bucket.to_u64(),
        0,
        source.order,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aggregation::agg_data::AggregationsSegmentCtx;
    use crate::aggregation::bucket::multi_terms::SegmentMultiTermsCollector;
    use crate::aggregation::bucket::CompositeAggregation;
    use crate::aggregation::buffered_sub_aggs::HighCardSubAggBuffer;
    use crate::aggregation::segment_agg_result::SegmentAggregationCollector;
    use crate::aggregation::AggregationLimitsGuard;
    use crate::schema::{Schema, FAST};
    use crate::Index;

    #[test]
    fn key_pages_support_every_source_count() -> crate::Result<()> {
        let mut schema = Schema::builder();
        let values = schema.add_u64_field("values", FAST);
        let index = Index::create_in_ram(schema.build());
        let mut writer = index.writer_for_tests()?;
        writer.add_document(crate::doc!(values => 1u64, values => 2u64))?;
        writer.commit()?;
        for count in 0..=MAX_SOURCES + 1 {
            let sources: Vec<_> = (0..count)
                .map(|i| serde_json::json!({format!("s{i}"): {"terms": {"field": "values"}}}))
                .collect();
            let request = serde_json::from_value(serde_json::json!({"page": {"composite": {
                "sources": sources, "size": 2
            }}}))?;
            let result = crate::aggregation::tests::exec_request(request, &index);
            if count == 0 || count > MAX_SOURCES {
                assert!(result.is_err());
                continue;
            }
            let result = result?;
            let buckets = result["page"]["buckets"].as_array().unwrap();
            assert_eq!(buckets.len(), 2);
            for (position, bucket) in buckets.iter().enumerate() {
                assert_eq!(bucket["doc_count"], 1);
                assert_eq!(bucket["key"].as_object().unwrap().len(), count);
                for i in 0..count {
                    assert_eq!(
                        bucket["key"][format!("s{i}")],
                        1 + usize::from(i == count - 1) * position
                    );
                }
            }
        }
        Ok(())
    }

    #[test]
    fn descending_page_keeps_max_value_after_another_type() -> crate::Result<()> {
        let mut schema = Schema::builder();
        let values = schema.add_u64_field("values", FAST);
        let index = Index::create_in_ram(schema.build());
        let mut writer = index.writer_for_tests()?;
        writer.add_document(crate::doc!(values => u64::MAX))?;
        writer.commit()?;
        // Dates precede numbers in descending type order. No date column is present,
        // so every numeric value follows this pagination key, including u64::MAX.
        let request = serde_json::from_value(serde_json::json!({"page": {"composite": {
            "sources": [{"value": {"terms": {"field": "values", "order": "desc"}}}],
            "size": 1,
            "after": {"value": "dt:0"}
        }}}))?;
        let result = crate::aggregation::tests::exec_request(request, &index)?;
        assert_eq!(
            result["page"]["buckets"],
            serde_json::json!([
                {"key": {"value": u64::MAX}, "doc_count": 1}
            ])
        );
        Ok(())
    }

    #[test]
    fn key_page_limits_buckets_and_expansion_across_blocks() -> crate::Result<()> {
        let mut schema = Schema::builder();
        let values = schema.add_u64_field("values", FAST);
        let index = Index::create_in_ram(schema.build());
        let mut writer = index.writer_for_tests()?;
        for _ in 0..130 {
            let mut doc = crate::TantivyDocument::default();
            for value in (0..32).rev() {
                doc.add_u64(values, value);
            }
            writer.add_document(doc)?;
        }
        writer.commit()?;
        let searcher = index.reader()?.searcher();
        let reader = searcher.segment_reader(0);
        let request: CompositeAggregation = serde_json::from_value(serde_json::json!({
            "sources": [
                {"first": {"terms": {"field": "values"}}},
                {"price": {"histogram": {"field": "values", "interval": 10}}},
                {"last": {"terms": {"field": "values"}}}
            ],
            "size": 2,
            "after": {"first": "u64:20", "price": "f64:99", "last": "u64:0"}
        }))?;
        let mut collector =
            SegmentMultiTermsCollector::<_, KeyPageBuckets<3>, HighCardSubAggBuffer>::new(
                request.build_multi_terms_segment_request("page", reader)?,
                BucketSortValueArrayPacking::<3>,
                0,
                None,
            );
        let mut ctx = AggregationsSegmentCtx::new(Default::default());
        // Reusing buffers across blocks must not charge their capacity again.
        ctx.context.limits = AggregationLimitsGuard::new(Some(256 * 1024), None);
        let docs: Vec<_> = (0..130).collect();
        for block in docs.chunks(64) {
            collector.collect(0, block, &mut ctx)?;
            assert!(collector.keys_buf.len() <= block.len() * 3);
            assert_eq!(collector.parent_buckets[0].map.size(), 2);
        }
        let page = collector.req_data.key_page.as_ref().unwrap();
        let mut buckets = collector.parent_buckets.pop().unwrap().into_vec();
        buckets.sort_unstable_by(|left, right| left.0.cmp(&right.0));
        for ((key, bucket), suffix) in buckets.into_iter().zip([0, 1]) {
            assert_eq!(bucket.count, 130);
            assert_eq!(
                page.resolve_key(&collector.packing, &key)?,
                vec![
                    CompositeIntermediateKey::U64(21),
                    CompositeIntermediateKey::F64(0.0),
                    CompositeIntermediateKey::U64(suffix),
                ]
            );
        }

        // The two retained buckets fit, but expanding the block exceeds the scratch budget.
        let mut collector =
            SegmentMultiTermsCollector::<_, KeyPageBuckets<3>, HighCardSubAggBuffer>::new(
                request.build_multi_terms_segment_request("page", reader)?,
                BucketSortValueArrayPacking::<3>,
                0,
                None,
            );
        ctx.context.limits = AggregationLimitsGuard::new(Some(64 * 1024), None);
        assert!(matches!(
            collector.collect(0, &docs[..64], &mut ctx),
            Err(TantivyError::AggregationError(
                crate::aggregation::AggregationError::MemoryExceeded { .. }
            ))
        ));
        // The limit is checked after collection, including buffer capacity growth.
        assert_eq!(collector.keys_buf.len(), 64 * 3);
        assert_eq!(collector.parent_buckets[0].map.size(), 2);
        Ok(())
    }
}

//! Values used to construct bucket keys and bucket storage for multi-terms.

use std::fmt::Debug;

use super::{fetch_field_block, missing_value_for_doc, MultiTermsAggReqData};
use crate::aggregation::bucket::term_agg::{AggregationMapKey, Bucket, TermAggregationMap};
use crate::aggregation::bucket::{BucketIdSlot, Order};
use crate::aggregation::segment_agg_result::BucketIdProvider;
use crate::aggregation::ColumnBlockAccessor;
use crate::DocId;

/// One grouping rule's bucket key values, deduplicated per document.
/// Document IDs are ascending, with each document's values grouped together.
/// For example, document IDs `[7, 7, 8]` and values `[10, 20, 20]` assign two values to document 7
/// and one to document 8. When `docids` is `None`, each input document has one value at the same
/// position in `values`.
pub(crate) struct DocumentValues<'a, V> {
    pub docids: Option<&'a [DocId]>,
    pub values: &'a [V],
    pub multivalued: bool,
}

impl<V: Copy> DocumentValues<'_, V> {
    pub fn has_one_value_per_doc(&self, docs: &[DocId]) -> bool {
        self.values.len() == docs.len() && self.docids.is_none_or(|ids| ids == docs)
    }

    pub fn iter<'a>(&'a self, docs: &'a [DocId]) -> impl Iterator<Item = (DocId, V)> + 'a {
        self.docids
            .unwrap_or(docs)
            .iter()
            .copied()
            .zip(self.values.iter().copied())
    }
}

/// Stores buckets for multi-terms aggregation and counts documents for each combination of field
/// values. Implementations may retain all combinations or a page of keys in the requested sort
/// order.
pub(crate) trait MultiTermsBuckets<TupleKey>: Debug + 'static {
    /// Bucket ID used by child aggregations, or `()` when there are no children.
    type ChildBucketId: BucketIdSlot;

    /// Whether `into_vec` returns buckets in ascending key order.
    const SORTED_BY_KEY: bool;

    /// Creates bucket storage for the request. `key_limit` sizes storage as described by
    /// [`TermAggregationMap::new`]. Page-of-keys storage uses the request's page size instead.
    fn new(
        request: &MultiTermsAggReqData,
        key_limit: u64,
        bucket_id_provider: &mut BucketIdProvider,
    ) -> Self;

    /// Estimates storage memory consumption in bytes.
    fn memory_consumption(&self) -> usize;

    /// Increments the tuple's count and returns its child bucket ID.
    /// Returns `None` when the tuple falls outside the retained page of keys.
    fn count_tuple(
        &mut self,
        key: &TupleKey,
        bucket_id_provider: &mut BucketIdProvider,
    ) -> Option<Self::ChildBucketId>;

    /// Consumes storage and returns keys paired with their buckets.
    fn into_vec(self) -> Vec<(TupleKey, Bucket<Self::ChildBucketId>)>;
}

impl<TupleKey: AggregationMapKey, BucketMap: TermAggregationMap<TupleKey>>
    MultiTermsBuckets<TupleKey> for BucketMap
{
    type ChildBucketId = <BucketMap as TermAggregationMap<TupleKey>>::ChildBucketId;

    const SORTED_BY_KEY: bool = BucketMap::SORTED_BY_KEY;

    fn new(
        _request: &MultiTermsAggReqData,
        key_limit: u64,
        bucket_id_provider: &mut BucketIdProvider,
    ) -> Self {
        BucketMap::new(key_limit, bucket_id_provider)
    }

    fn into_vec(self) -> Vec<(TupleKey, Bucket<Self::ChildBucketId>)> {
        TermAggregationMap::into_vec(self)
    }

    fn memory_consumption(&self) -> usize {
        self.get_memory_consumption()
    }

    #[inline]
    fn count_tuple(
        &mut self,
        key: &TupleKey,
        bucket_id_provider: &mut BucketIdProvider,
    ) -> Option<Self::ChildBucketId> {
        Some(self.term_entry(key.clone(), bucket_id_provider))
    }
}

/// A value contributed by one grouping rule to a bucket key.
///
/// For terms, `u64` holds the column's encoding of a field value. For example, an unsigned price
/// of 23 contributes 23. These values borrow the column block during loading.
/// [`super::key_order::BucketSortValue`] holds a mapped bucket value encoded for the requested
/// sort order. For example, a price of 23 grouped into intervals of 10 contributes bucket 20,
/// represented by the encoded bucket index 2. Loading can combine values from several columns
/// for the same field, then deduplicate the mapped values per document.
pub(crate) trait BucketKeyValue: Copy + Debug + 'static {
    /// Whether comparing values follows the requested bucket key order, including missing values
    /// and, for terms, column position.
    const ENCODES_KEY_ORDER: bool;

    /// Storage used during loading in addition to the column block accessor.
    /// `u64` borrows the accessor's values and uses `()`. Bucket sort values own their mapped
    /// buffers.
    type Buffer: Debug + Default;

    /// Bytes allocated by the buffer, measured by vector capacity.
    fn buffer_memory_consumption(buffer: &Self::Buffer) -> usize;

    /// Loads one grouping rule's bucket key values for the requested documents.
    /// Returned slices borrow the buffer or the column block accessor.
    fn fetch_field<'a>(
        req: &MultiTermsAggReqData,
        field: usize,
        docs: &[DocId],
        buffer: &'a mut Self::Buffer,
        accessor: &'a mut ColumnBlockAccessor,
    ) -> crate::Result<DocumentValues<'a, Self>>;

    /// Supplies a fallback when loading returned no values for this document and grouping rule.
    /// Returns `None` when no fallback applies. Bucket sort values insert missing buckets during
    /// loading, after checking all columns for the field.
    fn missing_value(req: &MultiTermsAggReqData, field: usize, doc: DocId) -> Option<Self>;

    /// Removes sort encoding and returns the column index and encoded column or bucket value.
    /// Histogram buckets use column index 0. Returns `None` for a missing bucket.
    fn decode(self, order: Order) -> Option<(usize, u64)>;
}

impl BucketKeyValue for u64 {
    const ENCODES_KEY_ORDER: bool = false;
    type Buffer = ();

    fn buffer_memory_consumption(_buffer: &()) -> usize {
        0
    }

    fn fetch_field<'a>(
        req: &MultiTermsAggReqData,
        field: usize,
        docs: &[DocId],
        _buffer: &'a mut (),
        accessor: &'a mut ColumnBlockAccessor,
    ) -> crate::Result<DocumentValues<'a, Self>> {
        let aligned = fetch_field_block(
            docs,
            &req.fields[field],
            req.missing_accessors[field].as_ref(),
            accessor,
        );
        Ok(DocumentValues {
            docids: if aligned {
                None
            } else {
                Some(accessor.docids())
            },
            values: accessor.values(),
            multivalued: accessor.is_batch_multivalued(),
        })
    }

    fn missing_value(req: &MultiTermsAggReqData, field: usize, doc: DocId) -> Option<Self> {
        missing_value_for_doc(req.missing_accessors[field].as_ref(), doc)
    }

    fn decode(self, _order: Order) -> Option<(usize, u64)> {
        Some((0, self))
    }
}

#[cfg(test)]
mod tests {
    use smallvec::SmallVec;

    use super::super::{expand_partial_combinations_for_field, U64ArrayKeyPacking};
    use super::DocumentValues;

    #[test]
    fn page_expansion_matches_cartesian_product() {
        let values = [1, 3, 5];
        let mut product = Vec::new();
        for a in values {
            for b in values {
                for c in values {
                    product.push([a, b, c]);
                }
            }
        }
        let packing = U64ArrayKeyPacking::new(3);
        let block = DocumentValues {
            docids: Some(&[7, 7, 7]),
            values: &values,
            multivalued: true,
        };
        // Pagination keys present and absent from the product, including prefixes with no later
        // suffix.
        for a in 0..=6 {
            for b in 0..=6 {
                for c in 0..=6 {
                    let after = [a, b, c];
                    for size in 1..=3 {
                        let mut keys = vec![SmallVec::new()];
                        let mut docs = vec![7];
                        let mut combination_docs = Vec::new();
                        for field in 0..3 {
                            if docs.is_empty() {
                                break;
                            }
                            expand_partial_combinations_for_field(
                                &packing,
                                field,
                                |_| None,
                                |key| {
                                    let after_prefix = &after[..key.len()];
                                    key.as_slice() > after_prefix
                                        || (key.as_slice() == after_prefix && key.len() < 3)
                                },
                                Some(size + 1),
                                &block,
                                &mut keys,
                                &mut docs,
                                &mut combination_docs,
                            );
                            assert!(keys.len() <= size + 1);
                        }
                        let actual: Vec<_> =
                            keys.iter().take(size).map(|key| key.as_slice()).collect();
                        let expected: Vec<_> = product
                            .iter()
                            .filter(|key| **key > after)
                            .take(size)
                            .map(|key| key.as_slice())
                            .collect();
                        assert_eq!(actual, expected, "after={after:?}, size={size}");
                    }
                }
            }
        }
    }
}

//! Source value representations and bucket storage for multi-terms.

use std::fmt::Debug;

use super::{fetch_field_block, missing_value_for_doc, MultiTermsAggReqData};
use crate::aggregation::bucket::term_agg::{AggregationMapKey, Bucket, TermAggregationMap};
use crate::aggregation::bucket::{BucketIdSlot, Order};
use crate::aggregation::segment_agg_result::BucketIdProvider;
use crate::aggregation::ColumnBlockAccessor;
use crate::DocId;

/// One source's unique values for each document. Document IDs are ascending and grouped.
/// An absent document-ID slice means values are positionally aligned with the input documents.
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

/// Stores and counts buckets for tuples of field values; rejects tuples whose buckets are not
/// retained.
pub(crate) trait TupleBuckets<K>: Debug + 'static {
    /// Identifies the bucket for child aggregations; `()` when no identifier is needed.
    type ChildBucketId: BucketIdSlot;

    const SORTED_BY_KEY: bool;

    fn new(req: &MultiTermsAggReqData, map_init_value: u64, ids: &mut BucketIdProvider) -> Self;
    fn memory_consumption(&self) -> usize;
    fn count_tuple(&mut self, key: &K, ids: &mut BucketIdProvider) -> Option<Self::ChildBucketId>;
    fn into_vec(self) -> Vec<(K, Bucket<Self::ChildBucketId>)>;
}

impl<K: AggregationMapKey, M: TermAggregationMap<K>> TupleBuckets<K> for M {
    type ChildBucketId = M::Slot;

    const SORTED_BY_KEY: bool = M::SORTED_BY_KEY;

    fn new(_req: &MultiTermsAggReqData, map_init_value: u64, ids: &mut BucketIdProvider) -> Self {
        M::new(map_init_value, ids)
    }

    fn into_vec(self) -> Vec<(K, Bucket<Self::ChildBucketId>)> {
        TermAggregationMap::into_vec(self)
    }

    fn memory_consumption(&self) -> usize {
        self.get_memory_consumption()
    }

    #[inline]
    fn count_tuple(&mut self, key: &K, ids: &mut BucketIdProvider) -> Option<Self::ChildBucketId> {
        Some(self.term_entry(key.clone(), ids))
    }
}

/// Loads source values as key components. Raw values borrow the column block;
/// ordered values encode bucket values and, for terms, their column positions.
pub(crate) trait SourceValue: Copy + Debug + 'static {
    /// Whether values encode the requested key order, including physical column position.
    const ENCODES_KEY_ORDER: bool;
    type Buffer: Debug + Default;

    fn buffer_memory_consumption(buffer: &Self::Buffer) -> usize;

    fn fetch_field<'a>(
        req: &MultiTermsAggReqData,
        field: usize,
        docs: &[DocId],
        buffer: &'a mut Self::Buffer,
        accessor: &'a mut ColumnBlockAccessor,
    ) -> crate::Result<DocumentValues<'a, Self>>;

    fn missing_value(req: &MultiTermsAggReqData, field: usize, doc: DocId) -> Option<Self>;

    /// Column index and value before sort encoding, or None for a missing bucket.
    fn decode(self, order: Order) -> Option<(usize, u64)>;
}

impl SourceValue for u64 {
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

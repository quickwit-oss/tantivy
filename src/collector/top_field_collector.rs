use crate::fastfield::FastFieldReader;
use crate::schema::Field;
use crate::{DocAddress, DocId, SegmentLocalId, SegmentReader};
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::marker::PhantomData;

/// Wraps score and fast field value
#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq)]
pub enum Feature<T> {
    /// Score
    Score(T),
    /// Unsigned 64-bits Integer `u64`
    U64(u64),
}

/// Contains a feature (field, score, etc.) of a document along with the document address and order fields.
///
/// It has a custom implementation of `PartialOrd` that reverses the order. This is because the
/// default Rust heap is a max heap, whereas a min heap is needed.
///
/// Additionally, it guarantees stable sorting: in case of a tie on the feature, the document
/// address is used.
///
/// WARNING: equality is not what you would expect here.
/// Two elements are equal if their feature is equal, and regardless of whether `doc`
/// is equal. This should be perfectly fine for this usage, but let's make sure this
/// struct is never public.
#[derive(Debug)]
pub(crate) struct FieldDoc<T, D> {
    pub feature: Vec<Feature<T>>,
    pub doc: D,
    pub order_fields: Vec<OrderField>,
}

impl<T: PartialOrd, D: PartialOrd> PartialOrd for FieldDoc<T, D> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(&other))
    }
}

impl<T: PartialOrd, D: PartialOrd> Ord for FieldDoc<T, D> {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reversed to make BinaryHeap work as a min-heap
        let mut by_feature = Ordering::Equal;
        for i in 0..self.order_fields.len() {
            by_feature = other.feature[i]
                .partial_cmp(&self.feature[i])
                .unwrap_or(Ordering::Equal);
            if by_feature != Ordering::Equal {
                if self.order_fields[i].order == Order::Less {
                    by_feature = by_feature.reverse();
                }
                break;
            }
        }

        let lazy_by_doc_address = || self.doc.partial_cmp(&other.doc).unwrap_or(Ordering::Equal);

        // In case of a tie on the feature, we sort by ascending
        // `DocAddress` in order to ensure a stable sorting of the
        // documents.
        return by_feature.then_with(lazy_by_doc_address);
    }
}

impl<T: PartialOrd, D: PartialOrd> PartialEq for FieldDoc<T, D> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl<T: PartialOrd, D: PartialOrd> Eq for FieldDoc<T, D> {}

/// An `Order` is the result of a comparison between two values.
/// Only support Greater and Less
#[derive(
    Copy, Clone, Debug, PartialEq, PartialOrd, Eq, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
pub enum Order {
    Greater,
    Less,
}

/// Sorts by score or fields desc / asc
#[derive(
    Copy, Clone, Debug, PartialEq, PartialOrd, Eq, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
pub struct OrderField {
    /// Order field
    /// None means order by Score
    /// Some means order by fast field
    pub field: Option<Field>,
    /// Order, only support desc / asc
    pub order: Order,
}

impl OrderField {
    /// Creates an order field, sort by score desc by default
    pub fn with_score() -> Self {
        Self {
            field: None,
            order: Order::Greater,
        }
    }

    /// Creates an order field, sort by field desc by default
    pub fn with_field(field: Field) -> Self {
        Self {
            field: Some(field),
            order: Order::Greater,
        }
    }

    /// Desc order
    pub fn desc(mut self) -> Self {
        self.order = Order::Greater;
        self
    }

    /// Asc order
    pub fn asc(mut self) -> Self {
        self.order = Order::Less;
        self
    }
}

pub(crate) struct TopFieldCollector<T> {
    pub limit: usize,
    pub offset: usize,
    pub order_fields: Vec<OrderField>,
    pub requires_scoring: bool,
    _marker: PhantomData<T>,
}

impl<T> TopFieldCollector<T>
where
    T: PartialOrd + Clone,
{
    /// Creates a top field collector, with a number of documents equal to "limit".
    ///
    /// # Panics
    /// The method panics if limit is 0
    pub fn with_limit(limit: usize) -> TopFieldCollector<T> {
        if limit < 1 {
            panic!("Limit must be strictly greater than 0.");
        }
        Self {
            limit,
            offset: 0,
            order_fields: vec![],
            requires_scoring: true,
            _marker: PhantomData,
        }
    }

    /// Skip the first "offset" documents when collecting.
    ///
    /// This is equivalent to `OFFSET` in MySQL or PostgreSQL and `start` in
    /// Lucene's TopDocsCollector.
    pub fn and_offset(mut self, offset: usize) -> TopFieldCollector<T> {
        self.offset = offset;
        self
    }

    /// Order by order_fields
    pub fn order_by(mut self, order_fields: &[OrderField]) -> TopFieldCollector<T> {
        self.requires_scoring = order_fields.iter().any(|order_field| order_field.field.is_none());
        self.order_fields = order_fields.to_vec();
        self
    }

    pub fn merge_fruits(
        &self,
        children: Vec<Vec<(Vec<Feature<T>>, DocAddress)>>,
    ) -> crate::Result<Vec<(Vec<Feature<T>>, DocAddress)>> {
        if self.limit == 0 {
            return Ok(vec![]);
        }
        let mut top_field_collector = BinaryHeap::new();
        for child_fruit in children {
            for (feature, doc) in child_fruit {
                let field_doc = FieldDoc {
                    feature,
                    doc,
                    order_fields: self.order_fields.clone(),
                };
                if top_field_collector.len() < (self.limit + self.offset) {
                    top_field_collector.push(field_doc);
                } else if let Some(mut head_field_doc) = top_field_collector.peek_mut() {
                    if field_doc.cmp(&*head_field_doc) == Ordering::Greater {
                        *head_field_doc = field_doc;
                    }
                }
            }
        }
        Ok(top_field_collector
            .into_sorted_vec()
            .into_iter()
            .skip(self.offset)
            .map(|doc| (doc.feature, doc.doc))
            .collect())
    }

    pub(crate) fn for_segment<F: PartialOrd>(
        &self,
        segment_id: SegmentLocalId,
        segment_reader: &SegmentReader,
    ) -> crate::Result<TopFieldSegmentCollector<F>> {
        let mut ff_readers = Vec::with_capacity(self.order_fields.len());
        for order_field in &self.order_fields {
            let field = order_field.field;
            if field.is_some() {
                let ff_reader = segment_reader
                    .fast_fields()
                    .u64(field.unwrap())
                    .ok_or_else(|| {
                        crate::TantivyError::SchemaError(format!(
                            "Field requested ({:?}) is not a i64/u64 field.",
                            field
                        ))
                    })?;
                ff_readers.push(Some(ff_reader));
            } else {
                ff_readers.push(None);
            }
        }
        Ok(TopFieldSegmentCollector::new(
            segment_id,
            self.limit + self.offset,
            &self.order_fields,
            ff_readers,
        ))
    }
}

/// The Top Field Segment Collector keeps track of the K documents
/// sorted by type `T`.
///
/// The implementation is based on a `BinaryHeap`.
/// The theorical complexity for collecting the top `K` out of `n` documents
/// is `O(n log K)`.
pub(crate) struct TopFieldSegmentCollector<T> {
    limit: usize,
    heap: BinaryHeap<FieldDoc<T, DocId>>,
    segment_id: SegmentLocalId,
    order_fields: Vec<OrderField>,
    ff_readers: Vec<Option<FastFieldReader<u64>>>,
}

impl<T: PartialOrd> TopFieldSegmentCollector<T> {
    fn new(
        segment_id: SegmentLocalId,
        limit: usize,
        order_fields: &[OrderField],
        ff_readers: Vec<Option<FastFieldReader<u64>>>,
    ) -> Self {
        Self {
            limit,
            heap: BinaryHeap::with_capacity(limit),
            segment_id,
            order_fields: order_fields.to_vec(),
            ff_readers,
        }
    }
}

impl<T: PartialOrd + Clone> TopFieldSegmentCollector<T> {
    pub fn harvest(self) -> Vec<(Vec<Feature<T>>, DocAddress)> {
        let segment_id = self.segment_id;
        self.heap
            .into_sorted_vec()
            .into_iter()
            .map(|field_doc| (field_doc.feature, DocAddress(segment_id, field_doc.doc)))
            .collect()
    }

    /// Return true iff at least K documents have gone through
    /// the collector.
    #[inline(always)]
    pub(crate) fn at_capacity(&self) -> bool {
        self.heap.len() >= self.limit
    }

    /// Collects a document scored by the given feature
    ///
    /// It collects documents until it has reached the max capacity. Once it reaches capacity, it
    /// will compare the lowest scoring item with the given one and keep whichever is greater.
    #[inline(always)]
    pub fn collect(&mut self, doc: DocId, score: T) {
        if self.at_capacity() {
            let field_doc = self.new_field_doc(doc, score);
            if let Some(limit_field_doc) = self.heap.peek() {
                if field_doc.cmp(limit_field_doc) == Ordering::Greater {
                    if let Some(mut head) = self.heap.peek_mut() {
                        *head = field_doc;
                    }
                }
            }
        } else {
            self.heap.push(self.new_field_doc(doc, score))
        }
    }

    fn new_field_doc(&self, doc: DocId, score: T) -> FieldDoc<T, DocId> {
        let mut feature = Vec::with_capacity(self.order_fields.len());
        for ff_reader in &self.ff_readers {
            match ff_reader {
                Some(reader) => feature.push(Feature::U64(reader.get(doc))),
                None => feature.push(Feature::Score(score.clone())),
            }
        }
        FieldDoc {
            feature,
            doc,
            order_fields: self.order_fields.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_field_doc() {
        let order_fields = vec![
            OrderField::with_score().desc(),
            OrderField::with_field(Field::from_field_id(0)).asc(),
            OrderField::with_field(Field::from_field_id(1)).desc(),
        ];

        let field_doc1 = FieldDoc {
            feature: vec![
                Feature::Score(1.0f32),
                Feature::U64(2u64),
                Feature::U64(2u64),
            ],
            doc: 0u32,
            order_fields: order_fields.clone(),
        };
        let field_doc2 = FieldDoc {
            feature: vec![
                Feature::Score(2.0f32),
                Feature::U64(1u64),
                Feature::U64(2u64),
            ],
            doc: 1u32,
            order_fields: order_fields.clone(),
        };
        let field_doc3 = FieldDoc {
            feature: vec![
                Feature::Score(1.0f32),
                Feature::U64(1u64),
                Feature::U64(2u64),
            ],
            doc: 2u32,
            order_fields: order_fields.clone(),
        };
        let field_doc4 = FieldDoc {
            feature: vec![
                Feature::Score(1.0f32),
                Feature::U64(1u64),
                Feature::U64(2u64),
            ],
            doc: 2u32,
            order_fields: order_fields.clone(),
        };
        let field_doc5 = FieldDoc {
            feature: vec![
                Feature::Score(1.0f32),
                Feature::U64(1u64),
                Feature::U64(3u64),
            ],
            doc: 3u32,
            order_fields: order_fields.clone(),
        };

        let mut heap = BinaryHeap::new();
        heap.push(&field_doc1);
        heap.push(&field_doc2);
        heap.push(&field_doc3);
        heap.push(&field_doc4);
        heap.push(&field_doc5);

        let vec: Vec<_> = heap
            .into_sorted_vec()
            .into_iter()
            .map(|field_doc| field_doc)
            .collect();

        assert_eq!(vec[0], &field_doc2);
        assert_eq!(vec[1], &field_doc5);
        assert_eq!(vec[2], &field_doc3);
        assert_eq!(vec[3], &field_doc4);
        assert_eq!(vec[4], &field_doc1);
    }
}

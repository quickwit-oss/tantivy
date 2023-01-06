use std::fmt::Debug;

use fastfield_codecs::Column;
use serde::{Deserialize, Serialize};

use crate::aggregation::f64_from_fastfield_u64;
use crate::schema::Type;
use crate::DocId;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// A single-value metric aggregation that computes the minimum of numeric values that are
/// extracted from the aggregated documents.
/// Supported field types are u64, i64, and f64.
/// See [super::SingleMetricResult] for return value.
///
/// # JSON Format
/// ```json
/// {
///     "min": {
///         "field": "price",
///     }
/// }
/// ```
pub struct MinAggregation {
    /// The field name to compute the minimum on.
    pub field: String,
}

impl MinAggregation {
    /// Creates a new [`MinAggregation`] from a field name.
    pub fn from_field_name(field_name: String) -> Self {
        Self { field: field_name }
    }

    /// Returns the field name.
    pub fn field_name(&self) -> &str {
        &self.field
    }
}

/// Holds the intermediate state of the minimum computation.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct IntermediateMin {
    pub(crate) min: f64,
    pub(crate) is_some: bool,
}

impl Default for IntermediateMin {
    fn default() -> Self {
        Self {
            min: f64::MAX,
            is_some: false,
        }
    }
}

impl IntermediateMin {
    pub(crate) fn from_collector(collector: SegmentMinCollector) -> Self {
        collector.min
    }

    /// Merges average data into this instance.
    pub fn merge_fruits(&mut self, other: IntermediateMin) {
        self.min = self.min.min(other.min);
        self.is_some |= other.is_some;
    }

    /// Returns min value.
    pub fn finalize(&self) -> Option<f64> {
        if self.is_some {
            Some(self.min)
        } else {
            None
        }
    }

    #[inline]
    fn collect(&mut self, val: f64) {
        self.min = self.min.min(val);
        self.is_some = true;
    }
}

#[derive(Clone, PartialEq)]
pub(crate) struct SegmentMinCollector {
    pub min: IntermediateMin,
    field_type: Type,
}

impl Debug for SegmentMinCollector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MinCollector")
            .field("min", &self.min)
            .finish()
    }
}

impl SegmentMinCollector {
    pub fn from_req(field_type: Type) -> Self {
        Self {
            min: Default::default(),
            field_type,
        }
    }
    pub(crate) fn collect_block(&mut self, docs: &[DocId], field: &dyn Column<u64>) {
        let mut chunks = docs.chunks_exact(4);
        for chunk in chunks.by_ref() {
            let val1 = field.get_val(chunk[0]);
            let val2 = field.get_val(chunk[1]);
            let val3 = field.get_val(chunk[2]);
            let val4 = field.get_val(chunk[3]);
            let val1 = f64_from_fastfield_u64(val1, &self.field_type);
            let val2 = f64_from_fastfield_u64(val2, &self.field_type);
            let val3 = f64_from_fastfield_u64(val3, &self.field_type);
            let val4 = f64_from_fastfield_u64(val4, &self.field_type);
            self.min.collect(val1);
            self.min.collect(val2);
            self.min.collect(val3);
            self.min.collect(val4);
        }
        for &doc_id in chunks.remainder() {
            let val = field.get_val(doc_id);
            let val = f64_from_fastfield_u64(val, &self.field_type);
            self.min.collect(val);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aggregation::agg_req::{Aggregation, Aggregations, MetricAggregation};
    use crate::aggregation::agg_result::AggregationResults;
    use crate::aggregation::tests::get_test_index_2_segments;
    use crate::aggregation::AggregationCollector;
    use crate::query::TermQuery;
    use crate::schema::IndexRecordOption;
    use crate::Term;

    #[test]
    fn test_min_aggregation() {
        let index = get_test_index_2_segments(false).unwrap();
        let reader = index.reader().unwrap();
        let text_field = reader.searcher().schema().get_field("text").unwrap();

        let term_query = TermQuery::new(
            Term::from_field_text(text_field, "cool"),
            IndexRecordOption::Basic,
        );
        let aggs: Aggregations = vec![(
            "min".to_string(),
            Aggregation::Metric(MetricAggregation::Min(MinAggregation::from_field_name(
                "score".to_string(),
            ))),
        )]
        .into_iter()
        .collect();

        let collector = AggregationCollector::from_aggs(aggs, None, index.schema());
        let searcher = reader.searcher();

        let agg_res: AggregationResults = searcher.search(&term_query, &collector).unwrap();
        assert_eq!(
            agg_res
                .get_value_from_aggregation("min", "")
                .unwrap()
                .unwrap(),
            1.0
        );
    }
}

use std::collections::{HashMap, HashSet};

use serde::*;

use super::bucket::*;
use super::metric::*;
pub type Aggregations = HashMap<String, AggregationDeser>;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AggregationDeser {
    /// Bucket aggregation strategy to group documents.
    #[serde(flatten)]
    pub agg: AggregationVariants,
    /// The sub_aggregations in the buckets. Each bucket will aggregate on the document set in the
    /// bucket.
    #[serde(rename = "aggs")]
    #[serde(default)]
    #[serde(skip_serializing_if = "Aggregations::is_empty")]
    pub sub_aggregation: Aggregations,
}

impl AggregationDeser {
    fn get_fast_field_names(&self, fast_field_names: &mut HashSet<String>) {
        fast_field_names.insert(self.agg.get_fast_field_name().to_string());
        fast_field_names.extend(get_fast_field_names(&self.sub_aggregation));
    }
}

/// Extract all fast field names used in the tree.
pub fn get_fast_field_names(aggs: &Aggregations) -> HashSet<String> {
    let mut fast_field_names = Default::default();
    for el in aggs.values() {
        el.get_fast_field_names(&mut fast_field_names)
    }
    fast_field_names
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum AggregationVariants {
    // Bucket aggregation types
    /// Put data into buckets of user-defined ranges.
    #[serde(rename = "range")]
    Range(RangeAggregation),
    /// Put data into a histogram.
    #[serde(rename = "histogram")]
    Histogram(HistogramAggregation),
    /// Put data into a date histogram.
    #[serde(rename = "date_histogram")]
    DateHistogram(DateHistogramAggregationReq),
    /// Put data into buckets of terms.
    #[serde(rename = "terms")]
    Terms(TermsAggregation),

    // Metric aggregation types
    /// Computes the average of the extracted values.
    #[serde(rename = "avg")]
    Average(AverageAggregation),
    /// Counts the number of extracted values.
    #[serde(rename = "value_count")]
    Count(CountAggregation),
    /// Finds the maximum value.
    #[serde(rename = "max")]
    Max(MaxAggregation),
    /// Finds the minimum value.
    #[serde(rename = "min")]
    Min(MinAggregation),
    /// Computes a collection of statistics (`min`, `max`, `sum`, `count`, and `avg`) over the
    /// extracted values.
    #[serde(rename = "stats")]
    Stats(StatsAggregation),
    /// Computes the sum of the extracted values.
    #[serde(rename = "sum")]
    Sum(SumAggregation),
    /// Computes the sum of the extracted values.
    #[serde(rename = "percentiles")]
    Percentiles(PercentilesAggregationReq),
}

impl AggregationVariants {
    fn get_fast_field_name(&self) -> &str {
        match self {
            AggregationVariants::Terms(terms) => terms.field.as_str(),
            AggregationVariants::Range(range) => range.field.as_str(),
            AggregationVariants::Histogram(histogram) => histogram.field.as_str(),
            AggregationVariants::DateHistogram(histogram) => histogram.field.as_str(),
            AggregationVariants::Average(avg) => avg.field_name(),
            AggregationVariants::Count(count) => count.field_name(),
            AggregationVariants::Max(max) => max.field_name(),
            AggregationVariants::Min(min) => min.field_name(),
            AggregationVariants::Stats(stats) => stats.field_name(),
            AggregationVariants::Sum(sum) => sum.field_name(),
            AggregationVariants::Percentiles(per) => per.field_name(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deser_json_test() {
        let agg_req_json = r#"{
            "price_avg": { "avg": { "field": "price" } },
            "price_count": { "value_count": { "field": "price" } },
            "price_max": { "max": { "field": "price" } },
            "price_min": { "min": { "field": "price" } },
            "price_stats": { "stats": { "field": "price" } },
            "price_sum": { "sum": { "field": "price" } }
        }"#;
        let _agg_req: Aggregations = serde_json::from_str(agg_req_json).unwrap();
    }

    #[test]
    fn deser_json_test_bucket() {
        let agg_req_json = r#"
    {
        "termagg": {
            "terms": {
                "field": "json.mixed_type",
                "order": { "min_price": "desc" }
            },
            "aggs": {
                "min_price": { "min": { "field": "json.mixed_type" } }
            }
        },
        "rangeagg": {
            "range": {
                "field": "json.mixed_type",
                "ranges": [
                    { "to": 3.0 },
                    { "from": 19.0, "to": 20.0 },
                    { "from": 20.0 }
                ]
            },
            "aggs": {
                "average_in_range": { "avg": { "field": "json.mixed_type" } }
            }
        }
    } "#;

        let _agg_req: Aggregations = serde_json::from_str(agg_req_json).unwrap();
    }
}

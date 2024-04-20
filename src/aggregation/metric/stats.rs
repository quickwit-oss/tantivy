use std::fmt::Debug;

use serde::{Deserialize, Serialize};

use super::*;
use crate::aggregation::agg_req_with_accessor::{
    AggregationWithAccessor, AggregationsWithAccessor,
};
use crate::aggregation::intermediate_agg_result::{
    IntermediateAggregationResult, IntermediateAggregationResults, IntermediateMetricResult,
};
use crate::aggregation::segment_agg_result::SegmentAggregationCollector;
use crate::aggregation::*;
use crate::{DocId, TantivyError};

/// A multi-value metric aggregation that computes a collection of statistics on numeric values that
/// are extracted from the aggregated documents.
/// See [`Stats`] for returned statistics.
///
/// # JSON Format
/// ```json
/// {
///     "stats": {
///         "field": "score"
///     }
///  }
/// ```

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct StatsAggregation {
    /// The field name to compute the stats on.
    pub field: String,
    /// The missing parameter defines how documents that are missing a value should be treated.
    /// By default they will be ignored but it is also possible to treat them as if they had a
    /// value. Examples in JSON format:
    /// { "field": "my_numbers", "missing": "10.0" }
    #[serde(default, deserialize_with = "deserialize_option_f64")]
    pub missing: Option<f64>,
}

impl StatsAggregation {
    /// Creates a new [`StatsAggregation`] instance from a field name.
    pub fn from_field_name(field_name: String) -> Self {
        StatsAggregation {
            field: field_name,
            missing: None,
        }
    }
    /// Returns the field name the aggregation is computed on.
    pub fn field_name(&self) -> &str {
        &self.field
    }
}

/// A multi-value metric aggregation that computes a collection of extended statistics
/// on numeric values that are extracted
/// from the aggregated documents.
/// See [`ExtendedStats`] for returned statistics.
///
/// # JSON Format
/// ```json
/// {
///     "extended_stats": {
///         "field": "score"
///     }
///  }
/// ```

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ExtendedStatsAggregation {
    /// The field name to compute the stats on.
    pub field: String,
    /// The missing parameter defines how documents that are missing a value should be treated.
    /// By default they will be ignored but it is also possible to treat them as if they had a
    /// value. Examples in JSON format:
    /// { "field": "my_numbers", "missing": "10.0" }
    #[serde(default)]
    pub missing: Option<f64>,
    /// The sigma parameter defines how standard_deviation_bound_are_calculated.
    /// This can be a useful way to visualize variance of your data.
    /// The default value is 2. Examples in JSON format:
    /// { "field": "my_numbers", "sigma": "3.0" }
    #[serde(default)]
    pub sigma: Option<f64>,
}

impl ExtendedStatsAggregation {
    /// Creates a new [`ExtendedStatsAggregation`] instance from a field name.
    pub fn from_field_name(field_name: String) -> Self {
        ExtendedStatsAggregation {
            field: field_name,
            missing: None,
            sigma: None,
        }
    }
    /// Returns the field name the aggregation is computed on.
    pub fn field_name(&self) -> &str {
        &self.field
    }
}

/// Stats contains a collection of statistics.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Stats {
    /// The number of documents.
    pub count: u64,
    /// The sum of the fast field values.
    pub sum: f64,
    /// The min value of the fast field values.
    pub min: Option<f64>,
    /// The max value of the fast field values.
    pub max: Option<f64>,
    /// The average of the fast field values. `None` if count equals zero.
    pub avg: Option<f64>,
}

impl Stats {
    pub(crate) fn get_value(&self, agg_property: &str) -> crate::Result<Option<f64>> {
        match agg_property {
            "count" => Ok(Some(self.count as f64)),
            "sum" => Ok(Some(self.sum)),
            "min" => Ok(self.min),
            "max" => Ok(self.max),
            "avg" => Ok(self.avg),
            _ => Err(TantivyError::InvalidArgument(format!(
                "Unknown property {agg_property} on stats metric aggregation"
            ))),
        }
    }
}

/// Extended stats contains a collection of statistics
/// they extends stats adding variance, standard deviation
/// and bound informations
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ExtendedStats {
    /// The number of documents.
    pub count: u64,
    /// The sum of the fast field values.
    pub sum: f64,
    /// The min value of the fast field values.
    pub min: Option<f64>,
    /// The max value of the fast field values.
    pub max: Option<f64>,
    /// The average of the fast field values. `None` if count equals zero.
    pub avg: Option<f64>,
    /// The sum of squares of the fast field values. `None` if count equals zero.
    pub sum_of_squares: Option<f64>,
    /// The variance of the fast field values. `None` if count is less then 2.
    pub variance: Option<f64>,
    /// The variance population of the fast field values, always equal to variance. `None` if count
    /// is less then 2.
    pub variance_population: Option<f64>,
    /// The variance sampling of the fast field values, always equal to variance. `None` if count
    /// is less then 2.
    pub variance_sampling: Option<f64>,
    /// The standard deviation of the fast field values. `None` if count is less then 2.
    pub std_deviation: Option<f64>,
    /// The standard deviation of the fast field values, always equal to variance. `None` if count
    /// is less then 2.
    pub std_deviation_population: Option<f64>,
    /// The standard deviation sampling of the fast field values. `None`
    /// if count is less then 2.
    pub std_deviation_sampling: Option<f64>,
    /// The standard deviation bounds of the fast field values, always equal to variance. `None`
    /// if count is less then 2.
    pub std_deviation_bounds: Option<StandardDeviationBounds>,
}

/// A sub struct for ExtendedStat containing deviation bounds
/// the values depend on sigma and represent
/// the bounds from the average with a distance of
/// std_deviation*sigma
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct StandardDeviationBounds {
    /// upper bound -> avg + std_dev*sigma
    pub upper: f64,
    /// lower bound -> avg - std_dev*sigma
    pub lower: f64,
    /// upper bound sampling -> avg + std_dev_sampling*sigma
    pub upper_sampling: f64,
    /// lower bound sampling -> avg - std_dev_sampling*sigma
    pub lower_sampling: f64,
    /// same as upper
    pub upper_population: f64,
    /// same as lower
    pub lower_population: f64,
}

impl ExtendedStats {
    pub(crate) fn get_value(&self, agg_property: &str) -> crate::Result<Option<f64>> {
        match agg_property {
            "count" => Ok(Some(self.count as f64)),
            "sum" => Ok(Some(self.sum)),
            "min" => Ok(self.min),
            "max" => Ok(self.max),
            "avg" => Ok(self.avg),
            "variance" => Ok(self.variance),
            "variance_sampling" => Ok(self.variance_sampling),
            "variance_population" => Ok(self.variance_population),
            "sum_of_squares" => Ok(self.sum_of_squares),
            "std_deviation" => Ok(self.std_deviation),
            "std_deviation_sampling" => Ok(self.std_deviation_sampling),
            "std_deviation_population" => Ok(self.std_deviation_population),
            "std_deviation_bounds.lower" => Ok(self
                .std_deviation_bounds
                .as_ref()
                .map(|bounds| bounds.lower)),
            "std_deviation_bounds.lower_population" => Ok(self
                .std_deviation_bounds
                .as_ref()
                .map(|bounds| bounds.lower_population)),
            "std_deviation_bounds.lower_sampling" => Ok(self
                .std_deviation_bounds
                .as_ref()
                .map(|bounds| bounds.lower_sampling)),
            "std_deviation_bounds.upper" => Ok(self
                .std_deviation_bounds
                .as_ref()
                .map(|bounds| bounds.upper)),
            "std_deviation_bounds.upper_population" => Ok(self
                .std_deviation_bounds
                .as_ref()
                .map(|bounds| bounds.upper_population)),
            "std_deviation_bounds.upper_sampling" => Ok(self
                .std_deviation_bounds
                .as_ref()
                .map(|bounds| bounds.upper_sampling)),
            _ => Err(TantivyError::InvalidArgument(format!(
                "Unknown property {agg_property} on stats metric aggregation"
            ))),
        }
    }
}

/// Intermediate result of the stats aggregation that can be combined with other intermediate
/// results.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct IntermediateStats {
    /// The number of extracted values.
    count: u64,
    /// The sum of the extracted values.
    sum: f64,
    /// delta for sum needed for [Kahan algorithm for summation](https://en.wikipedia.org/wiki/Kahan_summation_algorithm)
    delta: f64,
    /// The min value.
    min: f64,
    /// The max value.
    max: f64,
}

impl Default for IntermediateStats {
    fn default() -> Self {
        Self {
            count: 0,
            sum: 0.0,
            delta: 0.0,
            min: f64::MAX,
            max: f64::MIN,
        }
    }
}

impl IntermediateStats {
    /// Merges the other stats intermediate result into self.
    pub fn merge_fruits(&mut self, other: IntermediateStats) {
        self.count += other.count;
        self.sum += other.sum;
        self.delta += other.delta;
        self.min = self.min.min(other.min);
        self.max = self.max.max(other.max);
    }

    /// Computes the final stats value.
    pub fn finalize(&self) -> Stats {
        let min = if self.count == 0 {
            None
        } else {
            Some(self.min)
        };
        let max = if self.count == 0 {
            None
        } else {
            Some(self.max)
        };
        let avg = if self.count == 0 {
            None
        } else {
            Some(self.sum / (self.count as f64))
        };
        Stats {
            count: self.count,
            sum: self.sum,
            min,
            max,
            avg,
        }
    }

    #[inline]
    fn collect(&mut self, value: f64) {
        self.count += 1;

        // kahan algorithm for sum

        let y = value - self.delta;
        let t = self.sum + y;
        self.delta = (t - self.sum) - y;
        self.sum = t;

        // self.sum+=value;
        self.min = self.min.min(value);
        self.max = self.max.max(value);
    }
}

/// Intermediate result of the extended stats aggregation that can be combined with other
/// intermediate results.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct IntermediateExtendedStats {
    intermediate_stats: IntermediateStats,
    /// The number of extracted values.
    // The sum of square values, it's referred as M2 in Welford's online algorithm
    sum_of_squares: f64,
    // The sum of square values as computed by elastic search
    sum_of_squares_elastic: f64,
    /// delta for sum of squares  as computed by elastic search needed for the Kahan algorithm
    delta_sum_for_squares_elastic: f64,
    // The mean an intermediate value need for calculating the variance
    // as per [Welford's online algorithm](https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Welford's_online_algorithm)
    mean: f64,
    // the value used for computing standard deviation bounds
    sigma: f64,
}

impl Default for IntermediateExtendedStats {
    fn default() -> Self {
        Self {
            intermediate_stats: IntermediateStats::default(),
            sum_of_squares: 0.0,
            sum_of_squares_elastic: 0.0,
            delta_sum_for_squares_elastic: 0.0,
            mean: 0.0,
            sigma: 2.0,
        }
    }
}

impl IntermediateExtendedStats {
    /// Creates a new IntermediateExtendedStats using an option
    /// containing the sigma to be used for calculating bound values.
    pub fn with_sigma(sigma: Option<f64>) -> Self {
        Self {
            intermediate_stats: IntermediateStats::default(),
            sum_of_squares: 0.0,
            sum_of_squares_elastic: 0.0,
            delta_sum_for_squares_elastic: 0.0,
            mean: 0.0,
            sigma: sigma.unwrap_or(2.0),
        }
    }
    /// Merges the other stats intermediate result into self.
    pub fn merge_fruits(&mut self, other: IntermediateExtendedStats) {
        if other.intermediate_stats.count != 0 {
            if self.intermediate_stats.count == 0 {
                self.sum_of_squares = other.sum_of_squares;
                self.sum_of_squares_elastic = other.sum_of_squares_elastic;
                self.intermediate_stats = other.intermediate_stats;
                self.mean = other.mean;
                self.delta_sum_for_squares_elastic = other.delta_sum_for_squares_elastic
            } else {
                if other.intermediate_stats.count == 1 {
                    self.collect(other.intermediate_stats.sum);
                } else if self.intermediate_stats.count == 1 {
                    let sum = self.intermediate_stats.sum;
                    self.intermediate_stats = other.intermediate_stats;
                    self.sum_of_squares = other.sum_of_squares;
                    self.sum_of_squares_elastic = other.sum_of_squares_elastic;
                    self.mean = other.mean;
                    self.delta_sum_for_squares_elastic = other.delta_sum_for_squares_elastic;
                    self.collect(sum);
                } else {
                    // parallel version of Welford's online algorithm
                    // the mean is computed using sum and count because
                    // it's more precise (and sum is already available)
                    let new_count = self.intermediate_stats.count + other.intermediate_stats.count;
                    let delta = other.mean - self.mean;
                    self.sum_of_squares += other.sum_of_squares
                        + delta
                            * delta
                            * self.intermediate_stats.count as f64
                            * other.intermediate_stats.count as f64
                            / new_count as f64;
                    self.intermediate_stats.count = new_count;
                    // self.mean=self.mean + delta*other.count as f64/new_count as f64;
                    self.mean = (self.intermediate_stats.sum as f64
                        + other.intermediate_stats.sum as f64)
                        / new_count as f64;
                    self.intermediate_stats.sum += other.intermediate_stats.sum;
                    self.intermediate_stats.delta += other.intermediate_stats.delta;
                    self.sum_of_squares_elastic += other.sum_of_squares_elastic;
                    self.delta_sum_for_squares_elastic += other.delta_sum_for_squares_elastic
                }
            }
        }
    }

    /// Computes the final stats value.
    pub fn finalize(&self) -> Box<ExtendedStats> {
        let min = if self.intermediate_stats.count == 0 {
            None
        } else {
            Some(self.intermediate_stats.min)
        };
        let max = if self.intermediate_stats.count == 0 {
            None
        } else {
            Some(self.intermediate_stats.max)
        };
        let avg = if self.intermediate_stats.count == 0 {
            None
        } else {
            Some(self.mean)
        };
        let sum_of_squares = if self.intermediate_stats.count == 0 {
            None
        } else {
            Some(self.sum_of_squares_elastic)
        };
        let variance = if self.intermediate_stats.count <= 1 {
            None
        } else {
            Some(self.sum_of_squares / self.intermediate_stats.count as f64)
        };
        let variance_sampling = if self.intermediate_stats.count <= 1 {
            None
        } else {
            Some(self.sum_of_squares / (self.intermediate_stats.count - 1) as f64)
        };
        let std_deviation = variance.map(|v| v.sqrt());
        let std_deviation_sampling = variance_sampling.map(|v| v.sqrt());
        let std_deviation_bounds = if std_deviation.is_none() {
            None
        } else {
            let upper = self.mean + std_deviation.unwrap() * self.sigma;
            let lower = self.mean - std_deviation.unwrap() * self.sigma;
            let upper_sampling = self.mean + std_deviation_sampling.unwrap() * self.sigma;
            let lower_sampling = self.mean - std_deviation_sampling.unwrap() * self.sigma;
            Some(StandardDeviationBounds {
                upper,
                lower,
                upper_sampling,
                lower_sampling,
                upper_population: upper,
                lower_population: lower,
            })
        };
        Box::new(ExtendedStats {
            count: self.intermediate_stats.count,
            sum: self.intermediate_stats.sum,
            min,
            max,
            avg,
            sum_of_squares,
            variance,
            variance_population: variance,
            variance_sampling,
            std_deviation,
            std_deviation_population: std_deviation,
            std_deviation_sampling,
            std_deviation_bounds,
        })
    }

    fn update_variance(&mut self, value: f64) {
        let delta = value - self.mean;
        // this is not what the Welford's online algorithm prescribes but
        // using the pseudo code from wikipedia there was a small rounding
        // error (in 15th decimal place) that caused a test
        //(test_aggregation_level1 in agg_test.rs)
        // failure
        self.mean = self.intermediate_stats.sum / self.intermediate_stats.count as f64;
        // self.mean += delta / self.count as f64;
        let delta2 = value - self.mean;
        self.sum_of_squares += delta * delta2;
    }
}

impl IntermediateInnerCollector for IntermediateExtendedStats {
    #[inline]
    fn collect(&mut self, value: f64) {
        self.intermediate_stats.collect(value);
        // kahan algorithm for sum_of_squares_elastic
        let y = value * value - self.delta_sum_for_squares_elastic;
        let t = self.sum_of_squares_elastic + y;
        self.delta_sum_for_squares_elastic = (t - self.sum_of_squares_elastic) - y;
        self.sum_of_squares_elastic = t;
        self.update_variance(value);
    }

    fn into_intermediate_metric_result(self) -> IntermediateMetricResult {
        IntermediateMetricResult::ExtendedStats(self)
    }
}

/// trait implemented by IntermediateStats and
/// IntermediateExtendedStats that allows the
/// usage of a single [SegmentStatsCollector]
pub trait IntermediateInnerCollector {
    /// collects the single field value to form
    /// the statistic
    fn collect(&mut self, val: f64);
    /// transforms the intermediate stat
    /// into a [IntermediateMetricResult]
    fn into_intermediate_metric_result(self) -> IntermediateMetricResult;
}

/// Intermediate struct used by [SegmentStatsCollector]
/// for calculating Stats and creating [IntermediateMetricResult]
#[derive(Clone, Debug, PartialEq)]
pub struct IntermediateInnerStatsCollector {
    stats: IntermediateStats,
    collecting_for: SegmentStatsType,
}

impl IntermediateInnerStatsCollector {
    pub(crate) fn for_stat_type(collecting_for: SegmentStatsType) -> Self {
        IntermediateInnerStatsCollector {
            stats: IntermediateStats::default(),
            collecting_for: collecting_for,
        }
    }
}

impl IntermediateInnerCollector for IntermediateInnerStatsCollector {
    #[inline]
    fn collect(&mut self, value: f64) {
        self.stats.collect(value);
    }

    fn into_intermediate_metric_result(self) -> IntermediateMetricResult {
        match self.collecting_for {
            SegmentStatsType::Average => {
                IntermediateMetricResult::Average(IntermediateAverage::from_stats(self.stats))
            }
            SegmentStatsType::Count => {
                IntermediateMetricResult::Count(IntermediateCount::from_stats(self.stats))
            }
            SegmentStatsType::Max => {
                IntermediateMetricResult::Max(IntermediateMax::from_stats(self.stats))
            }
            SegmentStatsType::Min => {
                IntermediateMetricResult::Min(IntermediateMin::from_stats(self.stats))
            }
            SegmentStatsType::Stats => IntermediateMetricResult::Stats(self.stats),
            SegmentStatsType::Sum => {
                IntermediateMetricResult::Sum(IntermediateSum::from_stats(self.stats))
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum SegmentStatsType {
    Average,
    Count,
    Max,
    Min,
    Stats,
    Sum,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct SegmentStatsCollector<T: IntermediateInnerCollector> {
    missing: Option<u64>,
    field_type: ColumnType,
    inner_intermediate_collector: T,
    pub(crate) accessor_idx: usize,
    val_cache: Vec<u64>,
}

impl<T: IntermediateInnerCollector> SegmentStatsCollector<T> {
    pub fn from_req(
        field_type: ColumnType,
        // collecting_for: SegmentStatsType,
        inner_intermediate_collector: T,
        accessor_idx: usize,
        missing: Option<f64>,
    ) -> Self {
        let missing = missing.and_then(|val| f64_to_fastfield_u64(val, &field_type));
        Self {
            field_type,
            inner_intermediate_collector,
            // collecting_for,
            // stats: stats,
            accessor_idx,
            missing,
            val_cache: Default::default(),
        }
    }
    #[inline]
    pub(crate) fn collect_block_with_field(
        &mut self,
        docs: &[DocId],
        agg_accessor: &mut AggregationWithAccessor,
    ) {
        if let Some(missing) = self.missing.as_ref() {
            agg_accessor.column_block_accessor.fetch_block_with_missing(
                docs,
                &agg_accessor.accessor,
                *missing,
            );
        } else {
            agg_accessor
                .column_block_accessor
                .fetch_block(docs, &agg_accessor.accessor);
        }
        for val in agg_accessor.column_block_accessor.iter_vals() {
            let val1 = f64_from_fastfield_u64(val, &self.field_type);
            self.inner_intermediate_collector.collect(val1);
        }
    }
}

impl<T> SegmentAggregationCollector for SegmentStatsCollector<T>
where T: IntermediateInnerCollector + Debug + Clone + 'static
{
    #[inline]
    fn add_intermediate_aggregation_result(
        self: Box<Self>,
        agg_with_accessor: &AggregationsWithAccessor,
        results: &mut IntermediateAggregationResults,
    ) -> crate::Result<()> {
        let name = agg_with_accessor.aggs.keys[self.accessor_idx].to_string();

        let intermediate_metric_result = self
            .inner_intermediate_collector
            .into_intermediate_metric_result();
        results.push(
            name,
            IntermediateAggregationResult::Metric(intermediate_metric_result),
        )?;

        Ok(())
    }

    #[inline]
    fn collect(
        &mut self,
        doc: crate::DocId,
        agg_with_accessor: &mut AggregationsWithAccessor,
    ) -> crate::Result<()> {
        let field = &agg_with_accessor.aggs.values[self.accessor_idx].accessor;
        if let Some(missing) = self.missing {
            let mut has_val = false;
            for val in field.values_for_doc(doc) {
                let val1 = f64_from_fastfield_u64(val, &self.field_type);
                self.inner_intermediate_collector.collect(val1);
                has_val = true;
            }
            if !has_val {
                self.inner_intermediate_collector
                    .collect(f64_from_fastfield_u64(missing, &self.field_type));
            }
        } else {
            for val in field.values_for_doc(doc) {
                let val1 = f64_from_fastfield_u64(val, &self.field_type);
                self.inner_intermediate_collector.collect(val1);
            }
        }

        Ok(())
    }

    #[inline]
    fn collect_block(
        &mut self,
        docs: &[crate::DocId],
        agg_with_accessor: &mut AggregationsWithAccessor,
    ) -> crate::Result<()> {
        let field = &mut agg_with_accessor.aggs.values[self.accessor_idx];
        self.collect_block_with_field(docs, field);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use serde_json::Value;

    use crate::aggregation::agg_req::{Aggregation, Aggregations};
    use crate::aggregation::agg_result::AggregationResults;
    use crate::aggregation::metric::{IntermediateExtendedStats, IntermediateInnerCollector};
    use crate::aggregation::tests::{
        exec_request_with_query, get_test_index_2_segments, get_test_index_from_values,
    };
    use crate::aggregation::AggregationCollector;
    use crate::query::{AllQuery, TermQuery};
    use crate::schema::{IndexRecordOption, Schema, FAST};
    use crate::{assert_nearly_equals, Index, IndexWriter, Term};

    const EPSILON_FOR_TEST: f64 = 0.00000000000002;

    #[test]
    fn test_aggregation_stats_empty_index() -> crate::Result<()> {
        // test index without segments
        let values = vec![];

        let index = get_test_index_from_values(false, &values)?;

        let agg_req_1: Aggregations = serde_json::from_value(json!({
            "stats": {
                "stats": {
                    "field": "score",
                },
            }
        }))
        .unwrap();

        let collector = AggregationCollector::from_aggs(agg_req_1, Default::default());

        let reader = index.reader()?;
        let searcher = reader.searcher();
        let agg_res: AggregationResults = searcher.search(&AllQuery, &collector).unwrap();

        let res: Value = serde_json::from_str(&serde_json::to_string(&agg_res)?)?;
        assert_eq!(
            res["stats"],
            json!({
                "avg": Value::Null,
                "count": 0,
                "max": Value::Null,
                "min": Value::Null,
                "sum": 0.0
            })
        );

        Ok(())
    }

    #[test]
    fn test_aggregation_stats_simple() -> crate::Result<()> {
        let values = vec![10.0];

        let index = get_test_index_from_values(false, &values)?;

        let agg_req_1: Aggregations = serde_json::from_value(json!({
            "stats": {
                "stats": {
                    "field": "score",
                },
            }
        }))
        .unwrap();

        let collector = AggregationCollector::from_aggs(agg_req_1, Default::default());

        let reader = index.reader()?;
        let searcher = reader.searcher();
        let agg_res: AggregationResults = searcher.search(&AllQuery, &collector).unwrap();

        let res: Value = serde_json::from_str(&serde_json::to_string(&agg_res)?)?;
        assert_eq!(
            res["stats"],
            json!({
                "avg": 10.0,
                "count": 1,
                "max": 10.0,
                "min": 10.0,
                "sum": 10.0
            })
        );

        Ok(())
    }

    #[test]
    fn test_aggregation_stats() -> crate::Result<()> {
        let index = get_test_index_2_segments(false)?;

        let reader = index.reader()?;
        let text_field = reader.searcher().schema().get_field("text").unwrap();

        let term_query = TermQuery::new(
            Term::from_field_text(text_field, "cool"),
            IndexRecordOption::Basic,
        );

        let range_agg: Aggregation = {
            serde_json::from_value(json!({
                "range": {
                    "field": "score",
                    "ranges": [ { "from": 3.0f64, "to": 7.0f64 }, { "from": 7.0f64, "to": 19.0f64 }, { "from": 19.0f64, "to": 20.0f64 }  ]
                },
                "aggs": {
                    "stats": {
                        "stats": {
                            "field": "score"
                        }
                    }
                }
            }))
            .unwrap()
        };

        let agg_req_1: Aggregations = serde_json::from_value(json!({
            "stats_i64": {
                "stats": {
                    "field": "score_i64",
                },
            },
            "stats_f64": {
                "stats": {
                    "field": "score_f64",
                },
            },
            "stats": {
                "stats": {
                    "field": "score",
                },
            },
            "range": range_agg
        }))
        .unwrap();

        let collector = AggregationCollector::from_aggs(agg_req_1, Default::default());

        let searcher = reader.searcher();
        let agg_res: AggregationResults = searcher.search(&term_query, &collector).unwrap();

        let res: Value = serde_json::from_str(&serde_json::to_string(&agg_res)?)?;
        assert_eq!(
            res["stats"],
            json!({
                "avg": 12.142857142857142,
                "count": 7,
                "max": 44.0,
                "min": 1.0,
                "sum": 85.0
            })
        );

        assert_eq!(
            res["stats_i64"],
            json!({
                "avg": 12.142857142857142,
                "count": 7,
                "max": 44.0,
                "min": 1.0,
                "sum": 85.0
            })
        );

        assert_eq!(
            res["stats_f64"],
            json!({
                "avg":  12.214285714285714,
                "count": 7,
                "max": 44.5,
                "min": 1.0,
                "sum": 85.5
            })
        );

        assert_eq!(
            res["range"]["buckets"][2]["stats"],
            json!({
                "avg": 10.666666666666666,
                "count": 3,
                "max": 14.0,
                "min": 7.0,
                "sum": 32.0
            })
        );

        assert_eq!(
            res["range"]["buckets"][3]["stats"],
            json!({
                "avg": serde_json::Value::Null,
                "count": 0,
                "max": serde_json::Value::Null,
                "min": serde_json::Value::Null,
                "sum": 0.0,
            })
        );

        Ok(())
    }

    #[test]
    fn test_stats_json() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let json = schema_builder.add_json_field("json", FAST);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut index_writer: IndexWriter = index.writer_for_tests().unwrap();
        // => Segment with empty json
        index_writer.add_document(doc!()).unwrap();
        index_writer.commit().unwrap();
        // => Segment with json, but no field partially_empty
        index_writer
            .add_document(doc!(json => json!({"different_field": "blue"})))
            .unwrap();
        index_writer.commit().unwrap();
        //// => Segment with field partially_empty
        index_writer
            .add_document(doc!(json => json!({"partially_empty": 10.0})))
            .unwrap();
        index_writer.add_document(doc!())?;
        index_writer.commit().unwrap();

        let agg_req: Aggregations = serde_json::from_value(json!({
            "my_stats": {
                "stats": {
                    "field": "json.partially_empty"
                },
            }
        }))
        .unwrap();

        let res = exec_request_with_query(agg_req, &index, None)?;

        assert_eq!(
            res["my_stats"],
            json!({
                "avg":  10.0,
                "count": 1,
                "max": 10.0,
                "min": 10.0,
                "sum": 10.0
            })
        );

        Ok(())
    }

    #[test]
    fn test_stats_json_missing() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let json = schema_builder.add_json_field("json", FAST);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut index_writer: IndexWriter = index.writer_for_tests().unwrap();
        // => Segment with empty json
        index_writer.add_document(doc!()).unwrap();
        index_writer.commit().unwrap();
        // => Segment with json, but no field partially_empty
        index_writer
            .add_document(doc!(json => json!({"different_field": "blue"})))
            .unwrap();
        index_writer.commit().unwrap();
        //// => Segment with field partially_empty
        index_writer
            .add_document(doc!(json => json!({"partially_empty": 10.0})))
            .unwrap();
        index_writer.add_document(doc!())?;
        index_writer.commit().unwrap();

        let agg_req: Aggregations = serde_json::from_value(json!({
            "my_stats": {
                "stats": {
                    "field": "json.partially_empty",
                    "missing": 0.0
                },
            }
        }))
        .unwrap();

        let res = exec_request_with_query(agg_req, &index, None)?;

        assert_eq!(
            res["my_stats"],
            json!({
                "avg":  2.5,
                "count": 4,
                "max": 10.0,
                "min": 0.0,
                "sum": 10.0
            })
        );

        // From string
        let agg_req: Aggregations = serde_json::from_value(json!({
            "my_stats": {
                "stats": {
                    "field": "json.partially_empty",
                    "missing": "0.0"
                },
            }
        }))
        .unwrap();

        let res = exec_request_with_query(agg_req, &index, None)?;

        assert_eq!(
            res["my_stats"],
            json!({
                "avg":  2.5,
                "count": 4,
                "max": 10.0,
                "min": 0.0,
                "sum": 10.0
            })
        );

        Ok(())
    }

    #[test]
    fn test_stats_json_missing_sub_agg() -> crate::Result<()> {
        // This test verifies the `collect` method (in contrast to `collect_block`), which is
        // called when the sub-aggregations are flushed.
        let mut schema_builder = Schema::builder();
        let text_field = schema_builder.add_text_field("texts", FAST);
        let score_field_f64 = schema_builder.add_f64_field("score", FAST);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);

        {
            let mut index_writer = index.writer_for_tests()?;
            // writing the segment
            index_writer.add_document(doc!(
                score_field_f64 => 10.0f64,
                text_field => "a"
            ))?;

            index_writer.add_document(doc!(text_field => "a"))?;

            index_writer.commit()?;
        }

        let agg_req: Aggregations = {
            serde_json::from_value(json!({
                "range_with_stats": {
                    "terms": {
                        "field": "texts"
                    },
                    "aggs": {
                        "my_stats": {
                            "stats": {
                                "field": "score",
                                "missing": 0.0
                            }
                        }
                    }
                }
            }))
            .unwrap()
        };

        let res = exec_request_with_query(agg_req, &index, None)?;

        assert_eq!(
            res["range_with_stats"]["buckets"][0]["my_stats"]["count"],
            2
        );
        assert_eq!(
            res["range_with_stats"]["buckets"][0]["my_stats"]["min"],
            0.0
        );
        assert_eq!(
            res["range_with_stats"]["buckets"][0]["my_stats"]["avg"],
            5.0
        );

        Ok(())
    }

    #[test]
    fn test_aggregation_extended_stats_no_variance() -> crate::Result<()> {
        let values = vec![1.0];

        let index = get_test_index_from_values(false, &values)?;

        let agg_req_1: Aggregations = serde_json::from_value(json!({
            "my_stats": {
                "extended_stats": {
                    "field": "score_f64",
                },
            }
        }))
        .unwrap();

        let collector = AggregationCollector::from_aggs(agg_req_1, Default::default());

        let reader = index.reader()?;
        let searcher = reader.searcher();
        let agg_res: AggregationResults = searcher.search(&AllQuery, &collector).unwrap();
        assert_eq!(
            agg_res
                .get_value_from_aggregation("my_stats", "count")?
                .unwrap(),
            1.0
        );
        assert_eq!(
            agg_res
                .get_value_from_aggregation("my_stats", "min")?
                .unwrap(),
            1.0
        );
        assert_eq!(
            agg_res
                .get_value_from_aggregation("my_stats", "max")?
                .unwrap(),
            1.0
        );
        assert_eq!(
            agg_res
                .get_value_from_aggregation("my_stats", "sum")?
                .unwrap(),
            1.0
        );
        assert_eq!(
            agg_res
                .get_value_from_aggregation("my_stats", "avg")?
                .unwrap(),
            1.0
        );

        assert!(agg_res
            .get_value_from_aggregation("my_stats", "std_deviation")?
            .is_none());
        assert!(agg_res
            .get_value_from_aggregation("my_stats", "std_deviation_population")?
            .is_none());
        assert!(agg_res
            .get_value_from_aggregation("my_stats", "std_deviation_sampling")?
            .is_none());
        assert!(agg_res
            .get_value_from_aggregation("my_stats", "std_deviation_bounds.lower")?
            .is_none());
        assert!(agg_res
            .get_value_from_aggregation("my_stats", "std_deviation_bounds.lower_population")?
            .is_none());
        assert!(agg_res
            .get_value_from_aggregation("my_stats", "std_deviation_bounds.lower_sampling")?
            .is_none());
        assert!(agg_res
            .get_value_from_aggregation("my_stats", "std_deviation_bounds.upper")?
            .is_none());
        assert!(agg_res
            .get_value_from_aggregation("my_stats", "std_deviation_bounds.upper_population")?
            .is_none());
        assert!(agg_res
            .get_value_from_aggregation("my_stats", "std_deviation_bounds.upper_sampling")?
            .is_none());
        assert_eq!(
            agg_res
                .get_value_from_aggregation("my_stats", "sum_of_squares")?
                .unwrap(),
            1.0
        );
        assert!(agg_res
            .get_value_from_aggregation("my_stats", "variance_population")?
            .is_none());
        assert!(agg_res
            .get_value_from_aggregation("my_stats", "variance")?
            .is_none());
        assert!(agg_res
            .get_value_from_aggregation("my_stats", "variance_sampling")?
            .is_none());

        Ok(())
    }

    #[test]
    fn test_aggregation_extended_stats() -> crate::Result<()> {
        let values = vec![1.0, 3.0, 4.0, 5.0, 8.0, 10.0];

        let index = get_test_index_from_values(false, &values)?;

        let agg_req_1: Aggregations = serde_json::from_value(json!({
            "my_stats": {
                "extended_stats": {
                    "field": "score_f64",
                },
            }
        }))
        .unwrap();

        let collector = AggregationCollector::from_aggs(agg_req_1, Default::default());

        let reader = index.reader()?;
        let searcher = reader.searcher();
        let agg_res: AggregationResults = searcher.search(&AllQuery, &collector).unwrap();
        const EXPECTED_VARIANCE: f64 = 9.138888888888888;
        assert_eq!(
            agg_res
                .get_value_from_aggregation("my_stats", "count")?
                .unwrap(),
            6.0
        );
        assert_eq!(
            agg_res
                .get_value_from_aggregation("my_stats", "min")?
                .unwrap(),
            1.0
        );
        assert_eq!(
            agg_res
                .get_value_from_aggregation("my_stats", "max")?
                .unwrap(),
            10.0
        );
        assert_eq!(
            agg_res
                .get_value_from_aggregation("my_stats", "sum")?
                .unwrap(),
            31.0
        );
        assert_nearly_equals!(
            agg_res
                .get_value_from_aggregation("my_stats", "avg")?
                .unwrap(),
            5.166666666666667,
            EPSILON_FOR_TEST
        );
        assert_nearly_equals!(
            agg_res
                .get_value_from_aggregation("my_stats", "std_deviation")?
                .unwrap(),
            EXPECTED_VARIANCE.sqrt(),
            EPSILON_FOR_TEST
        );
        assert_nearly_equals!(
            agg_res
                .get_value_from_aggregation("my_stats", "std_deviation_population")?
                .unwrap(),
            EXPECTED_VARIANCE.sqrt(),
            EPSILON_FOR_TEST
        );
        assert_nearly_equals!(
            agg_res
                .get_value_from_aggregation("my_stats", "std_deviation_sampling")?
                .unwrap(),
            3.311595788538611,
            EPSILON_FOR_TEST
        );
        assert_nearly_equals!(
            agg_res
                .get_value_from_aggregation("my_stats", "std_deviation_bounds.lower")?
                .unwrap(),
            -0.8794523824056837,
            EPSILON_FOR_TEST
        );
        assert_nearly_equals!(
            agg_res
                .get_value_from_aggregation("my_stats", "std_deviation_bounds.lower_population")?
                .unwrap(),
            -0.8794523824056837,
            0.00000000000001
        );
        assert_nearly_equals!(
            agg_res
                .get_value_from_aggregation("my_stats", "std_deviation_bounds.lower_sampling")?
                .unwrap(),
            -1.4565249104105549,
            EPSILON_FOR_TEST
        );
        assert_nearly_equals!(
            agg_res
                .get_value_from_aggregation("my_stats", "std_deviation_bounds.upper")?
                .unwrap(),
            11.212785715739017,
            EPSILON_FOR_TEST
        );
        assert_nearly_equals!(
            agg_res
                .get_value_from_aggregation("my_stats", "std_deviation_bounds.upper_population")?
                .unwrap(),
            11.212785715739017,
            EPSILON_FOR_TEST
        );
        assert_nearly_equals!(
            agg_res
                .get_value_from_aggregation("my_stats", "std_deviation_bounds.upper_sampling")?
                .unwrap(),
            11.78985824374389,
            EPSILON_FOR_TEST
        );
        assert_eq!(
            agg_res
                .get_value_from_aggregation("my_stats", "sum_of_squares")?
                .unwrap(),
            215.0
        );
        assert_nearly_equals!(
            agg_res
                .get_value_from_aggregation("my_stats", "variance_population")?
                .unwrap(),
            EXPECTED_VARIANCE,
            EPSILON_FOR_TEST
        );
        assert_nearly_equals!(
            agg_res
                .get_value_from_aggregation("my_stats", "variance")?
                .unwrap(),
            EXPECTED_VARIANCE,
            EPSILON_FOR_TEST
        );
        assert_nearly_equals!(
            agg_res
                .get_value_from_aggregation("my_stats", "variance_sampling")?
                .unwrap(),
            10.966666666666663,
            EPSILON_FOR_TEST
        );

        Ok(())
    }

    #[test]
    fn test_aggregation_extended_stats_with_sigma() -> crate::Result<()> {
        let values = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0];

        let index = get_test_index_from_values(false, &values)?;

        let agg_req_1: Aggregations = serde_json::from_value(json!({
            "my_stats": {
                "extended_stats": {
                    "field": "score_f64",
                    "sigma": 1.5
                },
            }
        }))
        .unwrap();

        let collector = AggregationCollector::from_aggs(agg_req_1, Default::default());

        let reader = index.reader()?;
        let searcher = reader.searcher();
        let agg_res: AggregationResults = searcher.search(&AllQuery, &collector).unwrap();

        const EXPECTED_VARIANCE: f64 = 2.9166666666666665;
        assert_eq!(
            agg_res
                .get_value_from_aggregation("my_stats", "count")?
                .unwrap(),
            6.0
        );
        assert_eq!(
            agg_res
                .get_value_from_aggregation("my_stats", "min")?
                .unwrap(),
            1.0
        );
        assert_eq!(
            agg_res
                .get_value_from_aggregation("my_stats", "max")?
                .unwrap(),
            6.0
        );
        assert_eq!(
            agg_res
                .get_value_from_aggregation("my_stats", "sum")?
                .unwrap(),
            21.0
        );
        assert_eq!(
            agg_res
                .get_value_from_aggregation("my_stats", "avg")?
                .unwrap(),
            3.5
        );
        assert_nearly_equals!(
            agg_res
                .get_value_from_aggregation("my_stats", "std_deviation")?
                .unwrap(),
            EXPECTED_VARIANCE.sqrt(),
            EPSILON_FOR_TEST
        );
        assert_nearly_equals!(
            agg_res
                .get_value_from_aggregation("my_stats", "std_deviation_population")?
                .unwrap(),
            EXPECTED_VARIANCE.sqrt(),
            EPSILON_FOR_TEST
        );
        assert_nearly_equals!(
            agg_res
                .get_value_from_aggregation("my_stats", "std_deviation_sampling")?
                .unwrap(),
            1.8708286933869709,
            EPSILON_FOR_TEST
        );
        assert_nearly_equals!(
            agg_res
                .get_value_from_aggregation("my_stats", "std_deviation_bounds.lower")?
                .unwrap(),
            0.9382623085101005,
            EPSILON_FOR_TEST
        );
        assert_nearly_equals!(
            agg_res
                .get_value_from_aggregation("my_stats", "std_deviation_bounds.lower_population")?
                .unwrap(),
            0.9382623085101005,
            EPSILON_FOR_TEST
        );
        assert_nearly_equals!(
            agg_res
                .get_value_from_aggregation("my_stats", "std_deviation_bounds.lower_sampling")?
                .unwrap(),
            0.6937569599195434,
            EPSILON_FOR_TEST
        );
        assert_nearly_equals!(
            agg_res
                .get_value_from_aggregation("my_stats", "std_deviation_bounds.upper")?
                .unwrap(),
            6.061737691489899,
            EPSILON_FOR_TEST
        );
        assert_nearly_equals!(
            agg_res
                .get_value_from_aggregation("my_stats", "std_deviation_bounds.upper_population")?
                .unwrap(),
            6.061737691489899,
            EPSILON_FOR_TEST
        );
        assert_nearly_equals!(
            agg_res
                .get_value_from_aggregation("my_stats", "std_deviation_bounds.upper_sampling")?
                .unwrap(),
            6.3062430400804566,
            EPSILON_FOR_TEST
        );
        assert_eq!(
            agg_res
                .get_value_from_aggregation("my_stats", "sum_of_squares")?
                .unwrap(),
            91.0
        );
        assert_nearly_equals!(
            agg_res
                .get_value_from_aggregation("my_stats", "variance_population")?
                .unwrap(),
            EXPECTED_VARIANCE,
            EPSILON_FOR_TEST
        );
        assert_nearly_equals!(
            agg_res
                .get_value_from_aggregation("my_stats", "variance")?
                .unwrap(),
            EXPECTED_VARIANCE,
            EPSILON_FOR_TEST
        );
        assert_nearly_equals!(
            agg_res
                .get_value_from_aggregation("my_stats", "variance_sampling")?
                .unwrap(),
            3.5,
            EPSILON_FOR_TEST
        );

        Ok(())
    }

    #[test]
    fn test_aggregation_extended_stats_with_variance_similar_to_mean() -> crate::Result<()> {
        let values = vec![50.01, 50.02, 50.01, 50.03, 50.01, 50.02];

        let index = get_test_index_from_values(false, &values)?;

        let agg_req_1: Aggregations = serde_json::from_value(json!({
            "my_stats": {
                "extended_stats": {
                    "field": "score_f64",
                    "sigma": 1.5
                },
            }
        }))
        .unwrap();

        let collector = AggregationCollector::from_aggs(agg_req_1, Default::default());

        let reader = index.reader()?;
        let searcher = reader.searcher();
        let agg_res: AggregationResults = searcher.search(&AllQuery, &collector).unwrap();
        const EXPECTED_VARIANCE: f64 = 5.5555555555608854e-5;
        assert_eq!(
            agg_res
                .get_value_from_aggregation("my_stats", "count")?
                .unwrap(),
            6.0
        );
        assert_eq!(
            agg_res
                .get_value_from_aggregation("my_stats", "min")?
                .unwrap(),
            50.01
        );
        assert_eq!(
            agg_res
                .get_value_from_aggregation("my_stats", "max")?
                .unwrap(),
            50.03
        );
        assert_eq!(
            agg_res
                .get_value_from_aggregation("my_stats", "sum")?
                .unwrap(),
            300.1
        );
        assert_nearly_equals!(
            agg_res
                .get_value_from_aggregation("my_stats", "avg")?
                .unwrap(),
            50.01666666666667,
            EPSILON_FOR_TEST
        );
        assert_nearly_equals!(
            agg_res
                .get_value_from_aggregation("my_stats", "std_deviation")?
                .unwrap(),
            EXPECTED_VARIANCE.sqrt(),
            EPSILON_FOR_TEST
        );
        assert_nearly_equals!(
            agg_res
                .get_value_from_aggregation("my_stats", "std_deviation_population")?
                .unwrap(),
            EXPECTED_VARIANCE.sqrt(),
            EPSILON_FOR_TEST
        );
        assert_nearly_equals!(
            agg_res
                .get_value_from_aggregation("my_stats", "std_deviation_sampling")?
                .unwrap(),
            0.008164965809279263,
            EPSILON_FOR_TEST
        );
        assert_nearly_equals!(
            agg_res
                .get_value_from_aggregation("my_stats", "std_deviation_bounds.lower")?
                .unwrap(),
            50.00548632677917,
            EPSILON_FOR_TEST
        );
        assert_nearly_equals!(
            agg_res
                .get_value_from_aggregation("my_stats", "std_deviation_bounds.lower_population")?
                .unwrap(),
            50.00548632677917,
            EPSILON_FOR_TEST
        );
        assert_nearly_equals!(
            agg_res
                .get_value_from_aggregation("my_stats", "std_deviation_bounds.lower_sampling")?
                .unwrap(),
            50.00441921795275,
            EPSILON_FOR_TEST
        );
        assert_nearly_equals!(
            agg_res
                .get_value_from_aggregation("my_stats", "std_deviation_bounds.upper")?
                .unwrap(),
            50.027847006554175,
            EPSILON_FOR_TEST
        );
        assert_nearly_equals!(
            agg_res
                .get_value_from_aggregation("my_stats", "std_deviation_bounds.upper_population")?
                .unwrap(),
            50.027847006554175,
            EPSILON_FOR_TEST
        );
        assert_nearly_equals!(
            agg_res
                .get_value_from_aggregation("my_stats", "std_deviation_bounds.upper_sampling")?
                .unwrap(),
            50.028914115380594,
            EPSILON_FOR_TEST
        );
        assert_eq!(
            agg_res
                .get_value_from_aggregation("my_stats", "sum_of_squares")?
                .unwrap(),
            15010.002
        );
        assert_nearly_equals!(
            agg_res
                .get_value_from_aggregation("my_stats", "variance_population")?
                .unwrap(),
            EXPECTED_VARIANCE,
            EPSILON_FOR_TEST
        );
        assert_nearly_equals!(
            agg_res
                .get_value_from_aggregation("my_stats", "variance")?
                .unwrap(),
            EXPECTED_VARIANCE,
            EPSILON_FOR_TEST
        );
        assert_nearly_equals!(
            agg_res
                .get_value_from_aggregation("my_stats", "variance_sampling")?
                .unwrap(),
            6.666666666670718e-5,
            EPSILON_FOR_TEST
        );

        Ok(())
    }

    #[test]
    fn extended_stat_zero_value() {
        let intermediate_extend_stats = IntermediateExtendedStats::default();
        let extended_stats = intermediate_extend_stats.finalize();
        assert!(extended_stats.variance.is_none());
        assert!(extended_stats.variance_population.is_none());
        assert!(extended_stats.variance_sampling.is_none());
        assert!(extended_stats.sum_of_squares.is_none());
        assert!(extended_stats.std_deviation.is_none());
        assert!(extended_stats.std_deviation_population.is_none());
        assert!(extended_stats.std_deviation_sampling.is_none());
        assert!(extended_stats.std_deviation_bounds.is_none());
    }

    #[test]
    fn extended_stat_one_value() {
        let mut intermediate_extend_stats = IntermediateExtendedStats::default();
        intermediate_extend_stats.collect(1.0f64);
        let extended_stats = intermediate_extend_stats.finalize();
        assert!(extended_stats.variance.is_none());
        assert!(extended_stats.variance_population.is_none());
        assert!(extended_stats.variance_sampling.is_none());
        assert!(extended_stats.std_deviation.is_none());
        assert!(extended_stats.std_deviation_population.is_none());
        assert!(extended_stats.std_deviation_sampling.is_none());
        assert!(extended_stats.std_deviation_bounds.is_none());
        let sum_of_squares = extended_stats.sum_of_squares.unwrap();
        assert_eq!(1.0f64, sum_of_squares);
    }

    #[test]
    fn extended_stat_multiple_values() {
        let mut intermediate_extend_stats = IntermediateExtendedStats::default();
        intermediate_extend_stats.collect(1.0f64);
        intermediate_extend_stats.collect(3.0f64);
        intermediate_extend_stats.collect(4.0f64);
        intermediate_extend_stats.collect(5.0f64);
        intermediate_extend_stats.collect(8.0f64);
        intermediate_extend_stats.collect(10.0f64);
        let extended_stats = intermediate_extend_stats.finalize();
        let variance = extended_stats.variance.unwrap();
        const EXPECTED_VARIANCE: f64 = 9.138888888888888;
        assert_eq!(EXPECTED_VARIANCE, variance);
        let variance_population = extended_stats.variance_population.unwrap();
        assert_eq!(EXPECTED_VARIANCE, variance_population);
        let variance_sampling = extended_stats.variance_sampling.unwrap();
        assert_eq!(10.966666666666665f64, variance_sampling);
        let std_deviation = extended_stats.std_deviation.unwrap();
        assert_eq!(EXPECTED_VARIANCE.sqrt(), std_deviation);
        let std_deviation_population = extended_stats.std_deviation_population.unwrap();
        assert_eq!(EXPECTED_VARIANCE.sqrt(), std_deviation_population);
        let std_deviation_sampling = extended_stats.std_deviation_sampling.unwrap();
        assert_eq!(10.966666666666665f64.sqrt(), std_deviation_sampling);
        let sum_of_squares = extended_stats.sum_of_squares.unwrap();
        assert_eq!(215.0, sum_of_squares);
        let avg = extended_stats.avg.unwrap();
        assert_eq!(5.166666666666667, avg);
    }

    #[test]
    fn merge_empty_with_one_value() {
        let mut intermediate_extend_stats = IntermediateExtendedStats::default();
        let mut intermediate_extend_stats1 = IntermediateExtendedStats::default();
        intermediate_extend_stats1.collect(1.0f64);
        intermediate_extend_stats.merge_fruits(intermediate_extend_stats1);
        let extended_stats = intermediate_extend_stats.finalize();
        assert!(extended_stats.variance.is_none());
        assert!(extended_stats.variance_population.is_none());
        assert!(extended_stats.variance_sampling.is_none());
        assert!(extended_stats.std_deviation.is_none());
        assert!(extended_stats.std_deviation_population.is_none());
        assert!(extended_stats.std_deviation_sampling.is_none());
        let sum_of_squares = extended_stats.sum_of_squares.unwrap();
        assert_eq!(1.0f64, sum_of_squares);
    }

    #[test]
    fn merge_empty_with_multiple_values() {
        let mut intermediate_extend_stats1 = IntermediateExtendedStats::default();
        intermediate_extend_stats1.collect(1.0f64);
        intermediate_extend_stats1.collect(2.0f64);
        intermediate_extend_stats1.collect(3.0f64);
        intermediate_extend_stats1.collect(4.0f64);
        intermediate_extend_stats1.collect(5.0f64);

        let mut intermediate_extend_stats = IntermediateExtendedStats::default();
        intermediate_extend_stats.merge_fruits(intermediate_extend_stats1);
        let extended_stats = intermediate_extend_stats.finalize();
        const EXPECTED_VARIANCE: f64 = 2.0;
        let variance = extended_stats.variance.unwrap();
        assert_eq!(EXPECTED_VARIANCE, variance);
        let variance_population = extended_stats.variance_population.unwrap();
        assert_eq!(EXPECTED_VARIANCE, variance_population);
        let variance_sampling = extended_stats.variance_sampling.unwrap();
        assert_eq!(2.5f64, variance_sampling);
        let std_deviation = extended_stats.std_deviation.unwrap();
        assert_eq!(EXPECTED_VARIANCE.sqrt(), std_deviation);
        let std_deviation_population = extended_stats.std_deviation_population.unwrap();
        assert_eq!(EXPECTED_VARIANCE.sqrt(), std_deviation_population);
        let std_deviation_sampling = extended_stats.std_deviation_sampling.unwrap();
        assert_eq!(2.5f64.sqrt(), std_deviation_sampling);
        let sum_of_squares = extended_stats.sum_of_squares.unwrap();
        assert_eq!(55f64, sum_of_squares);
    }

    #[test]
    fn merge_non_empty_extended_stats() {
        let mut intermediate_extend_stats1 = IntermediateExtendedStats::default();
        intermediate_extend_stats1.collect(3.0f64);
        intermediate_extend_stats1.collect(4.0f64);
        intermediate_extend_stats1.collect(5.0f64);

        let mut intermediate_extend_stats = IntermediateExtendedStats::default();
        intermediate_extend_stats.collect(1.0f64);
        intermediate_extend_stats.collect(2.0f64);
        intermediate_extend_stats.merge_fruits(intermediate_extend_stats1);
        let extended_stats = intermediate_extend_stats.finalize();

        let variance = extended_stats.variance.unwrap();
        assert_eq!(2.0f64, variance);
        let variance_population = extended_stats.variance_population.unwrap();
        assert_eq!(2.0f64, variance_population);
        let variance_sampling = extended_stats.variance_sampling.unwrap();
        assert_eq!(2.5f64, variance_sampling);
        let std_deviation = extended_stats.std_deviation.unwrap();
        assert_eq!(2.0f64.sqrt(), std_deviation);
        let std_deviation_population = extended_stats.std_deviation_population.unwrap();
        assert_eq!(2.0f64.sqrt(), std_deviation_population);
        let std_deviation_sampling = extended_stats.std_deviation_sampling.unwrap();
        assert_eq!(2.5f64.sqrt(), std_deviation_sampling);
        let sum_of_squares = extended_stats.sum_of_squares.unwrap();
        assert_eq!(55f64, sum_of_squares);

        let mut intermediate_extend_stats = IntermediateExtendedStats::default();
        intermediate_extend_stats.collect(1.0f64);
        intermediate_extend_stats.collect(3.0f64);
        intermediate_extend_stats.collect(4.0f64);
        let mut intermediate_extend_stats1 = IntermediateExtendedStats::default();
        intermediate_extend_stats1.collect(5.0f64);
        intermediate_extend_stats1.collect(8.0f64);
        intermediate_extend_stats1.collect(10.0f64);
        intermediate_extend_stats.merge_fruits(intermediate_extend_stats1);
        let extended_stats = intermediate_extend_stats.finalize();
        const EXPECTED_VARIANCE: f64 = 9.138888888888888;
        let variance = extended_stats.variance.unwrap();
        assert_eq!(EXPECTED_VARIANCE, variance);
        let variance_population = extended_stats.variance_population.unwrap();
        assert_eq!(EXPECTED_VARIANCE, variance_population);
        let variance_sampling = extended_stats.variance_sampling.unwrap();
        assert_eq!(10.966666666666665f64, variance_sampling);
        let std_deviation = extended_stats.std_deviation.unwrap();
        assert_eq!(EXPECTED_VARIANCE.sqrt(), std_deviation);
        let std_deviation_population = extended_stats.std_deviation_population.unwrap();
        assert_eq!(EXPECTED_VARIANCE.sqrt(), std_deviation_population);
        let std_deviation_sampling = extended_stats.std_deviation_sampling.unwrap();
        assert_eq!(10.966666666666665f64.sqrt(), std_deviation_sampling);
        let sum_of_squares = extended_stats.sum_of_squares.unwrap();
        assert_eq!(215f64, sum_of_squares);
        let avg = extended_stats.avg.unwrap();
        assert_eq!(5.166666666666667, avg);
    }

    #[test]
    fn merge_and_then_collect_non_empty_extended_stats() {
        let mut intermediate_extend_stats = IntermediateExtendedStats::default();
        intermediate_extend_stats.collect(1.0f64);
        intermediate_extend_stats.collect(3.0f64);

        let mut intermediate_extend_stats1 = IntermediateExtendedStats::default();
        intermediate_extend_stats1.collect(5.0f64);
        intermediate_extend_stats1.collect(8.0f64);
        intermediate_extend_stats1.collect(10.0f64);
        intermediate_extend_stats.merge_fruits(intermediate_extend_stats1);
        intermediate_extend_stats.collect(4.0f64);
        let extended_stats = intermediate_extend_stats.finalize();
        const EXPECTED_VARIANCE: f64 = 9.138888888888888;
        let variance = extended_stats.variance.unwrap();
        assert_nearly_equals!(EXPECTED_VARIANCE, variance, EPSILON_FOR_TEST);
        let variance_population = extended_stats.variance_population.unwrap();
        assert_nearly_equals!(EXPECTED_VARIANCE, variance_population, EPSILON_FOR_TEST);
        let variance_sampling = extended_stats.variance_sampling.unwrap();
        assert_nearly_equals!(10.966666666666665, variance_sampling, EPSILON_FOR_TEST);
        let std_deviation = extended_stats.std_deviation.unwrap();
        assert_nearly_equals!(EXPECTED_VARIANCE.sqrt(), std_deviation, EPSILON_FOR_TEST);
        let std_deviation_population = extended_stats.std_deviation_population.unwrap();
        assert_nearly_equals!(
            EXPECTED_VARIANCE.sqrt(),
            std_deviation_population,
            EPSILON_FOR_TEST
        );
        let std_deviation_sampling = extended_stats.std_deviation_sampling.unwrap();
        assert_nearly_equals!(
            10.966666666666665_f64.sqrt(),
            std_deviation_sampling,
            EPSILON_FOR_TEST
        );
        let sum_of_squares = extended_stats.sum_of_squares.unwrap();
        assert_eq!(215.0, sum_of_squares);
        let avg = extended_stats.avg.unwrap();
        assert_eq!(5.166666666666667, avg);
    }
}

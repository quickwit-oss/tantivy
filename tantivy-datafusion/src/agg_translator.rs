use std::collections::HashMap;

use datafusion::common::{DataFusionError, Result};
use datafusion::dataframe::DataFrame;
use datafusion::logical_expr::expr::Sort;
use datafusion::logical_expr::{col, lit, Expr, ExprSchemable, SortExpr};
use tantivy::aggregation::agg_req::{Aggregation, AggregationVariants, Aggregations};
use tantivy::aggregation::bucket::{
    CustomOrder, DateHistogramAggregationReq, HistogramAggregation, Order as TantivyOrder,
    OrderTarget, RangeAggregation, RangeAggregationRange, TermsAggregation,
};
use tantivy::aggregation::metric::{
    AverageAggregation, CardinalityAggregationReq, CountAggregation, ExtendedStatsAggregation,
    MaxAggregation, MinAggregation, PercentilesAggregationReq, StatsAggregation, SumAggregation,
};

/// Translate tantivy `Aggregations` into one `DataFrame` per top-level agg key.
///
/// Different bucket aggs have different GROUP BY shapes, so they can't share
/// a single `DataFrame`. The caller provides a base `DataFrame` (from
/// `ctx.table("f")`), optionally pre-filtered.
pub fn translate_aggregations(
    df: DataFrame,
    aggs: &Aggregations,
) -> Result<HashMap<String, DataFrame>> {
    let mut result = HashMap::with_capacity(aggs.len());
    for (name, agg) in aggs {
        let translated = translate_single(df.clone(), name, agg)?;
        result.insert(name.clone(), translated);
    }
    Ok(result)
}

fn translate_single(df: DataFrame, name: &str, agg: &Aggregation) -> Result<DataFrame> {
    match &agg.agg {
        // Metric aggregations (no GROUP BY)
        AggregationVariants::Average(a) => translate_metric_only(df, name, MetricKind::Avg(a)),
        AggregationVariants::Sum(a) => translate_metric_only(df, name, MetricKind::Sum(a)),
        AggregationVariants::Min(a) => translate_metric_only(df, name, MetricKind::Min(a)),
        AggregationVariants::Max(a) => translate_metric_only(df, name, MetricKind::Max(a)),
        AggregationVariants::Count(a) => translate_metric_only(df, name, MetricKind::Count(a)),
        AggregationVariants::Stats(a) => translate_metric_only(df, name, MetricKind::Stats(a)),
        AggregationVariants::ExtendedStats(a) => {
            translate_metric_only(df, name, MetricKind::ExtendedStats(a))
        }
        AggregationVariants::Percentiles(a) => {
            translate_metric_only(df, name, MetricKind::Percentiles(a))
        }
        AggregationVariants::Cardinality(a) => {
            translate_metric_only(df, name, MetricKind::Cardinality(a))
        }

        // Bucket aggregations
        AggregationVariants::Terms(terms) => {
            translate_terms(df, name, terms, &agg.sub_aggregation)
        }
        AggregationVariants::Histogram(hist) => {
            translate_histogram(df, name, hist, &agg.sub_aggregation)
        }
        AggregationVariants::DateHistogram(dh) => {
            translate_date_histogram(df, name, dh, &agg.sub_aggregation)
        }
        AggregationVariants::Range(range) => {
            translate_range(df, name, range, &agg.sub_aggregation)
        }

        // Deferred
        AggregationVariants::Filter(_) => Err(DataFusionError::Plan(
            "Filter aggregation is not yet supported; pre-filter the DataFrame instead".into(),
        )),
        AggregationVariants::TopHits(_) => Err(DataFusionError::Plan(
            "TopHits sub-aggregation is not yet supported".into(),
        )),
    }
}

// ---------------------------------------------------------------------------
// Metric helpers
// ---------------------------------------------------------------------------

enum MetricKind<'a> {
    Avg(&'a AverageAggregation),
    Sum(&'a SumAggregation),
    Min(&'a MinAggregation),
    Max(&'a MaxAggregation),
    Count(&'a CountAggregation),
    Stats(&'a StatsAggregation),
    ExtendedStats(&'a ExtendedStatsAggregation),
    Percentiles(&'a PercentilesAggregationReq),
    Cardinality(&'a CardinalityAggregationReq),
}

fn metric_field_expr(field: &str, missing: Option<f64>) -> Expr {
    match missing {
        Some(val) => datafusion::functions::core::expr_fn::coalesce(vec![col(field), lit(val)]),
        None => col(field),
    }
}

fn single_metric_exprs(name: &str, kind: &MetricKind) -> Result<Vec<Expr>> {
    use datafusion::functions_aggregate::expr_fn::*;

    match kind {
        MetricKind::Avg(a) => {
            let f = metric_field_expr(&a.field, a.missing);
            Ok(vec![avg(f).alias(name)])
        }
        MetricKind::Sum(a) => {
            let f = metric_field_expr(&a.field, a.missing);
            Ok(vec![sum(f).alias(name)])
        }
        MetricKind::Min(a) => {
            let f = metric_field_expr(&a.field, a.missing);
            Ok(vec![min(f).alias(name)])
        }
        MetricKind::Max(a) => {
            let f = metric_field_expr(&a.field, a.missing);
            Ok(vec![max(f).alias(name)])
        }
        MetricKind::Count(a) => {
            let f = metric_field_expr(&a.field, a.missing);
            Ok(vec![count(f).alias(name)])
        }
        MetricKind::Stats(a) => {
            let f = metric_field_expr(&a.field, a.missing);
            Ok(vec![
                min(f.clone()).alias(&format!("{name}_min")),
                max(f.clone()).alias(&format!("{name}_max")),
                sum(f.clone()).alias(&format!("{name}_sum")),
                count(f.clone()).alias(&format!("{name}_count")),
                avg(f).alias(&format!("{name}_avg")),
            ])
        }
        MetricKind::ExtendedStats(a) => {
            let f = metric_field_expr(&a.field, a.missing);
            Ok(vec![
                min(f.clone()).alias(&format!("{name}_min")),
                max(f.clone()).alias(&format!("{name}_max")),
                sum(f.clone()).alias(&format!("{name}_sum")),
                count(f.clone()).alias(&format!("{name}_count")),
                avg(f.clone()).alias(&format!("{name}_avg")),
                var_pop(f.clone()).alias(&format!("{name}_variance_population")),
                stddev_pop(f).alias(&format!("{name}_std_deviation_population")),
            ])
        }
        MetricKind::Percentiles(a) => {
            let f = metric_field_expr(&a.field, a.missing);
            let percents = a
                .percents
                .as_deref()
                .unwrap_or(&[1.0, 5.0, 25.0, 50.0, 75.0, 95.0, 99.0]);
            Ok(percents
                .iter()
                .map(|p| {
                    let sort_expr = Sort {
                        expr: f.clone(),
                        asc: true,
                        nulls_first: true,
                    };
                    approx_percentile_cont(sort_expr, lit(*p / 100.0), None)
                        .alias(&format!("{name}_p{}", format_percent(*p)))
                })
                .collect())
        }
        MetricKind::Cardinality(a) => {
            // approx_distinct doesn't support Dictionary-encoded columns;
            // TryCast to Utf8 unwraps dictionary encoding for string fields
            // and is a no-op identity for already-Utf8 columns.
            let f = Expr::TryCast(datafusion::logical_expr::expr::TryCast {
                expr: Box::new(col(&a.field)),
                data_type: arrow::datatypes::DataType::Utf8,
            });
            Ok(vec![approx_distinct(f).alias(name)])
        }
    }
}

fn format_percent(p: f64) -> String {
    if p == p.floor() {
        format!("{}", p as i64)
    } else {
        format!("{p}")
    }
}

fn translate_metric_only(df: DataFrame, name: &str, kind: MetricKind) -> Result<DataFrame> {
    let exprs = single_metric_exprs(name, &kind)?;
    df.aggregate(vec![], exprs)
}

/// Collect metric `Expr`s for sub-aggregations inside a bucket agg.
fn collect_metric_exprs(sub_aggs: &Aggregations) -> Result<Vec<Expr>> {
    let mut exprs = Vec::new();
    for (sub_name, sub_agg) in sub_aggs {
        let kind = match &sub_agg.agg {
            AggregationVariants::Average(a) => MetricKind::Avg(a),
            AggregationVariants::Sum(a) => MetricKind::Sum(a),
            AggregationVariants::Min(a) => MetricKind::Min(a),
            AggregationVariants::Max(a) => MetricKind::Max(a),
            AggregationVariants::Count(a) => MetricKind::Count(a),
            AggregationVariants::Stats(a) => MetricKind::Stats(a),
            AggregationVariants::ExtendedStats(a) => MetricKind::ExtendedStats(a),
            AggregationVariants::Percentiles(a) => MetricKind::Percentiles(a),
            AggregationVariants::Cardinality(a) => MetricKind::Cardinality(a),
            AggregationVariants::TopHits(_) => {
                return Err(DataFusionError::Plan(
                    "TopHits sub-aggregation is not yet supported".into(),
                ));
            }
            other => {
                return Err(DataFusionError::Plan(format!(
                    "Nested bucket aggregation '{sub_name}' ({other:?}) inside bucket agg is not yet supported"
                )));
            }
        };
        exprs.extend(single_metric_exprs(sub_name, &kind)?);
    }
    Ok(exprs)
}

// ---------------------------------------------------------------------------
// Terms bucket
// ---------------------------------------------------------------------------

fn translate_terms(
    df: DataFrame,
    _name: &str,
    terms: &TermsAggregation,
    sub_aggs: &Aggregations,
) -> Result<DataFrame> {
    use datafusion::functions_aggregate::expr_fn::count;

    let field = &terms.field;
    let size = terms.size.unwrap_or(10) as usize;
    let min_doc_count = terms.min_doc_count.unwrap_or(1);

    let group_by = vec![col(field)];

    let mut agg_exprs = vec![count(lit(1)).alias("doc_count")];
    agg_exprs.extend(collect_metric_exprs(sub_aggs)?);

    let df = df.aggregate(group_by, agg_exprs)?;

    // HAVING: min_doc_count
    let df = if min_doc_count > 1 {
        df.filter(col("doc_count").gt_eq(lit(min_doc_count as i64)))?
    } else {
        df
    };

    // ORDER BY
    let sort_exprs = terms_sort_exprs(&terms.order, field);
    let df = df.sort(sort_exprs)?;

    // LIMIT
    df.limit(0, Some(size))
}

fn terms_sort_exprs(order: &Option<CustomOrder>, field: &str) -> Vec<SortExpr> {
    let (target_col, asc) = match order {
        Some(custom) => {
            let target = match &custom.target {
                OrderTarget::Count => col("doc_count"),
                OrderTarget::Key => col(field),
                OrderTarget::SubAggregation(sub) => col(sub.as_str()),
            };
            let asc = matches!(custom.order, TantivyOrder::Asc);
            (target, asc)
        }
        // Default: count DESC
        None => (col("doc_count"), false),
    };
    vec![target_col.sort(asc, true)]
}

// ---------------------------------------------------------------------------
// Histogram bucket
// ---------------------------------------------------------------------------

fn translate_histogram(
    df: DataFrame,
    _name: &str,
    hist: &HistogramAggregation,
    sub_aggs: &Aggregations,
) -> Result<DataFrame> {
    use datafusion::functions::math::expr_fn::floor;
    use datafusion::functions_aggregate::expr_fn::count;

    let field = &hist.field;
    let interval = hist.interval;
    let offset = hist.offset.unwrap_or(0.0);
    let min_doc_count = hist.min_doc_count.unwrap_or(0);

    // bucket_key = floor((col - offset) / interval) * interval + offset
    let bucket_expr = (floor((col(field) - lit(offset)) / lit(interval)) * lit(interval)
        + lit(offset))
    .alias("bucket");

    let mut agg_exprs = vec![count(lit(1)).alias("doc_count")];
    agg_exprs.extend(collect_metric_exprs(sub_aggs)?);

    let df = df.aggregate(vec![bucket_expr], agg_exprs)?;

    // min_doc_count filter
    let df = if min_doc_count > 0 {
        df.filter(col("doc_count").gt_eq(lit(min_doc_count as i64)))?
    } else {
        df
    };

    // hard_bounds filter
    let df = if let Some(bounds) = &hist.hard_bounds {
        let mut f = df;
        f = f.filter(col("bucket").gt_eq(lit(bounds.min)))?;
        f = f.filter(col("bucket").lt(lit(bounds.max)))?;
        f
    } else {
        df
    };

    // ORDER BY bucket ASC
    df.sort(vec![col("bucket").sort(true, true)])
}

// ---------------------------------------------------------------------------
// Date histogram bucket
// ---------------------------------------------------------------------------

fn translate_date_histogram(
    df: DataFrame,
    _name: &str,
    dh: &DateHistogramAggregationReq,
    sub_aggs: &Aggregations,
) -> Result<DataFrame> {
    use datafusion::functions_aggregate::expr_fn::count;

    let field = &dh.field;
    let min_doc_count = dh.min_doc_count.unwrap_or(0);

    let interval_str = dh.fixed_interval.as_deref().ok_or_else(|| {
        DataFusionError::Plan(
            "DateHistogram requires `fixed_interval`; `interval` and `calendar_interval` are not supported".into(),
        )
    })?;

    let bucket_expr = if let Some(trunc_unit) = map_interval_to_date_trunc(interval_str) {
        // Clean intervals: use date_trunc
        datafusion::functions::datetime::expr_fn::date_trunc(lit(trunc_unit), col(field))
            .alias("bucket")
    } else {
        // Arbitrary intervals: numeric approach on microseconds
        use datafusion::functions::math::expr_fn::floor;

        let micros = parse_interval_to_micros(interval_str)?;
        let offset_micros = dh
            .offset
            .as_deref()
            .map(parse_offset_to_micros)
            .transpose()?
            .unwrap_or(0i64);

        // Cast timestamp to i64 micros, apply histogram formula, cast back
        let ts_micros = col(field).cast_to(
            &arrow::datatypes::DataType::Int64,
            df.schema(),
        )?;
        let bucket_micros = floor(
            (ts_micros - lit(offset_micros)) / lit(micros),
        ) * lit(micros)
            + lit(offset_micros);
        bucket_micros
            .cast_to(
                &arrow::datatypes::DataType::Timestamp(
                    arrow::datatypes::TimeUnit::Microsecond,
                    None,
                ),
                df.schema(),
            )?
            .alias("bucket")
    };

    let mut agg_exprs = vec![count(lit(1)).alias("doc_count")];
    agg_exprs.extend(collect_metric_exprs(sub_aggs)?);

    let df = df.aggregate(vec![bucket_expr], agg_exprs)?;

    let df = if min_doc_count > 0 {
        df.filter(col("doc_count").gt_eq(lit(min_doc_count as i64)))?
    } else {
        df
    };

    if let Some(bounds) = &dh.hard_bounds {
        let mut f = df;
        f = f.filter(col("bucket").gt_eq(lit(bounds.min)))?;
        f = f.filter(col("bucket").lt(lit(bounds.max)))?;
        f.sort(vec![col("bucket").sort(true, true)])
    } else {
        df.sort(vec![col("bucket").sort(true, true)])
    }
}

/// Map clean fixed_interval strings to date_trunc units.
fn map_interval_to_date_trunc(interval: &str) -> Option<&'static str> {
    match interval {
        "1s" => Some("second"),
        "1m" => Some("minute"),
        "1h" => Some("hour"),
        "1d" => Some("day"),
        _ => None,
    }
}

/// Parse a fixed_interval string like "30m", "12h", "1000ms" into microseconds.
fn parse_interval_to_micros(interval: &str) -> Result<i64> {
    if let Some(val) = interval.strip_suffix("ms") {
        let n: i64 = val
            .parse()
            .map_err(|e| DataFusionError::Plan(format!("Invalid interval '{interval}': {e}")))?;
        return Ok(n * 1_000);
    }
    if let Some(val) = interval.strip_suffix('s') {
        let n: i64 = val
            .parse()
            .map_err(|e| DataFusionError::Plan(format!("Invalid interval '{interval}': {e}")))?;
        return Ok(n * 1_000_000);
    }
    if let Some(val) = interval.strip_suffix('m') {
        let n: i64 = val
            .parse()
            .map_err(|e| DataFusionError::Plan(format!("Invalid interval '{interval}': {e}")))?;
        return Ok(n * 60 * 1_000_000);
    }
    if let Some(val) = interval.strip_suffix('h') {
        let n: i64 = val
            .parse()
            .map_err(|e| DataFusionError::Plan(format!("Invalid interval '{interval}': {e}")))?;
        return Ok(n * 3600 * 1_000_000);
    }
    if let Some(val) = interval.strip_suffix('d') {
        let n: i64 = val
            .parse()
            .map_err(|e| DataFusionError::Plan(format!("Invalid interval '{interval}': {e}")))?;
        return Ok(n * 86_400 * 1_000_000);
    }
    Err(DataFusionError::Plan(format!(
        "Unsupported interval format: '{interval}'"
    )))
}

/// Parse offset strings like "-5d", "+2h", "1h" into microseconds.
fn parse_offset_to_micros(offset: &str) -> Result<i64> {
    let (sign, rest) = if let Some(s) = offset.strip_prefix('-') {
        (-1i64, s)
    } else if let Some(s) = offset.strip_prefix('+') {
        (1i64, s)
    } else {
        (1i64, offset)
    };
    Ok(sign * parse_interval_to_micros(rest)?)
}

// ---------------------------------------------------------------------------
// Range bucket
// ---------------------------------------------------------------------------

fn translate_range(
    df: DataFrame,
    _name: &str,
    range: &RangeAggregation,
    sub_aggs: &Aggregations,
) -> Result<DataFrame> {
    use datafusion::functions_aggregate::expr_fn::count;

    let field = &range.field;
    let base_metric_exprs = collect_metric_exprs(sub_aggs)?;

    // Build filtered aggregates: one (doc_count + metrics) set per bucket.
    // Single pass, no GROUP BY, no string allocations during scan.
    let mut agg_exprs = Vec::new();
    let mut bucket_keys = Vec::new();

    for (i, entry) in range.ranges.iter().enumerate() {
        let cond = range_condition(field, entry);
        bucket_keys.push(range_bucket_key(entry));

        // doc_count FILTER (WHERE cond)
        agg_exprs.push(
            add_agg_filter(count(lit(1)), cond.clone())
                .alias(&format!("__b{i}_doc_count")),
        );

        // sub-agg metrics FILTER (WHERE cond)
        for (j, base_expr) in base_metric_exprs.iter().enumerate() {
            let (inner, alias) = strip_alias(base_expr);
            agg_exprs.push(
                add_agg_filter(inner, cond.clone())
                    .alias(&format!("__b{i}_m{j}_{alias}")),
            );
        }
    }

    // Single-row aggregate with all buckets as columns
    let flat_df = df.aggregate(vec![], agg_exprs)?;

    // Restructure into one row per bucket via UNION ALL
    let metric_aliases: Vec<String> = base_metric_exprs
        .iter()
        .enumerate()
        .map(|(j, e)| {
            let (_, alias) = strip_alias(e);
            format!("m{j}_{alias}")
        })
        .collect();
    let metric_names: Vec<String> = base_metric_exprs
        .iter()
        .map(|e| strip_alias(e).1)
        .collect();

    let mut result: Option<DataFrame> = None;
    for (i, key) in bucket_keys.iter().enumerate() {
        let mut select_exprs = vec![
            lit(key.clone()).alias("bucket"),
            col(&format!("__b{i}_doc_count")).alias("doc_count"),
        ];
        for (j, m_alias) in metric_aliases.iter().enumerate() {
            select_exprs.push(
                col(&format!("__b{i}_{m_alias}")).alias(&metric_names[j]),
            );
        }
        let bucket_df = flat_df.clone().select(select_exprs)?;
        result = Some(match result {
            Some(prev) => prev.union(bucket_df)?,
            None => bucket_df,
        });
    }

    result
        .unwrap_or(flat_df)
        .sort(vec![col("bucket").sort(true, true)])
}

/// Inject a FILTER (WHERE cond) into an aggregate expression.
fn add_agg_filter(expr: Expr, filter: Expr) -> Expr {
    match expr {
        Expr::AggregateFunction(mut agg_fn) => {
            agg_fn.params.filter = Some(Box::new(filter));
            Expr::AggregateFunction(agg_fn)
        }
        // If wrapped in Alias, unwrap, add filter, re-wrap
        Expr::Alias(alias) => {
            let filtered = add_agg_filter(*alias.expr, filter);
            filtered.alias(alias.name)
        }
        other => other,
    }
}

/// Strip the outermost Alias from an Expr, returning (inner_expr, alias_name).
fn strip_alias(expr: &Expr) -> (Expr, String) {
    match expr {
        Expr::Alias(alias) => (*alias.expr.clone(), alias.name.to_string()),
        other => (other.clone(), "unnamed".to_string()),
    }
}

fn range_condition(field: &str, entry: &RangeAggregationRange) -> Expr {
    match (entry.from, entry.to) {
        (Some(from), Some(to)) => col(field).gt_eq(lit(from)).and(col(field).lt(lit(to))),
        (Some(from), None) => col(field).gt_eq(lit(from)),
        (None, Some(to)) => col(field).lt(lit(to)),
        (None, None) => lit(true),
    }
}

fn range_bucket_key(entry: &RangeAggregationRange) -> String {
    if let Some(key) = &entry.key {
        return key.clone();
    }
    match (entry.from, entry.to) {
        (Some(from), Some(to)) => format!("{from}-{to}"),
        (Some(from), None) => format!("{from}-*"),
        (None, Some(to)) => format!("*-{to}"),
        (None, None) => format!("*-*"),
    }
}

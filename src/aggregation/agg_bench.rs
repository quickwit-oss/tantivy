#[cfg(all(test, feature = "unstable"))]
mod bench {

    use rand::prelude::SliceRandom;
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};
    use rand_distr::Distribution;
    use test::{self, Bencher};

    use crate::aggregation::agg_req::{
        Aggregation, Aggregations, BucketAggregation, BucketAggregationType, MetricAggregation,
    };
    use crate::aggregation::bucket::{
        CustomOrder, HistogramAggregation, HistogramBounds, Order, OrderTarget, RangeAggregation,
        TermsAggregation,
    };
    use crate::aggregation::metric::{AverageAggregation, StatsAggregation};
    use crate::aggregation::AggregationCollector;
    use crate::query::{AllQuery, TermQuery};
    use crate::schema::{IndexRecordOption, Schema, TextFieldIndexing, FAST, STRING};
    use crate::{Index, Term};

    #[derive(Clone, Copy, Hash, Default, Debug, PartialEq, Eq, PartialOrd, Ord)]
    enum Cardinality {
        /// All documents contain exactly one value.
        /// `Full` is the default for auto-detecting the Cardinality, since it is the most strict.
        #[default]
        Full = 0,
        /// All documents contain at most one value.
        Optional = 1,
        /// All documents may contain any number of values.
        Multivalued = 2,
        /// 1 / 20 documents has a value
        Sparse = 3,
    }

    fn get_collector(agg_req: Aggregations) -> AggregationCollector {
        AggregationCollector::from_aggs(agg_req, Default::default())
    }

    fn get_test_index_bench(cardinality: Cardinality) -> crate::Result<Index> {
        let mut schema_builder = Schema::builder();
        let text_fieldtype = crate::schema::TextOptions::default()
            .set_indexing_options(
                TextFieldIndexing::default().set_index_option(IndexRecordOption::WithFreqs),
            )
            .set_stored();
        let text_field = schema_builder.add_text_field("text", text_fieldtype);
        let text_field_many_terms = schema_builder.add_text_field("text_many_terms", STRING | FAST);
        let text_field_few_terms = schema_builder.add_text_field("text_few_terms", STRING | FAST);
        let score_fieldtype = crate::schema::NumericOptions::default().set_fast();
        let score_field = schema_builder.add_u64_field("score", score_fieldtype.clone());
        let score_field_f64 = schema_builder.add_f64_field("score_f64", score_fieldtype.clone());
        let score_field_i64 = schema_builder.add_i64_field("score_i64", score_fieldtype);
        let index = Index::create_from_tempdir(schema_builder.build())?;
        let few_terms_data = vec!["INFO", "ERROR", "WARN", "DEBUG"];

        let lg_norm = rand_distr::LogNormal::new(2.996f64, 0.979f64).unwrap();

        let many_terms_data = (0..150_000)
            .map(|num| format!("author{}", num))
            .collect::<Vec<_>>();
        {
            let mut rng = StdRng::from_seed([1u8; 32]);
            let mut index_writer = index.writer_with_num_threads(1, 100_000_000)?;
            // To make the different test cases comparable we just change one doc to force the
            // cardinality
            if cardinality == Cardinality::Optional {
                index_writer.add_document(doc!())?;
            }
            if cardinality == Cardinality::Multivalued {
                index_writer.add_document(doc!(
                    text_field => "cool",
                    text_field => "cool",
                    text_field_many_terms => "cool",
                    text_field_many_terms => "cool",
                    text_field_few_terms => "cool",
                    text_field_few_terms => "cool",
                    score_field => 1u64,
                    score_field => 1u64,
                    score_field_f64 => lg_norm.sample(&mut rng),
                    score_field_f64 => lg_norm.sample(&mut rng),
                    score_field_i64 => 1i64,
                    score_field_i64 => 1i64,
                ))?;
            }
            let mut doc_with_value = 1_000_000;
            if cardinality == Cardinality::Sparse {
                doc_with_value /= 20;
            }
            for _ in 0..doc_with_value {
                let val: f64 = rng.gen_range(0.0..1_000_000.0);
                index_writer.add_document(doc!(
                    text_field => "cool",
                    text_field_many_terms => many_terms_data.choose(&mut rng).unwrap().to_string(),
                    text_field_few_terms => few_terms_data.choose(&mut rng).unwrap().to_string(),
                    score_field => val as u64,
                    score_field_f64 => lg_norm.sample(&mut rng),
                    score_field_i64 => val as i64,
                ))?;
                if cardinality == Cardinality::Sparse {
                    for _ in 0..20 {
                        index_writer.add_document(doc!(text_field => "cool"))?;
                    }
                }
            }
            // writing the segment
            index_writer.commit()?;
        }

        Ok(index)
    }

    use paste::paste;
    #[macro_export]
    macro_rules! bench_all_cardinalities {
        (  $x:ident ) => {
            paste! {
                #[bench]
                fn $x(b: &mut Bencher) {
                    [<$x _card>](b, Cardinality::Full)
                }

                #[bench]
                fn [<$x _opt>](b: &mut Bencher) {
                    [<$x _card>](b, Cardinality::Optional)
                }

                #[bench]
                fn [<$x _multi>](b: &mut Bencher) {
                    [<$x _card>](b, Cardinality::Multivalued)
                }

                #[bench]
                fn [<$x _sparse>](b: &mut Bencher) {
                    [<$x _card>](b, Cardinality::Sparse)
                }

            }
        };
    }

    bench_all_cardinalities!(bench_aggregation_average_u64);

    fn bench_aggregation_average_u64_card(b: &mut Bencher, cardinality: Cardinality) {
        let index = get_test_index_bench(cardinality).unwrap();
        let reader = index.reader().unwrap();
        let text_field = reader.searcher().schema().get_field("text").unwrap();

        b.iter(|| {
            let term_query = TermQuery::new(
                Term::from_field_text(text_field, "cool"),
                IndexRecordOption::Basic,
            );

            let agg_req_1: Aggregations = vec![(
                "average".to_string(),
                Aggregation::Metric(MetricAggregation::Average(
                    AverageAggregation::from_field_name("score".to_string()),
                )),
            )]
            .into_iter()
            .collect();

            let collector = get_collector(agg_req_1);

            let searcher = reader.searcher();
            searcher.search(&term_query, &collector).unwrap()
        });
    }

    bench_all_cardinalities!(bench_aggregation_stats_f64);

    fn bench_aggregation_stats_f64_card(b: &mut Bencher, cardinality: Cardinality) {
        let index = get_test_index_bench(cardinality).unwrap();
        let reader = index.reader().unwrap();
        let text_field = reader.searcher().schema().get_field("text").unwrap();

        b.iter(|| {
            let term_query = TermQuery::new(
                Term::from_field_text(text_field, "cool"),
                IndexRecordOption::Basic,
            );

            let agg_req_1: Aggregations = vec![(
                "average_f64".to_string(),
                Aggregation::Metric(MetricAggregation::Stats(StatsAggregation::from_field_name(
                    "score_f64".to_string(),
                ))),
            )]
            .into_iter()
            .collect();

            let collector = get_collector(agg_req_1);

            let searcher = reader.searcher();
            searcher.search(&term_query, &collector).unwrap()
        });
    }

    bench_all_cardinalities!(bench_aggregation_average_f64);

    fn bench_aggregation_average_f64_card(b: &mut Bencher, cardinality: Cardinality) {
        let index = get_test_index_bench(cardinality).unwrap();
        let reader = index.reader().unwrap();
        let text_field = reader.searcher().schema().get_field("text").unwrap();

        b.iter(|| {
            let term_query = TermQuery::new(
                Term::from_field_text(text_field, "cool"),
                IndexRecordOption::Basic,
            );

            let agg_req_1: Aggregations = vec![(
                "average_f64".to_string(),
                Aggregation::Metric(MetricAggregation::Average(
                    AverageAggregation::from_field_name("score_f64".to_string()),
                )),
            )]
            .into_iter()
            .collect();

            let collector = get_collector(agg_req_1);

            let searcher = reader.searcher();
            searcher.search(&term_query, &collector).unwrap()
        });
    }

    bench_all_cardinalities!(bench_aggregation_percentiles_f64);

    fn bench_aggregation_percentiles_f64_card(b: &mut Bencher, cardinality: Cardinality) {
        let index = get_test_index_bench(cardinality).unwrap();
        let reader = index.reader().unwrap();

        b.iter(|| {
            let agg_req_str = r#"
            {
              "mypercentiles": {
                "percentiles": {
                  "field": "score_f64",
                  "percents": [ 95, 99, 99.9 ]
                }
              }
            } "#;
            let agg_req_1: Aggregations = serde_json::from_str(agg_req_str).unwrap();

            let collector = get_collector(agg_req_1);

            let searcher = reader.searcher();
            searcher.search(&AllQuery, &collector).unwrap()
        });
    }

    bench_all_cardinalities!(bench_aggregation_average_u64_and_f64);

    fn bench_aggregation_average_u64_and_f64_card(b: &mut Bencher, cardinality: Cardinality) {
        let index = get_test_index_bench(cardinality).unwrap();
        let reader = index.reader().unwrap();
        let text_field = reader.searcher().schema().get_field("text").unwrap();

        b.iter(|| {
            let term_query = TermQuery::new(
                Term::from_field_text(text_field, "cool"),
                IndexRecordOption::Basic,
            );

            let agg_req_1: Aggregations = vec![
                (
                    "average_f64".to_string(),
                    Aggregation::Metric(MetricAggregation::Average(
                        AverageAggregation::from_field_name("score_f64".to_string()),
                    )),
                ),
                (
                    "average".to_string(),
                    Aggregation::Metric(MetricAggregation::Average(
                        AverageAggregation::from_field_name("score".to_string()),
                    )),
                ),
            ]
            .into_iter()
            .collect();

            let collector = get_collector(agg_req_1);

            let searcher = reader.searcher();
            searcher.search(&term_query, &collector).unwrap()
        });
    }

    bench_all_cardinalities!(bench_aggregation_terms_few);

    fn bench_aggregation_terms_few_card(b: &mut Bencher, cardinality: Cardinality) {
        let index = get_test_index_bench(cardinality).unwrap();
        let reader = index.reader().unwrap();

        b.iter(|| {
            let agg_req: Aggregations = vec![(
                "my_texts".to_string(),
                Aggregation::Bucket(
                    BucketAggregation {
                        bucket_agg: BucketAggregationType::Terms(TermsAggregation {
                            field: "text_few_terms".to_string(),
                            ..Default::default()
                        }),
                        sub_aggregation: Default::default(),
                    }
                    .into(),
                ),
            )]
            .into_iter()
            .collect();

            let collector = get_collector(agg_req);

            let searcher = reader.searcher();
            searcher.search(&AllQuery, &collector).unwrap()
        });
    }

    bench_all_cardinalities!(bench_aggregation_terms_many_with_sub_agg);

    fn bench_aggregation_terms_many_with_sub_agg_card(b: &mut Bencher, cardinality: Cardinality) {
        let index = get_test_index_bench(cardinality).unwrap();
        let reader = index.reader().unwrap();

        b.iter(|| {
            let sub_agg_req: Aggregations = vec![(
                "average_f64".to_string(),
                Aggregation::Metric(MetricAggregation::Average(
                    AverageAggregation::from_field_name("score_f64".to_string()),
                )),
            )]
            .into_iter()
            .collect();

            let agg_req: Aggregations = vec![(
                "my_texts".to_string(),
                Aggregation::Bucket(
                    BucketAggregation {
                        bucket_agg: BucketAggregationType::Terms(TermsAggregation {
                            field: "text_many_terms".to_string(),
                            ..Default::default()
                        }),
                        sub_aggregation: sub_agg_req,
                    }
                    .into(),
                ),
            )]
            .into_iter()
            .collect();

            let collector = get_collector(agg_req);

            let searcher = reader.searcher();
            searcher.search(&AllQuery, &collector).unwrap()
        });
    }

    bench_all_cardinalities!(bench_aggregation_terms_many2);

    fn bench_aggregation_terms_many2_card(b: &mut Bencher, cardinality: Cardinality) {
        let index = get_test_index_bench(cardinality).unwrap();
        let reader = index.reader().unwrap();

        b.iter(|| {
            let agg_req: Aggregations = vec![(
                "my_texts".to_string(),
                Aggregation::Bucket(
                    BucketAggregation {
                        bucket_agg: BucketAggregationType::Terms(TermsAggregation {
                            field: "text_many_terms".to_string(),
                            ..Default::default()
                        }),
                        sub_aggregation: Default::default(),
                    }
                    .into(),
                ),
            )]
            .into_iter()
            .collect();

            let collector = get_collector(agg_req);

            let searcher = reader.searcher();
            searcher.search(&AllQuery, &collector).unwrap()
        });
    }

    bench_all_cardinalities!(bench_aggregation_terms_many_order_by_term);

    fn bench_aggregation_terms_many_order_by_term_card(b: &mut Bencher, cardinality: Cardinality) {
        let index = get_test_index_bench(cardinality).unwrap();
        let reader = index.reader().unwrap();

        b.iter(|| {
            let agg_req: Aggregations = vec![(
                "my_texts".to_string(),
                Aggregation::Bucket(
                    BucketAggregation {
                        bucket_agg: BucketAggregationType::Terms(TermsAggregation {
                            field: "text_many_terms".to_string(),
                            order: Some(CustomOrder {
                                order: Order::Desc,
                                target: OrderTarget::Key,
                            }),
                            ..Default::default()
                        }),
                        sub_aggregation: Default::default(),
                    }
                    .into(),
                ),
            )]
            .into_iter()
            .collect();

            let collector = get_collector(agg_req);

            let searcher = reader.searcher();
            searcher.search(&AllQuery, &collector).unwrap()
        });
    }

    bench_all_cardinalities!(bench_aggregation_range_only);

    fn bench_aggregation_range_only_card(b: &mut Bencher, cardinality: Cardinality) {
        let index = get_test_index_bench(cardinality).unwrap();
        let reader = index.reader().unwrap();

        b.iter(|| {
            let agg_req_1: Aggregations = vec![(
                "rangef64".to_string(),
                Aggregation::Bucket(
                    BucketAggregation {
                        bucket_agg: BucketAggregationType::Range(RangeAggregation {
                            field: "score_f64".to_string(),
                            ranges: vec![
                                (3f64..7000f64).into(),
                                (7000f64..20000f64).into(),
                                (20000f64..30000f64).into(),
                                (30000f64..40000f64).into(),
                                (40000f64..50000f64).into(),
                                (50000f64..60000f64).into(),
                            ],
                            ..Default::default()
                        }),
                        sub_aggregation: Default::default(),
                    }
                    .into(),
                ),
            )]
            .into_iter()
            .collect();

            let collector = get_collector(agg_req_1);

            let searcher = reader.searcher();
            searcher.search(&AllQuery, &collector).unwrap()
        });
    }

    bench_all_cardinalities!(bench_aggregation_range_with_avg);

    fn bench_aggregation_range_with_avg_card(b: &mut Bencher, cardinality: Cardinality) {
        let index = get_test_index_bench(cardinality).unwrap();
        let reader = index.reader().unwrap();

        b.iter(|| {
            let sub_agg_req: Aggregations = vec![(
                "average_f64".to_string(),
                Aggregation::Metric(MetricAggregation::Average(
                    AverageAggregation::from_field_name("score_f64".to_string()),
                )),
            )]
            .into_iter()
            .collect();

            let agg_req_1: Aggregations = vec![(
                "rangef64".to_string(),
                Aggregation::Bucket(
                    BucketAggregation {
                        bucket_agg: BucketAggregationType::Range(RangeAggregation {
                            field: "score_f64".to_string(),
                            ranges: vec![
                                (3f64..7000f64).into(),
                                (7000f64..20000f64).into(),
                                (20000f64..30000f64).into(),
                                (30000f64..40000f64).into(),
                                (40000f64..50000f64).into(),
                                (50000f64..60000f64).into(),
                            ],
                            ..Default::default()
                        }),
                        sub_aggregation: sub_agg_req,
                    }
                    .into(),
                ),
            )]
            .into_iter()
            .collect();

            let collector = get_collector(agg_req_1);

            let searcher = reader.searcher();
            searcher.search(&AllQuery, &collector).unwrap()
        });
    }

    // hard bounds has a different algorithm, because it actually limits collection range
    //
    bench_all_cardinalities!(bench_aggregation_histogram_only_hard_bounds);

    fn bench_aggregation_histogram_only_hard_bounds_card(
        b: &mut Bencher,
        cardinality: Cardinality,
    ) {
        let index = get_test_index_bench(cardinality).unwrap();
        let reader = index.reader().unwrap();

        b.iter(|| {
            let agg_req_1: Aggregations = vec![(
                "rangef64".to_string(),
                Aggregation::Bucket(
                    BucketAggregation {
                        bucket_agg: BucketAggregationType::Histogram(HistogramAggregation {
                            field: "score_f64".to_string(),
                            interval: 100f64,
                            hard_bounds: Some(HistogramBounds {
                                min: 1000.0,
                                max: 300_000.0,
                            }),
                            ..Default::default()
                        }),
                        sub_aggregation: Default::default(),
                    }
                    .into(),
                ),
            )]
            .into_iter()
            .collect();

            let collector = get_collector(agg_req_1);
            let searcher = reader.searcher();
            searcher.search(&AllQuery, &collector).unwrap()
        });
    }

    bench_all_cardinalities!(bench_aggregation_histogram_with_avg);

    fn bench_aggregation_histogram_with_avg_card(b: &mut Bencher, cardinality: Cardinality) {
        let index = get_test_index_bench(cardinality).unwrap();
        let reader = index.reader().unwrap();

        b.iter(|| {
            let sub_agg_req: Aggregations = vec![(
                "average_f64".to_string(),
                Aggregation::Metric(MetricAggregation::Average(
                    AverageAggregation::from_field_name("score_f64".to_string()),
                )),
            )]
            .into_iter()
            .collect();

            let agg_req_1: Aggregations = vec![(
                "rangef64".to_string(),
                Aggregation::Bucket(
                    BucketAggregation {
                        bucket_agg: BucketAggregationType::Histogram(HistogramAggregation {
                            field: "score_f64".to_string(),
                            interval: 100f64, // 1000 buckets
                            ..Default::default()
                        }),
                        sub_aggregation: sub_agg_req,
                    }
                    .into(),
                ),
            )]
            .into_iter()
            .collect();

            let collector = get_collector(agg_req_1);

            let searcher = reader.searcher();
            searcher.search(&AllQuery, &collector).unwrap()
        });
    }

    bench_all_cardinalities!(bench_aggregation_histogram_only);

    fn bench_aggregation_histogram_only_card(b: &mut Bencher, cardinality: Cardinality) {
        let index = get_test_index_bench(cardinality).unwrap();
        let reader = index.reader().unwrap();

        b.iter(|| {
            let agg_req_1: Aggregations = vec![(
                "rangef64".to_string(),
                Aggregation::Bucket(
                    BucketAggregation {
                        bucket_agg: BucketAggregationType::Histogram(HistogramAggregation {
                            field: "score_f64".to_string(),
                            interval: 100f64, // 1000 buckets
                            ..Default::default()
                        }),
                        sub_aggregation: Default::default(),
                    }
                    .into(),
                ),
            )]
            .into_iter()
            .collect();

            let collector = get_collector(agg_req_1);

            let searcher = reader.searcher();
            searcher.search(&AllQuery, &collector).unwrap()
        });
    }

    bench_all_cardinalities!(bench_aggregation_avg_and_range_with_avg);

    fn bench_aggregation_avg_and_range_with_avg_card(b: &mut Bencher, cardinality: Cardinality) {
        let index = get_test_index_bench(cardinality).unwrap();
        let reader = index.reader().unwrap();
        let text_field = reader.searcher().schema().get_field("text").unwrap();

        b.iter(|| {
            let term_query = TermQuery::new(
                Term::from_field_text(text_field, "cool"),
                IndexRecordOption::Basic,
            );

            let sub_agg_req_1: Aggregations = vec![(
                "average_in_range".to_string(),
                Aggregation::Metric(MetricAggregation::Average(
                    AverageAggregation::from_field_name("score".to_string()),
                )),
            )]
            .into_iter()
            .collect();

            let agg_req_1: Aggregations = vec![
                (
                    "average".to_string(),
                    Aggregation::Metric(MetricAggregation::Average(
                        AverageAggregation::from_field_name("score".to_string()),
                    )),
                ),
                (
                    "rangef64".to_string(),
                    Aggregation::Bucket(
                        BucketAggregation {
                            bucket_agg: BucketAggregationType::Range(RangeAggregation {
                                field: "score_f64".to_string(),
                                ranges: vec![
                                    (3f64..7000f64).into(),
                                    (7000f64..20000f64).into(),
                                    (20000f64..60000f64).into(),
                                ],
                                ..Default::default()
                            }),
                            sub_aggregation: sub_agg_req_1,
                        }
                        .into(),
                    ),
                ),
            ]
            .into_iter()
            .collect();

            let collector = get_collector(agg_req_1);

            let searcher = reader.searcher();
            searcher.search(&term_query, &collector).unwrap()
        });
    }
}

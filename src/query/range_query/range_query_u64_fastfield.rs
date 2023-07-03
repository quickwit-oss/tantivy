//! Fastfields support efficient scanning for range queries.
//! We use this variant only if the fastfield exists, otherwise the default in `range_query` is
//! used, which uses the term dictionary + postings.

use std::ops::{Bound, RangeInclusive};

use columnar::{ColumnType, HasAssociatedColumnType, MonotonicallyMappableToU64};

use super::fast_field_range_query::RangeDocSet;
use super::map_bound;
use crate::query::{ConstScorer, EmptyScorer, Explanation, Query, Scorer, Weight};
use crate::{DocId, DocSet, Score, SegmentReader, TantivyError};

/// `FastFieldRangeWeight` uses the fast field to execute range queries.
#[derive(Clone, Debug)]
pub struct FastFieldRangeWeight {
    field: String,
    lower_bound: Bound<u64>,
    upper_bound: Bound<u64>,
    column_type_opt: Option<ColumnType>,
}

impl FastFieldRangeWeight {
    /// Create a new FastFieldRangeWeight, using the u64 representation of any fast field.
    pub(crate) fn new_u64_lenient(
        field: String,
        lower_bound: Bound<u64>,
        upper_bound: Bound<u64>,
    ) -> Self {
        let lower_bound = map_bound(&lower_bound, |val| *val);
        let upper_bound = map_bound(&upper_bound, |val| *val);
        Self {
            field,
            lower_bound,
            upper_bound,
            column_type_opt: None,
        }
    }

    /// Create a new `FastFieldRangeWeight` for a range of a u64-mappable type .
    pub fn new<T: HasAssociatedColumnType + MonotonicallyMappableToU64>(
        field: String,
        lower_bound: Bound<T>,
        upper_bound: Bound<T>,
    ) -> Self {
        let lower_bound = map_bound(&lower_bound, |val| val.to_u64());
        let upper_bound = map_bound(&upper_bound, |val| val.to_u64());
        Self {
            field,
            lower_bound,
            upper_bound,
            column_type_opt: Some(T::column_type()),
        }
    }
}

impl Query for FastFieldRangeWeight {
    fn weight(
        &self,
        _enable_scoring: crate::query::EnableScoring<'_>,
    ) -> crate::Result<Box<dyn Weight>> {
        Ok(Box::new(self.clone()))
    }
}

impl Weight for FastFieldRangeWeight {
    fn scorer(&self, reader: &SegmentReader, boost: Score) -> crate::Result<Box<dyn Scorer>> {
        let fast_field_reader = reader.fast_fields();
        let column_type_opt: Option<[ColumnType; 1]> =
            self.column_type_opt.map(|column_type| [column_type]);
        let column_type_opt_ref: Option<&[ColumnType]> = column_type_opt
            .as_ref()
            .map(|column_types| column_types.as_slice());
        let Some((column, _)) =
            fast_field_reader.u64_lenient_for_type(column_type_opt_ref, &self.field)?
        else {
            return Ok(Box::new(EmptyScorer));
        };
        let value_range = bound_to_value_range(
            &self.lower_bound,
            &self.upper_bound,
            column.min_value(),
            column.max_value(),
        );
        if value_range.is_empty() {
            return Ok(Box::new(EmptyScorer));
        }
        let docset = RangeDocSet::new(value_range, column);
        Ok(Box::new(ConstScorer::new(docset, boost)))
    }

    fn explain(&self, reader: &SegmentReader, doc: DocId) -> crate::Result<Explanation> {
        let mut scorer = self.scorer(reader, 1.0)?;
        if scorer.seek(doc) != doc {
            return Err(TantivyError::InvalidArgument(format!(
                "Document #({doc}) does not match"
            )));
        }
        let explanation = Explanation::new("Const", scorer.score());

        Ok(explanation)
    }
}

fn bound_to_value_range<T: MonotonicallyMappableToU64>(
    lower_bound: &Bound<T>,
    upper_bound: &Bound<T>,
    min_value: T,
    max_value: T,
) -> RangeInclusive<T> {
    let mut start_value = match lower_bound {
        Bound::Included(val) => *val,
        Bound::Excluded(val) => T::from_u64(val.to_u64() + 1),
        Bound::Unbounded => min_value,
    };
    if start_value.partial_cmp(&min_value) == Some(std::cmp::Ordering::Less) {
        start_value = min_value;
    }
    let end_value = match upper_bound {
        Bound::Included(val) => *val,
        Bound::Excluded(val) => T::from_u64(val.to_u64() - 1),
        Bound::Unbounded => max_value,
    };
    start_value..=end_value
}

#[cfg(test)]
pub mod tests {
    use std::ops::{Bound, RangeInclusive};

    use proptest::prelude::*;
    use rand::rngs::StdRng;
    use rand::seq::SliceRandom;
    use rand::SeedableRng;

    use crate::collector::Count;
    use crate::query::range_query::range_query_u64_fastfield::FastFieldRangeWeight;
    use crate::query::{QueryParser, Weight};
    use crate::schema::{NumericOptions, Schema, SchemaBuilder, FAST, INDEXED, STORED, STRING};
    use crate::{Index, TERMINATED};

    #[derive(Clone, Debug)]
    pub struct Doc {
        pub id_name: String,
        pub id: u64,
    }

    fn operation_strategy() -> impl Strategy<Value = Doc> {
        prop_oneof![
            (0u64..10_000u64).prop_map(doc_from_id_1),
            (1u64..10_000u64).prop_map(doc_from_id_2),
        ]
    }

    fn doc_from_id_1(id: u64) -> Doc {
        let id = id * 1000;
        Doc {
            id_name: id.to_string(),
            id,
        }
    }
    fn doc_from_id_2(id: u64) -> Doc {
        let id = id * 1000;
        Doc {
            id_name: (id - 1).to_string(),
            id,
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(10))]
        #[test]
        fn test_range_for_docs_prop(ops in proptest::collection::vec(operation_strategy(), 1..1000)) {
            assert!(test_id_range_for_docs(ops).is_ok());
        }
    }

    #[test]
    fn range_regression1_test() {
        let ops = vec![doc_from_id_1(0)];
        assert!(test_id_range_for_docs(ops).is_ok());
    }

    #[test]
    fn test_range_regression2() {
        let ops = vec![
            doc_from_id_1(52),
            doc_from_id_1(63),
            doc_from_id_1(12),
            doc_from_id_2(91),
            doc_from_id_2(33),
        ];
        assert!(test_id_range_for_docs(ops).is_ok());
    }

    #[test]
    fn test_range_regression3() {
        let ops = vec![doc_from_id_1(9), doc_from_id_1(0), doc_from_id_1(13)];
        assert!(test_id_range_for_docs(ops).is_ok());
    }

    #[test]
    fn test_range_regression_simplified() {
        let mut schema_builder = SchemaBuilder::new();
        let field = schema_builder.add_u64_field("test_field", FAST);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut writer = index.writer_for_tests().unwrap();
        writer.add_document(doc!(field=>52_000u64)).unwrap();
        writer.commit().unwrap();
        let searcher = index.reader().unwrap().searcher();
        let range_query = FastFieldRangeWeight::new_u64_lenient(
            "test_field".to_string(),
            Bound::Included(50_000),
            Bound::Included(50_002),
        );
        let scorer = range_query
            .scorer(searcher.segment_reader(0), 1.0f32)
            .unwrap();
        assert_eq!(scorer.doc(), TERMINATED);
    }

    #[test]
    fn range_regression3_test() {
        let ops = vec![doc_from_id_1(1), doc_from_id_1(2), doc_from_id_1(3)];
        assert!(test_id_range_for_docs(ops).is_ok());
    }

    #[test]
    fn range_regression4_test() {
        let ops = vec![doc_from_id_2(100)];
        assert!(test_id_range_for_docs(ops).is_ok());
    }

    pub fn create_index_from_docs(docs: &[Doc]) -> Index {
        let mut schema_builder = Schema::builder();
        let id_u64_field = schema_builder.add_u64_field("id", INDEXED | STORED | FAST);
        let ids_u64_field =
            schema_builder.add_u64_field("ids", NumericOptions::default().set_fast().set_indexed());

        let id_f64_field = schema_builder.add_f64_field("id_f64", INDEXED | STORED | FAST);
        let ids_f64_field = schema_builder.add_f64_field(
            "ids_f64",
            NumericOptions::default().set_fast().set_indexed(),
        );

        let id_i64_field = schema_builder.add_i64_field("id_i64", INDEXED | STORED | FAST);
        let ids_i64_field = schema_builder.add_i64_field(
            "ids_i64",
            NumericOptions::default().set_fast().set_indexed(),
        );

        let text_field = schema_builder.add_text_field("id_name", STRING | STORED);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);

        {
            let mut index_writer = index.writer_with_num_threads(1, 50_000_000).unwrap();
            for doc in docs.iter() {
                index_writer
                    .add_document(doc!(
                        ids_i64_field => doc.id as i64,
                        ids_i64_field => doc.id as i64,
                        ids_f64_field => doc.id as f64,
                        ids_f64_field => doc.id as f64,
                        ids_u64_field => doc.id,
                        ids_u64_field => doc.id,
                        id_u64_field => doc.id,
                        id_f64_field => doc.id as f64,
                        id_i64_field => doc.id as i64,
                        text_field => doc.id_name.to_string(),
                    ))
                    .unwrap();
            }

            index_writer.commit().unwrap();
        }
        index
    }

    fn test_id_range_for_docs(docs: Vec<Doc>) -> crate::Result<()> {
        let index = create_index_from_docs(&docs);
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();

        let mut rng: StdRng = StdRng::from_seed([1u8; 32]);

        let get_num_hits = |query| searcher.search(&query, &Count).unwrap();
        let query_from_text = |text: &str| {
            QueryParser::for_index(&index, vec![])
                .parse_query(text)
                .unwrap()
        };

        let gen_query_inclusive = |field: &str, range: RangeInclusive<u64>| {
            format!("{}:[{} TO {}]", field, range.start(), range.end())
        };

        let test_sample = |sample_docs: Vec<Doc>| {
            let mut ids: Vec<u64> = sample_docs.iter().map(|doc| doc.id).collect();
            ids.sort();
            let expected_num_hits = docs
                .iter()
                .filter(|doc| (ids[0]..=ids[1]).contains(&doc.id))
                .count();

            let query = gen_query_inclusive("id", ids[0]..=ids[1]);
            assert_eq!(get_num_hits(query_from_text(&query)), expected_num_hits);

            let query = gen_query_inclusive("ids", ids[0]..=ids[1]);
            assert_eq!(get_num_hits(query_from_text(&query)), expected_num_hits);

            // Intersection search
            let id_filter = sample_docs[0].id_name.to_string();
            let expected_num_hits = docs
                .iter()
                .filter(|doc| (ids[0]..=ids[1]).contains(&doc.id) && doc.id_name == id_filter)
                .count();
            let query = format!(
                "{} AND id_name:{}",
                gen_query_inclusive("id", ids[0]..=ids[1]),
                &id_filter
            );
            assert_eq!(get_num_hits(query_from_text(&query)), expected_num_hits);
            let query = format!(
                "{} AND id_name:{}",
                gen_query_inclusive("id_f64", ids[0]..=ids[1]),
                &id_filter
            );
            assert_eq!(get_num_hits(query_from_text(&query)), expected_num_hits);
            let query = format!(
                "{} AND id_name:{}",
                gen_query_inclusive("id_i64", ids[0]..=ids[1]),
                &id_filter
            );
            assert_eq!(get_num_hits(query_from_text(&query)), expected_num_hits);

            // Intersection search on multivalue id field
            let id_filter = sample_docs[0].id_name.to_string();
            let query = format!(
                "{} AND id_name:{}",
                gen_query_inclusive("ids", ids[0]..=ids[1]),
                &id_filter
            );
            assert_eq!(get_num_hits(query_from_text(&query)), expected_num_hits);
            let query = format!(
                "{} AND id_name:{}",
                gen_query_inclusive("ids_f64", ids[0]..=ids[1]),
                &id_filter
            );
            assert_eq!(get_num_hits(query_from_text(&query)), expected_num_hits);
            let query = format!(
                "{} AND id_name:{}",
                gen_query_inclusive("ids_i64", ids[0]..=ids[1]),
                &id_filter
            );
            assert_eq!(get_num_hits(query_from_text(&query)), expected_num_hits);
        };

        test_sample(vec![docs[0].clone(), docs[0].clone()]);

        let samples: Vec<_> = docs.choose_multiple(&mut rng, 3).collect();

        if samples.len() > 1 {
            test_sample(vec![samples[0].clone(), samples[1].clone()]);
            test_sample(vec![samples[1].clone(), samples[1].clone()]);
        }
        if samples.len() > 2 {
            test_sample(vec![samples[1].clone(), samples[2].clone()]);
        }

        Ok(())
    }
}

#[cfg(all(test, feature = "unstable"))]
mod bench {

    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};
    use test::Bencher;

    use super::tests::*;
    use super::*;
    use crate::collector::Count;
    use crate::query::QueryParser;
    use crate::Index;

    fn get_index_0_to_100() -> Index {
        let mut rng = StdRng::from_seed([1u8; 32]);
        let num_vals = 100_000;
        let docs: Vec<_> = (0..num_vals)
            .map(|_i| {
                let id_name = if rng.gen_bool(0.01) {
                    "veryfew".to_string() // 1%
                } else if rng.gen_bool(0.1) {
                    "few".to_string() // 9%
                } else {
                    "many".to_string() // 90%
                };
                Doc {
                    id_name,
                    id: rng.gen_range(0..100),
                }
            })
            .collect();

        create_index_from_docs(&docs)
    }

    fn get_90_percent() -> RangeInclusive<u64> {
        0..=90
    }

    fn get_10_percent() -> RangeInclusive<u64> {
        0..=10
    }

    fn get_1_percent() -> RangeInclusive<u64> {
        10..=10
    }

    fn execute_query(
        field: &str,
        id_range: RangeInclusive<u64>,
        suffix: &str,
        index: &Index,
    ) -> usize {
        let gen_query_inclusive = |from: &u64, to: &u64| {
            format!(
                "{}:[{} TO {}] {}",
                field,
                &from.to_string(),
                &to.to_string(),
                suffix
            )
        };

        let query = gen_query_inclusive(id_range.start(), id_range.end());
        let query_from_text = |text: &str| {
            QueryParser::for_index(index, vec![])
                .parse_query(text)
                .unwrap()
        };
        let query = query_from_text(&query);
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        searcher.search(&query, &(Count)).unwrap()
    }

    #[bench]
    fn bench_id_range_hit_90_percent(bench: &mut Bencher) {
        let index = get_index_0_to_100();
        bench.iter(|| execute_query("id", get_90_percent(), "", &index));
    }

    #[bench]
    fn bench_id_range_hit_10_percent(bench: &mut Bencher) {
        let index = get_index_0_to_100();
        bench.iter(|| execute_query("id", get_10_percent(), "", &index));
    }

    #[bench]
    fn bench_id_range_hit_1_percent(bench: &mut Bencher) {
        let index = get_index_0_to_100();
        bench.iter(|| execute_query("id", get_1_percent(), "", &index));
    }

    #[bench]
    fn bench_id_range_hit_10_percent_intersect_with_10_percent(bench: &mut Bencher) {
        let index = get_index_0_to_100();
        bench.iter(|| execute_query("id", get_10_percent(), "AND id_name:few", &index));
    }

    #[bench]
    fn bench_id_range_hit_1_percent_intersect_with_10_percent(bench: &mut Bencher) {
        let index = get_index_0_to_100();
        bench.iter(|| execute_query("id", get_1_percent(), "AND id_name:few", &index));
    }

    #[bench]
    fn bench_id_range_hit_1_percent_intersect_with_90_percent(bench: &mut Bencher) {
        let index = get_index_0_to_100();
        bench.iter(|| execute_query("id", get_1_percent(), "AND id_name:many", &index));
    }

    #[bench]
    fn bench_id_range_hit_1_percent_intersect_with_1_percent(bench: &mut Bencher) {
        let index = get_index_0_to_100();
        bench.iter(|| execute_query("id", get_1_percent(), "AND id_name:veryfew", &index));
    }

    #[bench]
    fn bench_id_range_hit_10_percent_intersect_with_90_percent(bench: &mut Bencher) {
        let index = get_index_0_to_100();
        bench.iter(|| execute_query("id", get_10_percent(), "AND id_name:many", &index));
    }

    #[bench]
    fn bench_id_range_hit_90_percent_intersect_with_90_percent(bench: &mut Bencher) {
        let index = get_index_0_to_100();
        bench.iter(|| execute_query("id", get_90_percent(), "AND id_name:many", &index));
    }

    #[bench]
    fn bench_id_range_hit_90_percent_intersect_with_10_percent(bench: &mut Bencher) {
        let index = get_index_0_to_100();
        bench.iter(|| execute_query("id", get_90_percent(), "AND id_name:few", &index));
    }

    #[bench]
    fn bench_id_range_hit_90_percent_intersect_with_1_percent(bench: &mut Bencher) {
        let index = get_index_0_to_100();
        bench.iter(|| execute_query("id", get_90_percent(), "AND id_name:veryfew", &index));
    }

    #[bench]
    fn bench_id_range_hit_90_percent_multi(bench: &mut Bencher) {
        let index = get_index_0_to_100();
        bench.iter(|| execute_query("ids", get_90_percent(), "", &index));
    }

    #[bench]
    fn bench_id_range_hit_10_percent_multi(bench: &mut Bencher) {
        let index = get_index_0_to_100();
        bench.iter(|| execute_query("ids", get_10_percent(), "", &index));
    }

    #[bench]
    fn bench_id_range_hit_1_percent_multi(bench: &mut Bencher) {
        let index = get_index_0_to_100();
        bench.iter(|| execute_query("ids", get_1_percent(), "", &index));
    }

    #[bench]
    fn bench_id_range_hit_10_percent_intersect_with_10_percent_multi(bench: &mut Bencher) {
        let index = get_index_0_to_100();
        bench.iter(|| execute_query("ids", get_10_percent(), "AND id_name:few", &index));
    }

    #[bench]
    fn bench_id_range_hit_1_percent_intersect_with_10_percent_multi(bench: &mut Bencher) {
        let index = get_index_0_to_100();
        bench.iter(|| execute_query("ids", get_1_percent(), "AND id_name:few", &index));
    }

    #[bench]
    fn bench_id_range_hit_1_percent_intersect_with_90_percent_multi(bench: &mut Bencher) {
        let index = get_index_0_to_100();
        bench.iter(|| execute_query("ids", get_1_percent(), "AND id_name:many", &index));
    }

    #[bench]
    fn bench_id_range_hit_1_percent_intersect_with_1_percent_multi(bench: &mut Bencher) {
        let index = get_index_0_to_100();
        bench.iter(|| execute_query("ids", get_1_percent(), "AND id_name:veryfew", &index));
    }

    #[bench]
    fn bench_id_range_hit_10_percent_intersect_with_90_percent_multi(bench: &mut Bencher) {
        let index = get_index_0_to_100();
        bench.iter(|| execute_query("ids", get_10_percent(), "AND id_name:many", &index));
    }

    #[bench]
    fn bench_id_range_hit_90_percent_intersect_with_90_percent_multi(bench: &mut Bencher) {
        let index = get_index_0_to_100();
        bench.iter(|| execute_query("ids", get_90_percent(), "AND id_name:many", &index));
    }

    #[bench]
    fn bench_id_range_hit_90_percent_intersect_with_10_percent_multi(bench: &mut Bencher) {
        let index = get_index_0_to_100();
        bench.iter(|| execute_query("ids", get_90_percent(), "AND id_name:few", &index));
    }

    #[bench]
    fn bench_id_range_hit_90_percent_intersect_with_1_percent_multi(bench: &mut Bencher) {
        let index = get_index_0_to_100();
        bench.iter(|| execute_query("ids", get_90_percent(), "AND id_name:veryfew", &index));
    }
}

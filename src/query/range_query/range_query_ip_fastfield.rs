//! IP Fastfields support efficient scanning for range queries.
//! We use this variant only if the fastfield exists, otherwise the default in `range_query` is
//! used, which uses the term dictionary + postings.

use std::net::Ipv6Addr;
use std::ops::{Bound, RangeInclusive};

use common::BinarySerializable;
use fastfield_codecs::MonotonicallyMappableToU128;

use super::fast_field_range_query::{FastFieldCardinality, RangeDocSet};
use super::range_query::map_bound;
use crate::query::{ConstScorer, Explanation, Scorer, Weight};
use crate::schema::{Cardinality, Field};
use crate::{DocId, DocSet, Score, SegmentReader, TantivyError};

/// `IPFastFieldRangeWeight` uses the ip address fast field to execute range queries.
pub struct IPFastFieldRangeWeight {
    field: Field,
    left_bound: Bound<Ipv6Addr>,
    right_bound: Bound<Ipv6Addr>,
}

impl IPFastFieldRangeWeight {
    pub fn new(field: Field, left_bound: &Bound<Vec<u8>>, right_bound: &Bound<Vec<u8>>) -> Self {
        let ip_from_bound_raw_data = |data: &Vec<u8>| {
            let left_ip_u128: u128 =
                u128::from_be(BinarySerializable::deserialize(&mut &data[..]).unwrap());
            Ipv6Addr::from_u128(left_ip_u128)
        };
        let left_bound = map_bound(left_bound, &ip_from_bound_raw_data);
        let right_bound = map_bound(right_bound, &ip_from_bound_raw_data);
        Self {
            field,
            left_bound,
            right_bound,
        }
    }
}

impl Weight for IPFastFieldRangeWeight {
    fn scorer(&self, reader: &SegmentReader, boost: Score) -> crate::Result<Box<dyn Scorer>> {
        let field_type = reader.schema().get_field_entry(self.field).field_type();
        match field_type.fastfield_cardinality().unwrap() {
            Cardinality::SingleValue => {
                let ip_addr_fast_field = reader.fast_fields().ip_addr(self.field)?;
                let value_range = bound_to_value_range(
                    &self.left_bound,
                    &self.right_bound,
                    ip_addr_fast_field.min_value(),
                    ip_addr_fast_field.max_value(),
                );
                let docset = RangeDocSet::new(
                    value_range,
                    FastFieldCardinality::SingleValue(ip_addr_fast_field),
                );
                Ok(Box::new(ConstScorer::new(docset, boost)))
            }
            Cardinality::MultiValues => {
                let ip_addr_fast_field = reader.fast_fields().ip_addrs(self.field)?;
                let value_range = bound_to_value_range(
                    &self.left_bound,
                    &self.right_bound,
                    ip_addr_fast_field.min_value(),
                    ip_addr_fast_field.max_value(),
                );
                let docset = RangeDocSet::new(
                    value_range,
                    FastFieldCardinality::MultiValue(ip_addr_fast_field),
                );
                Ok(Box::new(ConstScorer::new(docset, boost)))
            }
        }
    }

    fn explain(&self, reader: &SegmentReader, doc: DocId) -> crate::Result<Explanation> {
        let mut scorer = self.scorer(reader, 1.0)?;
        if scorer.seek(doc) != doc {
            return Err(TantivyError::InvalidArgument(format!(
                "Document #({}) does not match",
                doc
            )));
        }
        let explanation = Explanation::new("Const", scorer.score());

        Ok(explanation)
    }
}

fn bound_to_value_range(
    left_bound: &Bound<Ipv6Addr>,
    right_bound: &Bound<Ipv6Addr>,
    min_value: Ipv6Addr,
    max_value: Ipv6Addr,
) -> RangeInclusive<Ipv6Addr> {
    let start_value = match left_bound {
        Bound::Included(ip_addr) => *ip_addr,
        Bound::Excluded(ip_addr) => Ipv6Addr::from(ip_addr.to_u128() + 1),
        Bound::Unbounded => min_value,
    };

    let end_value = match right_bound {
        Bound::Included(ip_addr) => *ip_addr,
        Bound::Excluded(ip_addr) => Ipv6Addr::from(ip_addr.to_u128() - 1),
        Bound::Unbounded => max_value,
    };
    start_value..=end_value
}

#[cfg(test)]
mod tests {
    use proptest::prelude::ProptestConfig;
    use proptest::strategy::Strategy;
    use proptest::{prop_oneof, proptest};

    use super::*;
    use crate::collector::Count;
    use crate::query::QueryParser;
    use crate::schema::{IpAddrOptions, Schema, FAST, STORED, STRING};
    use crate::Index;

    #[derive(Clone, Debug)]
    pub struct Doc {
        pub id: String,
        pub ip: Ipv6Addr,
    }

    fn operation_strategy() -> impl Strategy<Value = Doc> {
        prop_oneof![
            (0u64..10_000u64).prop_map(doc_from_id_1),
            (1u64..10_000u64).prop_map(doc_from_id_2),
        ]
    }

    pub fn doc_from_id_1(id: u64) -> Doc {
        let id = id * 1000;
        Doc {
            // ip != id
            id: id.to_string(),
            ip: Ipv6Addr::from_u128(id as u128),
        }
    }
    fn doc_from_id_2(id: u64) -> Doc {
        let id = id * 1000;
        Doc {
            // ip != id
            id: (id - 1).to_string(),
            ip: Ipv6Addr::from_u128(id as u128),
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(10))]
        #[test]
        fn test_ip_range_for_docs_prop(ops in proptest::collection::vec(operation_strategy(), 1..1000)) {
            assert!(test_ip_range_for_docs(ops).is_ok());
        }
    }

    #[test]
    fn ip_range_regression1_test() {
        let ops = vec![doc_from_id_1(0)];
        assert!(test_ip_range_for_docs(ops).is_ok());
    }

    #[test]
    fn ip_range_regression2_test() {
        let ops = vec![
            doc_from_id_1(52),
            doc_from_id_1(63),
            doc_from_id_1(12),
            doc_from_id_2(91),
            doc_from_id_2(33),
        ];
        assert!(test_ip_range_for_docs(ops).is_ok());
    }

    #[test]
    fn ip_range_regression3_test() {
        let ops = vec![doc_from_id_1(1), doc_from_id_1(2), doc_from_id_1(3)];
        assert!(test_ip_range_for_docs(ops).is_ok());
    }

    pub fn create_index_from_docs(docs: &[Doc]) -> Index {
        let mut schema_builder = Schema::builder();
        let ip_field = schema_builder.add_ip_addr_field("ip", STORED | FAST);
        let ips_field = schema_builder.add_ip_addr_field(
            "ips",
            IpAddrOptions::default()
                .set_fast(Cardinality::MultiValues)
                .set_indexed(),
        );
        let text_field = schema_builder.add_text_field("id", STRING | STORED);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);

        {
            let mut index_writer = index.writer(10_000_000).unwrap();
            for doc in docs.iter() {
                index_writer
                    .add_document(doc!(
                        ips_field => doc.ip,
                        ips_field => doc.ip,
                        ip_field => doc.ip,
                        text_field => doc.id.to_string(),
                    ))
                    .unwrap();
            }

            index_writer.commit().unwrap();
        }
        index
    }

    fn test_ip_range_for_docs(docs: Vec<Doc>) -> crate::Result<()> {
        let index = create_index_from_docs(&docs);
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();

        let get_num_hits = |query| searcher.search(&query, &(Count)).unwrap();
        let query_from_text = |text: &str| {
            QueryParser::for_index(&index, vec![])
                .parse_query(text)
                .unwrap()
        };

        let gen_query_inclusive = |field: &str, from: Ipv6Addr, to: Ipv6Addr| {
            format!("{}:[{} TO {}]", field, &from.to_string(), &to.to_string())
        };

        let test_sample = |sample_docs: Vec<Doc>| {
            let mut ips: Vec<Ipv6Addr> = sample_docs.iter().map(|doc| doc.ip).collect();
            ips.sort();
            let expected_num_hits = docs
                .iter()
                .filter(|doc| (ips[0]..=ips[1]).contains(&doc.ip))
                .count();

            let query = gen_query_inclusive("ip", ips[0], ips[1]);
            assert_eq!(get_num_hits(query_from_text(&query)), expected_num_hits);

            let query = gen_query_inclusive("ips", ips[0], ips[1]);
            assert_eq!(get_num_hits(query_from_text(&query)), expected_num_hits);

            // Intersection search
            let id_filter = sample_docs[0].id.to_string();
            let expected_num_hits = docs
                .iter()
                .filter(|doc| (ips[0]..=ips[1]).contains(&doc.ip) && doc.id == id_filter)
                .count();
            let query = format!(
                "{} AND id:{}",
                gen_query_inclusive("ip", ips[0], ips[1]),
                &id_filter
            );
            assert_eq!(get_num_hits(query_from_text(&query)), expected_num_hits);

            // Intersection search on multivalue ip field
            let id_filter = sample_docs[0].id.to_string();
            let query = format!(
                "{} AND id:{}",
                gen_query_inclusive("ips", ips[0], ips[1]),
                &id_filter
            );
            assert_eq!(get_num_hits(query_from_text(&query)), expected_num_hits);
        };

        test_sample(vec![docs[0].clone(), docs[0].clone()]);
        if docs.len() > 1 {
            test_sample(vec![docs[0].clone(), docs[1].clone()]);
            test_sample(vec![docs[1].clone(), docs[1].clone()]);
        }
        if docs.len() > 2 {
            test_sample(vec![docs[1].clone(), docs[2].clone()]);
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
                let id = if rng.gen_bool(0.01) {
                    "veryfew".to_string() // 1%
                } else if rng.gen_bool(0.1) {
                    "few".to_string() // 9%
                } else {
                    "many".to_string() // 90%
                };
                Doc {
                    id,
                    // Multiply by 1000, so that we create many buckets in the compact space
                    // The benches depend on this range to select n-percent of elements with the
                    // methods below.
                    ip: Ipv6Addr::from_u128(rng.gen_range(0..100) * 1000),
                }
            })
            .collect();

        let index = create_index_from_docs(&docs);
        index
    }

    fn get_90_percent() -> RangeInclusive<Ipv6Addr> {
        let start = Ipv6Addr::from_u128(0);
        let end = Ipv6Addr::from_u128(90 * 1000);
        start..=end
    }

    fn get_10_percent() -> RangeInclusive<Ipv6Addr> {
        let start = Ipv6Addr::from_u128(0);
        let end = Ipv6Addr::from_u128(10 * 1000);
        start..=end
    }

    fn get_1_percent() -> RangeInclusive<Ipv6Addr> {
        let start = Ipv6Addr::from_u128(10 * 1000);
        let end = Ipv6Addr::from_u128(10 * 1000);
        start..=end
    }

    fn excute_query(
        field: &str,
        ip_range: RangeInclusive<Ipv6Addr>,
        suffix: &str,
        index: &Index,
    ) -> usize {
        let gen_query_inclusive = |from: &Ipv6Addr, to: &Ipv6Addr| {
            format!(
                "{}:[{} TO {}] {}",
                field,
                &from.to_string(),
                &to.to_string(),
                suffix
            )
        };

        let query = gen_query_inclusive(ip_range.start(), ip_range.end());
        let query_from_text = |text: &str| {
            QueryParser::for_index(&index, vec![])
                .parse_query(text)
                .unwrap()
        };
        let query = query_from_text(&query);
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        searcher.search(&query, &(Count)).unwrap()
    }

    #[bench]
    fn bench_ip_range_hit_90_percent(bench: &mut Bencher) {
        let index = get_index_0_to_100();

        bench.iter(|| excute_query("ip", get_90_percent(), "", &index));
    }

    #[bench]
    fn bench_ip_range_hit_10_percent(bench: &mut Bencher) {
        let index = get_index_0_to_100();

        bench.iter(|| excute_query("ip", get_10_percent(), "", &index));
    }

    #[bench]
    fn bench_ip_range_hit_1_percent(bench: &mut Bencher) {
        let index = get_index_0_to_100();

        bench.iter(|| excute_query("ip", get_1_percent(), "", &index));
    }

    #[bench]
    fn bench_ip_range_hit_10_percent_intersect_with_10_percent(bench: &mut Bencher) {
        let index = get_index_0_to_100();

        bench.iter(|| excute_query("ip", get_10_percent(), "AND id:few", &index));
    }

    #[bench]
    fn bench_ip_range_hit_1_percent_intersect_with_10_percent(bench: &mut Bencher) {
        let index = get_index_0_to_100();

        bench.iter(|| excute_query("ip", get_1_percent(), "AND id:few", &index));
    }

    #[bench]
    fn bench_ip_range_hit_1_percent_intersect_with_90_percent(bench: &mut Bencher) {
        let index = get_index_0_to_100();

        bench.iter(|| excute_query("ip", get_1_percent(), "AND id:many", &index));
    }

    #[bench]
    fn bench_ip_range_hit_1_percent_intersect_with_1_percent(bench: &mut Bencher) {
        let index = get_index_0_to_100();

        bench.iter(|| excute_query("ip", get_1_percent(), "AND id:veryfew", &index));
    }

    #[bench]
    fn bench_ip_range_hit_10_percent_intersect_with_90_percent(bench: &mut Bencher) {
        let index = get_index_0_to_100();

        bench.iter(|| excute_query("ip", get_10_percent(), "AND id:many", &index));
    }

    #[bench]
    fn bench_ip_range_hit_90_percent_intersect_with_90_percent(bench: &mut Bencher) {
        let index = get_index_0_to_100();

        bench.iter(|| excute_query("ip", get_90_percent(), "AND id:many", &index));
    }

    #[bench]
    fn bench_ip_range_hit_90_percent_intersect_with_10_percent(bench: &mut Bencher) {
        let index = get_index_0_to_100();

        bench.iter(|| excute_query("ip", get_90_percent(), "AND id:few", &index));
    }

    #[bench]
    fn bench_ip_range_hit_90_percent_intersect_with_1_percent(bench: &mut Bencher) {
        let index = get_index_0_to_100();

        bench.iter(|| excute_query("ip", get_90_percent(), "AND id:veryfew", &index));
    }

    #[bench]
    fn bench_ip_range_hit_90_percent_multi(bench: &mut Bencher) {
        let index = get_index_0_to_100();

        bench.iter(|| excute_query("ips", get_90_percent(), "", &index));
    }

    #[bench]
    fn bench_ip_range_hit_10_percent_multi(bench: &mut Bencher) {
        let index = get_index_0_to_100();

        bench.iter(|| excute_query("ips", get_10_percent(), "", &index));
    }

    #[bench]
    fn bench_ip_range_hit_1_percent_multi(bench: &mut Bencher) {
        let index = get_index_0_to_100();

        bench.iter(|| excute_query("ips", get_1_percent(), "", &index));
    }

    #[bench]
    fn bench_ip_range_hit_10_percent_intersect_with_10_percent_multi(bench: &mut Bencher) {
        let index = get_index_0_to_100();

        bench.iter(|| excute_query("ips", get_10_percent(), "AND id:few", &index));
    }

    #[bench]
    fn bench_ip_range_hit_1_percent_intersect_with_10_percent_multi(bench: &mut Bencher) {
        let index = get_index_0_to_100();

        bench.iter(|| excute_query("ips", get_1_percent(), "AND id:few", &index));
    }

    #[bench]
    fn bench_ip_range_hit_1_percent_intersect_with_90_percent_multi(bench: &mut Bencher) {
        let index = get_index_0_to_100();

        bench.iter(|| excute_query("ips", get_1_percent(), "AND id:many", &index));
    }

    #[bench]
    fn bench_ip_range_hit_1_percent_intersect_with_1_percent_multi(bench: &mut Bencher) {
        let index = get_index_0_to_100();

        bench.iter(|| excute_query("ips", get_1_percent(), "AND id:veryfew", &index));
    }

    #[bench]
    fn bench_ip_range_hit_10_percent_intersect_with_90_percent_multi(bench: &mut Bencher) {
        let index = get_index_0_to_100();

        bench.iter(|| excute_query("ips", get_10_percent(), "AND id:many", &index));
    }

    #[bench]
    fn bench_ip_range_hit_90_percent_intersect_with_90_percent_multi(bench: &mut Bencher) {
        let index = get_index_0_to_100();

        bench.iter(|| excute_query("ips", get_90_percent(), "AND id:many", &index));
    }

    #[bench]
    fn bench_ip_range_hit_90_percent_intersect_with_10_percent_multi(bench: &mut Bencher) {
        let index = get_index_0_to_100();

        bench.iter(|| excute_query("ips", get_90_percent(), "AND id:few", &index));
    }

    #[bench]
    fn bench_ip_range_hit_90_percent_intersect_with_1_percent_multi(bench: &mut Bencher) {
        let index = get_index_0_to_100();

        bench.iter(|| excute_query("ips", get_90_percent(), "AND id:veryfew", &index));
    }
}

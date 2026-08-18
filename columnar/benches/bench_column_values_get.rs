use std::sync::Arc;

use binggan::{InputGroup, black_box};
use common::OwnedBytes;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use tantivy_columnar::column_values::{
    CodecType, load_u64_based_column_values, serialize_and_load_u64_based_column_values,
    serialize_u64_based_column_values,
};
use tantivy_columnar::{ColumnValues, MonotonicallyMappableToU64};

fn get_data() -> Vec<u64> {
    let mut rng = StdRng::seed_from_u64(2u64);
    let mut data: Vec<_> = (100..55_000_u64)
        .map(|num| num + rng.random::<u8>() as u64)
        .collect();
    data.push(99_000);
    data.insert(1000, 2000);
    data.insert(2000, 100);
    data.insert(3000, 4100);
    data.insert(4000, 100);
    data.insert(5000, 800);
    data
}

/// Decimal values, in the u64 bit mapping the codecs see: `Alp` refuses
/// integer data, so it can only be compared on a shape it accepts.
fn get_decimal_data() -> Vec<u64> {
    let mut rng = StdRng::seed_from_u64(3u64);
    (0..55_000)
        .map(|_| f64::to_u64(rng.random_range(0..10_000_000) as f64 / 100.0))
        .collect()
}

#[inline(never)]
fn value_iter() -> impl Iterator<Item = u64> {
    0..20_000
}

type Col = Arc<dyn ColumnValues<u64>>;

fn columns(data: &[u64], codecs: &[(&str, CodecType)]) -> Vec<(String, Col)> {
    codecs
        .iter()
        .map(|(name, codec)| {
            (
                name.to_string(),
                serialize_and_load_u64_based_column_values(&data, &[*codec]),
            )
        })
        .collect()
}

/// `get_val` per row, plus the batched `get_range` shapes that actually occur:
/// a doc block at a time (what an aggregation fetches), a whole-column scan,
/// and short takes at scattered offsets (what a per-bucket doc run produces).
fn register(group: &mut InputGroup<Col>) {
    group.register("get_val", |col: &Col| {
        let mut sum = 0u64;
        for pos in value_iter() {
            sum = sum.wrapping_add(col.get_val(pos as u32));
        }
        black_box(sum);
    });

    for take in [32usize, 128] {
        group.register(format!("get_range_seq{take}"), move |col: &Col| {
            let n = col.num_vals() as usize;
            let mut buf = vec![0u64; take];
            let mut sum = 0u64;
            let mut start = 0usize;
            while start + take < n {
                col.get_range(start as u64, &mut buf);
                sum = sum.wrapping_add(buf[0]);
                start += take;
            }
            black_box(sum);
        });

        // Coprime-ish stride, so takes land on every phase of the 128-value
        // block grid instead of always on a boundary.
        group.register(format!("get_range_scattered{take}"), move |col: &Col| {
            let n = col.num_vals() as usize;
            let mut buf = vec![0u64; take];
            let mut sum = 0u64;
            let mut start = 1usize;
            while start + take < n {
                col.get_range(start as u64, &mut buf);
                sum = sum.wrapping_add(buf[0]);
                start += take + 191;
            }
            black_box(sum);
        });
    }

    group.register("get_range_scan", |col: &Col| {
        let n = col.num_vals() as usize;
        let mut buf = vec![0u64; 4096];
        let mut sum = 0u64;
        let mut start = 0usize;
        while start < n {
            let take = 4096.min(n - start);
            col.get_range(start as u64, &mut buf[..take]);
            for &v in &buf[..take] {
                sum = sum.wrapping_add(v);
            }
            start += take;
        }
        black_box(sum);
    });
}

const DATE_NUM_VALS: usize = 2_000_000;

fn date_shapes() -> Vec<(&'static str, Vec<u64>)> {
    let n = DATE_NUM_VALS as u64;
    vec![
        (
            "timestamps",
            (0..n)
                .map(|i| 1_700_000_000_000 + i * 250 + (i.wrapping_mul(2_654_435_761) % 4_000))
                .collect(),
        ),
        (
            "near_sorted_ids",
            (0..n)
                .map(|i| i * 32 + (i.wrapping_mul(2_654_435_761) % 100_000))
                .collect(),
        ),
        (
            "noisy_ramp",
            (0..n)
                .map(|i| i * 16 + (i.wrapping_mul(2_654_435_761) % (1 << 20)))
                .collect(),
        ),
        ("const_outliers", {
            let mut vals = vec![42_000u64; DATE_NUM_VALS];
            for i in (0..DATE_NUM_VALS).step_by(9_973) {
                vals[i] = u64::MAX / 2;
            }
            vals
        }),
    ]
}

fn build(vals: &[u64], codecs: &[CodecType]) -> (String, Col) {
    let mut buffer = Vec::new();
    serialize_u64_based_column_values(&vals, codecs, &mut buffer).unwrap();
    let chosen = CodecType::from_code(buffer[0]).unwrap();
    let col = load_u64_based_column_values::<u64>(OwnedBytes::new(buffer)).unwrap();
    (chosen.to_string(), col)
}

fn register_large(group: &mut InputGroup<Col>) {
    let probes: Vec<u32> = (0..100_000u64)
        .map(|i| (i.wrapping_mul(0x9E37_79B9_7F4A_7C15) % DATE_NUM_VALS as u64) as u32)
        .collect();
    group.register("get_val_random", move |col: &Col| {
        let mut sum = 0u64;
        for &row in &probes {
            sum = sum.wrapping_add(col.get_val(row));
        }
        black_box(sum);
    });

    group.register("filter_walk_only", |col: &Col| {
        let hi = col.max_value();
        let mut hits = Vec::new();
        col.get_row_ids_for_value_range(hi + 1..=hi + 2, 0..col.num_vals(), &mut hits);
        black_box(hits.len());
    });

    group.register("filter_decode_all", |col: &Col| {
        let mut hits = Vec::with_capacity(col.num_vals() as usize);
        col.get_row_ids_for_value_range(
            col.min_value()..=col.max_value(),
            0..col.num_vals(),
            &mut hits,
        );
        black_box(hits.len());
    });

    group.register("filter_1pct_span", |col: &Col| {
        let (lo, hi) = (col.min_value(), col.max_value());
        let mid = lo + (hi - lo) / 2;
        let mut hits = Vec::new();
        col.get_row_ids_for_value_range(mid..=mid + (hi - lo) / 100, 0..col.num_vals(), &mut hits);
        black_box(hits.len());
    });
}

fn main() {
    let mut group: InputGroup<Col> = InputGroup::new_with_inputs(columns(
        &get_data(),
        &[
            ("bitpacked", CodecType::Bitpacked),
            ("linear", CodecType::Linear),
            ("blockwise_linear", CodecType::BlockwiseLinear),
            ("block_for", CodecType::BlockFor),
        ],
    ));
    register(&mut group);
    group.run();

    let mut group: InputGroup<Col> = InputGroup::new_with_inputs(columns(
        &get_decimal_data(),
        &[
            ("bitpacked_dec", CodecType::Bitpacked),
            ("linear_dec", CodecType::Linear),
            ("blockwise_linear_dec", CodecType::BlockwiseLinear),
            ("block_for_dec", CodecType::BlockFor),
        ],
    ));
    register(&mut group);
    group.run();

    for (shape, vals) in date_shapes() {
        let (incumbent, regular) = build(
            &vals,
            &[
                CodecType::Bitpacked,
                CodecType::Linear,
                CodecType::BlockwiseLinear,
            ],
        );
        let (_, block_for) = build(&vals, &[CodecType::BlockFor]);
        let mut group: InputGroup<Col> = InputGroup::new_with_inputs(vec![
            (format!("{shape}_{incumbent}"), regular),
            (format!("{shape}_block_for"), block_for),
        ]);
        register(&mut group);
        register_large(&mut group);
        group.run();
    }
}

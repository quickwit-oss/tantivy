use proptest::prelude::*;
use proptest::{prop_oneof, proptest};
use rand::Rng;

#[test]
fn test_serialize_and_load_simple() {
    let mut buffer = Vec::new();
    let vals = &[1u64, 2u64, 5u64];
    serialize_u64_based_column_values(
        &&vals[..],
        &[CodecType::Bitpacked, CodecType::BlockwiseLinear],
        &mut buffer,
    )
    .unwrap();
    assert_eq!(buffer.len(), 7);
    let col = load_u64_based_column_values::<u64>(OwnedBytes::new(buffer)).unwrap();
    assert_eq!(col.num_vals(), 3);
    assert_eq!(col.get_val(0), 1);
    assert_eq!(col.get_val(1), 2);
    assert_eq!(col.get_val(2), 5);
}

#[test]
fn test_empty_column_i64() {
    let vals: [i64; 0] = [];
    let mut num_acceptable_codecs = 0;
    for codec in ALL_U64_CODEC_TYPES {
        let mut buffer = Vec::new();
        if serialize_u64_based_column_values(&&vals[..], &[codec], &mut buffer).is_err() {
            continue;
        }
        num_acceptable_codecs += 1;
        let col = load_u64_based_column_values::<i64>(OwnedBytes::new(buffer)).unwrap();
        assert_eq!(col.num_vals(), 0);
        assert_eq!(col.min_value(), i64::MIN);
        assert_eq!(col.max_value(), i64::MIN);
    }
    assert!(num_acceptable_codecs > 0);
}

#[test]
fn test_empty_column_u64() {
    let vals: [u64; 0] = [];
    let mut num_acceptable_codecs = 0;
    for codec in ALL_U64_CODEC_TYPES {
        let mut buffer = Vec::new();
        if serialize_u64_based_column_values(&&vals[..], &[codec], &mut buffer).is_err() {
            continue;
        }
        num_acceptable_codecs += 1;
        let col = load_u64_based_column_values::<u64>(OwnedBytes::new(buffer)).unwrap();
        assert_eq!(col.num_vals(), 0);
        assert_eq!(col.min_value(), u64::MIN);
        assert_eq!(col.max_value(), u64::MIN);
    }
    assert!(num_acceptable_codecs > 0);
}

#[test]
fn test_empty_column_f64() {
    let vals: [f64; 0] = [];
    let mut num_acceptable_codecs = 0;
    for codec in ALL_U64_CODEC_TYPES {
        let mut buffer = Vec::new();
        if serialize_u64_based_column_values(&&vals[..], &[codec], &mut buffer).is_err() {
            continue;
        }
        num_acceptable_codecs += 1;
        let col = load_u64_based_column_values::<f64>(OwnedBytes::new(buffer)).unwrap();
        assert_eq!(col.num_vals(), 0);
        // FIXME. f64::MIN would be better!
        assert!(col.min_value().is_nan());
        assert!(col.max_value().is_nan());
    }
    assert!(num_acceptable_codecs > 0);
}

pub(crate) fn create_and_validate<TColumnCodec: ColumnCodec>(
    vals: &[u64],
    name: &str,
) -> Option<(f32, f32)> {
    let mut stats_collector = StatsCollector::default();
    let mut codec_estimator: TColumnCodec::Estimator = Default::default();

    for val in vals.boxed_iter() {
        stats_collector.collect(val);
        codec_estimator.collect(val);
    }
    codec_estimator.finalize();
    let stats = stats_collector.stats();
    let estimation = codec_estimator.estimate(&stats)?;

    let mut buffer = Vec::new();
    codec_estimator
        .serialize(&stats, vals.boxed_iter().as_mut(), &mut buffer)
        .unwrap();

    let actual_compression = buffer.len() as u64;

    let reader = TColumnCodec::load(OwnedBytes::new(buffer)).unwrap();
    assert_eq!(reader.num_vals(), vals.len() as u32);
    let mut buffer = Vec::new();
    for (doc, orig_val) in vals.iter().copied().enumerate() {
        let val = reader.get_val(doc as u32);
        assert_eq!(
            val, orig_val,
            "val `{val}` does not match orig_val {orig_val:?}, in data set {name}, data `{vals:?}`",
        );

        buffer.resize(1, 0);
        reader.get_vals(&[doc as u32], &mut buffer);
        let val = buffer[0];
        assert_eq!(
            val, orig_val,
            "val `{val}` does not match orig_val {orig_val:?}, in data set {name}, data `{vals:?}`",
        );
    }

    let all_docs: Vec<u32> = (0..vals.len() as u32).collect();
    buffer.resize(all_docs.len(), 0);
    reader.get_vals(&all_docs, &mut buffer);
    assert_eq!(vals, buffer);

    // Validate `get_range` over the full column and a sub-range. The sub-range starts
    // at a non-zero offset to exercise the entrance-ramp alignment of the batch decode.
    buffer.resize(all_docs.len(), 0);
    reader.get_range(0, &mut buffer);
    assert_eq!(vals, buffer, "get_range (full) mismatch in data set {name}");
    if vals.len() >= 2 {
        // Offset 1 exercises the entrance ramp of a batch decode; the
        // mid-column offset lands on different alignments as lengths vary.
        for start in [1usize, vals.len() / 2] {
            buffer.resize(vals.len() - start, 0);
            reader.get_range(start as u64, &mut buffer);
            assert_eq!(
                &vals[start..],
                &buffer[..],
                "get_range (start {start}) mismatch in data set {name}"
            );
        }
    }

    if !vals.is_empty() {
        let test_rand_idx = rand::rng().random_range(0..=vals.len() - 1);
        let expected_positions: Vec<u32> = vals
            .iter()
            .enumerate()
            .filter(|(_, el)| **el == vals[test_rand_idx])
            .map(|(pos, _)| pos as u32)
            .collect();
        let mut positions = Vec::new();
        reader.get_row_ids_for_value_range(
            vals[test_rand_idx]..=vals[test_rand_idx],
            0..vals.len() as u32,
            &mut positions,
        );
        assert_eq!(expected_positions, positions);
    }
    // Range filter on a sub row-range with a proper value range, against a
    // naive filter.
    if vals.len() >= 3 {
        let lo = *vals.iter().min().unwrap();
        let hi = *vals.iter().max().unwrap();
        let value_lo = lo + (hi - lo) / 4;
        let value_hi = lo + (hi - lo) / 4 * 3;
        let row_range = 1u32..(vals.len() as u32 - 1);
        let expected: Vec<u32> = (row_range.start..row_range.end)
            .filter(|&row| (value_lo..=value_hi).contains(&vals[row as usize]))
            .collect();
        let mut positions = Vec::new();
        reader.get_row_ids_for_value_range(value_lo..=value_hi, row_range, &mut positions);
        assert_eq!(
            expected, positions,
            "get_row_ids_for_value_range mismatch in data set {name}"
        );
    }
    if actual_compression > 1000 {
        assert!(relative_difference(estimation, actual_compression) < 0.10f32);
    }
    Some((
        compression_rate(estimation, stats.num_rows),
        compression_rate(actual_compression, stats.num_rows),
    ))
}

fn compression_rate(num_bytes: u64, num_values: u32) -> f32 {
    num_bytes as f32 / (num_values as f32 * 8.0)
}

fn relative_difference(left: u64, right: u64) -> f32 {
    let left = left as f32;
    let right = right as f32;
    2.0f32 * (left - right).abs() / (left + right)
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    #[test]
    fn test_proptest_small_bitpacked(data in proptest::collection::vec(num_strategy(), 1..10)) {
        create_and_validate::<BitpackedCodec>(&data, "proptest bitpacked");
    }

    #[test]
    fn test_proptest_small_linear(data in proptest::collection::vec(num_strategy(), 1..10)) {
        create_and_validate::<LinearCodec>(&data, "proptest linearinterpol");
    }


    #[test]
    fn test_proptest_small_blockwise_linear(data in proptest::collection::vec(num_strategy(), 1..10)) {
        create_and_validate::<BlockwiseLinearCodec>(&data, "proptest multilinearinterpol");
    }
}

#[test]
fn test_small_blockwise_linear_example() {
    create_and_validate::<BlockwiseLinearCodec>(
        &[9223372036854775808, 9223370937344622593],
        "proptest multilinearinterpol",
    );
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(10))]

    #[test]
    fn test_proptest_large_bitpacked(data in proptest::collection::vec(num_strategy(), 1..6000)) {
        create_and_validate::<BitpackedCodec>(&data, "proptest bitpacked");
    }

    #[test]
    fn test_proptest_large_linear(data in proptest::collection::vec(num_strategy(), 1..6000)) {
        create_and_validate::<LinearCodec>(&data, "proptest linearinterpol");
    }

    #[test]
    fn test_proptest_large_blockwise_linear(data in proptest::collection::vec(num_strategy(), 1..6000)) {
        create_and_validate::<BlockwiseLinearCodec>(&data, "proptest multilinearinterpol");
    }
}

fn num_strategy() -> impl Strategy<Value = u64> {
    prop_oneof![
        1 => prop::num::u64::ANY.prop_map(|num| u64::MAX - (num % 10) ),
        1 => prop::num::u64::ANY.prop_map(|num| num % 10 ),
        20 => prop::num::u64::ANY,
    ]
}

pub fn get_codec_test_datasets() -> Vec<(Vec<u64>, &'static str)> {
    let mut data_and_names = vec![];

    let data = (10..=10_000_u64).collect::<Vec<_>>();
    data_and_names.push((data, "simple monotonically increasing"));

    data_and_names.push((
        vec![5, 6, 7, 8, 9, 10, 99, 100],
        "offset in linear interpol",
    ));
    data_and_names.push((vec![5, 50, 3, 13, 1, 1000, 35], "rand small"));
    data_and_names.push((vec![10], "single value"));

    data_and_names.push((
        vec![1572656989877777, 1170935903116329, 720575940379279, 0],
        "overflow error",
    ));

    data_and_names
}

fn test_codec<C: ColumnCodec>() {
    let codec_name = std::any::type_name::<C>();
    for (data, dataset_name) in get_codec_test_datasets() {
        let estimate_actual_opt: Option<(f32, f32)> =
            tests::create_and_validate::<C>(&data, dataset_name);
        let result = if let Some((estimate, actual)) = estimate_actual_opt {
            format!("Estimate `{estimate}` Actual `{actual}`")
        } else {
            "Disabled".to_string()
        };
        println!("Codec {codec_name}, DataSet {dataset_name}, {result}");
    }
}
#[test]
fn test_codec_bitpacking() {
    test_codec::<BitpackedCodec>();
}
#[test]
fn test_codec_interpolation() {
    test_codec::<LinearCodec>();
}
#[test]
fn test_codec_multi_interpolation() {
    test_codec::<BlockwiseLinearCodec>();
}
#[test]
fn test_codec_block_for() {
    test_codec::<BlockForCodec>();
}
#[test]
fn test_codec_alp() {
    test_codec::<alp::AlpCodec>();
}

/// Data shapes long enough to span several 128-value blocks, including a
/// trailing partial one.
fn multi_block_datasets() -> Vec<(Vec<u64>, &'static str)> {
    use rand::SeedableRng;
    let mut rng = rand::rngs::StdRng::from_seed([9u8; 32]);
    vec![
        ((0..600u64).map(|i| i * 3 + 7).collect(), "monotonic"),
        (
            (0..600).map(|_| rng.random_range(0..1_000_000)).collect(),
            "random",
        ),
        (vec![42u64; 600], "constant"),
        (
            (0..600u64)
                .map(|i| if i % 97 == 0 { u64::MAX / 2 } else { i })
                .collect(),
            "outliers",
        ),
        (
            (0..600u64).map(|i| 1_700_000_000_000 + i * 1_000).collect(),
            "timestamps",
        ),
        // Decimal f64 values, stored as the u64 bit-mapping the codecs see.
        (
            (0..600u64)
                .map(|i| f64::to_u64((i as f64) / 100.0))
                .collect(),
            "decimals",
        ),
    ]
}

/// Names of every codec, so a test can prove it actually exercised all of
/// them rather than skipping some via a `continue`.
fn all_codec_names() -> Vec<String> {
    let mut names: Vec<String> = ALL_U64_CODEC_TYPES
        .iter()
        .map(|c| format!("{c:?}"))
        .collect();
    names.sort();
    names
}

/// What `DateTimePrecision` is worth on a date fast field: truncation keeps
/// values in nanoseconds, so the saving comes entirely from
/// `ColumnStats::gcd` dividing the common factor out.
///
/// ```text
/// cargo test --release -p tantivy-columnar --lib date_precision_sizes \
///     -- --ignored --nocapture
/// ```
#[test]
#[ignore]
fn date_precision_sizes() {
    const N: usize = 500_000;
    // Millisecond-resolution timestamps, as they arrive.
    let ms: Vec<u64> = (0..N as u64)
        .map(|i| 1_700_000_000_000 + i * 250 + (i.wrapping_mul(2_654_435_761) % 4_000))
        .collect();

    println!(
        "\n{:>14} {:>10} {:>16} {:>8} {:>9}",
        "precision", "gcd", "chosen", "b/val", "MB"
    );
    for (label, unit_ns) in [
        ("nanoseconds", 1u64),
        ("microseconds", 1_000),
        ("milliseconds", 1_000_000),
        ("seconds", 1_000_000_000),
    ] {
        // to_u64 is nanos; truncate() zeroes below the precision, so every
        // value stays in nanos and becomes a multiple of `unit_ns`.
        let nanos: Vec<u64> = ms
            .iter()
            .map(|&v| (v * 1_000_000 / unit_ns) * unit_ns)
            .collect();
        let mut collector = StatsCollector::default();
        for &v in &nanos {
            collector.collect(v);
        }
        let gcd = collector.stats().gcd.get();
        let mut buf = Vec::new();
        serialize_u64_based_column_values(&&nanos[..], &ALL_U64_CODEC_TYPES, &mut buf).unwrap();
        println!(
            "{label:>14} {gcd:>10} {:>16} {:>8.2} {:>9.2}",
            format!("{:?}", CodecType::try_from_code(buf[0]).unwrap()),
            buf.len() as f64 * 8.0 / N as f64,
            buf.len() as f64 / 1e6,
        );
    }
}

/// Range-filter cost on a *sorted* column: `BlockFor` prunes blocks by their
/// metadata, `BlockwiseLinear` decodes every block in range and filters --
/// and on a sorted column the selection prefers `BlockwiseLinear` on size.
///
/// ```text
/// cargo test --release -p tantivy-columnar --lib sorted_range_filter_ab \
///     -- --ignored --nocapture
/// ```
#[test]
#[ignore]
fn sorted_range_filter_ab() {
    use std::hint::black_box;
    use std::time::Instant;

    use crate::column_values::u64_based::{BlockForCodec, BlockwiseLinearCodec};

    const N: usize = 2_000_000;
    // Timestamps in an index sorted by that timestamp.
    let mut vals: Vec<u64> = (0..N as u64)
        .map(|i| 1_700_000_000_000 + i * 250 + (i.wrapping_mul(2_654_435_761) % 4_000))
        .collect();
    vals.sort_unstable();
    let (lo, hi) = (vals[0], *vals.last().unwrap());

    fn bytes_of(vals: &[u64], codec: CodecType) -> Vec<u8> {
        let mut buf = Vec::new();
        serialize_u64_based_column_values(&&vals[..], &[codec], &mut buf).unwrap();
        buf[1..].to_vec()
    }
    let mut chosen = Vec::new();
    serialize_u64_based_column_values(&&vals[..], &ALL_U64_CODEC_TYPES, &mut chosen).unwrap();
    println!(
        "\nsorted column, {N} rows: selection picks {:?} at {:.2} b/val",
        CodecType::try_from_code(chosen[0]).unwrap(),
        chosen.len() as f64 * 8.0 / N as f64
    );

    let bwl =
        BlockwiseLinearCodec::load(OwnedBytes::new(bytes_of(&vals, CodecType::BlockwiseLinear)))
            .unwrap();
    let bfor = BlockForCodec::load(OwnedBytes::new(bytes_of(&vals, CodecType::BlockFor))).unwrap();

    println!(
        "{:>12} {:>10} {:>12} {:>9} {:>10}",
        "selectivity", "bwl_us", "blockfor_us", "ratio", "hits"
    );
    let mut hits: Vec<u32> = Vec::with_capacity(N);
    for (label, frac) in [("0.1%", 1000u64), ("1%", 100), ("10%", 10), ("100%", 1)] {
        let span = (hi - lo) / frac;
        let start = (lo + (hi - lo) / 2).min(hi - span);
        let range = start..=start + span;

        let mut time = |run: &mut dyn FnMut(&mut Vec<u32>)| {
            for _ in 0..2 {
                hits.clear();
                run(&mut hits);
            }
            const REPS: usize = 5;
            let t0 = Instant::now();
            for _ in 0..REPS {
                hits.clear();
                run(&mut hits);
                black_box(hits.len());
            }
            (
                t0.elapsed().as_nanos() as f64 / REPS as f64 / 1000.0,
                hits.len(),
            )
        };
        let (bwl_us, n_bwl) =
            time(&mut |h| bwl.get_row_ids_for_value_range(range.clone(), 0..N as u32, h));
        let (bf_us, n_bf) =
            time(&mut |h| bfor.get_row_ids_for_value_range(range.clone(), 0..N as u32, h));
        assert_eq!(n_bwl, n_bf, "{label}: codecs disagree on hit count");
        println!(
            "{label:>12} {bwl_us:>10.0} {bf_us:>12.0} {:>9.2} {n_bwl:>10}",
            bwl_us / bf_us
        );
    }
}

/// Calibration harness for the `get_row_ids_for_value_range` overrides:
/// batched decode-then-filter vs the `ColumnValues` per-value default. Run
/// this before adding or removing an override -- which codecs win is not
/// obvious and does not generalize.
///
/// ```text
/// cargo test --release -p tantivy-columnar --lib range_filter_ab \
///     -- --ignored --nocapture
/// ```
#[test]
#[ignore]
fn range_filter_ab() {
    use std::time::Instant;

    use crate::column_values::u64_based::{BlockwiseLinearCodec, LinearCodec};

    const N: usize = 1 << 16;

    /// The `ColumnValues::get_row_ids_for_value_range` default, verbatim.
    fn per_value<C: ColumnValues<u64>>(
        col: &C,
        value_range: std::ops::RangeInclusive<u64>,
        row_id_range: std::ops::Range<u32>,
        hits: &mut Vec<u32>,
    ) {
        let row_id_range = row_id_range.start..row_id_range.end.min(col.num_vals());
        for idx in row_id_range {
            if value_range.contains(&col.get_val(idx)) {
                hits.push(idx);
            }
        }
    }

    fn ab<C: ColumnCodec<u64>>(label: &str, bytes: Vec<u8>, spread: u64) {
        let col = C::load(OwnedBytes::new(bytes)).unwrap();
        println!("\n=== {label} ===");
        println!(
            "{:>12} {:>10} {:>12} {:>8}",
            "selectivity", "batch_ns", "pervalue_ns", "ratio"
        );
        for (sel, frac) in [("1%", 100u64), ("25%", 4), ("50%", 2), ("100%", 1)] {
            let hi = spread / frac;
            let mut batched = Vec::with_capacity(N);
            let mut naive = Vec::with_capacity(N);
            // Correctness guard: an A/B that measures two different answers is
            // measuring nothing.
            batched.clear();
            naive.clear();
            col.get_row_ids_for_value_range(0..=hi, 0..N as u32, &mut batched);
            per_value(&col, 0..=hi, 0..N as u32, &mut naive);
            assert_eq!(batched, naive, "{label} {sel}: arms disagree");

            const REPS: usize = 60;
            for _ in 0..8 {
                batched.clear();
                col.get_row_ids_for_value_range(0..=hi, 0..N as u32, &mut batched);
                naive.clear();
                per_value(&col, 0..=hi, 0..N as u32, &mut naive);
            }
            let t0 = Instant::now();
            for _ in 0..REPS {
                batched.clear();
                col.get_row_ids_for_value_range(0..=hi, 0..N as u32, &mut batched);
                std::hint::black_box(batched.len());
            }
            let batch_ns = t0.elapsed().as_nanos() as f64 / (REPS * N) as f64;
            let t1 = Instant::now();
            for _ in 0..REPS {
                naive.clear();
                per_value(&col, 0..=hi, 0..N as u32, &mut naive);
                std::hint::black_box(naive.len());
            }
            let pv_ns = t1.elapsed().as_nanos() as f64 / (REPS * N) as f64;
            println!(
                "{sel:>12} {batch_ns:>10.4} {pv_ns:>12.4} {:>8.2}",
                pv_ns / batch_ns
            );
        }
    }

    // A noisy ramp: what Linear is actually chosen for.
    const SPREAD: u64 = 1 << 20;
    let vals: Vec<u64> = (0..N as u64)
        .map(|i| i * 16 + (i.wrapping_mul(2_654_435_761) % SPREAD))
        .collect();

    let mut buf = Vec::new();
    serialize_u64_based_column_values(&&vals[..], &[CodecType::Linear], &mut buf).unwrap();
    ab::<LinearCodec>("Linear", buf[1..].to_vec(), vals[N - 1]);

    let mut buf = Vec::new();
    serialize_u64_based_column_values(&&vals[..], &[CodecType::BlockwiseLinear], &mut buf).unwrap();
    ab::<BlockwiseLinearCodec>("BlockwiseLinear", buf[1..].to_vec(), vals[N - 1]);
}

/// Bits per value per codec on timestamp shapes, including gaps; answers what
/// a delta codec would be worth over the existing set.
///
/// ```text
/// cargo test --release -p tantivy-columnar --lib timestamp_sizes \
///     -- --ignored --nocapture
/// ```
#[test]
#[ignore]
fn timestamp_sizes() {
    const N: usize = 100_000;
    const DAY_MS: u64 = 86_400_000;

    /// Bits/value if consecutive deltas were bitpacked per 128-value block at
    /// each block's own width -- an upper bound on what a delta codec could
    /// do, ignoring the per-block metadata it would also need.
    fn delta_bound(vals: &[u64]) -> f64 {
        let mut bits = 0u64;
        for block in vals.chunks(128) {
            let mut prev = block[0];
            let mut max_delta = 0u64;
            for &v in &block[1..] {
                max_delta = max_delta.max(v.wrapping_sub(prev));
                prev = v;
            }
            bits += 64 + tantivy_bitpacker::compute_num_bits(max_delta) as u64 * 128;
        }
        bits as f64 / vals.len() as f64
    }

    let dense: Vec<u64> = (0..N as u64)
        .map(|i| 1_700_000_000_000 + i * 250 + (i.wrapping_mul(2_654_435_761) % 4_000))
        .collect();
    // One day of silence in the middle: the shape the question is about.
    let one_gap: Vec<u64> = dense
        .iter()
        .enumerate()
        .map(|(i, &v)| if i >= N / 2 { v + DAY_MS } else { v })
        .collect();
    // A gap every 10k rows, e.g. a nightly batch job.
    let many_gaps: Vec<u64> = dense
        .iter()
        .enumerate()
        .map(|(i, &v)| v + DAY_MS * (i / 10_000) as u64)
        .collect();
    // Bursty arrivals: no steady rate for a line to fit.
    let bursty: Vec<u64> = {
        let mut t = 1_700_000_000_000u64;
        (0..N as u64)
            .map(|i| {
                t += if i % 997 == 0 { DAY_MS } else { i % 40 };
                t
            })
            .collect()
    };

    println!(
        "{:>12} {:>10} {:>8} {:>8} {:>8} {:>8} {:>10}",
        "shape", "codec", "Bitpckd", "Linear", "BWL", "BlockFor", "delta_bnd"
    );
    // Coarser granularities: what an index that stores seconds or minutes
    // rather than milliseconds hands the codec.
    let by_second: Vec<u64> = dense.iter().map(|v| v / 1_000 * 1_000).collect();
    let by_minute: Vec<u64> = dense.iter().map(|v| v / 60_000 * 60_000).collect();

    for (name, vals) in [
        ("dense", &dense),
        ("one day gap", &one_gap),
        ("gap/10k", &many_gaps),
        ("bursty", &bursty),
        ("by second", &by_second),
        ("by minute", &by_minute),
    ] {
        let mut bits = Vec::new();
        for codec in [
            CodecType::Bitpacked,
            CodecType::Linear,
            CodecType::BlockwiseLinear,
            CodecType::BlockFor,
        ] {
            let mut buf = Vec::new();
            serialize_u64_based_column_values(&&vals[..], &[codec], &mut buf).unwrap();
            bits.push(buf.len() as f64 * 8.0 / vals.len() as f64);
        }
        // What the selection actually ships.
        let mut buf = Vec::new();
        serialize_u64_based_column_values(&&vals[..], &ALL_U64_CODEC_TYPES, &mut buf).unwrap();
        let chosen = CodecType::try_from_code(buf[0]).unwrap();
        println!(
            "{name:>12} {:>10} {:>8.2} {:>8.2} {:>8.2} {:>8.2} {:>10.2}",
            format!("{chosen:?}"),
            bits[0],
            bits[1],
            bits[2],
            bits[3],
            delta_bound(vals)
        );
    }
}

/// Every packed residual width reaches a SIMD unpack kernel, so `Linear`
/// reports the same threshold at all of them. Nothing about the *values*
/// changes with the threshold, so only an assertion on the threshold itself
/// notices if a width silently drops back to scalar.
#[test]
fn test_linear_batches_at_every_width() {
    use crate::column_values::u64_based::block_decode::{DecodeCost, min_batch_rows};

    // Residuals run several bits wider than the noise term, so these straddle
    // the narrow/wide kernel boundary with margin on both sides.
    for noise_bits in [4u32, 12, 30, 48] {
        let vals: Vec<u64> = (0..2_000u64)
            .map(|i| i * 16 + (i.wrapping_mul(2_654_435_761) % (1u64 << noise_bits)))
            .collect();
        let mut buffer = Vec::new();
        if serialize_u64_based_column_values(&&vals[..], &[CodecType::Linear], &mut buffer).is_err()
        {
            continue;
        }
        let column = load_u64_based_column_values_raw(OwnedBytes::new(buffer)).unwrap();
        assert_eq!(
            column.min_batch_rows(),
            min_batch_rows(DecodeCost::Interpolated),
            "noise_bits={noise_bits} must report the Interpolated threshold"
        );
        let mut out = vec![0u64; 300];
        column.get_range(137, &mut out);
        for (i, o) in out.iter().enumerate() {
            assert_eq!(*o, vals[137 + i], "noise_bits={noise_bits} i={i}");
        }
    }
}

#[test]
fn test_min_batch_rows_per_codec() {
    use crate::column_values::u64_based::block_decode::{DecodeCost, min_batch_rows};

    let mut covered: Vec<String> = Vec::new();
    for (vals, _shape) in multi_block_datasets() {
        for codec in ALL_U64_CODEC_TYPES {
            let mut buffer = Vec::new();
            if serialize_u64_based_column_values(&&vals[..], &[codec], &mut buffer).is_err() {
                continue;
            }
            covered.push(format!("{codec:?}"));
            let column = load_u64_based_column_values_raw(OwnedBytes::new(buffer)).unwrap();
            let got = column.min_batch_rows();
            let expected = min_batch_rows(match codec {
                CodecType::Bitpacked => DecodeCost::Flat,
                CodecType::Linear => DecodeCost::Interpolated,
                _ => DecodeCost::Blocked,
            });
            assert!(
                min_batch_rows(DecodeCost::Interpolated) > min_batch_rows(DecodeCost::Flat)
                    && min_batch_rows(DecodeCost::Flat) > min_batch_rows(DecodeCost::Blocked),
                "DecodeCost families are out of order"
            );
            assert_eq!(
                got, expected,
                "{codec:?} reports the wrong DecodeCost family"
            );
        }
    }
    covered.sort();
    covered.dedup();
    assert_eq!(covered, all_codec_names(), "a codec was skipped");
}

/// `get_range` must agree with the source values at every phase relative to
/// the 128-value block grid, on every codec that can encode the data.
/// `load_u64_based_column_values_raw` is deliberate: it skips the monotonic
/// wrapper, so the codec's batch path is reached at every length rather than
/// only above the wrapper's `min_batch_rows` gate.
#[test]
fn test_get_range_all_codecs_all_block_phases() {
    let mut covered: Vec<String> = Vec::new();
    for (vals, shape) in multi_block_datasets() {
        for codec in ALL_U64_CODEC_TYPES {
            let mut buffer = Vec::new();
            if serialize_u64_based_column_values(&&vals[..], &[codec], &mut buffer).is_err() {
                continue;
            }
            covered.push(format!("{codec:?}"));
            let column = load_u64_based_column_values_raw(OwnedBytes::new(buffer.clone())).unwrap();
            assert_eq!(column.num_vals() as usize, vals.len(), "{codec:?}/{shape}");

            for start in 0..140usize {
                for len in [1usize, 2, 47, 48, 79, 80, 128, 129, 256, 400] {
                    if start + len > vals.len() {
                        continue;
                    }
                    let mut output = vec![0u64; len];
                    column.get_range(start as u64, &mut output);
                    assert_eq!(
                        output,
                        vals[start..start + len],
                        "{codec:?}/{shape}: get_range(start={start}, len={len})"
                    );
                }
            }

            // Ranges that end exactly at the column end, i.e. run through the
            // trailing partial block.
            for len in [1usize, 64, 128, 200, 600] {
                let start = vals.len() - len;
                let mut output = vec![0u64; len];
                column.get_range(start as u64, &mut output);
                assert_eq!(
                    output,
                    vals[start..],
                    "{codec:?}/{shape}: tail get_range(start={start}, len={len})"
                );
            }

            let wrapped: Arc<dyn ColumnValues<u64>> =
                load_u64_based_column_values(OwnedBytes::new(buffer)).unwrap();
            for start in [0usize, 1, 63, 127, 128, 129] {
                for len in [1usize, 47, 96, 128, 300] {
                    if start + len > vals.len() {
                        continue;
                    }
                    let mut output = vec![0u64; len];
                    wrapped.get_range(start as u64, &mut output);
                    assert_eq!(
                        output,
                        vals[start..start + len],
                        "{codec:?}/{shape}: wrapped get_range(start={start}, len={len})"
                    );
                }
            }
        }
    }
}

use super::*;

fn estimate<C: ColumnCodec>(vals: &[u64]) -> Option<f32> {
    let mut stats_collector = StatsCollector::default();
    let mut estimator = C::Estimator::default();
    for &val in vals {
        stats_collector.collect(val);
        estimator.collect(val);
    }
    estimator.finalize();
    let stats = stats_collector.stats();
    let num_bytes = estimator.estimate(&stats)?;
    if stats.num_rows == 0 {
        return None;
    }
    Some(num_bytes as f32 / (8.0 * stats.num_rows as f32))
}

#[test]
fn estimation_good_interpolation_case() {
    let data = (10..=20000_u64).collect::<Vec<_>>();

    let linear_interpol_estimation = estimate::<LinearCodec>(&data).unwrap();
    assert_le!(linear_interpol_estimation, 0.01);

    let multi_linear_interpol_estimation = estimate::<BlockwiseLinearCodec>(&data).unwrap();
    assert_le!(multi_linear_interpol_estimation, 0.2);
    assert_lt!(linear_interpol_estimation, multi_linear_interpol_estimation);

    let bitpacked_estimation = estimate::<BitpackedCodec>(&data).unwrap();
    assert_lt!(linear_interpol_estimation, bitpacked_estimation);
}

#[test]
fn estimation_test_bad_interpolation_case_monotonically_increasing() {
    let mut data: Vec<u64> = (201..=20000_u64).collect();
    data.push(1_000_000);

    let linear_interpol_estimation = estimate::<LinearCodec>(&data[..]).unwrap();
    assert_le!(linear_interpol_estimation, 0.35);

    let bitpacked_estimation = estimate::<BitpackedCodec>(&data).unwrap();
    assert_le!(bitpacked_estimation, 0.32);
    assert_le!(bitpacked_estimation, linear_interpol_estimation);
}

#[test]
fn test_fast_field_codec_type_to_code() {
    let mut count_codec = 0;
    for code in 0..=255 {
        if let Some(codec_type) = CodecType::try_from_code(code) {
            assert_eq!(codec_type.to_code(), code);
            count_codec += 1;
        }
    }
    assert_eq!(count_codec, 4);
}

fn test_fastfield_gcd_i64_with_codec(codec_type: CodecType, num_vals: usize) -> io::Result<()> {
    let mut vals: Vec<i64> = (-4..=(num_vals as i64) - 5).map(|val| val * 1000).collect();
    let mut buffer: Vec<u8> = Vec::new();
    crate::column_values::serialize_u64_based_column_values(
        &&vals[..],
        &[codec_type],
        &mut buffer,
    )?;
    let buffer = OwnedBytes::new(buffer);
    let column = crate::column_values::load_u64_based_column_values::<i64>(buffer.clone())?;
    assert_eq!(column.get_val(0), -4000i64);
    assert_eq!(column.get_val(1), -3000i64);
    assert_eq!(column.get_val(2), -2000i64);
    assert_eq!(column.max_value(), (num_vals as i64 - 5) * 1000);
    assert_eq!(column.min_value(), -4000i64);

    // Can't apply gcd
    let mut buffer_without_gcd = Vec::new();
    vals.pop();
    vals.push(1001i64);
    crate::column_values::serialize_u64_based_column_values(
        &&vals[..],
        &[codec_type],
        &mut buffer_without_gcd,
    )?;
    let buffer_without_gcd = OwnedBytes::new(buffer_without_gcd);
    assert!(buffer_without_gcd.len() > buffer.len());

    Ok(())
}

#[test]
fn test_fastfield_gcd_i64() -> io::Result<()> {
    for &codec_type in &[
        CodecType::Bitpacked,
        CodecType::BlockwiseLinear,
        CodecType::Linear,
    ] {
        test_fastfield_gcd_i64_with_codec(codec_type, 5500)?;
    }
    Ok(())
}

fn test_fastfield_gcd_u64_with_codec(codec_type: CodecType, num_vals: usize) -> io::Result<()> {
    let mut vals: Vec<u64> = (1..=num_vals).map(|i| i as u64 * 1000u64).collect();
    let mut buffer: Vec<u8> = Vec::new();
    crate::column_values::serialize_u64_based_column_values(
        &&vals[..],
        &[codec_type],
        &mut buffer,
    )?;
    let buffer = OwnedBytes::new(buffer);
    let column = crate::column_values::load_u64_based_column_values::<u64>(buffer.clone())?;
    assert_eq!(column.get_val(0), 1000u64);
    assert_eq!(column.get_val(1), 2000u64);
    assert_eq!(column.get_val(2), 3000u64);
    assert_eq!(column.max_value(), num_vals as u64 * 1000);
    assert_eq!(column.min_value(), 1000u64);

    // Can't apply gcd
    let mut buffer_without_gcd = Vec::new();
    vals.pop();
    vals.push(1001u64);
    crate::column_values::serialize_u64_based_column_values(
        &&vals[..],
        &[codec_type],
        &mut buffer_without_gcd,
    )?;
    let buffer_without_gcd = OwnedBytes::new(buffer_without_gcd);
    assert!(buffer_without_gcd.len() > buffer.len());
    Ok(())
}

#[test]
fn test_fastfield_gcd_u64() -> io::Result<()> {
    for &codec_type in &[
        CodecType::Bitpacked,
        CodecType::BlockwiseLinear,
        CodecType::Linear,
    ] {
        test_fastfield_gcd_u64_with_codec(codec_type, 5500)?;
    }
    Ok(())
}

#[test]
pub fn test_fastfield2() {
    let test_fastfield = crate::column_values::serialize_and_load_u64_based_column_values::<u64>(
        &&[100u64, 200u64, 300u64][..],
        &ALL_U64_CODEC_TYPES,
    );
    assert_eq!(test_fastfield.get_val(0), 100);
    assert_eq!(test_fastfield.get_val(1), 200);
    assert_eq!(test_fastfield.get_val(2), 300);
}

use std::fmt::Debug;
use std::marker::PhantomData;
use std::ops::{Range, RangeInclusive};

use crate::ColumnValues;
use crate::column_values::monotonic_mapping::StrictlyMonotonicFn;

struct MonotonicMappingColumn<C, T, Input> {
    from_column: C,
    monotonic_mapping: T,
    _phantom: PhantomData<Input>,
}

/// Creates a view of a column transformed by a strictly monotonic mapping. See
/// [`StrictlyMonotonicFn`].
///
/// E.g. apply a gcd monotonic_mapping([100, 200, 300]) == [1, 2, 3]
/// monotonic_mapping.mapping() is expected to be injective, and we should always have
/// monotonic_mapping.inverse(monotonic_mapping.mapping(el)) == el
///
/// The inverse of the mapping is required for:
/// `fn get_positions_for_value_range(&self, range: RangeInclusive<T>) -> Vec<u64> `
/// The user provides the original value range and we need to monotonic map them in the same way the
/// serialization does before calling the underlying column.
///
/// Note that when opening a codec, the monotonic_mapping should be the inverse of the mapping
/// during serialization. And therefore the monotonic_mapping_inv when opening is the same as
/// monotonic_mapping during serialization.
pub fn monotonic_map_column<C, T, Input, Output>(
    from_column: C,
    monotonic_mapping: T,
) -> impl ColumnValues<Output>
where
    C: ColumnValues<Input> + 'static,
    T: StrictlyMonotonicFn<Input, Output> + Send + Sync + 'static,
    Input: PartialOrd + Debug + Send + Sync + Clone + 'static,
    Output: PartialOrd + Debug + Send + Sync + Clone + 'static,
{
    MonotonicMappingColumn {
        from_column,
        monotonic_mapping,
        _phantom: PhantomData,
    }
}

impl<C, T, Input, Output> ColumnValues<Output> for MonotonicMappingColumn<C, T, Input>
where
    C: ColumnValues<Input> + 'static,
    T: StrictlyMonotonicFn<Input, Output> + Send + Sync + 'static,
    Input: PartialOrd + Send + Debug + Sync + Clone + 'static,
    Output: PartialOrd + Send + Debug + Sync + Clone + 'static,
{
    #[inline(always)]
    fn get_val(&self, idx: u32) -> Output {
        let from_val = self.from_column.get_val(idx);
        self.monotonic_mapping.mapping(from_val)
    }

    fn min_value(&self) -> Output {
        let from_min_value = self.from_column.min_value();
        self.monotonic_mapping.mapping(from_min_value)
    }

    fn max_value(&self) -> Output {
        let from_max_value = self.from_column.max_value();
        self.monotonic_mapping.mapping(from_max_value)
    }

    fn num_vals(&self) -> u32 {
        self.from_column.num_vals()
    }

    /// Forwards to the codec's `get_range`. When the mapping keeps the type
    /// (the identity every u64 column is loaded with) the decode lands
    /// straight in the caller's buffer, unconditionally: every codec's
    /// `get_range` is at least as fast as its `get_val` at any length. A
    /// type-changing mapping over u64 storage (i64/f64/date) decodes into a
    /// stack scratch and maps from there, from [`MIN_MAPPED_BATCH_ROWS`] up;
    /// below that the scratch setup outweighs the rows and a per-value loop
    /// runs instead (`ab_mapped_crossover` below: 64-bit columns win from 32
    /// rows, the rest from 16).
    #[inline]
    fn get_range(&self, start: u64, output: &mut [Output]) {
        if is_same_type::<Input, Output>() {
            forward_in_place(&self.from_column, &self.monotonic_mapping, start, output);
        } else if is_same_type::<Input, u64>() && output.len() >= MIN_MAPPED_BATCH_ROWS {
            map_via_scratch(&self.from_column, &self.monotonic_mapping, start, output);
        } else {
            map_per_value(&self.from_column, &self.monotonic_mapping, start, output);
        }
    }

    fn iter(&self) -> Box<dyn Iterator<Item = Output> + '_> {
        Box::new(
            self.from_column
                .iter()
                .map(|el| self.monotonic_mapping.mapping(el)),
        )
    }

    fn get_row_ids_for_value_range(
        &self,
        range: RangeInclusive<Output>,
        doc_id_range: Range<u32>,
        positions: &mut Vec<u32>,
    ) {
        self.from_column.get_row_ids_for_value_range(
            self.monotonic_mapping.inverse(range.start().clone())
                ..=self.monotonic_mapping.inverse(range.end().clone()),
            doc_id_range,
            positions,
        )
    }
}

/// Shortest range the wrapper batches through [`map_via_scratch`]; below it
/// the scratch setup outweighs the rows.
const MIN_MAPPED_BATCH_ROWS: usize = 32;

#[inline(always)]
fn is_same_type<A: 'static, B: 'static>() -> bool {
    std::any::TypeId::of::<A>() == std::any::TypeId::of::<B>()
}

/// `get_range` for a mapping from a type to itself: decodes into `output`
/// directly and maps in place. For the identity mapping the fix-up loop
/// compiles to nothing.
///
/// # Panics
///
/// If `Input` and `Output` are not the same type.
#[inline(always)]
fn forward_in_place<C, T, Input, Output>(col: &C, mapping: &T, start: u64, output: &mut [Output])
where
    C: ColumnValues<Input>,
    T: StrictlyMonotonicFn<Input, Output>,
    Input: PartialOrd + Clone + 'static,
    Output: PartialOrd + 'static,
{
    assert!(is_same_type::<Input, Output>());
    // Safety (both casts): `TypeId` equality of two `'static` types proves
    // `Input` and `Output` are the same type, the basis `Any::downcast` rests
    // on. Values are duplicated with `Clone`, not bit-copied, so a mapping that
    // panics leaves the slice holding its original, still-valid values.
    let output_as_input: &mut [Input] = unsafe { &mut *(output as *mut [Output] as *mut [Input]) };
    col.get_range(start, output_as_input);
    for val in output.iter_mut() {
        let raw: Input = unsafe { &*(val as *const Output as *const Input) }.clone();
        *val = mapping.mapping(raw);
    }
}

/// Rows per `get_range` call on the codec in [`map_via_scratch`]; a multiple
/// of the kernels' 128-row group so aligned chunks decode without ramps.
const SCRATCH_ROWS: usize = 128;

/// `get_range` for `Input = u64` under a type-changing mapping: batch-decodes
/// into a stack scratch, one group-aligned chunk at a time, and maps each
/// chunk into `output`.
///
/// # Panics
///
/// If `Input` is not `u64`.
#[inline(always)]
fn map_via_scratch<C, T, Input, Output>(col: &C, mapping: &T, start: u64, output: &mut [Output])
where
    C: ColumnValues<Input>,
    T: StrictlyMonotonicFn<Input, Output>,
    Input: PartialOrd + Clone + 'static,
    Output: PartialOrd,
{
    assert!(is_same_type::<Input, u64>());
    // Initialize only the rows a chunk can use: zeroing the whole array is a
    // 1 KB memset that dominated calls of a few dozen rows.
    let mut scratch = [std::mem::MaybeUninit::<u64>::uninit(); SCRATCH_ROWS];
    let used = SCRATCH_ROWS.min(output.len());
    for slot in &mut scratch[..used] {
        slot.write(0);
    }
    // Safety: the first `used` slots were just written, and `TypeId` equality
    // above proves `Input` is `u64`, which `MaybeUninit<u64>` has the layout of.
    let scratch: &mut [Input] = unsafe {
        &mut *(&mut scratch[..used] as *mut [std::mem::MaybeUninit<u64>] as *mut [Input])
    };
    let mut pos = start;
    let mut output = output;
    while !output.is_empty() {
        let len = (SCRATCH_ROWS - pos as usize % SCRATCH_ROWS).min(output.len());
        let (head, tail) = output.split_at_mut(len);
        let buf = &mut scratch[..len];
        col.get_range(pos, buf);
        for (out, raw) in head.iter_mut().zip(buf.iter()) {
            *out = mapping.mapping(raw.clone());
        }
        pos += len as u64;
        output = tail;
    }
}

/// `get_range` through `get_val`, four rows at a time like the trait default,
/// with the mapping applied inline.
#[inline(always)]
fn map_per_value<C, T, Input, Output>(col: &C, mapping: &T, start: u64, output: &mut [Output])
where
    C: ColumnValues<Input>,
    T: StrictlyMonotonicFn<Input, Output>,
    Input: PartialOrd,
    Output: PartialOrd,
{
    assert!(
        start + output.len() as u64 <= col.num_vals() as u64,
        "Requested index is out of bounds."
    );
    let (out_chunks, out_rem) = output.as_chunks_mut::<4>();
    let mut idx = start as u32;
    for out_x4 in out_chunks {
        out_x4[0] = mapping.mapping(col.get_val(idx));
        out_x4[1] = mapping.mapping(col.get_val(idx + 1));
        out_x4[2] = mapping.mapping(col.get_val(idx + 2));
        out_x4[3] = mapping.mapping(col.get_val(idx + 3));
        idx += 4;
    }
    for out in out_rem {
        *out = mapping.mapping(col.get_val(idx));
        idx += 1;
    }
}

/// A/Bs this wrapper's `get_range` override against the `ColumnValues` default
/// body it displaces, per arm.
///
/// Both columns wrap the same concrete `BitpackedReader` and are called as
/// concrete types: routing either arm through `Arc<dyn>` charges the per-value
/// side one virtual call per row that it does not pay in production, which is
/// enough to invert the sign of the result.
///
/// ```text
/// cargo test --release -p tantivy-columnar --lib ab_get_range_override \
///     -- --ignored --nocapture
/// ```
#[cfg(test)]
mod ab_get_range_override {
    use std::fmt::Debug;
    use std::hint::black_box;
    use std::marker::PhantomData;
    use std::time::Instant;

    use super::*;
    use crate::column_values::monotonic_mapping::{
        StrictlyMonotonicMappingInverter, StrictlyMonotonicMappingToInternal,
    };
    use crate::column_values::u64_based::bitpacked::BitpackedCodec;
    use crate::column_values::u64_based::{ColumnCodec, serialize_u64_based_column_values};
    use crate::column_values::{CodecType, MonotonicallyMappableToU64};

    /// Same fields and `get_val` as [`MonotonicMappingColumn`], no `get_range`
    /// override, so it runs the trait default.
    struct NoOverride<C, T, Input> {
        from_column: C,
        monotonic_mapping: T,
        _phantom: PhantomData<Input>,
    }

    impl<C, T, Input, Output> ColumnValues<Output> for NoOverride<C, T, Input>
    where
        C: ColumnValues<Input> + 'static,
        T: StrictlyMonotonicFn<Input, Output> + Send + Sync + 'static,
        Input: PartialOrd + Send + Debug + Sync + Clone + 'static,
        Output: PartialOrd + Send + Debug + Sync + Clone + 'static,
    {
        #[inline(always)]
        fn get_val(&self, idx: u32) -> Output {
            self.monotonic_mapping
                .mapping(self.from_column.get_val(idx))
        }
        fn min_value(&self) -> Output {
            self.monotonic_mapping.mapping(self.from_column.min_value())
        }
        fn max_value(&self) -> Output {
            self.monotonic_mapping.mapping(self.from_column.max_value())
        }
        fn num_vals(&self) -> u32 {
            self.from_column.num_vals()
        }
    }

    const N: u64 = 1_000_000;
    const ROUNDS: usize = 11;
    const TAKES: [usize; 8] = [16, 32, 64, 96, 128, 256, 1024, 4096];

    fn reader(vals: &[u64]) -> super::super::u64_based::bitpacked::BitpackedReader {
        let mut buffer = Vec::new();
        serialize_u64_based_column_values(&vals, &[CodecType::Bitpacked], &mut buffer).unwrap();
        // `load` wants the codec payload, without the leading codec tag.
        BitpackedCodec::load(common::OwnedBytes::new(buffer[1..].to_vec())).unwrap()
    }

    fn sweep<T, Output>(label: &str, vals: Vec<u64>, mapping: impl Fn() -> T)
    where
        T: StrictlyMonotonicFn<u64, Output> + Send + Sync + 'static,
        Output: PartialOrd + Send + Debug + Sync + Clone + Default + 'static,
    {
        let col = reader(&vals);
        let with = monotonic_map_column(col.clone(), mapping());
        let without = NoOverride {
            from_column: col,
            monotonic_mapping: mapping(),
            _phantom: PhantomData::<u64>,
        };
        println!("\n{label}");
        println!(
            "{:>6} {:>10} {:>10} {:>9}",
            "take", "default", "override", "delta"
        );
        for take in TAKES {
            let calls = 1_000_000usize / take;
            let mut a = vec![Output::default(); take];
            let mut b = vec![Output::default(); take];
            let (mut ba, mut bb) = (f64::MAX, f64::MAX);
            for _ in 0..ROUNDS {
                let t = Instant::now();
                for c in 0..calls {
                    without.get_range(((c * take) as u64) % (N - take as u64), &mut a);
                    black_box(&a[take - 1]);
                }
                ba = ba.min(t.elapsed().as_secs_f64() * 1e3);
                let t = Instant::now();
                for c in 0..calls {
                    with.get_range(((c * take) as u64) % (N - take as u64), &mut b);
                    black_box(&b[take - 1]);
                }
                bb = bb.min(t.elapsed().as_secs_f64() * 1e3);
            }
            assert!(a == b, "arms disagree at take={take}");
            println!(
                "{take:>6} {ba:>10.3} {bb:>10.3} {:>8.1}%",
                (bb / ba - 1.0) * 100.0
            );
        }
    }

    /// Where the batch (`forward_in_place`) arm crosses the per-value
    /// (`map_per_value`) arm, per codec. Below `u64_based::MIN_BATCH_ROWS`
    /// the codecs go per-value themselves, so the raw crossover needs that
    /// floor set to 1; with it in place the rows under it read as parity:
    /// both arms run at every take, so the crossover reads directly off the
    /// sign flip.
    ///
    /// ```text
    /// cargo test --release -p tantivy-columnar --lib ab_batch_crossover \
    ///     -- --ignored --nocapture
    /// ```
    #[test]
    #[ignore]
    fn ab_batch_crossover() {
        use crate::column_values::u64_based::{BlockwiseLinearCodec, LinearCodec};

        fn load<Codec: crate::column_values::u64_based::ColumnCodec>(
            vals: &[u64],
            codec_type: CodecType,
        ) -> Codec::ColumnValues {
            let mut buffer = Vec::new();
            serialize_u64_based_column_values(&vals, &[codec_type], &mut buffer).unwrap();
            Codec::load(common::OwnedBytes::new(buffer[1..].to_vec())).unwrap()
        }

        fn sweep_codec<C>(label: &str, col: C)
        where C: ColumnValues<u64> + 'static {
            let mapping = StrictlyMonotonicMappingToInternal::<u64>::new();
            let n = col.num_vals() as u64;
            println!("\n{label}");
            println!(
                "{:>6} {:>11} {:>11} {:>9}",
                "take", "per_value", "batch", "delta"
            );
            for take in [1usize, 2, 4, 8, 16, 24, 32, 48, 64, 96, 128, 256] {
                let calls = 1_000_000usize / take;
                let mut a = vec![0u64; take];
                let mut b = vec![0u64; take];
                let (mut pv, mut fw) = (f64::MAX, f64::MAX);
                for round in 0..ROUNDS {
                    let run_pv = |buf: &mut Vec<u64>| {
                        let t = Instant::now();
                        for c in 0..calls {
                            map_per_value(
                                &col,
                                &mapping,
                                ((c * take) as u64) % (n - take as u64),
                                buf,
                            );
                            black_box(&buf[take - 1]);
                        }
                        t.elapsed().as_secs_f64() * 1e3
                    };
                    let run_fw = |buf: &mut Vec<u64>| {
                        let t = Instant::now();
                        for c in 0..calls {
                            forward_in_place(
                                &col,
                                &mapping,
                                ((c * take) as u64) % (n - take as u64),
                                buf,
                            );
                            black_box(&buf[take - 1]);
                        }
                        t.elapsed().as_secs_f64() * 1e3
                    };
                    if round % 2 == 0 {
                        pv = pv.min(run_pv(&mut a));
                        fw = fw.min(run_fw(&mut b));
                    } else {
                        fw = fw.min(run_fw(&mut b));
                        pv = pv.min(run_pv(&mut a));
                    }
                    assert!(a == b, "arms disagree at take={take}");
                }
                println!(
                    "{take:>6} {pv:>11.3} {fw:>11.3} {:>8.1}%",
                    (fw / pv - 1.0) * 100.0
                );
            }
        }

        let w20: Vec<u64> = (0..N).map(|i| (i * 2_654_435_761) % 1_000_000).collect();
        sweep_codec("bitpacked w20 (Flat)", reader(&w20));

        let w64: Vec<u64> = {
            let mut x = 0x9E37_79B9_7F4A_7C15u64;
            (0..N)
                .map(|_| {
                    x ^= x << 13;
                    x ^= x >> 7;
                    x ^= x << 17;
                    x
                })
                .collect()
        };
        sweep_codec("bitpacked w64 (Flat)", reader(&w64));

        let linearish: Vec<u64> = (0..N)
            .map(|i| i * 1000 + (i * 2_654_435_761) % 500)
            .collect();
        sweep_codec(
            "linear (Interpolated)",
            load::<LinearCodec>(&linearish, CodecType::Linear),
        );
        sweep_codec(
            "blockwise linear (BlockedInterpolated)",
            load::<BlockwiseLinearCodec>(&linearish, CodecType::BlockwiseLinear),
        );
    }

    #[test]
    #[ignore]
    fn ab_get_range_override() {
        sweep::<_, u64>(
            "identity u64 -> u64 (forward_in_place)",
            (0..N).map(|i| (i * 2_654_435_761) % 1_000_000).collect(),
            StrictlyMonotonicMappingToInternal::<u64>::new,
        );
        sweep::<_, f64>(
            "mapped u64 -> f64",
            (0..N)
                .map(|i| f64::to_u64(((i * 2_654_435_761) % 1_000_000) as f64 * 0.5))
                .collect(),
            || {
                StrictlyMonotonicMappingInverter::from(
                    StrictlyMonotonicMappingToInternal::<f64>::new(),
                )
            },
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::column_values::VecColumn;
    use crate::column_values::monotonic_mapping::{
        StrictlyMonotonicMappingInverter, StrictlyMonotonicMappingToInternal,
    };

    #[test]
    fn test_monotonic_mapping_iter() {
        let vals: Vec<u64> = (0..100u64).map(|el| el * 10).collect();
        let col = VecColumn::from(vals);
        let mapped = monotonic_map_column(
            col,
            StrictlyMonotonicMappingInverter::from(StrictlyMonotonicMappingToInternal::<i64>::new()),
        );
        let val_i64s: Vec<u64> = mapped.iter().collect();
        for i in 0..100 {
            assert_eq!(val_i64s[i as usize], mapped.get_val(i));
        }
    }
}

/// A/Bs the wrapper's two mapped arms against each other -- `map_per_value`
/// vs `map_via_scratch`, both on the concrete reader -- which is where the
/// [`MIN_MAPPED_BATCH_ROWS`] floor is read off.
///
/// ```text
/// cargo test --release -p tantivy-columnar --lib ab_mapped_crossover \
///     -- --ignored --nocapture
/// ```
#[cfg(test)]
mod ab_mapped_crossover {
    use std::hint::black_box;
    use std::time::Instant;

    use super::*;
    use crate::column_values::monotonic_mapping::{
        StrictlyMonotonicMappingInverter, StrictlyMonotonicMappingToInternal,
    };
    use crate::column_values::u64_based::bitpacked::BitpackedCodec;
    use crate::column_values::u64_based::{
        BlockwiseLinearCodec, ColumnCodec, LinearCodec, serialize_u64_based_column_values,
    };
    use crate::column_values::{CodecType, MonotonicallyMappableToU64};

    fn load<Codec: ColumnCodec>(vals: &[u64], codec_type: CodecType) -> Codec::ColumnValues {
        let mut buffer = Vec::new();
        serialize_u64_based_column_values(&vals, &[codec_type], &mut buffer).unwrap();
        Codec::load(common::OwnedBytes::new(buffer[1..].to_vec())).unwrap()
    }

    fn sweep<C: ColumnValues<u64>>(label: &str, col: C) {
        let mapping = StrictlyMonotonicMappingInverter::from(StrictlyMonotonicMappingToInternal::<
            f64,
        >::new());
        let n = col.num_vals() as u64;
        println!("\n{label}");
        println!(
            "{:>6} {:>11} {:>11} {:>9}",
            "take", "per_value", "scratch", "delta"
        );
        for take in [4usize, 8, 16, 32, 64, 128, 256, 1024, 4096] {
            let calls = 1_000_000usize / take;
            let mut a = vec![0f64; take];
            let mut b = vec![0f64; take];
            let (mut pv, mut sc) = (f64::MAX, f64::MAX);
            for round in 0..9 {
                let run_pv = |buf: &mut Vec<f64>| {
                    let t = Instant::now();
                    for c in 0..calls {
                        map_per_value(&col, &mapping, ((c * take) as u64) % (n - take as u64), buf);
                        black_box(&buf[take - 1]);
                    }
                    t.elapsed().as_secs_f64() * 1e3
                };
                let run_sc = |buf: &mut Vec<f64>| {
                    let t = Instant::now();
                    for c in 0..calls {
                        map_via_scratch(
                            &col,
                            &mapping,
                            ((c * take) as u64) % (n - take as u64),
                            buf,
                        );
                        black_box(&buf[take - 1]);
                    }
                    t.elapsed().as_secs_f64() * 1e3
                };
                if round % 2 == 0 {
                    pv = pv.min(run_pv(&mut a));
                    sc = sc.min(run_sc(&mut b));
                } else {
                    sc = sc.min(run_sc(&mut b));
                    pv = pv.min(run_pv(&mut a));
                }
                assert!(
                    a.iter()
                        .map(|v| v.to_bits())
                        .eq(b.iter().map(|v| v.to_bits())),
                    "arms disagree at take={take}"
                );
            }
            println!(
                "{take:>6} {pv:>11.3} {sc:>11.3} {:>8.1}%",
                (sc / pv - 1.0) * 100.0
            );
        }
    }

    #[test]
    #[ignore]
    fn ab_mapped_crossover() {
        const N: u64 = 1_000_000;
        let prices: Vec<u64> = (0..N)
            .map(|i| f64::to_u64(((i * 2_654_435_761) % 1_000_000) as f64 / 100.0))
            .collect();
        let col = load::<BitpackedCodec>(&prices, CodecType::Bitpacked);
        println!("prices bitpacked width {}", {
            let mut b = Vec::new();
            serialize_u64_based_column_values(&&prices[..], &[CodecType::Bitpacked], &mut b)
                .unwrap();
            b.len() * 8 / N as usize
        });
        sweep("prices (f64 from bitpacked)", col);
        let dates: Vec<u64> = (0..N)
            .map(|i| f64::to_u64(1_700_000_000.0 + i as f64))
            .collect();
        sweep(
            "dates linear-ish (f64 from bitpacked)",
            load::<BitpackedCodec>(&dates, CodecType::Bitpacked),
        );
        sweep(
            "dates (f64 from linear)",
            load::<LinearCodec>(&dates, CodecType::Linear),
        );
        sweep(
            "dates (f64 from blockwise)",
            load::<BlockwiseLinearCodec>(&dates, CodecType::BlockwiseLinear),
        );
        let wide: Vec<u64> = {
            let mut x = 0x9E37_79B9_7F4A_7C15u64;
            (0..N)
                .map(|_| {
                    x ^= x << 13;
                    x ^= x >> 7;
                    x ^= x << 17;
                    x
                })
                .collect()
        };
        sweep(
            "random w64 (f64 from bitpacked)",
            load::<BitpackedCodec>(&wide, CodecType::Bitpacked),
        );
    }
}

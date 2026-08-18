use std::fmt::Debug;
use std::marker::PhantomData;
use std::ops::{Range, RangeInclusive};

use crate::ColumnValues;
use crate::column_values::monotonic_mapping::StrictlyMonotonicFn;

/// The buffered path's crossover, given the codec's own: the extra buffer
/// fill and mapping pass move it ~4/3 further out. Saturating, so a column
/// that never batches (`usize::MAX`) still never batches here.
#[inline]
fn buffered_min_rows(codec_min_rows: usize) -> usize {
    codec_min_rows.saturating_mul(4).div_ceil(3)
}

#[inline(always)]
fn is_same_type<A: 'static, B: 'static>() -> bool {
    std::any::TypeId::of::<A>() == std::any::TypeId::of::<B>()
}

struct MonotonicMappingColumn<C, T, Input> {
    from_column: C,
    monotonic_mapping: T,
    /// Resolved once at construction: `get_range` consults it on every call,
    /// and the live chain (a possibly virtual call into the codec plus an
    /// atomic load) costs about as much as a one-row take.
    min_batch_rows: usize,
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
    let codec_min_rows = from_column.min_batch_rows();
    let min_batch_rows = if is_same_type::<Input, Output>() {
        codec_min_rows
    } else {
        buffered_min_rows(codec_min_rows)
    };
    MonotonicMappingColumn {
        from_column,
        monotonic_mapping,
        min_batch_rows,
        _phantom: PhantomData,
    }
}

/// Short takes: monomorphized, so this inlines straight into the codec and
/// beats both batch paths below their crossover.
#[inline(always)]
fn map_per_value<C, T, Input, Output>(col: &C, mapping: &T, start: u64, output: &mut [Output])
where
    C: ColumnValues<Input>,
    T: StrictlyMonotonicFn<Input, Output>,
    Input: PartialOrd,
    Output: PartialOrd,
{
    for (i, out) in output.iter_mut().enumerate() {
        *out = mapping.mapping(col.get_val(start as u32 + i as u32));
    }
}

/// `Input == Output` -- the identity wrapper every u64 column is loaded with.
/// Decodes straight into the caller's buffer and applies the mapping in place,
/// which for an identity mapping compiles the fix-up loop away.
///
/// Callers must have checked `is_same_type::<Input, Output>()`.
#[inline(always)]
fn forward_in_place<C, T, Input, Output>(col: &C, mapping: &T, start: u64, output: &mut [Output])
where
    C: ColumnValues<Input>,
    T: StrictlyMonotonicFn<Input, Output>,
    Input: PartialOrd + Clone + 'static,
    Output: PartialOrd + 'static,
{
    debug_assert!(is_same_type::<Input, Output>());
    // Safety (both casts): `TypeId` equality of two `'static` types proves
    // `Input` and `Output` are the same type -- the same basis `Any::downcast`
    // rests on. Values are duplicated with `Clone`, not bit-copied, so a
    // mapping that panics leaves the slice holding its original, still-valid
    // values.
    let output_as_input: &mut [Input] = unsafe { &mut *(output as *mut [Output] as *mut [Input]) };
    col.get_range(start, output_as_input);
    for val in output.iter_mut() {
        let raw: Input = unsafe { &*(val as *const Output as *const Input) }.clone();
        *val = mapping.mapping(raw);
    }
}

/// `Input != Output`, e.g. an f64 column over u64 storage: decode into a
/// temporary `Input` buffer, then map into the output.
#[inline(always)]
fn map_via_buffer<C, T, Input, Output>(col: &C, mapping: &T, start: u64, output: &mut [Output])
where
    C: ColumnValues<Input>,
    T: StrictlyMonotonicFn<Input, Output>,
    Input: PartialOrd + Clone,
    Output: PartialOrd,
{
    // One codec block, aligned on CHUNK boundaries in column space, so every
    // take the underlying codec sees is a whole block. Block-sized so the
    // buffer fits on the stack instead of allocating per call.
    const CHUNK: usize = 128;
    let init: Input = col.get_val(start as u32);
    let mut buffer: [Input; CHUNK] = std::array::from_fn(|_| init.clone());
    let mut offset = 0usize;
    while offset < output.len() {
        let pos = start + offset as u64;
        let len = (CHUNK - (pos as usize % CHUNK)).min(output.len() - offset);
        let buf = &mut buffer[..len];
        col.get_range(pos, buf);
        for (out, inp) in output[offset..offset + len].iter_mut().zip(buf.iter()) {
            *out = mapping.mapping(inp.clone());
        }
        offset += len;
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

    #[inline(always)]
    fn min_batch_rows(&self) -> usize {
        self.min_batch_rows
    }

    /// Three ways to fill `output`, cheapest applicable one first. The
    /// threshold comes from the column rather than a constant here because it
    /// moves with the codec's `get_val` cost and with whether a SIMD block
    /// kernel is available. A column that does not decode in batches reports
    /// `usize::MAX` and only ever takes the first.
    fn get_range(&self, start: u64, output: &mut [Output]) {
        let (col, mapping) = (&self.from_column, &self.monotonic_mapping);
        if output.len() < self.min_batch_rows() {
            map_per_value(col, mapping, start, output);
        } else if is_same_type::<Input, Output>() {
            forward_in_place(col, mapping, start, output);
        } else {
            map_via_buffer(col, mapping, start, output);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

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

    /// `get_range` on the buffered general path (`Input=u64 != Output=f64`)
    /// must agree with `get_val` for every (start, len) phase.
    #[test]
    fn test_get_range_buffered_path_matches_get_val() {
        use crate::column_values::{
            CodecType, load_u64_based_column_values, serialize_u64_based_column_values,
        };

        let vals: Vec<f64> = (0..1_000u64).map(|i| (i as f64) / 100.0).collect();
        let mut buffer = Vec::new();
        serialize_u64_based_column_values(&&vals[..], &[CodecType::Bitpacked], &mut buffer)
            .unwrap();
        let column = load_u64_based_column_values::<f64>(common::OwnedBytes::new(buffer)).unwrap();

        for len in [1usize, 63, 64, 65, 127, 128, 129, 200, 256, 512, 600] {
            for start in (0..300usize).chain([1_000 - len]) {
                if start + len > vals.len() {
                    continue;
                }
                let mut output = vec![0f64; len];
                column.get_range(start as u64, &mut output);
                let expected: Vec<f64> = (0..len)
                    .map(|i| column.get_val((start + i) as u32))
                    .collect();
                assert_eq!(output, expected, "start={start} len={len}");
                assert_eq!(output, vals[start..start + len], "start={start} len={len}");
            }
        }
    }

    /// Every wrapper in the chain has to forward `min_batch_rows` or the
    /// value silently reverts to `usize::MAX` and `get_range` never batches.
    #[test]
    fn test_min_batch_rows_survives_wrappers() {
        use crate::column_values::{
            CodecType, load_u64_based_column_values, serialize_u64_based_column_values,
        };

        let vals: Vec<u64> = (0..1_000u64).map(|i| i * 7 + 3).collect();
        let mut buffer = Vec::new();
        serialize_u64_based_column_values(&&vals[..], &[CodecType::Bitpacked], &mut buffer)
            .unwrap();
        let column: Arc<dyn ColumnValues<u64>> =
            load_u64_based_column_values(common::OwnedBytes::new(buffer)).unwrap();

        let batched = column.min_batch_rows();
        assert!(
            batched < usize::MAX,
            "Bitpacked reports a batch threshold, but it did not survive the wrappers"
        );

        // `VecColumn` reports 1, so this also pins that the buffered path's
        // scaling never rounds a real threshold down to zero.
        let buffered: Box<dyn ColumnValues<i64>> = Box::new(monotonic_map_column(
            VecColumn::from(vals.clone()),
            StrictlyMonotonicMappingInverter::from(StrictlyMonotonicMappingToInternal::<i64>::new()),
        ));
        assert_eq!(VecColumn::from(vals).min_batch_rows(), 1);
        assert_eq!(buffered.min_batch_rows(), 2);

        // A column that does not batch must not have one invented for it:
        // `usize::MAX` has to survive the buffered path's multiplication.
        struct NoBatch;
        impl ColumnValues<u64> for NoBatch {
            fn get_val(&self, idx: u32) -> u64 {
                idx as u64
            }
            fn min_value(&self) -> u64 {
                0
            }
            fn max_value(&self) -> u64 {
                1_000
            }
            fn num_vals(&self) -> u32 {
                1_000
            }
        }
        assert_eq!(NoBatch.min_batch_rows(), usize::MAX);
        let over_no_batch: Box<dyn ColumnValues<i64>> = Box::new(monotonic_map_column(
            NoBatch,
            StrictlyMonotonicMappingInverter::from(StrictlyMonotonicMappingToInternal::<i64>::new()),
        ));
        assert!(
            over_no_batch.min_batch_rows() >= 1_000,
            "a non-batching column must not get a usable threshold from the wrapper"
        );
    }

    /// The same, on the identity fast path (`Input == Output`), which forwards
    /// straight to the codec instead of buffering.
    #[test]
    fn test_get_range_identity_path_matches_get_val() {
        use crate::column_values::{
            CodecType, load_u64_based_column_values, serialize_u64_based_column_values,
        };

        let vals: Vec<u64> = (0..1_000u64).map(|i| i * 7 + 3).collect();
        let mut buffer = Vec::new();
        serialize_u64_based_column_values(&&vals[..], &[CodecType::Bitpacked], &mut buffer)
            .unwrap();
        let column = load_u64_based_column_values::<u64>(common::OwnedBytes::new(buffer)).unwrap();

        for len in [1usize, 64, 128, 129, 512] {
            for start in (0..300usize).chain([1_000 - len]) {
                if start + len > vals.len() {
                    continue;
                }
                let mut output = vec![0u64; len];
                column.get_range(start as u64, &mut output);
                assert_eq!(output, vals[start..start + len], "start={start} len={len}");
            }
        }
    }

    /// Calibration harness for `min_batch_rows`: A/Bs the batch and per-value
    /// arms of `get_range` over a (start phase x length) grid per codec, and
    /// scores what the shipped gate decides at each cell.
    ///
    /// ```text
    /// cargo test --release -p tantivy-columnar --lib gate_sweep \
    ///     -- --ignored --nocapture
    /// TANTIVY_BITPACKER_SCALAR=1 cargo test --release ...   # scalar kernel
    /// ```
    ///
    /// It lives here rather than in `benches/` because it drives the arms the
    /// gate rejects, and those are private to this module. Run both arms over
    /// the concrete codec reader (through `Arc<dyn ColumnValues>` the
    /// per-value arm pays a virtual call per row that production never pays),
    /// and sweep both `simd_enabled()` states.
    #[test]
    #[ignore]
    fn gate_sweep() {
        use std::time::Instant;

        use crate::column_values::monotonic_mapping::{
            StrictlyMonotonicMappingInverter, StrictlyMonotonicMappingToInternal,
        };
        use crate::column_values::u64_based::{
            BitpackedCodec, BlockForCodec, BlockwiseLinearCodec, ColumnCodec, LinearCodec,
        };
        use crate::column_values::{CodecType, serialize_u64_based_column_values};

        const N: usize = 1 << 16;
        const SWEEP_BLOCK: usize = 128;

        fn header(label: &str, threshold: usize) {
            println!("\n=== {label} (min_batch_rows={threshold}) ===");
            println!(
                "{:>5} {:>6} {:>7} {:>9} {:>9} {:>7}  verdict",
                "len", "phase", "maxblk", "batch_ns", "loop_ns", "ratio"
            );
        }

        /// `max_block_take`: rows the take gets from whichever block it takes
        /// the most from -- the rejected alignment term, kept here only to
        /// score it against the shipped flat gate.
        fn maxblk(phase: usize, len: usize) -> usize {
            let first = (SWEEP_BLOCK - phase % SWEEP_BLOCK).min(len);
            if first == len {
                return len;
            }
            first.max((len - first).min(SWEEP_BLOCK))
        }

        fn verdict(threshold: usize, phase: usize, len: usize, ratio: f64) -> String {
            let score = |batches: bool| {
                if batches && ratio < 0.95 {
                    "LOSS"
                } else if !batches && ratio > 1.05 {
                    "misses"
                } else {
                    "ok"
                }
            };
            format!(
                "old={:<6} new={}",
                score(len >= threshold),
                score(maxblk(phase, len) >= threshold)
            )
        }

        // `Input == Output`: the forwarding path.
        fn sweep<C: ColumnCodec<u64>>(label: &str, bytes: Vec<u8>) {
            let col = C::load(common::OwnedBytes::new(bytes)).unwrap();
            let mapping = StrictlyMonotonicMappingToInternal::<u64>::new();
            let threshold = col.min_batch_rows();
            header(&format!("{label} forwarding"), threshold);
            for len in [16usize, 32, 64, 96, 128, 192, 256, 512, 1024, 4096, 16384] {
                for phase in [0usize, 16, 32, 64, 96, 100, 112, 127] {
                    let mut out = vec![0u64; len];
                    let reps = (1 << 21) / len;
                    let starts: Vec<u64> = (0..64)
                        .map(|k| (phase + k * 512 * SWEEP_BLOCK) as u64)
                        .filter(|s| *s as usize + len <= N)
                        .collect();
                    for _ in 0..64 {
                        forward_in_place(&col, &mapping, starts[0], &mut out);
                        map_per_value(&col, &mapping, starts[0], &mut out);
                    }
                    let mut sink = 0u64;
                    let t0 = Instant::now();
                    for i in 0..reps {
                        forward_in_place(&col, &mapping, starts[i % starts.len()], &mut out);
                        sink = sink.wrapping_add(out[len - 1]);
                    }
                    let batch_ns = t0.elapsed().as_nanos() as f64 / (reps * len) as f64;
                    let t1 = Instant::now();
                    for i in 0..reps {
                        map_per_value(&col, &mapping, starts[i % starts.len()], &mut out);
                        sink = sink.wrapping_add(out[len - 1]);
                    }
                    let loop_ns = t1.elapsed().as_nanos() as f64 / (reps * len) as f64;
                    std::hint::black_box(sink);
                    let ratio = loop_ns / batch_ns;
                    println!(
                        "{len:>5} {phase:>6} {:>7} {batch_ns:>9.3} {loop_ns:>9.3} {ratio:>7.2}  {}",
                        maxblk(phase, len),
                        verdict(threshold, phase, len, ratio)
                    );
                }
            }
        }

        // `Input != Output`: the buffered path.
        fn sweep_buffered<C: ColumnCodec<u64>>(label: &str, bytes: Vec<u8>) {
            let col = C::load(common::OwnedBytes::new(bytes)).unwrap();
            let mapping = StrictlyMonotonicMappingInverter::from(
                StrictlyMonotonicMappingToInternal::<f64>::new(),
            );
            let threshold = buffered_min_rows(col.min_batch_rows());
            header(&format!("{label} buffered"), threshold);
            for len in [16usize, 32, 64, 96, 128, 192, 256, 512, 768] {
                for phase in [0usize, 32, 64, 100, 127] {
                    let mut out = vec![0f64; len];
                    let reps = (1 << 21) / len;
                    let starts: Vec<u64> = (0..64)
                        .map(|k| (phase + k * 512 * SWEEP_BLOCK) as u64)
                        .filter(|s| *s as usize + len <= N)
                        .collect();
                    for _ in 0..64 {
                        map_via_buffer(&col, &mapping, starts[0], &mut out);
                        map_per_value(&col, &mapping, starts[0], &mut out);
                    }
                    let mut sink = 0f64;
                    let t0 = Instant::now();
                    for i in 0..reps {
                        map_via_buffer(&col, &mapping, starts[i % starts.len()], &mut out);
                        sink += out[len - 1];
                    }
                    let batch_ns = t0.elapsed().as_nanos() as f64 / (reps * len) as f64;
                    let t1 = Instant::now();
                    for i in 0..reps {
                        map_per_value(&col, &mapping, starts[i % starts.len()], &mut out);
                        sink += out[len - 1];
                    }
                    let loop_ns = t1.elapsed().as_nanos() as f64 / (reps * len) as f64;
                    std::hint::black_box(sink);
                    let ratio = loop_ns / batch_ns;
                    println!(
                        "{len:>5} {phase:>6} {:>7} {batch_ns:>9.3} {loop_ns:>9.3} {ratio:>7.2}  {}",
                        maxblk(phase, len),
                        verdict(threshold, phase, len, ratio)
                    );
                }
            }
        }

        println!(
            "SIMD block kernel: {}",
            if std::env::var_os("TANTIVY_BITPACKER_SCALAR").is_some() {
                "disabled (TANTIVY_BITPACKER_SCALAR set)"
            } else {
                "enabled"
            }
        );

        let ints: Vec<u64> = (0..N as u64)
            .map(|i| (i.wrapping_mul(2_654_435_761)) % 100_000)
            .collect();
        let mut buf = Vec::new();
        serialize_u64_based_column_values(&&ints[..], &[CodecType::Bitpacked], &mut buf).unwrap();
        sweep::<BitpackedCodec>("Bitpacked", buf[1..].to_vec());

        let mut buf = Vec::new();
        serialize_u64_based_column_values(&&ints[..], &[CodecType::BlockFor], &mut buf).unwrap();
        sweep::<BlockForCodec>("BlockFor", buf[1..].to_vec());

        // A noisy ramp, which is what the two line codecs are chosen for.
        let ramp: Vec<u64> = (0..N as u64)
            .map(|i| i * 16 + (i.wrapping_mul(2_654_435_761) % (1 << 20)))
            .collect();

        let mut buf = Vec::new();
        serialize_u64_based_column_values(&&ramp[..], &[CodecType::Linear], &mut buf).unwrap();
        sweep::<LinearCodec>("Linear", buf[1..].to_vec());

        let mut buf = Vec::new();
        serialize_u64_based_column_values(&&ramp[..], &[CodecType::BlockwiseLinear], &mut buf)
            .unwrap();
        sweep::<BlockwiseLinearCodec>("BlockwiseLinear", buf[1..].to_vec());

        // Residual width drives which unpack kernel runs: the narrow (u32
        // lane) one covers 1..=25 and the wide (u64 lane) one 26..=56, at
        // 3.10-3.29x and 2.83-2.96x over scalar. Sweep across that boundary.
        // Same question for a second Flat codec: is the crossover a property
        // of the unpack kernel rather than of the codec?
        for bits in [10u32, 16, 22, 24, 26, 30, 40] {
            let vals: Vec<u64> = (0..N as u64)
                .map(|i| i.wrapping_mul(2_654_435_761) % (1u64 << bits))
                .collect();
            let mut buf = Vec::new();
            serialize_u64_based_column_values(&&vals[..], &[CodecType::Bitpacked], &mut buf)
                .unwrap();
            let packed = (buf.len() as f64 * 8.0) / N as f64;
            println!("\n[Bitpacked-w{bits}] ~{packed:.1} packed bits/value");
            sweep::<BitpackedCodec>(&format!("Bitpacked-w{bits}"), buf[1..].to_vec());
        }

        for (label, bits) in [
            ("Linear-w4", 4u32),
            ("Linear-w8", 8),
            ("Linear-w12", 12),
            ("Linear-w14", 14),
            ("Linear-w16", 16),
            ("Linear-w20", 20),
            ("Linear-w25", 25),
            ("Linear-w32", 32),
            ("Linear-w40", 40),
        ] {
            let vals: Vec<u64> = (0..N as u64)
                .map(|i| i * 16 + (i.wrapping_mul(2_654_435_761) % (1u64 << bits)))
                .collect();
            let mut buf = Vec::new();
            serialize_u64_based_column_values(&&vals[..], &[CodecType::Linear], &mut buf).unwrap();
            // What actually got packed, rather than what the generator asked
            // for: the bitpacked residual is the deviation from the trained
            // line, not the noise term.
            let packed_bits = (buf.len() as f64 * 8.0) / N as f64;
            println!("\n[{label}] generator bits={bits} -> ~{packed_bits:.1} packed bits/value");
            sweep::<LinearCodec>(label, buf[1..].to_vec());

            // Same width, the other line codec: does its per-block hoisting
            // carry it past the scalar cutoff the way Bitpacked's does?
            let mut buf = Vec::new();
            serialize_u64_based_column_values(&&vals[..], &[CodecType::BlockwiseLinear], &mut buf)
                .unwrap();
            let packed_bits = (buf.len() as f64 * 8.0) / N as f64;
            let bwl_label = label.replace("Linear", "BWL");
            println!("\n[{bwl_label}] generator bits={bits} -> ~{packed_bits:.1} packed bits/value");
            sweep::<BlockwiseLinearCodec>(&bwl_label, buf[1..].to_vec());
        }

        let mut buf = Vec::new();
        serialize_u64_based_column_values(&&ints[..], &[CodecType::Bitpacked], &mut buf).unwrap();
        sweep_buffered::<BitpackedCodec>("Bitpacked", buf[1..].to_vec());

        let mut buf = Vec::new();
        serialize_u64_based_column_values(&&ints[..], &[CodecType::BlockFor], &mut buf).unwrap();
        sweep_buffered::<BlockForCodec>("BlockFor", buf[1..].to_vec());
    }
}

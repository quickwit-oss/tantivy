//! In-process A/B of the batch block-decode paths against the per-value
//! defaults they replaced, reproduced verbatim as the `_pervalue` variants.
//!
//! `TANTIVY_BITPACKER_SCALAR=1` additionally disables the SIMD kernel, which
//! separates "batching won" from "SIMD won".

use std::sync::Arc;

use binggan::{InputGroup, black_box};
use rand::prelude::*;
use tantivy_columnar::column_values::{CodecType, serialize_and_load_u64_based_column_values};
use tantivy_columnar::*;

const NUM_VALS: usize = 100_000;

/// Values needing exactly `bits` bits after gcd normalization (gcd == 1).
fn data_with_width(bits: u32) -> Vec<u64> {
    let mut rng = StdRng::from_seed([3u8; 32]);
    let max = if bits >= 64 {
        u64::MAX
    } else {
        (1u64 << bits) - 1
    };
    let mut vals: Vec<u64> = (0..NUM_VALS).map(|_| rng.random_range(0..=max)).collect();
    // Pin the amplitude to `bits` and keep gcd at 1.
    vals[0] = 0;
    vals[1] = max;
    vals[2] = 1;
    vals
}

/// The `ColumnValues::get_range` default, verbatim: 4 `get_val`s per step.
fn get_range_per_value<T: PartialOrd + Copy + std::fmt::Debug + 'static>(
    col: &dyn ColumnValues<T>,
    start: usize,
    output: &mut [T],
) {
    let (out_chunks, out_rem) = output.as_chunks_mut::<4>();
    let mut idx = start as u64;
    for out_x4 in out_chunks {
        out_x4[0] = col.get_val(idx as u32);
        out_x4[1] = col.get_val((idx + 1) as u32);
        out_x4[2] = col.get_val((idx + 2) as u32);
        out_x4[3] = col.get_val((idx + 3) as u32);
        idx += 4;
    }
    for out in out_rem {
        *out = col.get_val(idx as u32);
        idx += 1;
    }
}

type Col = Arc<dyn ColumnValues<u64>>;

fn main() {
    let mut inputs: Vec<(String, Col)> = Vec::new();
    // 8: byte aligned, NEON. 17/25: NEON, unaligned. 33: above the kernel's
    // 25-bit ceiling -> scalar block path. 64: plain u64 loads.
    for bits in [8u32, 17, 25, 33, 64] {
        let col: Col = serialize_and_load_u64_based_column_values(
            &&data_with_width(bits)[..],
            &[CodecType::Bitpacked],
        );
        inputs.push((format!("w{bits}"), col));
    }
    // The linear codecs share the residual-stream layout and take the same
    // kernels, but pay a line evaluation (and for blockwise, a per-block
    // metadata fetch) on top: measured separately, not assumed.
    {
        let mut rng = StdRng::from_seed([7u8; 32]);
        let linear_ish: Vec<u64> = (0..NUM_VALS)
            .map(|i| 1_000_000 + 37 * i as u64 + rng.random_range(0..256))
            .collect();
        let col: Col =
            serialize_and_load_u64_based_column_values(&&linear_ish[..], &[CodecType::Linear]);
        inputs.push(("linear".to_string(), col));
        let col: Col = serialize_and_load_u64_based_column_values(
            &&linear_ish[..],
            &[CodecType::BlockwiseLinear],
        );
        inputs.push(("blockwise_linear".to_string(), col));
    }
    let mut group: InputGroup<Col> = InputGroup::new_with_inputs(inputs);

    group.register("getrange_batch", |col: &Col| {
        let n = col.num_vals() as usize;
        let mut buf = vec![0u64; 4096];
        let mut acc = 0u64;
        let mut start = 0usize;
        while start < n {
            let take = 4096.min(n - start);
            col.get_range(start as u64, &mut buf[..take]);
            for &v in &buf[..take] {
                acc += v;
            }
            start += take;
        }
        black_box(acc);
    });

    // Exactly the old `ColumnValues::get_range` default: 4 `get_val`s per step.
    group.register("getrange_pervalue", |col: &Col| {
        let n = col.num_vals() as usize;
        let mut buf = vec![0u64; 4096];
        let mut acc = 0u64;
        let mut start = 0usize;
        while start < n {
            let take = 4096.min(n - start);
            get_range_per_value(col.as_ref(), start, &mut buf[..take]);
            for &v in &buf[..take] {
                acc += v;
            }
            start += take;
        }
        black_box(acc);
    });

    // Short ranges at random offsets: the shape aggregations produce. These
    // guard the partial-block threshold in `block_decode.rs`.
    for take in [1usize, 16, 64] {
        group.register(format!("short{take}_batch"), move |col: &Col| {
            let n = col.num_vals() as usize;
            let mut buf = vec![0u64; take];
            let mut acc = 0u64;
            let mut start = 1usize;
            while start + take < n {
                col.get_range(start as u64, &mut buf);
                acc ^= buf[0];
                // Coprime-ish stride so offsets cycle through block phases.
                start += 191;
            }
            black_box(acc);
        });
        group.register(format!("short{take}_pervalue"), move |col: &Col| {
            let n = col.num_vals() as usize;
            let mut buf = vec![0u64; take];
            let mut acc = 0u64;
            let mut start = 1usize;
            while start + take < n {
                get_range_per_value(col.as_ref(), start, &mut buf);
                acc ^= buf[0];
                start += 191;
            }
            black_box(acc);
        });
    }

    // Quantifies what the `Box<dyn Iterator>` itself costs against an inline
    // `get_val` loop (pre-existing, unchanged by the batch decode).
    group.register("iter_trait", |col: &Col| {
        let acc: u64 = col.iter().sum();
        black_box(acc);
    });

    group.register("iter_inline_getval", |col: &Col| {
        let acc: u64 = (0..col.num_vals()).map(|idx| col.get_val(idx)).sum();
        black_box(acc);
    });

    group.run();

    bench_f64_wrapper();
}

type F64Col = (Arc<dyn ColumnValues<f64>>, Arc<dyn ColumnValues<u64>>);

/// The buffered general path as it stood before the buffer moved to the
/// stack: a `Vec<Input>` allocated per call, chunked on 512 boundaries.
fn get_range_vec512(col: &dyn ColumnValues<u64>, start: u64, output: &mut [f64]) {
    if output.len() >= 64 {
        const CHUNK: usize = 512;
        let init: u64 = col.get_val(start as u32);
        let mut buffer: Vec<u64> = vec![init; CHUNK.min(output.len())];
        let mut offset = 0usize;
        while offset < output.len() {
            let pos = start + offset as u64;
            let len = (CHUNK - (pos as usize % CHUNK)).min(output.len() - offset);
            let buf = &mut buffer[..len];
            col.get_range(pos, buf);
            for (out, inp) in output[offset..offset + len].iter_mut().zip(buf.iter()) {
                *out = f64::from_u64(*inp);
            }
            offset += len;
        }
    } else {
        for (i, out) in output.iter_mut().enumerate() {
            *out = f64::from_u64(col.get_val(start as u32 + i as u32));
        }
    }
}

/// f64 columns take the monotonic wrapper's *buffered* forwarding path
/// (`Input=u64 != Output=f64`). Compares:
///
/// - `wrapper_batch`: what ships now (buffered forward, block-sized stack buffer),
/// - `wrapper_vec512`: the previous buffered path (per-call `Vec`, 512 chunks),
/// - `wrapper_fused_pervalue`: the pre-batching behaviour,
/// - `scratch_batch`: the batch decode with the buffer hoisted out of the loop entirely, the
///   ceiling any in-wrapper buffering can reach.
///
/// `chunk` is the `output.len()` per call; the `_misaligned` pair drives the
/// same calls from starts that are not on the chunk grid.
fn bench_f64_wrapper() {
    use tantivy_columnar::column_values::{
        load_u64_based_column_values, serialize_u64_based_column_values,
    };

    fn build(vals: &[f64]) -> F64Col {
        let mut buffer = Vec::new();
        serialize_u64_based_column_values(&vals, &[CodecType::Bitpacked], &mut buffer).unwrap();
        let bytes = common::OwnedBytes::new(buffer);
        let as_f64 = load_u64_based_column_values::<f64>(bytes.clone()).unwrap();
        // The same bytes read as raw u64s, so the scratch variant can drive the
        // inner column directly and apply the mapping itself.
        let as_u64 = load_u64_based_column_values::<u64>(bytes).unwrap();
        (as_f64, as_u64)
    }

    let mut rng = StdRng::from_seed([5u8; 32]);
    let prices: Vec<f64> = (0..NUM_VALS)
        .map(|_| (rng.random_range(0..10_000_000) as f64) / 100.0)
        .collect();
    let dates: Vec<f64> = (0..NUM_VALS).map(|i| 1_700_000_000.0 + i as f64).collect();

    // Guard: the scratch variant reproduces the wrapper's mapping itself, so
    // prove all three agree before timing them.
    for vals in [&prices, &dates] {
        let (as_f64, as_u64) = build(vals);
        let mut via_wrapper = vec![0f64; 300];
        as_f64.get_range(64, &mut via_wrapper);
        let mut raw = vec![0u64; 300];
        as_u64.get_range(64, &mut raw);
        let via_scratch: Vec<f64> = raw.iter().map(|&r| f64::from_u64(r)).collect();
        assert_eq!(via_wrapper, via_scratch, "scratch variant maps differently");
        assert_eq!(via_wrapper, vals[64..364], "wrapper get_range is wrong");
        // Same for the previous-implementation replica, on an aligned and a
        // deliberately misaligned start.
        for start in [64usize, 377] {
            let mut via_vec512 = vec![0f64; 300];
            get_range_vec512(as_u64.as_ref(), start as u64, &mut via_vec512);
            assert_eq!(
                via_vec512,
                vals[start..start + 300],
                "vec512 replica is wrong at start={start}"
            );
        }
    }

    for chunk in [128usize, 512, 4096] {
        let mut inputs: Vec<(String, F64Col)> = Vec::new();
        inputs.push((format!("prices_chunk{chunk}"), build(&prices)));
        inputs.push((format!("dates_chunk{chunk}"), build(&dates)));
        let mut group: InputGroup<F64Col> = InputGroup::new_with_inputs(inputs);

        group.register("wrapper_batch", move |inp: &F64Col| {
            let n = inp.0.num_vals() as usize;
            let mut out = vec![0f64; chunk];
            let mut acc = 0f64;
            let mut start = 0usize;
            while start < n {
                let take = chunk.min(n - start);
                inp.0.get_range(start as u64, &mut out[..take]);
                for &v in &out[..take] {
                    acc += v;
                }
                start += take;
            }
            black_box(acc);
        });

        // The previous buffered path: per-call `Vec`, 512-aligned chunks.
        group.register("wrapper_vec512", move |inp: &F64Col| {
            let n = inp.0.num_vals() as usize;
            let mut out = vec![0f64; chunk];
            let mut acc = 0f64;
            let mut start = 0usize;
            while start < n {
                let take = chunk.min(n - start);
                get_range_vec512(inp.1.as_ref(), start as u64, &mut out[..take]);
                for &v in &out[..take] {
                    acc += v;
                }
                start += take;
            }
            black_box(acc);
        });

        // Misaligned starts: a coprime stride walks the range across every
        // phase of the chunk grid instead of landing on it every time.
        group.register("wrapper_batch_misaligned", move |inp: &F64Col| {
            let n = inp.0.num_vals() as usize;
            let mut out = vec![0f64; chunk];
            let mut acc = 0f64;
            let mut start = 1usize;
            while start + chunk < n {
                inp.0.get_range(start as u64, &mut out);
                acc += out[0];
                start += chunk + 191;
            }
            black_box(acc);
        });

        group.register("wrapper_vec512_misaligned", move |inp: &F64Col| {
            let n = inp.0.num_vals() as usize;
            let mut out = vec![0f64; chunk];
            let mut acc = 0f64;
            let mut start = 1usize;
            while start + chunk < n {
                get_range_vec512(inp.1.as_ref(), start as u64, &mut out);
                acc += out[0];
                start += chunk + 191;
            }
            black_box(acc);
        });

        // The wrapper's fused else-branch, verbatim: what f64 columns ran
        // before `Bitpacked` declared a specialized `get_range`.
        group.register("wrapper_fused_pervalue", move |inp: &F64Col| {
            let n = inp.0.num_vals() as usize;
            let mut out = vec![0f64; chunk];
            let mut acc = 0f64;
            let mut start = 0usize;
            while start < n {
                let take = chunk.min(n - start);
                get_range_per_value(inp.0.as_ref(), start, &mut out[..take]);
                for &v in &out[..take] {
                    acc += v;
                }
                start += take;
            }
            black_box(acc);
        });

        // Batch decode with the intermediate buffer allocated once.
        group.register("scratch_batch", move |inp: &F64Col| {
            let n = inp.0.num_vals() as usize;
            let mut out = vec![0f64; chunk];
            let mut scratch = vec![0u64; chunk];
            let mut acc = 0f64;
            let mut start = 0usize;
            while start < n {
                let take = chunk.min(n - start);
                inp.1.get_range(start as u64, &mut scratch[..take]);
                for (o, &raw) in out[..take].iter_mut().zip(scratch[..take].iter()) {
                    *o = f64::from_u64(raw);
                }
                for &v in &out[..take] {
                    acc += v;
                }
                start += take;
            }
            black_box(acc);
        });

        group.run();
    }
}

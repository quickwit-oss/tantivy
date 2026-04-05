//! GPU vs CPU benchmarks for tantivy-gpu.
//!
//! Measures pure compute time by pre-initializing GPU context and kernels once,
//! then running data through pre-compiled pipelines.
//!
//! C1 fix: GpuContext initialized once, reused across all iterations.
//! C2 fix: CPU baseline uses f32 to match GPU precision for fair comparison.
//!         f64 baseline also shown separately for reference.
//! C4 fix: Aborts if no hardware GPU detected.
//!
//! Run with: cargo bench -p tantivy-gpu --bench gpu_bench

use std::hint::black_box;

use tantivy_gpu::buffer::pool::BufferPool;
use tantivy_gpu::buffer::{Bm25DocInput, Bm25Params};
use tantivy_gpu::device::GpuContext;
use tantivy_gpu::kernel::bm25::Bm25Kernel;
use tantivy_gpu::kernel::histogram::HistogramParams;
use tantivy_gpu::kernel::{GpuKernel, HistogramKernel, StatsKernel};
use tantivy_gpu::scorer::GpuBm25Weight;
use tantivy_gpu::vector::distance::DistanceMetric;
use tantivy_gpu::vector::hnsw::HnswIndex;

// ─── Helpers ───

fn gen_f32_data(n: usize) -> Vec<f32> {
    (0..n).map(|i| (i as f32) * 0.7 + 1.0).collect()
}

/// CPU stats baseline in f32 (fair comparison with GPU f32 shader).
fn cpu_stats_f32(values: &[f32]) -> (u32, f32, f32, f32) {
    let mut count = 0u32;
    let mut sum = 0.0f32;
    let mut min = f32::MAX;
    let mut max = f32::MIN;
    for &v in values {
        count += 1;
        sum += v;
        min = min.min(v);
        max = max.max(v);
    }
    (count, sum, min, max)
}

/// CPU stats baseline in f64 (Tantivy's actual precision).
fn cpu_stats_f64(values: &[f32]) -> (u32, f64, f64, f64) {
    let mut count = 0u32;
    let mut sum = 0.0f64;
    let mut min = f64::MAX;
    let mut max = f64::MIN;
    for &v in values {
        count += 1;
        let v64 = v as f64;
        sum += v64;
        min = min.min(v64);
        max = max.max(v64);
    }
    (count, sum, min, max)
}

fn cpu_bm25_score(weight: f32, k1: f32, b: f32, avg_dl: f32, tf: u32, dl: f32) -> f32 {
    let norm = k1 * (1.0 - b + b * dl / avg_dl);
    let tf_f = tf as f32;
    weight * tf_f / (tf_f + norm)
}

// ─── Main ───

fn main() {
    // ── C4: Abort if no hardware GPU ──
    let ctx = GpuContext::init().unwrap();
    println!("=== tantivy-gpu Benchmarks ===");
    println!(
        "GPU: {} ({}) | HW GPU: {}",
        ctx.info().name,
        ctx.info().backend,
        ctx.is_hardware_gpu()
    );
    if !ctx.is_hardware_gpu() {
        eprintln!("ERROR: No hardware GPU detected. Aborting GPU benchmark.");
        eprintln!("       Use --features cpu-fallback for CPU-only testing.");
        std::process::exit(1);
    }
    println!();

    // ── C1: Pre-compile all kernels once ──
    let stats_kernel = StatsKernel::compile(&ctx).unwrap();
    let histogram_kernel = HistogramKernel::compile(&ctx).unwrap();
    let bm25_kernel = Bm25Kernel::compile(&ctx).unwrap();
    let pool = BufferPool::from_ctx(&ctx);

    // ═══════════════════════════════════════════════════════════
    // Stats Reduction
    // ═══════════════════════════════════════════════════════════
    println!("--- Stats Reduction (GPU=f32, CPU-f32=f32) ---");
    for &n in &[1_000, 10_000, 100_000, 1_000_000, 10_000_000] {
        let data = gen_f32_data(n);

        // CPU f32 baseline
        let start = std::time::Instant::now();
        for _ in 0..10 {
            black_box(cpu_stats_f32(&data));
        }
        let cpu_f32_time = start.elapsed() / 10;

        // GPU (no pool) — warm up
        let _ = stats_kernel.execute(&data);
        let start = std::time::Instant::now();
        for _ in 0..10 {
            black_box(stats_kernel.execute(&data).unwrap());
        }
        let gpu_time = start.elapsed() / 10;

        // GPU (pooled) — warm up
        let _ = stats_kernel.execute_pooled(&data, &pool);
        let start = std::time::Instant::now();
        for _ in 0..10 {
            black_box(stats_kernel.execute_pooled(&data, &pool).unwrap());
        }
        let gpu_pooled_time = start.elapsed() / 10;

        println!(
            "  n={n:>10}: CPU {:>8.2?}  GPU {:>8.2?}  Pooled {:>8.2?}  pool/CPU {:.2}x",
            cpu_f32_time,
            gpu_time,
            gpu_pooled_time,
            cpu_f32_time.as_nanos() as f64 / gpu_pooled_time.as_nanos().max(1) as f64
        );
    }
    println!();

    // ═══════════════════════════════════════════════════════════
    // BM25 Batch Scoring
    // ═══════════════════════════════════════════════════════════
    println!("--- BM25 Batch Scoring ---");
    let bm25_params = Bm25Params {
        weight: 2.5,
        k1: 1.2,
        b: 0.75,
        avg_fieldnorm: 120.0,
    };

    for &n in &[128, 512, 2048, 8192, 32768, 131072, 1_000_000] {
        let term_freqs: Vec<u32> = (0..n).map(|i| (i % 10 + 1) as u32).collect();

        // Pack BM25 inputs
        let inputs: Vec<Bm25DocInput> = (0..n)
            .map(|i| Bm25DocInput {
                doc_id: i as u32,
                term_freq: term_freqs[i],
                fieldnorm_id: (i % 40) as u32,
                _pad: 0,
            })
            .collect();

        // CPU baseline
        let start = std::time::Instant::now();
        for _ in 0..20 {
            let mut scores = Vec::with_capacity(n);
            for i in 0..n {
                scores.push(cpu_bm25_score(
                    2.5,
                    1.2,
                    0.75,
                    120.0,
                    term_freqs[i],
                    (i % 40) as f32,
                ));
            }
            black_box(&scores);
        }
        let cpu_time = start.elapsed() / 20;

        // GPU (no pool) — warm up
        let _ = bm25_kernel.execute(&inputs, &bm25_params);
        let start = std::time::Instant::now();
        for _ in 0..20 {
            black_box(bm25_kernel.execute(&inputs, &bm25_params).unwrap());
        }
        let gpu_time = start.elapsed() / 20;

        // GPU (pooled) — warm up
        let _ = bm25_kernel.execute_pooled(&inputs, &bm25_params, &pool);
        let start = std::time::Instant::now();
        for _ in 0..20 {
            black_box(
                bm25_kernel
                    .execute_pooled(&inputs, &bm25_params, &pool)
                    .unwrap(),
            );
        }
        let gpu_pooled_time = start.elapsed() / 20;

        println!(
            "  n={n:>10}: CPU {:>8.2?}  GPU {:>8.2?}  Pooled {:>8.2?}  pool/CPU {:.2}x",
            cpu_time,
            gpu_time,
            gpu_pooled_time,
            cpu_time.as_nanos() as f64 / gpu_pooled_time.as_nanos().max(1) as f64
        );
    }
    println!();

    // ═══════════════════════════════════════════════════════════
    // Histogram
    // ═══════════════════════════════════════════════════════════
    println!("--- Histogram (100 buckets) ---");
    let params = HistogramParams {
        num_buckets: 100,
        interval: 10.0,
        offset: 0.0,
    };
    for &n in &[10_000, 100_000, 1_000_000] {
        let data: Vec<f64> = (0..n).map(|i| (i as f64) * 0.7 + 1.0).collect();

        // GPU — warm up
        let _ = histogram_kernel.execute(&data, &params);
        let start = std::time::Instant::now();
        for _ in 0..10 {
            black_box(histogram_kernel.execute(&data, &params).unwrap());
        }
        let gpu_time = start.elapsed() / 10;
        println!("  n={n:>10}: GPU {:>8.2?}", gpu_time);
    }
    println!();

    // ═══════════════════════════════════════════════════════════
    // Vector kNN
    // ═══════════════════════════════════════════════════════════
    println!("--- Vector kNN (HNSW, k=10, ef=100) ---");
    for &(n, dim) in &[(1_000, 128), (10_000, 128), (1_000, 768)] {
        let mut index = HnswIndex::with_gpu(dim, DistanceMetric::L2, &ctx).unwrap();
        for i in 0..n {
            let v: Vec<f32> = (0..dim).map(|d| (i * dim + d) as f32 * 0.01).collect();
            index.insert(v).unwrap();
        }

        let query: Vec<f32> = (0..dim).map(|d| d as f32 * 0.5).collect();

        // CPU search
        let start = std::time::Instant::now();
        for _ in 0..10 {
            black_box(index.search(&query, 10, 100));
        }
        let cpu_time = start.elapsed() / 10;

        // GPU search — warm up
        let _ = index.search_gpu(&query, 10, 100);
        let start = std::time::Instant::now();
        for _ in 0..10 {
            black_box(index.search_gpu(&query, 10, 100).unwrap());
        }
        let gpu_time = start.elapsed() / 10;

        let speedup = cpu_time.as_nanos() as f64 / gpu_time.as_nanos().max(1) as f64;
        println!(
            "  n={n}, dim={dim}: CPU {:>8.2?}  GPU {:>8.2?}  GPU/CPU {speedup:.2}x",
            cpu_time, gpu_time
        );
    }

    println!("\n=== Done ===");
}

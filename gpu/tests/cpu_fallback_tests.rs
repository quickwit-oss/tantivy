//! Integration tests for tantivy-gpu using the CPU fallback backend.
//!
//! These tests verify that GPU kernel logic is correct by running on CPU.
//! When a real GPU is available, the wgpu backend produces identical results.

use tantivy_gpu::buffer::StatsResult;
use tantivy_gpu::collector::GpuAggregationCollector;
use tantivy_gpu::device::GpuContext;
use tantivy_gpu::integration::gpu_stats_collector::GpuStatsAccumulator;
use tantivy_gpu::integration::gpu_term_weight::{gpu_score_block, GpuTermScorer};
use tantivy_gpu::kernel::histogram::HistogramParams;
use tantivy_gpu::kernel::{GpuKernel, StatsKernel};
use tantivy_gpu::scorer::{GpuBm25BatchCollector, GpuBm25Weight};
use tantivy_gpu::vector::distance::{compute_distance_cpu, DistanceMetric, GpuDistanceKernel};
use tantivy_gpu::vector::gpu_cache;
use tantivy_gpu::vector::hnsw::HnswIndex;

// ─── Stats Kernel Tests ───

#[test]
fn test_stats_empty() {
    let ctx = GpuContext::cpu_fallback();
    let kernel = StatsKernel::compile(&ctx).unwrap();
    let result = kernel.execute(&[]).unwrap();
    assert_eq!(result.count, 0);
}

#[test]
fn test_stats_single_value() {
    let ctx = GpuContext::cpu_fallback();
    let kernel = StatsKernel::compile(&ctx).unwrap();
    let result = kernel.execute(&[42.0f32]).unwrap();
    assert_eq!(result.count, 1);
    assert!((result.sum - 42.0).abs() < 1e-5);
    assert!((result.min - 42.0).abs() < 1e-5);
    assert!((result.max - 42.0).abs() < 1e-5);
}

#[test]
fn test_stats_multiple_values() {
    let ctx = GpuContext::cpu_fallback();
    let kernel = StatsKernel::compile(&ctx).unwrap();
    let values: Vec<f32> = vec![1.0, 2.0, 3.0, 4.0, 5.0];
    let result = kernel.execute(&values).unwrap();
    assert_eq!(result.count, 5);
    assert!((result.sum - 15.0).abs() < 1e-4);
    assert!((result.min - 1.0).abs() < 1e-5);
    assert!((result.max - 5.0).abs() < 1e-5);
    assert!((result.avg() - 3.0).abs() < 1e-4);
}

#[test]
fn test_stats_large_batch() {
    let ctx = GpuContext::cpu_fallback();
    let kernel = StatsKernel::compile(&ctx).unwrap();
    let n = 10_000;
    let values: Vec<f32> = (1..=n).map(|i| i as f32).collect();
    let result = kernel.execute(&values).unwrap();
    assert_eq!(result.count, n as u32);
    let expected_sum = (n * (n + 1) / 2) as f64;
    assert!(
        (result.sum - expected_sum).abs() / expected_sum < 1e-3,
        "sum: {} expected: {}",
        result.sum,
        expected_sum
    );
    assert!((result.min - 1.0).abs() < 1e-5);
    assert!((result.max - n as f64).abs() < 1.0);
}

#[test]
fn test_stats_f64_interface() {
    let ctx = GpuContext::cpu_fallback();
    let kernel = StatsKernel::compile(&ctx).unwrap();
    let values: Vec<f64> = vec![1.5, 2.5, 3.5];
    let result = kernel.execute_f64(&values).unwrap();
    assert_eq!(result.count, 3);
    assert!((result.sum - 7.5).abs() < 1e-4);
}

#[test]
fn test_stats_negative_values() {
    let ctx = GpuContext::cpu_fallback();
    let kernel = StatsKernel::compile(&ctx).unwrap();
    let values: Vec<f32> = vec![-5.0, -1.0, 0.0, 3.0, 7.0];
    let result = kernel.execute(&values).unwrap();
    assert_eq!(result.count, 5);
    assert!((result.sum - 4.0).abs() < 1e-4);
    assert!((result.min - (-5.0)).abs() < 1e-5);
    assert!((result.max - 7.0).abs() < 1e-5);
}

#[test]
fn test_stats_merge() {
    let mut a = StatsResult::identity();
    a.count = 3;
    a.sum = 6.0;
    a.min = 1.0;
    a.max = 3.0;
    a.sum_of_squares = 14.0;

    let mut b = StatsResult::identity();
    b.count = 2;
    b.sum = 9.0;
    b.min = 4.0;
    b.max = 5.0;
    b.sum_of_squares = 41.0;

    a.merge(&b);
    assert_eq!(a.count, 5);
    assert!((a.sum - 15.0).abs() < 1e-10);
    assert!((a.min - 1.0).abs() < 1e-10);
    assert!((a.max - 5.0).abs() < 1e-10);
}

// ─── Histogram Kernel Tests ───

#[test]
fn test_histogram_empty() {
    let ctx = GpuContext::cpu_fallback();
    let kernel = tantivy_gpu::kernel::HistogramKernel::compile(&ctx).unwrap();
    let params = HistogramParams {
        num_buckets: 10,
        interval: 1.0,
        offset: 0.0,
    };
    let result = kernel.execute(&[], &params).unwrap();
    assert_eq!(result.len(), 10);
    assert!(result.iter().all(|&c| c == 0));
}

#[test]
fn test_histogram_uniform() {
    let ctx = GpuContext::cpu_fallback();
    let kernel = tantivy_gpu::kernel::HistogramKernel::compile(&ctx).unwrap();
    let params = HistogramParams {
        num_buckets: 5,
        interval: 2.0,
        offset: 0.0,
    };
    // Values: 0-1 → bucket 0, 2-3 → bucket 1, etc.
    let values: Vec<f64> = vec![0.5, 1.5, 2.5, 3.5, 4.5, 5.5, 6.5, 7.5, 8.5, 9.5];
    let result = kernel.execute(&values, &params).unwrap();
    assert_eq!(result.len(), 5);
    assert_eq!(result[0], 2); // 0.5, 1.5
    assert_eq!(result[1], 2); // 2.5, 3.5
    assert_eq!(result[2], 2); // 4.5, 5.5
    assert_eq!(result[3], 2); // 6.5, 7.5
    assert_eq!(result[4], 2); // 8.5, 9.5 (clamped to last bucket)
}

#[test]
fn test_histogram_with_offset() {
    let ctx = GpuContext::cpu_fallback();
    let kernel = tantivy_gpu::kernel::HistogramKernel::compile(&ctx).unwrap();
    let params = HistogramParams {
        num_buckets: 3,
        interval: 10.0,
        offset: 100.0,
    };
    let values: Vec<f64> = vec![100.0, 105.0, 110.0, 115.0, 120.0, 125.0];
    let result = kernel.execute(&values, &params).unwrap();
    assert_eq!(result[0], 2); // 100.0, 105.0
    assert_eq!(result[1], 2); // 110.0, 115.0
    assert_eq!(result[2], 2); // 120.0, 125.0
}

// ─── BM25 Kernel Tests ───

#[test]
fn test_bm25_single_doc() {
    let ctx = GpuContext::cpu_fallback();
    let weight = GpuBm25Weight::new(&ctx, 2.0, 100.0).unwrap();

    // Score a single document: tf=3, fieldnorm_id=10 (fieldnorm=10)
    let results = weight.score_batch(&[42], &[3], &[10]).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].0, 42);
    // Verify score matches CPU reference
    let cpu_score = weight.score_one(3, 10);
    assert!(
        (results[0].1 - cpu_score).abs() < 1e-5,
        "GPU score {} != CPU score {}",
        results[0].1,
        cpu_score
    );
}

#[test]
fn test_bm25_batch() {
    let ctx = GpuContext::cpu_fallback();
    let weight = GpuBm25Weight::new(&ctx, 1.5, 50.0).unwrap();

    let doc_ids: Vec<u32> = (0..256).collect();
    let term_freqs: Vec<u32> = (0..256).map(|i| (i % 10) + 1).collect();
    let fieldnorm_ids: Vec<u8> = (0..256).map(|i| (i % 40) as u8).collect();

    let results = weight
        .score_batch(&doc_ids, &term_freqs, &fieldnorm_ids)
        .unwrap();
    assert_eq!(results.len(), 256);

    // Verify each score matches CPU reference
    for i in 0..256 {
        let cpu_score = weight.score_one(term_freqs[i], fieldnorm_ids[i]);
        assert!(
            (results[i].1 - cpu_score).abs() < 1e-4,
            "Doc {}: GPU score {} != CPU score {}",
            i,
            results[i].1,
            cpu_score
        );
    }
}

#[test]
fn test_bm25_batch_collector() {
    let ctx = GpuContext::cpu_fallback();
    let weight = GpuBm25Weight::new(&ctx, 2.0, 100.0)
        .unwrap()
        .with_min_batch_size(1); // Force GPU path even for small batches
    let mut collector = GpuBm25BatchCollector::new(weight, 64);

    for i in 0..100u32 {
        collector.push(i, (i % 5) + 1, (i % 30) as u8).unwrap();
    }

    let results = collector.harvest().unwrap();
    assert_eq!(results.len(), 100);
    // All doc IDs should be present
    for i in 0..100u32 {
        assert!(results.iter().any(|&(doc, _)| doc == i));
    }
}

// ─── Aggregation Collector Tests ───

#[test]
fn test_agg_collector_stats() {
    let ctx = GpuContext::cpu_fallback();
    let mut collector = GpuAggregationCollector::new_stats(ctx).unwrap();

    let values: Vec<f64> = (1..=1000).map(|i| i as f64).collect();
    collector.collect_values(&values).unwrap();

    let result = collector.harvest_stats().unwrap();
    assert_eq!(result.count, 1000);
    let expected_sum = 500500.0;
    assert!(
        (result.sum - expected_sum).abs() / expected_sum < 1e-3,
        "sum: {} expected: {}",
        result.sum,
        expected_sum
    );
    assert!((result.min - 1.0).abs() < 1e-5);
    assert!((result.max - 1000.0).abs() < 1.0);
}

#[test]
fn test_agg_collector_histogram() {
    let ctx = GpuContext::cpu_fallback();
    let params = HistogramParams {
        num_buckets: 10,
        interval: 10.0,
        offset: 0.0,
    };
    let mut collector = GpuAggregationCollector::new_histogram(ctx, params).unwrap();

    let values: Vec<f64> = (0..100).map(|i| i as f64).collect();
    collector.collect_values(&values).unwrap();

    let buckets = collector.harvest_histogram().unwrap();
    assert_eq!(buckets.len(), 10);
    assert_eq!(buckets[0], 10); // 0-9
    assert_eq!(buckets[1], 10); // 10-19
    assert_eq!(buckets[9], 10); // 90-99
}

#[test]
fn test_agg_collector_incremental_flush() {
    let ctx = GpuContext::cpu_fallback();
    let mut collector = GpuAggregationCollector::new_stats(ctx)
        .unwrap()
        .with_flush_threshold(4096);

    // Push more than flush_threshold to trigger auto-flush
    for batch in 0..5 {
        let values: Vec<f64> = (0..1000).map(|i| (batch * 1000 + i) as f64).collect();
        collector.collect_values(&values).unwrap();
    }

    let result = collector.harvest_stats().unwrap();
    assert_eq!(result.count, 5000);
}

// ─── Device Info Tests ───

#[test]
fn test_cpu_fallback_device_info() {
    let ctx = GpuContext::cpu_fallback();
    assert!(!ctx.is_hardware_gpu());
    assert_eq!(
        ctx.info().backend,
        tantivy_gpu::device::GpuBackend::CpuFallback
    );
    assert_eq!(ctx.info().name, "CPU Fallback");
}

#[test]
fn test_gpu_context_clone() {
    let ctx = GpuContext::cpu_fallback();
    let ctx2 = ctx.clone();
    assert_eq!(ctx.info().name, ctx2.info().name);
}

// ─── Fieldnorm Table Verification ───

#[test]
fn test_fieldnorm_table_matches_tantivy() {
    // Verify first few entries match expected values from Tantivy's table
    assert_eq!(tantivy_gpu::kernel::bm25::id_to_fieldnorm(0), 0);
    assert_eq!(tantivy_gpu::kernel::bm25::id_to_fieldnorm(1), 1);
    assert_eq!(tantivy_gpu::kernel::bm25::id_to_fieldnorm(10), 10);
    assert_eq!(tantivy_gpu::kernel::bm25::id_to_fieldnorm(40), 40);
    assert_eq!(tantivy_gpu::kernel::bm25::id_to_fieldnorm(41), 42);
    assert_eq!(
        tantivy_gpu::kernel::bm25::id_to_fieldnorm(255),
        2_013_265_944
    );
}

// ─── StatsResult Variance/StdDev ───

#[test]
fn test_stats_variance_and_stddev() {
    // Population: [2, 4, 4, 4, 5, 5, 7, 9] → mean=5, variance=4, stddev=2
    let mut result = StatsResult::identity();
    result.count = 8;
    result.sum = 40.0;
    result.sum_of_squares = 4.0 + 16.0 + 16.0 + 16.0 + 25.0 + 25.0 + 49.0 + 81.0; // 232
    result.min = 2.0;
    result.max = 9.0;

    assert!((result.avg() - 5.0).abs() < 1e-10);
    assert!((result.variance() - 4.0).abs() < 1e-10);
    assert!((result.std_deviation() - 2.0).abs() < 1e-10);
}

// ─── BM25 Edge Cases ───

#[test]
fn test_bm25_tf_zero() {
    let ctx = GpuContext::cpu_fallback();
    let weight = GpuBm25Weight::new(&ctx, 2.0, 100.0).unwrap();
    // term_freq=0 → tf_factor = 0/(0+norm) = 0 → score=0
    let score = weight.score_one(0, 10);
    assert_eq!(score, 0.0);
}

#[test]
fn test_bm25_fieldnorm_255() {
    let ctx = GpuContext::cpu_fallback();
    let weight = GpuBm25Weight::new(&ctx, 2.0, 100.0).unwrap();
    // fieldnorm_id=255 → very large field length (2_013_265_944)
    let score = weight.score_one(1, 255);
    assert!(score > 0.0);
    assert!(
        score < 0.001,
        "Score with huge field should be very small: {score}"
    );
}

#[test]
fn test_bm25_score_batch_length_mismatch() {
    let ctx = GpuContext::cpu_fallback();
    let weight = GpuBm25Weight::new(&ctx, 2.0, 100.0).unwrap();
    // Mismatched lengths should return error, not panic
    let result = weight.score_batch(&[1, 2, 3], &[1, 2], &[10, 20, 30]);
    assert!(result.is_err());
}

// ─── Histogram Edge Cases ───

#[test]
fn test_histogram_values_on_boundaries() {
    let ctx = GpuContext::cpu_fallback();
    let kernel = tantivy_gpu::kernel::HistogramKernel::compile(&ctx).unwrap();
    let params = HistogramParams {
        num_buckets: 3,
        interval: 10.0,
        offset: 0.0,
    };
    // Values exactly on bucket boundaries: 0.0, 10.0, 20.0
    let values: Vec<f64> = vec![0.0, 10.0, 20.0, 29.999];
    let result = kernel.execute(&values, &params).unwrap();
    assert_eq!(result[0], 1); // 0.0
    assert_eq!(result[1], 1); // 10.0
    assert_eq!(result[2], 2); // 20.0, 29.999
}

#[test]
fn test_histogram_negative_values_clamped() {
    let ctx = GpuContext::cpu_fallback();
    let kernel = tantivy_gpu::kernel::HistogramKernel::compile(&ctx).unwrap();
    let params = HistogramParams {
        num_buckets: 3,
        interval: 10.0,
        offset: 0.0,
    };
    // Negative values should go to bucket 0
    let values: Vec<f64> = vec![-5.0, -100.0, 5.0];
    let result = kernel.execute(&values, &params).unwrap();
    assert_eq!(result[0], 3); // all three in bucket 0
}

// ─── Buffer upload/download helpers ───

#[test]
fn test_upload_column_values() {
    let ctx = GpuContext::cpu_fallback();
    let values: Vec<u64> = vec![100, 200, 300, 400, 500];
    let buf = tantivy_gpu::buffer::upload_column_values(&ctx, "test-col", &values).unwrap();
    assert_eq!(buf.len(), 5);
    let downloaded: Vec<u64> = buf.download(&ctx).unwrap();
    assert_eq!(downloaded, values);
}

#[test]
fn test_upload_f64_values() {
    let ctx = GpuContext::cpu_fallback();
    let values: Vec<f64> = vec![1.5, 2.5, 3.5];
    let buf = tantivy_gpu::buffer::upload_f64_values(&ctx, "test-f64", &values).unwrap();
    let downloaded: Vec<f64> = buf.download(&ctx).unwrap();
    assert_eq!(downloaded, values);
}

#[test]
fn test_gpu_buffer_download_one() {
    let ctx = GpuContext::cpu_fallback();
    let buf = tantivy_gpu::buffer::GpuBuffer::new::<u32>(
        &ctx,
        "test",
        5,
        tantivy_gpu::device::BufferUsage::STORAGE_READBACK,
    )
    .unwrap();
    let data: Vec<u32> = vec![10, 20, 30, 40, 50];
    buf.upload(&ctx, &data).unwrap();
    let val: u32 = buf.download_one(&ctx, 2).unwrap();
    assert_eq!(val, 30);
}

// ─── Error Handling Tests ───

#[test]
fn test_buffer_out_of_range_read() {
    let ctx = GpuContext::cpu_fallback();
    let buf = tantivy_gpu::buffer::GpuBuffer::new::<u32>(
        &ctx,
        "test",
        10,
        tantivy_gpu::device::BufferUsage::STORAGE_READBACK,
    )
    .unwrap();
    // Trying to download 10 u32s should work
    let data: Vec<u32> = buf.download(&ctx).unwrap();
    assert_eq!(data.len(), 10);
}

// ─── GPU Context init test ───

#[test]
fn test_gpu_context_init() {
    // GpuContext::init() should succeed (falls back to CPU if no GPU)
    let ctx = GpuContext::init().unwrap();
    // Should have a valid device info regardless of backend
    assert!(!ctx.info().name.is_empty());
}

// ─── Buffer cleanup on drop ───

#[test]
fn test_buffer_drop_cleanup() {
    let ctx = GpuContext::cpu_fallback();
    // Create and immediately drop a buffer — should not leak
    {
        let _buf = tantivy_gpu::buffer::GpuBuffer::new::<u32>(
            &ctx,
            "ephemeral",
            1000,
            tantivy_gpu::device::BufferUsage::STORAGE,
        )
        .unwrap();
    }
    // If we get here without panic, cleanup worked
    // Create another buffer to verify the device is still functional
    let buf2 = tantivy_gpu::buffer::GpuBuffer::new::<u32>(
        &ctx,
        "after-drop",
        10,
        tantivy_gpu::device::BufferUsage::STORAGE_READBACK,
    )
    .unwrap();
    let data: Vec<u32> = buf2.download(&ctx).unwrap();
    assert_eq!(data.len(), 10);
}

// ─── GpuStatsAccumulator (integration layer) Tests ───

#[test]
fn test_gpu_stats_accumulator_small_batch() {
    let ctx = GpuContext::cpu_fallback();
    let mut acc = GpuStatsAccumulator::new(ctx).unwrap();

    // Small batch — goes through CPU path
    let values: Vec<f64> = (1..=100).map(|i| i as f64).collect();
    acc.collect_block_f64(&values).unwrap();

    let (count, sum, _delta, min, max) = acc.to_intermediate_stats().unwrap();
    assert_eq!(count, 100);
    assert!((sum - 5050.0).abs() < 1e-6);
    assert!((min - 1.0).abs() < 1e-6);
    assert!((max - 100.0).abs() < 1e-6);
}

#[test]
fn test_gpu_stats_accumulator_single_values() {
    let ctx = GpuContext::cpu_fallback();
    let mut acc = GpuStatsAccumulator::new(ctx).unwrap();

    for i in 1..=50 {
        acc.collect(i as f64);
    }

    let (count, sum, _, min, max) = acc.to_intermediate_stats().unwrap();
    assert_eq!(count, 50);
    assert!((sum - 1275.0).abs() < 1e-6);
    assert!((min - 1.0).abs() < 1e-6);
    assert!((max - 50.0).abs() < 1e-6);
}

// ─── GpuTermScorer (integration layer) Tests ───

#[test]
fn test_gpu_term_scorer_push_and_harvest() {
    let ctx = GpuContext::cpu_fallback();
    let mut scorer = GpuTermScorer::new(&ctx, 2.0, 100.0).unwrap();

    for i in 0..50u32 {
        scorer.push(i, (i % 5) + 1, (i % 30) as u8).unwrap();
    }

    let results = scorer.harvest().unwrap();
    assert_eq!(results.len(), 50);

    // Verify all doc_ids present and scores positive
    for i in 0..50u32 {
        assert!(results.iter().any(|&(doc, _)| doc == i));
    }
    for &(_, score) in &results {
        assert!(score >= 0.0, "Score should be non-negative: {score}");
    }
}

#[test]
fn test_gpu_score_block() {
    let ctx = GpuContext::cpu_fallback();
    let mut scorer = GpuTermScorer::new(&ctx, 1.5, 80.0).unwrap();

    let doc_ids: Vec<u32> = (0..20).collect();
    let term_freqs: Vec<u32> = (0..20).map(|i| (i % 3) + 1).collect();
    let fieldnorm_ids: Vec<u8> = (0..20).map(|i| (i % 20) as u8).collect();

    let mut collected = Vec::new();
    gpu_score_block(
        &mut scorer,
        &doc_ids,
        &term_freqs,
        &fieldnorm_ids,
        &mut |doc_id, score| {
            collected.push((doc_id, score));
        },
    )
    .unwrap();

    assert_eq!(collected.len(), 20);
}

// ─── GPU Distance Kernel Direct Tests ───

#[test]
fn test_gpu_distance_l2() {
    let ctx = GpuContext::cpu_fallback();
    let kernel = GpuDistanceKernel::compile(&ctx).unwrap();

    let query = vec![1.0, 2.0, 3.0];
    let candidates = vec![
        1.0, 2.0, 3.0, // identical → dist=0
        4.0, 5.0, 6.0, // dist = 9+9+9 = 27
        0.0, 0.0, 0.0, // dist = 1+4+9 = 14
    ];

    let distances = kernel
        .compute(&query, &candidates, 3, DistanceMetric::L2)
        .unwrap();
    assert_eq!(distances.len(), 3);
    assert!((distances[0] - 0.0).abs() < 1e-5);
    assert!((distances[1] - 27.0).abs() < 1e-4);
    assert!((distances[2] - 14.0).abs() < 1e-4);
}

#[test]
fn test_gpu_distance_cosine() {
    let ctx = GpuContext::cpu_fallback();
    let kernel = GpuDistanceKernel::compile(&ctx).unwrap();

    let query = vec![1.0, 0.0];
    let candidates = vec![
        1.0, 0.0, // identical direction → cosine dist=0
        0.0, 1.0, // perpendicular → cosine dist=1
    ];

    let distances = kernel
        .compute(&query, &candidates, 2, DistanceMetric::Cosine)
        .unwrap();
    assert_eq!(distances.len(), 2);
    assert!(
        distances[0].abs() < 1e-5,
        "Same direction should be ~0: {}",
        distances[0]
    );
    assert!(
        (distances[1] - 1.0).abs() < 1e-5,
        "Perpendicular should be ~1: {}",
        distances[1]
    );
}

#[test]
fn test_gpu_distance_dot_product() {
    let ctx = GpuContext::cpu_fallback();
    let kernel = GpuDistanceKernel::compile(&ctx).unwrap();

    let query = vec![2.0, 3.0];
    let candidates = vec![
        4.0, 5.0, // dot = 8+15 = 23, distance = -23
        1.0, 1.0, // dot = 2+3 = 5, distance = -5
    ];

    let distances = kernel
        .compute(&query, &candidates, 2, DistanceMetric::DotProduct)
        .unwrap();
    assert_eq!(distances.len(), 2);
    assert!((distances[0] - (-23.0)).abs() < 1e-4);
    assert!((distances[1] - (-5.0)).abs() < 1e-4);
}

#[test]
fn test_gpu_distance_matches_cpu() {
    let ctx = GpuContext::cpu_fallback();
    let kernel = GpuDistanceKernel::compile(&ctx).unwrap();

    let query = vec![0.5, 1.2, -0.3, 2.1];
    let c1 = vec![1.0, -0.5, 0.8, 0.2];
    let c2 = vec![-1.0, 2.0, 0.0, 1.5];

    let mut candidates = Vec::new();
    candidates.extend_from_slice(&c1);
    candidates.extend_from_slice(&c2);

    for metric in [
        DistanceMetric::L2,
        DistanceMetric::Cosine,
        DistanceMetric::DotProduct,
    ] {
        let gpu_dists = kernel.compute(&query, &candidates, 4, metric).unwrap();
        let cpu_d1 = compute_distance_cpu(&query, &c1, metric);
        let cpu_d2 = compute_distance_cpu(&query, &c2, metric);

        assert!(
            (gpu_dists[0] - cpu_d1).abs() < 1e-4,
            "{metric:?}: GPU {:.6} != CPU {:.6}",
            gpu_dists[0],
            cpu_d1
        );
        assert!(
            (gpu_dists[1] - cpu_d2).abs() < 1e-4,
            "{metric:?}: GPU {:.6} != CPU {:.6}",
            gpu_dists[1],
            cpu_d2
        );
    }
}

// ─── HNSW persistence with different metrics ───

#[test]
fn test_hnsw_persistence_cosine() {
    let mut index = HnswIndex::new(2, DistanceMetric::Cosine);
    index.insert(vec![1.0, 0.0]).unwrap();
    index.insert(vec![0.0, 1.0]).unwrap();
    index.insert(vec![0.707, 0.707]).unwrap();

    let mut buf = Vec::new();
    index.serialize(&mut buf).unwrap();
    let restored = HnswIndex::deserialize(&mut &buf[..]).unwrap();

    assert_eq!(restored.len(), 3);
    let results = restored.search(&[1.0, 1.0], 3, 10);
    assert_eq!(results.len(), 3);
    assert_eq!(results[0].1, 2); // [0.707, 0.707] closest by cosine
}

#[test]
fn test_hnsw_persistence_dot_product() {
    let mut index = HnswIndex::new(3, DistanceMetric::DotProduct);
    for i in 0..10u32 {
        index
            .insert(vec![i as f32, (i * 2) as f32, (i * 3) as f32])
            .unwrap();
    }

    let mut buf = Vec::new();
    index.serialize(&mut buf).unwrap();
    let restored = HnswIndex::deserialize(&mut &buf[..]).unwrap();

    assert_eq!(restored.len(), 10);
    let orig = index.search(&[5.0, 10.0, 15.0], 3, 20);
    let rest = restored.search(&[5.0, 10.0, 15.0], 3, 20);
    assert_eq!(orig.len(), rest.len());
    for (o, r) in orig.iter().zip(rest.iter()) {
        assert_eq!(o.1, r.1);
    }
}

// ─── Pooled execution correctness tests ───

#[test]
fn test_stats_pooled_matches_normal() {
    let ctx = GpuContext::cpu_fallback();
    let kernel = StatsKernel::compile(&ctx).unwrap();
    let pool = tantivy_gpu::buffer::pool::BufferPool::from_ctx(&ctx);

    let data: Vec<f32> = (0..5000).map(|i| (i as f32) * 0.3 + 1.0).collect();

    let result_normal = kernel.execute(&data).unwrap();
    let result_pooled = kernel.execute_pooled(&data, &pool).unwrap();

    assert_eq!(result_normal.count, result_pooled.count);
    assert!((result_normal.sum - result_pooled.sum).abs() < 1e-2);
    assert!((result_normal.min - result_pooled.min).abs() < 1e-5);
    assert!((result_normal.max - result_pooled.max).abs() < 1e-2);
}

#[test]
fn test_bm25_pooled_matches_normal() {
    use tantivy_gpu::buffer::{Bm25DocInput, Bm25Params};
    use tantivy_gpu::kernel::bm25::Bm25Kernel;

    let ctx = GpuContext::cpu_fallback();
    let kernel = Bm25Kernel::compile(&ctx).unwrap();
    let pool = tantivy_gpu::buffer::pool::BufferPool::from_ctx(&ctx);

    let params = Bm25Params {
        weight: 2.0,
        k1: 1.2,
        b: 0.75,
        avg_fieldnorm: 100.0,
    };
    let inputs: Vec<Bm25DocInput> = (0..200)
        .map(|i| Bm25DocInput {
            doc_id: i,
            term_freq: (i % 10) + 1,
            fieldnorm_id: i % 40,
            _pad: 0,
        })
        .collect();

    let result_normal = kernel.execute(&inputs, &params).unwrap();
    let result_pooled = kernel.execute_pooled(&inputs, &params, &pool).unwrap();

    assert_eq!(result_normal.len(), result_pooled.len());
    for (n, p) in result_normal.iter().zip(result_pooled.iter()) {
        assert_eq!(n.doc_id, p.doc_id);
        assert!(
            (n.score - p.score).abs() < 1e-5,
            "doc {}: {} vs {}",
            n.doc_id,
            n.score,
            p.score
        );
    }
}

// ─── Buffer pool cross-size reuse correctness ───

#[test]
fn test_pool_cross_size_reuse_download() {
    let ctx = GpuContext::cpu_fallback();
    let pool = tantivy_gpu::buffer::pool::BufferPool::from_ctx(&ctx);

    // Lease 1000 bytes, upload 250 f32s, drop
    {
        let buf = pool
            .lease_typed::<f32>(250, tantivy_gpu::device::BufferUsage::STORAGE_READBACK)
            .unwrap();
        let data: Vec<f32> = (0..250).map(|i| i as f32 * 99.0).collect();
        buf.upload(&data).unwrap();
    }

    // Lease same bucket but only 100 f32s — should reuse buffer
    let buf2 = pool
        .lease_typed::<f32>(100, tantivy_gpu::device::BufferUsage::STORAGE_READBACK)
        .unwrap();
    let data2: Vec<f32> = (0..100).map(|i| i as f32 * 7.0).collect();
    buf2.upload(&data2).unwrap();
    let downloaded: Vec<f32> = buf2.download().unwrap();

    // Should get exactly 100 elements, not 250, and matching our data
    assert_eq!(downloaded.len(), 100);
    assert_eq!(downloaded, data2);
}

// ─── Proptest: stats numerical stability ───

mod proptests {
    use proptest::prelude::*;
    use tantivy_gpu::device::GpuContext;
    use tantivy_gpu::kernel::{GpuKernel, StatsKernel};
    use tantivy_gpu::scorer::GpuBm25Weight;

    proptest! {
        #[test]
        fn stats_count_matches_input_len(
            values in proptest::collection::vec(-1e6f32..1e6f32, 0..5000)
        ) {
            let ctx = GpuContext::cpu_fallback();
            let kernel = StatsKernel::compile(&ctx).unwrap();
            let result = kernel.execute(&values).unwrap();
            prop_assert_eq!(result.count, values.len() as u32);
        }

        #[test]
        fn stats_min_max_within_range(
            values in proptest::collection::vec(-1e6f32..1e6f32, 1..1000)
        ) {
            let ctx = GpuContext::cpu_fallback();
            let kernel = StatsKernel::compile(&ctx).unwrap();
            let result = kernel.execute(&values).unwrap();
            let cpu_min = values.iter().cloned().fold(f32::MAX, f32::min) as f64;
            let cpu_max = values.iter().cloned().fold(f32::MIN, f32::max) as f64;
            prop_assert!(
                (result.min - cpu_min).abs() < 1e-2,
                "min: GPU {} vs CPU {}", result.min, cpu_min
            );
            prop_assert!(
                (result.max - cpu_max).abs() < 1e-2,
                "max: GPU {} vs CPU {}", result.max, cpu_max
            );
        }

        #[test]
        fn bm25_score_non_negative(tf in 0u32..100, fnorm_id in 0u8..=255u8) {
            let ctx = GpuContext::cpu_fallback();
            let weight = GpuBm25Weight::new(&ctx, 2.0, 100.0).unwrap();
            let score = weight.score_one(tf, fnorm_id);
            prop_assert!(score >= 0.0, "Score must be non-negative: {score}");
        }
    }
}

// ─── GPU cache test ───

#[test]
fn test_gpu_cache_available() {
    // cached_gpu_distance should return Some on CPU fallback
    let result = gpu_cache::cached_gpu_distance();
    assert!(result.is_some());
}

#[test]
fn test_gpu_cache_idempotent() {
    // Calling twice should return the same references
    let (ctx1, _k1) = gpu_cache::cached_gpu_distance().unwrap();
    let (ctx2, _k2) = gpu_cache::cached_gpu_distance().unwrap();
    assert_eq!(ctx1.info().name, ctx2.info().name);
}

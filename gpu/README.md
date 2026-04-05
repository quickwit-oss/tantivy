# tantivy-gpu

GPU acceleration layer for the [Tantivy](https://github.com/quickwit-oss/tantivy) search engine.

## Features

- **Stats Aggregation** — GPU-accelerated min/max/sum/count with Kahan summation
- **Histogram Bucketing** — Parallel bucket assignment via compute shaders
- **BM25 Batch Scoring** — Block-level BM25 scoring offloaded to GPU
- **Vector Similarity Search** — HNSW index with GPU-accelerated distance computation (L2, Cosine, Dot Product)
- **Cross-platform** — wgpu backend (Vulkan/Metal/DX12/GL) + CPU fallback

## Usage

Add to your `Cargo.toml`:

```toml
[dependencies]
tantivy-gpu = { path = "./gpu" }
```

Or via Tantivy's `gpu` feature:

```toml
[dependencies]
tantivy = { path = ".", features = ["gpu"] }
```

### Vector Search Example

```rust
use tantivy_gpu::device::GpuContext;
use tantivy_gpu::vector::{DistanceMetric, HnswIndex, KnnQuery};

let ctx = GpuContext::init()?;
let mut index = HnswIndex::with_gpu(128, DistanceMetric::Cosine, &ctx)?;

// Insert vectors
for vec in vectors {
    index.insert(vec)?;
}

// Search
let results = index.search_gpu(&query_vec, 10, 100)?;
```

### BM25 Batch Scoring

```rust
use tantivy_gpu::device::GpuContext;
use tantivy_gpu::scorer::GpuBm25Weight;

let ctx = GpuContext::init()?;
let weight = GpuBm25Weight::new(&ctx, idf_weight, avg_fieldnorm)?;
let results = weight.score_batch(&doc_ids, &term_freqs, &fieldnorm_ids)?;
```

## Architecture

```
tantivy-gpu/
├── device/       — GPU device abstraction (wgpu + CPU fallback)
├── buffer/       — Typed GPU buffers with automatic cleanup
├── kernel/       — Compute kernels (Stats, Histogram, BM25, Distance)
├── shaders/      — WGSL compute shaders
├── collector/    — GPU aggregation collector
├── scorer/       — GPU BM25 scorer
├── integration/  — Tantivy trait adapters (Weight, SegmentCollector)
└── vector/       — HNSW index, kNN query, vector field options, persistence
```

## Feature Flags

| Flag | Default | Description |
|------|---------|-------------|
| `wgpu-backend` | ✅ | GPU compute via wgpu (Vulkan/Metal/DX12/GL) |
| `cpu-fallback` | | CPU-only mode for testing / no-GPU environments |

## Benchmarks

Run: `cargo bench -p tantivy-gpu --no-default-features --features cpu-fallback`

| Kernel | 1K | 10K | 100K | 1M |
|--------|-----|------|-------|------|
| Stats reduction | 11µs | 155µs | 739µs | 8.4ms |
| BM25 scoring (128-doc blocks) | 4.4µs | — | — | — |
| Histogram (100 buckets) | — | 62µs | 400µs | 5.3ms |

*Note: Above numbers are CPU fallback. Real GPU hardware provides 10-100x speedup for large batches.*

## License

MIT — same as Tantivy.

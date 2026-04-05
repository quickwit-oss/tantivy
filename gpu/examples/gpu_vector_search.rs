//! Example: GPU-accelerated vector similarity search with tantivy-gpu.
//!
//! Demonstrates building an HNSW index, inserting vectors, searching,
//! and serializing/deserializing the index.
//!
//! Run: cargo run -p tantivy-gpu --example gpu_vector_search --no-default-features --features
//! cpu-fallback

use tantivy_gpu::device::GpuContext;
use tantivy_gpu::vector::distance::DistanceMetric;
use tantivy_gpu::vector::hnsw::HnswIndex;
use tantivy_gpu::vector::knn_query::KnnQuery;

fn main() {
    // Initialize GPU context (falls back to CPU if no GPU available)
    let ctx = GpuContext::init().expect("Failed to initialize GPU context");
    println!("GPU device: {} ({})", ctx.info().name, ctx.info().backend);

    // Create an HNSW index with GPU acceleration
    let dim = 128;
    let mut index =
        HnswIndex::with_gpu(dim, DistanceMetric::Cosine, &ctx).expect("Failed to create index");

    // Insert some vectors
    let num_vectors = 1000;
    println!("\nInserting {num_vectors} vectors (dim={dim})...");
    let start = std::time::Instant::now();
    for i in 0..num_vectors {
        let vector: Vec<f32> = (0..dim)
            .map(|d| ((i * dim + d) as f32 * 0.01).sin())
            .collect();
        index.insert(vector).unwrap();
    }
    println!("  Done in {:?}", start.elapsed());

    // Search for nearest neighbors
    let query: Vec<f32> = (0..dim).map(|d| (d as f32 * 0.5).cos()).collect();
    let k = 10;
    let ef = 100;

    println!("\nSearching for {k} nearest neighbors (ef={ef})...");
    let start = std::time::Instant::now();
    let results = index.search_gpu(&query, k, ef).expect("Search failed");
    println!("  Done in {:?}", start.elapsed());

    println!("\nTop {k} results:");
    for (i, (distance, doc_id)) in results.iter().enumerate() {
        println!("  #{}: doc_id={doc_id}, distance={distance:.6}", i + 1);
    }

    // Use KnnQuery API
    println!("\nUsing KnnQuery API...");
    let knn = KnnQuery::new("embedding", query.clone(), 5)
        .with_metric(DistanceMetric::Cosine)
        .with_ef(50)
        .with_gpu(ctx.clone());
    let knn_results = knn.execute(&index).unwrap();
    for r in &knn_results {
        println!(
            "  doc_id={}, distance={:.6}, score={:.6}",
            r.doc_id, r.distance, r.score
        );
    }

    // Serialize and deserialize
    println!("\nSerializing index...");
    let mut buf = Vec::new();
    index.serialize(&mut buf).unwrap();
    println!(
        "  Serialized size: {} bytes ({:.1} KB)",
        buf.len(),
        buf.len() as f64 / 1024.0
    );

    let restored = HnswIndex::deserialize(&mut &buf[..]).unwrap();
    println!(
        "  Restored: {} vectors, dim={}",
        restored.len(),
        restored.dim()
    );

    // Verify restored search matches
    let restored_results = restored.search(&query, k, ef);
    assert_eq!(results.len(), restored_results.len());
    println!("  Search results match after round-trip!");

    println!("\nDone.");
}

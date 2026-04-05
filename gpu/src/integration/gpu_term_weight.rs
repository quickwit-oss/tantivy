//! GPU-accelerated BM25 scoring for term queries.
//!
//! Provides `GpuTermScorer` that batches 128-doc blocks from `BlockSegmentPostings`
//! and dispatches BM25 scoring to the GPU.
//!
//! ## Integration
//!
//! When `gpu` feature is enabled, `TermWeight::for_each()` can use this scorer
//! instead of the per-document `TermScorer::score()` path:
//!
//! ```ignore
//! // Instead of: for_each_scorer(&mut term_scorer, callback)
//! // Use:        gpu_for_each_scorer(&mut term_scorer, &gpu_weight, callback)
//! ```

use crate::buffer::{Bm25DocInput, Bm25Params};
use crate::device::GpuContext;
use crate::error::GpuResult;
use crate::kernel::bm25::Bm25Kernel;
use crate::kernel::GpuKernel;

/// GPU-accelerated BM25 batch scorer for term queries.
///
/// Collects blocks of (doc_id, term_freq, fieldnorm_id) from the postings
/// iterator and dispatches them to the GPU for parallel scoring.
pub struct GpuTermScorer {
    kernel: Bm25Kernel,
    params: Bm25Params,
    /// Buffered inputs for GPU batch dispatch
    input_buffer: Vec<Bm25DocInput>,
    /// Batch size for GPU dispatch (matches COMPRESSION_BLOCK_SIZE = 128)
    batch_size: usize,
    /// Accumulated (doc_id, score) results
    results: Vec<(u32, f32)>,
}

/// Default batch size — matches Tantivy's postings block size.
const DEFAULT_BATCH_SIZE: usize = 512;

/// Minimum batch for GPU to be worthwhile.
const MIN_GPU_BATCH: usize = 128;

impl GpuTermScorer {
    /// Create a new GPU term scorer.
    ///
    /// # Arguments
    /// - `ctx`: GPU context
    /// - `idf_weight`: Pre-computed `IDF * (1 + K1)` from `Bm25Weight::weight`
    /// - `avg_fieldnorm`: Average field length
    pub fn new(ctx: &GpuContext, idf_weight: f32, avg_fieldnorm: f32) -> GpuResult<Self> {
        let kernel = Bm25Kernel::compile(ctx)?;
        Ok(Self {
            kernel,
            params: Bm25Params {
                weight: idf_weight,
                k1: 1.2,
                b: 0.75,
                avg_fieldnorm,
            },
            input_buffer: Vec::with_capacity(DEFAULT_BATCH_SIZE),
            batch_size: DEFAULT_BATCH_SIZE,
            results: Vec::new(),
        })
    }

    /// Push a document for batch scoring.
    ///
    /// Called once per document during postings iteration.
    /// When the buffer reaches `batch_size`, automatically dispatches to GPU.
    #[inline]
    pub fn push(&mut self, doc_id: u32, term_freq: u32, fieldnorm_id: u8) -> GpuResult<()> {
        self.input_buffer.push(Bm25DocInput {
            doc_id,
            term_freq,
            fieldnorm_id: fieldnorm_id as u32,
            _pad: 0,
        });

        if self.input_buffer.len() >= self.batch_size {
            self.flush()?;
        }
        Ok(())
    }

    /// Flush buffered documents to GPU for scoring.
    pub fn flush(&mut self) -> GpuResult<()> {
        if self.input_buffer.is_empty() {
            return Ok(());
        }

        let inputs = std::mem::take(&mut self.input_buffer);

        if inputs.len() >= MIN_GPU_BATCH {
            // GPU path
            let outputs = self.kernel.execute(&inputs, &self.params)?;
            self.results
                .extend(outputs.iter().map(|o| (o.doc_id, o.score)));
        } else {
            // CPU fallback for small batches
            for input in &inputs {
                let score = self.score_one_cpu(input.term_freq, input.fieldnorm_id as u8);
                self.results.push((input.doc_id, score));
            }
        }

        self.input_buffer = Vec::with_capacity(self.batch_size);
        Ok(())
    }

    /// Score a single document on CPU (BM25 formula).
    #[inline]
    fn score_one_cpu(&self, term_freq: u32, fieldnorm_id: u8) -> f32 {
        let dl = crate::kernel::bm25::id_to_fieldnorm(fieldnorm_id) as f32;
        let tf = term_freq as f32;
        let norm =
            self.params.k1 * (1.0 - self.params.b + self.params.b * dl / self.params.avg_fieldnorm);
        let tf_factor = tf / (tf + norm);
        self.params.weight * tf_factor
    }

    /// Flush remaining docs and return all scored results.
    ///
    /// Results are returned in the order documents were pushed.
    pub fn harvest(mut self) -> GpuResult<Vec<(u32, f32)>> {
        self.flush()?;
        Ok(self.results)
    }

    /// Flush remaining docs and call a callback for each (doc_id, score).
    ///
    /// This is the primary integration method for `Weight::for_each()`.
    pub fn flush_and_callback(&mut self, callback: &mut dyn FnMut(u32, f32)) -> GpuResult<()> {
        self.flush()?;
        for &(doc_id, score) in &self.results {
            callback(doc_id, score);
        }
        self.results.clear();
        Ok(())
    }
}

/// Iterate through a TermScorer's postings, batch-scoring on GPU.
///
/// This function replaces `for_each_scorer()` in the GPU path.
/// It reads blocks of (doc_id, term_freq, fieldnorm_id) from the postings
/// and dispatches them to the GPU in batches.
///
/// # Arguments
/// - `gpu_scorer`: The GPU term scorer with compiled kernel
/// - `doc_ids`: Pre-collected document IDs from the postings iterator
/// - `term_freqs`: Term frequencies (one per doc)
/// - `fieldnorm_ids`: Field norm IDs (one per doc, u8)
/// - `callback`: Called with (doc_id, score) for each scored document
pub fn gpu_score_block(
    gpu_scorer: &mut GpuTermScorer,
    doc_ids: &[u32],
    term_freqs: &[u32],
    fieldnorm_ids: &[u8],
    callback: &mut dyn FnMut(u32, f32),
) -> GpuResult<()> {
    for i in 0..doc_ids.len() {
        gpu_scorer.push(doc_ids[i], term_freqs[i], fieldnorm_ids[i])?;
    }
    gpu_scorer.flush_and_callback(callback)
}

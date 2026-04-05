//! GPU-accelerated BM25 scoring.
//!
//! Provides `GpuBm25Weight` which wraps Tantivy's `Bm25Weight` and offloads
//! batch scoring to the GPU when block sizes are large enough.

use crate::buffer::{Bm25DocInput, Bm25Params};
use crate::device::GpuContext;
use crate::error::GpuResult;
use crate::kernel::{Bm25Kernel, GpuKernel};

/// GPU-accelerated BM25 weight.
///
/// Wraps the BM25 parameters and a compiled GPU kernel.
/// Used as a drop-in replacement for Tantivy's `Bm25Weight` in the
/// scoring pipeline when GPU acceleration is enabled.
///
/// ## Integration Point
///
/// In the Tantivy `Weight::for_each` path, instead of per-document
/// `score()` calls, the GPU weight collects a block of (doc_id, tf, fieldnorm)
/// tuples and dispatches them to the GPU in one batch.
pub struct GpuBm25Weight {
    params: Bm25Params,
    kernel: Bm25Kernel,
    /// Minimum batch size for GPU dispatch. Below this, CPU scoring is used.
    min_batch_size: usize,
}

/// Default minimum batch size for GPU BM25 scoring.
/// 128 matches Tantivy's COMPRESSION_BLOCK_SIZE.
const DEFAULT_MIN_BATCH: usize = 128;

impl GpuBm25Weight {
    /// Create a new GPU BM25 weight.
    ///
    /// # Arguments
    /// - `ctx`: GPU context
    /// - `idf_weight`: Pre-computed `IDF * (1 + K1)` from Tantivy's `Bm25Weight`
    /// - `avg_fieldnorm`: Average field length across all documents
    pub fn new(ctx: &GpuContext, idf_weight: f32, avg_fieldnorm: f32) -> GpuResult<Self> {
        let kernel = Bm25Kernel::compile(ctx)?;
        Ok(Self {
            params: Bm25Params {
                weight: idf_weight,
                k1: 1.2,
                b: 0.75,
                avg_fieldnorm,
            },
            kernel,
            min_batch_size: DEFAULT_MIN_BATCH,
        })
    }

    /// Set minimum batch size for GPU dispatch.
    pub fn with_min_batch_size(mut self, size: usize) -> Self {
        self.min_batch_size = size;
        self
    }

    /// Score a batch of documents on the GPU.
    ///
    /// # Arguments
    /// - `doc_ids`: Document IDs
    /// - `term_freqs`: Term frequency for each document
    /// - `fieldnorm_ids`: Field norm IDs (0-255) for each document
    ///
    /// # Returns
    /// Vec of (doc_id, score) pairs, matching the input order.
    pub fn score_batch(
        &self,
        doc_ids: &[u32],
        term_freqs: &[u32],
        fieldnorm_ids: &[u8],
    ) -> GpuResult<Vec<(u32, f32)>> {
        let n = doc_ids.len();
        if n != term_freqs.len() || n != fieldnorm_ids.len() {
            return Err(crate::error::GpuError::Dispatch(format!(
                "BM25 score_batch: input length mismatch: doc_ids={}, term_freqs={}, \
                 fieldnorm_ids={}",
                n,
                term_freqs.len(),
                fieldnorm_ids.len()
            )));
        }

        if n < self.min_batch_size {
            // CPU path for small batches
            return Ok(self.score_batch_cpu(doc_ids, term_freqs, fieldnorm_ids));
        }

        // Pack inputs for GPU
        let inputs: Vec<Bm25DocInput> = (0..n)
            .map(|i| Bm25DocInput {
                doc_id: doc_ids[i],
                term_freq: term_freqs[i],
                fieldnorm_id: fieldnorm_ids[i] as u32,
                _pad: 0,
            })
            .collect();

        let outputs = self.kernel.execute(&inputs, &self.params)?;

        Ok(outputs.iter().map(|o| (o.doc_id, o.score)).collect())
    }

    /// CPU fallback scoring for small batches.
    fn score_batch_cpu(
        &self,
        doc_ids: &[u32],
        term_freqs: &[u32],
        fieldnorm_ids: &[u8],
    ) -> Vec<(u32, f32)> {
        doc_ids
            .iter()
            .zip(term_freqs.iter())
            .zip(fieldnorm_ids.iter())
            .map(|((&doc_id, &tf), &fnorm_id)| {
                let score = self.score_one(tf, fnorm_id);
                (doc_id, score)
            })
            .collect()
    }

    /// Score a single document (CPU, matching BM25 formula exactly).
    #[inline]
    pub fn score_one(&self, term_freq: u32, fieldnorm_id: u8) -> f32 {
        let dl = id_to_fieldnorm(fieldnorm_id) as f32;
        let tf = term_freq as f32;
        let norm =
            self.params.k1 * (1.0 - self.params.b + self.params.b * dl / self.params.avg_fieldnorm);
        let tf_factor = tf / (tf + norm);
        self.params.weight * tf_factor
    }

    /// Get the BM25 parameters.
    pub fn params(&self) -> &Bm25Params {
        &self.params
    }
}

/// Look up field norm using the same table as the GPU kernel.
fn id_to_fieldnorm(id: u8) -> u32 {
    crate::kernel::bm25::id_to_fieldnorm(id)
}

/// Adapter for collecting BM25 inputs during Tantivy's scoring loop
/// and batch-dispatching to GPU.
pub struct GpuBm25BatchCollector {
    weight: GpuBm25Weight,
    doc_ids: Vec<u32>,
    term_freqs: Vec<u32>,
    fieldnorm_ids: Vec<u8>,
    results: Vec<(u32, f32)>,
    batch_size: usize,
}

impl GpuBm25BatchCollector {
    /// Create a new batch collector.
    pub fn new(weight: GpuBm25Weight, batch_size: usize) -> Self {
        Self {
            weight,
            doc_ids: Vec::with_capacity(batch_size),
            term_freqs: Vec::with_capacity(batch_size),
            fieldnorm_ids: Vec::with_capacity(batch_size),
            results: Vec::new(),
            batch_size,
        }
    }

    /// Push a single document for scoring.
    #[inline]
    pub fn push(&mut self, doc_id: u32, term_freq: u32, fieldnorm_id: u8) -> GpuResult<()> {
        self.doc_ids.push(doc_id);
        self.term_freqs.push(term_freq);
        self.fieldnorm_ids.push(fieldnorm_id);

        if self.doc_ids.len() >= self.batch_size {
            self.flush()?;
        }
        Ok(())
    }

    /// Flush accumulated documents to GPU for scoring.
    pub fn flush(&mut self) -> GpuResult<()> {
        if self.doc_ids.is_empty() {
            return Ok(());
        }

        let scored =
            self.weight
                .score_batch(&self.doc_ids, &self.term_freqs, &self.fieldnorm_ids)?;
        self.results.extend(scored);

        self.doc_ids.clear();
        self.term_freqs.clear();
        self.fieldnorm_ids.clear();
        Ok(())
    }

    /// Flush and return all scored documents.
    pub fn harvest(mut self) -> GpuResult<Vec<(u32, f32)>> {
        self.flush()?;
        Ok(self.results)
    }
}

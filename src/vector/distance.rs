//! Distance kernels for the flat vector index.
//!
//! Each kernel uses a chunked-scalar accumulator pattern: an array of
//! `LANES` independent f32 accumulators with no loop-carried dependency.
//! LLVM's autovectorizer turns this into AVX2 / AVX-512 / NEON FMA loops
//! on the platforms that support them, without any explicit `std::arch`
//! intrinsics.
//!
//! `LANES = 16` matches the f32 width of an AVX-512 register
//! (`512 bits / sizeof::<f32>() == 16`), and is a multiple of the AVX2
//! (8) and NEON (4) widths.
//!
//! Kernels are generic over [`VectorElement`]. For `f32` the arithmetic
//! methods compile to plain `fsub` / `fmul` / `fadd`; quantized dtypes
//! plug in their own decode + arithmetic via the trait.

use crate::schema::Metric;
use crate::vector::VectorElement;

/// 16 = 512 (avx512 register width) / 32 (sizeof::<f32>() in bits).
const LANES: usize = 16;

/// Squared Euclidean distance.
#[inline]
pub fn l2_squared<T: VectorElement>(a: &[T], b: &[T]) -> f32 {
    debug_assert_eq!(a.len(), b.len());
    let a_chunks = a.chunks_exact(LANES);
    let b_chunks = b.chunks_exact(LANES);
    let a_tail = a_chunks.remainder();
    let b_tail = b_chunks.remainder();

    let mut sums = [0f32; LANES];
    for (ac, bc) in a_chunks.zip(b_chunks) {
        for i in 0..LANES {
            sums[i] += T::squared_diff(ac[i], bc[i]);
        }
    }
    let mut acc: f32 = sums.iter().sum();
    for (&x, &y) in a_tail.iter().zip(b_tail.iter()) {
        acc += T::squared_diff(x, y);
    }
    acc
}

/// Dot product.
#[inline]
pub fn dot<T: VectorElement>(a: &[T], b: &[T]) -> f32 {
    debug_assert_eq!(a.len(), b.len());
    let a_chunks = a.chunks_exact(LANES);
    let b_chunks = b.chunks_exact(LANES);
    let a_tail = a_chunks.remainder();
    let b_tail = b_chunks.remainder();

    let mut sums = [0f32; LANES];
    for (ac, bc) in a_chunks.zip(b_chunks) {
        for i in 0..LANES {
            sums[i] += T::product(ac[i], bc[i]);
        }
    }
    let mut acc: f32 = sums.iter().sum();
    for (&x, &y) in a_tail.iter().zip(b_tail.iter()) {
        acc += T::product(x, y);
    }
    acc
}

/// Sum of squares (squared L2 norm).
#[inline]
pub fn norm_squared<T: VectorElement>(a: &[T]) -> f32 {
    let chunks = a.chunks_exact(LANES);
    let tail = chunks.remainder();

    let mut sums = [0f32; LANES];
    for c in chunks {
        for i in 0..LANES {
            sums[i] += T::product(c[i], c[i]);
        }
    }
    let mut acc: f32 = sums.iter().sum();
    for &x in tail {
        acc += T::product(x, x);
    }
    acc
}

/// Cosine similarity: `dot(a, b) / (||a|| * ||b||)`. Returns 0.0 if either
/// vector has zero norm — avoids NaN propagating into top-K heaps.
#[inline]
pub fn cosine<T: VectorElement>(a: &[T], b: &[T]) -> f32 {
    let na = norm_squared(a).sqrt();
    let nb = norm_squared(b).sqrt();
    if na == 0.0 || nb == 0.0 {
        return 0.0;
    }
    dot(a, b) / (na * nb)
}

// =====================================================================
// Byte-input variants: avoid materializing the doc-side vector into an
// intermediate scratch buffer. The doc bytes are decoded inline inside
// the chunked accumulator, so the segment scan touches each byte
// exactly once regardless of directory backend (mmap, RAM, or custom).
// =====================================================================

/// `l2_squared` where the doc side is little-endian bytes encoding `T`.
///
/// Uses `chunks_exact` so LLVM sees fixed-length inner slices and can
/// elide bounds checks + autovectorize the chunked accumulator into
/// 4-wide NEON / 8-wide AVX2 / 16-wide AVX-512 SIMD.
#[inline]
pub fn l2_squared_bytes<T: VectorElement>(query: &[T], doc_bytes: &[u8]) -> f32 {
    debug_assert_eq!(doc_bytes.len(), query.len() * T::SIZE_BYTES);
    let q_chunks = query.chunks_exact(LANES);
    let b_chunks = doc_bytes.chunks_exact(LANES * T::SIZE_BYTES);
    let q_tail = q_chunks.remainder();
    let b_tail = b_chunks.remainder();

    let mut sums = [0f32; LANES];
    for (qc, bc) in q_chunks.zip(b_chunks) {
        for i in 0..LANES {
            let v = T::decode_le(&bc[i * T::SIZE_BYTES..(i + 1) * T::SIZE_BYTES]);
            sums[i] += T::squared_diff(qc[i], v);
        }
    }
    let mut acc: f32 = sums.iter().sum();
    for (i, &q) in q_tail.iter().enumerate() {
        let v = T::decode_le(&b_tail[i * T::SIZE_BYTES..(i + 1) * T::SIZE_BYTES]);
        acc += T::squared_diff(q, v);
    }
    acc
}

/// `dot` where the doc side is little-endian bytes encoding `T`.
#[inline]
pub fn dot_bytes<T: VectorElement>(query: &[T], doc_bytes: &[u8]) -> f32 {
    debug_assert_eq!(doc_bytes.len(), query.len() * T::SIZE_BYTES);
    let q_chunks = query.chunks_exact(LANES);
    let b_chunks = doc_bytes.chunks_exact(LANES * T::SIZE_BYTES);
    let q_tail = q_chunks.remainder();
    let b_tail = b_chunks.remainder();

    let mut sums = [0f32; LANES];
    for (qc, bc) in q_chunks.zip(b_chunks) {
        for i in 0..LANES {
            let v = T::decode_le(&bc[i * T::SIZE_BYTES..(i + 1) * T::SIZE_BYTES]);
            sums[i] += T::product(qc[i], v);
        }
    }
    let mut acc: f32 = sums.iter().sum();
    for (i, &q) in q_tail.iter().enumerate() {
        let v = T::decode_le(&b_tail[i * T::SIZE_BYTES..(i + 1) * T::SIZE_BYTES]);
        acc += T::product(q, v);
    }
    acc
}

/// `norm_squared` over little-endian bytes encoding `T`.
#[inline]
pub fn norm_squared_bytes<T: VectorElement>(doc_bytes: &[u8]) -> f32 {
    debug_assert_eq!(doc_bytes.len() % T::SIZE_BYTES, 0);
    let b_chunks = doc_bytes.chunks_exact(LANES * T::SIZE_BYTES);
    let b_tail = b_chunks.remainder();

    let mut sums = [0f32; LANES];
    for bc in b_chunks {
        for i in 0..LANES {
            let v = T::decode_le(&bc[i * T::SIZE_BYTES..(i + 1) * T::SIZE_BYTES]);
            sums[i] += T::product(v, v);
        }
    }
    let mut acc: f32 = sums.iter().sum();
    let tail_dim = b_tail.len() / T::SIZE_BYTES;
    for i in 0..tail_dim {
        let v = T::decode_le(&b_tail[i * T::SIZE_BYTES..(i + 1) * T::SIZE_BYTES]);
        acc += T::product(v, v);
    }
    acc
}

/// `cosine` where the doc side is little-endian bytes encoding `T`.
#[inline]
pub fn cosine_bytes<T: VectorElement>(query: &[T], doc_bytes: &[u8]) -> f32 {
    let nq = norm_squared(query).sqrt();
    let nd = norm_squared_bytes::<T>(doc_bytes).sqrt();
    if nq == 0.0 || nd == 0.0 {
        return 0.0;
    }
    dot_bytes(query, doc_bytes) / (nq * nd)
}

impl Metric {
    /// Compute a "higher is better" similarity score between two vectors.
    ///
    /// L2 distance is negated (squared, then sign-flipped) so all metrics
    /// share the same ranking convention. Magnitude differences across
    /// metrics are the caller's problem.
    #[inline]
    pub fn similarity<T: VectorElement>(self, query: &[T], doc: &[T]) -> f32 {
        match self {
            Metric::L2 => -l2_squared(query, doc),
            Metric::Cosine => cosine(query, doc),
            Metric::Dot => dot(query, doc),
        }
    }

    /// Like [`similarity`](Self::similarity), but the doc side is
    /// little-endian bytes — typically a borrowed slice straight out
    /// of the segment's file.
    #[inline]
    pub fn similarity_bytes<T: VectorElement>(self, query: &[T], doc_bytes: &[u8]) -> f32 {
        match self {
            Metric::L2 => -l2_squared_bytes(query, doc_bytes),
            Metric::Cosine => cosine_bytes(query, doc_bytes),
            Metric::Dot => dot_bytes(query, doc_bytes),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::Metric;

    #[test]
    fn test_l2_squared() {
        let a: [f32; 4] = [1.0, 2.0, 3.0, 4.0];
        let b: [f32; 4] = [4.0, 3.0, 2.0, 1.0];
        // (1-4)^2 + (2-3)^2 + (3-2)^2 + (4-1)^2 = 9+1+1+9 = 20
        assert!((l2_squared(&a, &b) - 20.0).abs() < 1e-6);
    }

    #[test]
    fn test_dot() {
        let a: [f32; 3] = [1.0, 2.0, 3.0];
        let b: [f32; 3] = [4.0, 5.0, 6.0];
        // 1*4 + 2*5 + 3*6 = 4+10+18 = 32
        assert!((dot(&a, &b) - 32.0).abs() < 1e-6);
    }

    #[test]
    fn test_cosine_identical() {
        let a: [f32; 2] = [3.0, 4.0];
        assert!((cosine(&a, &a) - 1.0).abs() < 1e-6);
    }

    #[test]
    fn test_cosine_orthogonal() {
        let a: [f32; 2] = [1.0, 0.0];
        let b: [f32; 2] = [0.0, 1.0];
        assert!(cosine(&a, &b).abs() < 1e-6);
    }

    #[test]
    fn test_cosine_opposite() {
        let a: [f32; 2] = [1.0, 0.0];
        let b: [f32; 2] = [-1.0, 0.0];
        assert!((cosine(&a, &b) + 1.0).abs() < 1e-6);
    }

    #[test]
    fn test_cosine_zero_norm() {
        let a: [f32; 2] = [0.0, 0.0];
        let b: [f32; 2] = [1.0, 0.0];
        assert_eq!(cosine(&a, &b), 0.0);
    }

    #[test]
    fn test_higher_is_better_ranking() {
        let query: [f32; 3] = [1.0, 0.0, 0.0];
        let near: [f32; 3] = [1.0, 0.1, 0.0];
        let far: [f32; 3] = [-1.0, 0.0, 0.0];
        for m in [Metric::L2, Metric::Cosine, Metric::Dot] {
            let s_near = m.similarity(&query, &near);
            let s_far = m.similarity(&query, &far);
            assert!(s_near > s_far, "metric {m:?}: {s_near} vs {s_far}");
        }
    }

    #[test]
    fn test_byte_kernels_match_f32_kernels() {
        // Random-ish data that exercises both the chunked path and the tail.
        let dim = LANES + 5;
        let a: Vec<f32> = (0..dim).map(|i| (i as f32) * 0.137 - 1.3).collect();
        let b: Vec<f32> = (0..dim).map(|i| (i as f32).sin()).collect();
        let b_bytes: Vec<u8> = b.iter().flat_map(|v| v.to_le_bytes()).collect();

        let eps = 1e-5;
        assert!((l2_squared(&a, &b) - l2_squared_bytes::<f32>(&a, &b_bytes)).abs() < eps);
        assert!((dot(&a, &b) - dot_bytes::<f32>(&a, &b_bytes)).abs() < eps);
        assert!((cosine(&a, &b) - cosine_bytes::<f32>(&a, &b_bytes)).abs() < eps);
        assert!((norm_squared(&b) - norm_squared_bytes::<f32>(&b_bytes)).abs() < eps);
    }

    #[test]
    fn test_long_vector_chunking() {
        // Exercise both the chunked path and the tail.
        let dim = LANES * 2 + 3;
        let a: Vec<f32> = (0..dim).map(|i| i as f32 * 0.1).collect();
        let b: Vec<f32> = (0..dim).map(|i| (i + 1) as f32 * 0.1).collect();
        // L2² = sum (-0.1)^2 = dim * 0.01
        let expected = dim as f32 * 0.01;
        assert!((l2_squared(&a, &b) - expected).abs() < 1e-4);
    }
}

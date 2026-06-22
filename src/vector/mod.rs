//! Distance kernels, the vector element trait, and the per-segment storage plugin.
//!
//! The schema-level field configuration ([`VectorOptions`](crate::schema::VectorOptions),
//! [`Metric`](crate::schema::Metric), [`VectorDType`]) lives in the schema module and is
//! re-exported here; the element trait [`VectorElement`], the vector storage abstraction
//! [`VectorArena`], and the distance kernels live here.
//! The on-disk formats live in submodules: [`flat`] for the dense full-precision layout and
//! [`ivf`] for the partitioned/clustered accelerator. Both are owned by a single
//! [`VectorPlugin`] which picks between them per merge based on
//! [`IndexSettings::vector_clustering_threshold`](crate::index::IndexSettings::vector_clustering_threshold).
//! Top-N vector queries dispatch over them via [`VectorBackend`].

use std::io;

mod backend;
mod collector;
mod distance;
mod header;
mod index_reader;
mod plugin;
mod prepared;

pub mod flat;
pub mod ivf;

#[cfg(test)]
pub(crate) mod tests;

pub(crate) const VEC_EXT: &str = "vec";

pub use backend::{ProbeStats, ProbeTermination, VectorBackend};
pub use collector::TopDocsByVectorSimilarity;
pub use distance::{
    cosine, cosine_bytes, dot, dot_bytes, l2_squared, l2_squared_bytes, Similarity,
};
pub use flat::FlatVecWriter;
pub use index_reader::{VectorClusterStats, VectorIndexReader, VectorInfo, VectorStorageFormat};
pub use ivf::{
    Candidate, Graph, IvfCentroids, IvfClusterer, IvfIndex, IvfMatrix, IvfMatrixView,
    IvfMergeSettings, IvfSearchMetrics, IvfVectorBatch, IvfVectors, NeighborhoodGraphConfig,
    NeighborhoodGraphSearchMetrics, NodeId, RelativeNeighborhoodGraph, ResumableSearchIterator,
    SearchIterator, SearchTerminationReason, Workspace,
};
pub use plugin::VectorPlugin;
pub use prepared::PreparedQuery;

// The schema-level vector types are re-exported here so `crate::vector::{...}`
// resolves for callers and tests that work entirely within the vector module.
pub use crate::schema::{Metric, VectorDType, VectorOptions};

/// Wide accumulator used by the reduction kernels (norms). Element
/// types choose their accumulator via [`VectorElement::Acc`]; this
/// trait is the vocabulary a kernel needs to *use* that choice:
/// make a zero, add, read out as f64 at the fold.
///
/// Deliberately a custom trait rather than std bounds: the natural
/// accumulator for a future quantized element is an integer (e.g.
/// `u64` for `u8` elements — exact, matches integer-SIMD hardware),
/// and `Into<f64>` does not exist for `u64` because std reserves
/// `From`/`Into` for lossless conversions. `to_f64` is explicitly
/// the lossy read-out at the end of a reduction.
///
/// Kept separate from `VectorElement` because these operations never
/// mention the element type — how to sum two `f64`s is a fact about
/// `f64` — and because accumulator impls are shared: `f32` and a
/// future `f16` would both declare `type Acc = f64` and reuse this
/// one impl.
pub trait Accumulator: Copy + Send + Sync + 'static {
    const ZERO: Self;
    fn add(self, rhs: Self) -> Self;
    fn to_f64(self) -> f64;
}

impl Accumulator for f64 {
    const ZERO: Self = 0.0;

    #[inline(always)]
    fn add(self, rhs: Self) -> Self {
        self + rhs
    }

    #[inline(always)]
    fn to_f64(self) -> f64 {
        self
    }
}

/// A vector element type with the primitives needed by the storage
/// layer and the distance kernels.
///
/// Implemented for the element types supported by [`VectorDType`]. The
/// `DTYPE` associated constant lets callers reject mismatches between
/// the declared schema dtype and the type passed at runtime. The
/// arithmetic methods (`squared_diff`, `product`) return `f32` so that
/// kernels can use a uniform accumulator type across dtypes; the
/// reduction kernels (norms) instead accumulate in [`Self::Acc`] via
/// [`Self::mul_wide`].
pub trait VectorElement: Copy + Send + Sync + 'static {
    const DTYPE: VectorDType;
    const SIZE_BYTES: usize;

    /// Accumulator for this element's squares in reduction kernels.
    /// Each element type declares how much headroom it needs: f32 -> f64
    /// (squaring doubles the exponent, so |v| > ~1.8e19 overflows an f32
    /// product; f64 makes any sum of finite-f32 squares finite).
    type Acc: Accumulator;

    fn encode_le<W: io::Write + ?Sized>(&self, buf: &mut W) -> io::Result<()>;

    /// Decode one element from its little-endian byte representation.
    /// `bytes.len()` must be `SIZE_BYTES`.
    fn decode_le(bytes: &[u8]) -> Self;

    /// `(a - b)^2` promoted to `f32` for accumulator-friendly distance
    /// computation. For `f32` this is the obvious arithmetic; for
    /// quantized types it may promote through a wider integer first.
    fn squared_diff(a: Self, b: Self) -> f32;

    /// `a * b` promoted to `f32`. Same rationale as `squared_diff`.
    fn product(a: Self, b: Self) -> f32;

    /// Widen-THEN-multiply: the cast happens before the square so the
    /// product cannot overflow the narrow type. One operation, so the
    /// ordering invariant is unforgettable per element type.
    fn mul_wide(a: Self, b: Self) -> Self::Acc;

    /// Lossless widening to `f32`.
    fn to_f32(self) -> f32;

    /// Narrowing from `f32`; used at normalization write-back where
    /// values are already `<= 1`.
    fn from_f32(v: f32) -> Self;
}

impl VectorElement for f32 {
    const DTYPE: VectorDType = VectorDType::F32;
    const SIZE_BYTES: usize = 4;

    type Acc = f64;

    #[inline(always)]
    fn encode_le<W: io::Write + ?Sized>(&self, buf: &mut W) -> io::Result<()> {
        buf.write_all(&self.to_le_bytes())
    }

    #[inline(always)]
    fn decode_le(bytes: &[u8]) -> Self {
        f32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]])
    }

    #[inline(always)]
    fn squared_diff(a: Self, b: Self) -> f32 {
        let d = a - b;
        d * d
    }

    #[inline(always)]
    fn product(a: Self, b: Self) -> f32 {
        a * b
    }

    #[inline(always)]
    fn mul_wide(a: Self, b: Self) -> f64 {
        (a as f64) * (b as f64)
    }

    #[inline(always)]
    fn to_f32(self) -> f32 {
        self
    }

    #[inline(always)]
    fn from_f32(v: f32) -> Self {
        v
    }
}

/// A flat, `dim`-strided arena of vectors a [`Graph`] is built over or
/// searched against.
///
/// The arena owns its representation — typed slices via the blanket impl,
/// raw little-endian file bytes for reloaded storage — and scores a typed
/// query against a stored vector with the kernel matching that
/// representation. The [`Metric`] is a parameter: an arena never holds one.
pub trait VectorArena {
    /// Element type of the vectors; queries are `&[Elem]`.
    type Elem: VectorElement;

    /// The number of vectors held, at `dim` elements each.
    fn num_vectors(&self, dim: usize) -> usize;

    /// [`Similarity`] of `query` to vector `node`.
    fn similarity(
        &self,
        metric: Metric,
        dim: usize,
        node: NodeId,
        query: &[Self::Elem],
    ) -> Similarity;
}

/// Any `[T]`-shaped storage (`&[T]`, `Vec<T>`, …), scored with the typed kernels.
impl<T: VectorElement, S: std::ops::Deref<Target = [T]>> VectorArena for S {
    type Elem = T;

    #[inline]
    fn num_vectors(&self, dim: usize) -> usize {
        assert_eq!(self.len() % dim, 0, "arena not a multiple of dim");
        self.len() / dim
    }

    #[inline]
    fn similarity(&self, metric: Metric, dim: usize, node: NodeId, query: &[T]) -> Similarity {
        metric.similarity(query, &self[node as usize * dim..][..dim])
    }
}

/// A [`VectorArena`] over raw little-endian `T` rows behind a
/// [`FileSlice`](crate::directory::FileSlice): each [`similarity`] call
/// fetches only that node's row with one stride-sized ranged read, scored
/// with the byte kernels ([`Metric::similarity_bytes`]). The arena is never
/// materialized whole — under `MmapDirectory` a read is a zero-copy view,
/// and under a copying `Directory` only the visited nodes' bytes are fetched.
///
/// This is the search-time counterpart of the typed blanket impl: a
/// [`Graph`] reloaded from disk wraps its file-resident vectors in one of
/// these and is search-only (refinement needs typed storage).
///
/// [`similarity`]: VectorArena::similarity
pub struct FileSliceArena<T> {
    slice: crate::directory::FileSlice,
    _elem: std::marker::PhantomData<T>,
}

impl<T> FileSliceArena<T> {
    /// Wraps a slice of contiguous `dim`-strided little-endian `T` rows.
    pub fn new(slice: crate::directory::FileSlice) -> Self {
        FileSliceArena {
            slice,
            _elem: std::marker::PhantomData,
        }
    }
}

impl<T: VectorElement> VectorArena for FileSliceArena<T> {
    type Elem = T;

    #[inline]
    fn num_vectors(&self, dim: usize) -> usize {
        use common::HasLen;
        let stride = dim * T::SIZE_BYTES;
        assert_eq!(self.slice.len() % stride, 0, "arena not a multiple of dim");
        self.slice.len() / stride
    }

    /// # Panics
    ///
    /// The trait has no error channel, so a failed read panics. Row bounds
    /// are guaranteed by the graph (node ids come from the adjacency, which
    /// is length-validated against this arena at open); an I/O failure here
    /// means the underlying `Directory` could not produce bytes it already
    /// promised via the slice.
    #[inline]
    fn similarity(&self, metric: Metric, dim: usize, node: NodeId, query: &[T]) -> Similarity {
        let stride = dim * T::SIZE_BYTES;
        let bytes = self
            .slice
            .slice(node as usize * stride..(node as usize + 1) * stride)
            .read_bytes()
            .expect("failed to read vector arena row");
        metric.similarity_bytes(query, &bytes)
    }
}

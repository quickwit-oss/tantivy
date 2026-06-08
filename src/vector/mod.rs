//! Distance kernels, the vector element trait, and the per-segment storage plugin.
//!
//! The schema-level field configuration ([`VectorOptions`](crate::schema::VectorOptions),
//! [`Metric`](crate::schema::Metric), [`VectorDType`]) lives in the schema module; the
//! element trait [`VectorElement`] and the distance kernels live here. The on-disk format
//! lives in the [`flat`] submodule (dense full-precision layout), owned by the
//! [`VectorPlugin`]. Top-N vector queries dispatch over it via [`VectorBackend`].

use std::io;

use crate::schema::VectorDType;

mod backend;
mod collector;
mod distance;
mod meta;
mod plugin;
mod reader;

pub mod flat;

pub use backend::VectorBackend;
pub use collector::TopDocsByVectorSimilarity;
pub use distance::{cosine, cosine_bytes, dot, dot_bytes, l2_squared, l2_squared_bytes};
pub use flat::{FlatVecReader, FlatVecWriter, FlatVectorColumn};
pub use plugin::VectorPlugin;
pub use reader::{VectorColumn, VectorColumnReader, VectorReader};

/// A vector element type with the primitives needed by the storage
/// layer and the distance kernels.
///
/// Implemented for the element types supported by [`VectorDType`]. The
/// `DTYPE` associated constant lets callers reject mismatches between
/// the declared schema dtype and the type passed at runtime. The
/// arithmetic methods (`squared_diff`, `product`) return `f32` so that
/// kernels can use a uniform accumulator type across dtypes.
pub trait VectorElement: Copy + Send + Sync + 'static {
    const DTYPE: VectorDType;
    const SIZE_BYTES: usize;

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
}

impl VectorElement for f32 {
    const DTYPE: VectorDType = VectorDType::F32;
    const SIZE_BYTES: usize = 4;

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
}

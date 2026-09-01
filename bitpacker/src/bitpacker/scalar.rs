//! The `simd` backend for a target with no kernels.
//!
//! The arch halves of [`super::Lane`] and [`super::Store`] are empty here, and
//! every type has them, so the traits compose exactly as they do on a target
//! that decodes in registers. Nothing calls the kernels: the sites that would
//! are gated on the same targets this module fills in for.

pub trait SimdLane {}
impl<T> SimdLane for T {}

pub trait SimdStore<Out> {}
impl<T, Out> SimdStore<Out> for T {}

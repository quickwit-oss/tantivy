//! Integration layer between tantivy-gpu and Tantivy's search pipeline.
//!
//! This module provides the bridge types that implement Tantivy's traits
//! (SegmentCollector, Weight) while delegating computation to GPU kernels.
//!
//! These types are used by Tantivy when the `gpu` feature is enabled.

pub mod gpu_stats_collector;
pub mod gpu_term_weight;

#![warn(missing_docs)]
#![cfg_attr(all(feature = "unstable", test), feature(test))]

//! # `fastfield_codecs`
//!
//! - Columnar storage of data for tantivy [`Column`].
//! - Encode data in different codecs.
//! - Monotonically map values to u64/u128

pub use columnar::ColumnValues as Column;

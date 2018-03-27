//! The fieldnorm represents the length associated to
//! a given Field of a given document.
//!
//! This metric is important to compute the score of a
//! document : a document having a query word in one its short fields
//! (e.g. title)  is likely to be more relevant than in one of its longer field
//! (e.g. body).
//!
//! It encodes `fieldnorm` on one byte with some precision loss,
//! using the exact same scheme as Lucene. Each value is place on a log-scale
//! that takes values from `0` to `255`.
//!
//! A value on this scale is identified by a `fieldnorm_id`.
//! Apart from compression, this scale also makes it possible to
//! precompute computationally expensive functions of the fieldnorm
//! in a very short array.
//!
//! This trick is used by the [BM25 similarity]().
mod code;
mod serializer;
mod writer;
mod reader;

pub use self::reader::FieldNormReader;
pub use self::writer::FieldNormsWriter;
pub use self::serializer::FieldNormsSerializer;

use self::code::{fieldnorm_to_id, id_to_fieldnorm};


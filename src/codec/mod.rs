use crate::codec::postings::PostingsSerializer;

mod postings;
mod standard;

pub use standard::StandardCodec;

pub trait Codec: Default + Clone + std::fmt::Debug {
    type PostingsCodec;

}

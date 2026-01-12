use crate::codec::postings::PostingsCodec;

mod block;
mod postings_serializer;
mod skip;

pub use postings_serializer::StandardPostingsSerializer;

pub struct StandardPostingsCodec;

impl PostingsCodec for StandardPostingsCodec {
    type PostingsSerializer = StandardPostingsSerializer;
}

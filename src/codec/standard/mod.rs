use crate::codec::standard::postings::StandardPostingsCodec;
use crate::codec::Codec;

mod postings;

#[derive(Debug, Default, Clone)]
pub struct StandardCodec;

impl Codec for StandardCodec {
    type PostingsCodec = StandardPostingsCodec;
}

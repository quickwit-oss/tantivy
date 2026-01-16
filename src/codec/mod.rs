pub mod postings;
pub mod standard;

use std::borrow::Cow;

use common::OwnedBytes;
use serde::{Deserialize, Serialize};
pub use standard::StandardCodec;

use crate::{codec::postings::PostingsCodec, postings::Postings, schema::IndexRecordOption};

pub trait Codec: Clone + std::fmt::Debug + Send + Sync + 'static {
    type PostingsCodec: PostingsCodec;

    const NAME: &'static str;

    fn from_json_props(json_value: &serde_json::Value) -> crate::Result<Self>;
    fn to_json_props(&self) -> serde_json::Value;

    fn postings_codec(&self) -> &Self::PostingsCodec;
}


#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct CodecConfiguration {
    name: Cow<'static, str>,
    #[serde(default, skip_serializing_if = "serde_json::Value::is_null")]
    props: serde_json::Value,
}

impl CodecConfiguration {
    pub fn from_codec<C: Codec>(codec: &C) -> Self {
        CodecConfiguration {
            name: Cow::Borrowed(C::NAME),
            props: codec.to_json_props(),
        }
    }

    pub fn to_codec<C: Codec>(&self) -> crate::Result<C> {
        if self.name != C::NAME {
            return Err(crate::TantivyError::InvalidArgument(format!(
                "Codec name mismatch: expected {}, got {}",
                C::NAME,
                self.name
            )));
        }
        C::from_json_props(&self.props)
    }
}

impl Default for CodecConfiguration {
    fn default() -> Self {
        CodecConfiguration::from_codec(&StandardCodec)
    }
}

pub trait CodecPostingsLoader {
    fn load_postings_type_erased(&self,
        doc_freq: u32,
        postings_data: OwnedBytes,
        record_option: IndexRecordOption,
        requested_option: IndexRecordOption,
        positions_data: Option<OwnedBytes>,
    ) -> crate::Result<Box<dyn Postings>>;
}

impl<TPostingsCodec: PostingsCodec> CodecPostingsLoader for TPostingsCodec {
    fn load_postings_type_erased(&self,
            doc_freq: u32,
            postings_data: OwnedBytes,
            record_option: IndexRecordOption,
            requested_option: IndexRecordOption,
            positions_data: Option<OwnedBytes>,
        ) -> crate::Result<Box<dyn Postings>> {
            let postings: <Self as PostingsCodec>::Postings = self.load_postings(doc_freq, postings_data, record_option, requested_option, positions_data)?;
            let boxed_postings: Box<dyn Postings> = Box::new(postings);
            Ok(boxed_postings)
     }
}

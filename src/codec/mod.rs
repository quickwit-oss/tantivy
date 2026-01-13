mod postings;
mod standard;

use std::borrow::Cow;

use serde::{Deserialize, Serialize};
pub use standard::StandardCodec;

pub trait Codec: Clone + std::fmt::Debug + Send + Sync + 'static {
    type PostingsCodec;

    const NAME: &'static str;

    fn from_json_props(json_value: &serde_json::Value) -> crate::Result<Self>;
    fn to_json_props(&self) -> serde_json::Value;
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

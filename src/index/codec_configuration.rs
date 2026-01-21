use std::borrow::Cow;

use serde::{Deserialize, Serialize};

use crate::codec::{Codec, StandardCodec};

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

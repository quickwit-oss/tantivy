use std::borrow::Cow;

use serde::{Deserialize, Serialize};

use crate::codec::{Codec, StandardCodec};

/// A Codec configuration is just a serializable object.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct CodecConfiguration {
    codec_id: Cow<'static, str>,
    #[serde(default, skip_serializing_if = "serde_json::Value::is_null")]
    props: serde_json::Value,
}

impl CodecConfiguration {
    /// Returns true if the codec is the standard codec.
    pub fn is_standard(&self) -> bool {
        self.codec_id == StandardCodec::ID && self.props.is_null()
    }

    /// Creates a codec instance from the configuration.
    ///
    /// If the codec id does not match the code's name, an error is returned.
    pub fn to_codec<C: Codec>(&self) -> crate::Result<C> {
        if self.codec_id != C::ID {
            return Err(crate::TantivyError::InvalidArgument(format!(
                "Codec id mismatch: expected {}, got {}",
                C::ID,
                self.codec_id
            )));
        }
        C::from_json_props(&self.props)
    }
}

impl<'a, C: Codec> From<&'a C> for CodecConfiguration {
    fn from(codec: &'a C) -> Self {
        CodecConfiguration {
            codec_id: Cow::Borrowed(C::ID),
            props: codec.to_json_props(),
        }
    }
}

impl Default for CodecConfiguration {
    fn default() -> Self {
        CodecConfiguration::from(&StandardCodec)
    }
}

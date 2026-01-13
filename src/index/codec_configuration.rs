use std::borrow::Cow;

use serde::{Deserialize, Serialize};

use crate::codec::{Codec, StandardCodec};

/// A small struct representing a codec configuration.
/// It is meant to be serialized in the index metadata file.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct CodecConfiguration {
    name: Cow<'static, str>,
    #[serde(default, skip_serializing_if = "serde_json::Value::is_null")]
    props: serde_json::Value,
}

impl CodecConfiguration {
    /// Builds the codec given a codec_configuration.
    ///
    /// If the configuration codec name does not match the type C,
    /// a tantivy error is returned.
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

impl<'a, C: Codec> From<&'a C> for CodecConfiguration {
    fn from(codec: &'a C) -> Self {
        CodecConfiguration {
            name: Cow::Borrowed(C::NAME),
            props: codec.to_json_props(),
        }
    }
}

impl Default for CodecConfiguration {
    fn default() -> Self {
        CodecConfiguration::from(&StandardCodec)
    }
}

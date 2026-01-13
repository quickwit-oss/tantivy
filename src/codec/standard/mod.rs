use serde::{Deserialize, Serialize};

use crate::codec::standard::postings::StandardPostingsCodec;
use crate::codec::Codec;

pub mod postings;

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct StandardCodec;

impl Codec for StandardCodec {
    type PostingsCodec = StandardPostingsCodec;

    const NAME: &'static str = "standard";

    fn from_json_props(json_value: &serde_json::Value) -> crate::Result<Self> {
        if !json_value.is_null() {
            return Err(crate::TantivyError::InvalidArgument(format!(
                "Codec property for the StandardCodec are unexpected. expected null, got {}",
                json_value.as_str().unwrap_or("null")
            )));
        }
        Ok(StandardCodec)
    }

    fn to_json_props(&self) -> serde_json::Value {
        serde_json::Value::Null
    }
}

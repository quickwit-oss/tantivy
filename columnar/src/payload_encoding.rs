use serde::{Deserialize, Serialize};

use crate::InvalidData;

/// Encoding used to store string and byte column payloads.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PayloadEncoding {
    /// Store values in a dictionary and address them by ordinal.
    #[default]
    Dictionary,
    /// Store values directly without assigning them dictionary ordinals.
    Plain,
}

impl PayloadEncoding {
    pub(crate) fn to_code(self) -> u8 {
        match self {
            PayloadEncoding::Dictionary => 0,
            PayloadEncoding::Plain => 1,
        }
    }

    pub(crate) fn try_from_code(code: u8) -> Result<Self, InvalidData> {
        match code {
            0 => Ok(PayloadEncoding::Dictionary),
            1 => Ok(PayloadEncoding::Plain),
            _ => Err(InvalidData),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_payload_encoding_codes() {
        let mut valid_codes = Vec::new();
        for code in u8::MIN..=u8::MAX {
            if let Ok(encoding) = PayloadEncoding::try_from_code(code) {
                assert_eq!(encoding.to_code(), code);
                valid_codes.push(code);
            }
        }
        assert_eq!(valid_codes, [0, 1]);
    }
}

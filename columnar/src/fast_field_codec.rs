use std::io;

use serde::{Deserialize, Serialize};

/// Storage for string and byte fast fields. Numeric columns use their own codecs.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "codec", rename_all = "snake_case", deny_unknown_fields)]
pub enum FastFieldCodec {
    /// Store distinct whole values in a sorted dictionary.
    #[default]
    Dictionary,
    /// Compress independent groups with OnPair16 substring dictionaries.
    Compact(CompactCodecOptions),
}

/// Limits used when building compact row groups.
///
/// Groups never split a document. A single document may exceed the byte target.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct CompactCodecOptions {
    pub row_group_max_docs: u32,
    pub row_group_target_bytes: u64,
    pub max_dict_bits: u8,
}

impl Default for CompactCodecOptions {
    fn default() -> Self {
        Self {
            row_group_max_docs: 4096,
            row_group_target_bytes: 1 << 20,
            max_dict_bits: 12,
        }
    }
}

impl CompactCodecOptions {
    pub fn validate(&self) -> io::Result<()> {
        if self.row_group_max_docs == 0 || self.row_group_target_bytes == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "compact group limits must be positive",
            ));
        }
        onpair::MaxDictBits::new(self.max_dict_bits).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "max_dict_bits must be in 9..=16",
            )
        })?;
        Ok(())
    }
}

impl FastFieldCodec {
    pub fn validate(&self) -> io::Result<()> {
        if let Self::Compact(options) = self {
            options.validate()?;
        }
        Ok(())
    }
}

use serde::{Deserialize, Serialize};
use std::io;

pub trait StoreCompressor {
    fn compress(&self, uncompressed: &[u8], compressed: &mut Vec<u8>) -> io::Result<()>;
    fn decompress(&self, compressed: &[u8], decompressed: &mut Vec<u8>) -> io::Result<()>;
    fn get_compressor_id() -> u8;
}

/// Compressor can be used on `IndexSettings` to choose
/// the compressor used to compress the doc store.
///
/// The default is Lz4Block, but also depends on the enabled feature flags.
#[derive(Clone, Debug, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Compressor {
    #[serde(rename = "lz4")]
    /// Use the lz4 compressor (block format)
    Lz4,
    #[serde(rename = "brotli")]
    /// Use the brotli compressor
    Brotli,
    #[serde(rename = "snappy")]
    /// Use the snap compressor
    Snappy,
}

impl Default for Compressor {
    fn default() -> Self {
        if cfg!(feature = "lz4-compression") {
            Compressor::Lz4
        } else if cfg!(feature = "brotli-compression") {
            Compressor::Brotli
        } else if cfg!(feature = "snappy-compression") {
            Compressor::Snappy
        } else {
            panic!(
                "all compressor feature flags like are disabled (e.g. lz4-compression), can't choose default compressor"
            );
        }
    }
}

impl Compressor {
    pub(crate) fn from_id(id: u8) -> Compressor {
        match id {
            1 => Compressor::Lz4,
            2 => Compressor::Brotli,
            3 => Compressor::Snappy,
            _ => panic!("unknown compressor id {:?}", id),
        }
    }
    pub(crate) fn get_id(&self) -> u8 {
        match self {
            Self::Lz4 => 1,
            Self::Brotli => 2,
            Self::Snappy => 3,
        }
    }
    pub(crate) fn compress(&self, uncompressed: &[u8], compressed: &mut Vec<u8>) -> io::Result<()> {
        match self {
            Self::Lz4 => {
                #[cfg(feature = "lz4_flex")]
                {
                    super::compression_lz4_block::compress(uncompressed, compressed)
                }
                #[cfg(not(feature = "lz4_flex"))]
                {
                    panic!("lz4-compression feature flag not activated");
                }
            }
            Self::Brotli => {
                #[cfg(feature = "brotli")]
                {
                    super::compression_brotli::compress(uncompressed, compressed)
                }
                #[cfg(not(feature = "brotli"))]
                {
                    panic!("brotli-compression feature flag not activated");
                }
            }
            Self::Snappy => {
                #[cfg(feature = "snap")]
                {
                    super::compression_snap::compress(uncompressed, compressed)
                }
                #[cfg(not(feature = "snap"))]
                {
                    panic!("snap-compression feature flag not activated");
                }
            }
        }
    }

    pub(crate) fn decompress(
        &self,
        compressed: &[u8],
        decompressed: &mut Vec<u8>,
    ) -> io::Result<()> {
        match self {
            Self::Lz4 => {
                #[cfg(feature = "lz4_flex")]
                {
                    super::compression_lz4_block::decompress(compressed, decompressed)
                }
                #[cfg(not(feature = "lz4_flex"))]
                {
                    panic!("lz4_flex feature flag not activated");
                }
            }
            Self::Brotli => {
                #[cfg(feature = "brotli")]
                {
                    super::compression_brotli::decompress(compressed, decompressed)
                }
                #[cfg(not(feature = "brotli"))]
                {
                    panic!("brotli feature flag not activated");
                }
            }
            Self::Snappy => {
                #[cfg(feature = "snap")]
                {
                    super::compression_snap::decompress(compressed, decompressed)
                }
                #[cfg(not(feature = "snap"))]
                {
                    panic!("snap feature flag not activated");
                }
            }
        }
    }
}

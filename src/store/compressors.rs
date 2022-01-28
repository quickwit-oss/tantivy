use std::io;

use serde::{Deserialize, Serialize};

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
    #[serde(rename = "none")]
    /// No compression
    None,
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
            Compressor::None
        }
    }
}

impl Compressor {
    pub(crate) fn from_id(id: u8) -> Compressor {
        match id {
            0 => Compressor::None,
            1 => Compressor::Lz4,
            2 => Compressor::Brotli,
            3 => Compressor::Snappy,
            _ => panic!("unknown compressor id {:?}", id),
        }
    }
    pub(crate) fn get_id(&self) -> u8 {
        match self {
            Self::None => 0,
            Self::Lz4 => 1,
            Self::Brotli => 2,
            Self::Snappy => 3,
        }
    }
    #[inline]
    pub(crate) fn compress(&self, uncompressed: &[u8], compressed: &mut Vec<u8>) -> io::Result<()> {
        match self {
            Self::None => {
                compressed.clear();
                compressed.extend_from_slice(uncompressed);
                Ok(())
            }
            Self::Lz4 => {
                #[cfg(feature = "lz4-compression")]
                {
                    super::compression_lz4_block::compress(uncompressed, compressed)
                }
                #[cfg(not(feature = "lz4-compression"))]
                {
                    panic!("lz4-compression feature flag not activated");
                }
            }
            Self::Brotli => {
                #[cfg(feature = "brotli-compression")]
                {
                    super::compression_brotli::compress(uncompressed, compressed)
                }
                #[cfg(not(feature = "brotli-compression"))]
                {
                    panic!("brotli-compression-compression feature flag not activated");
                }
            }
            Self::Snappy => {
                #[cfg(feature = "snappy-compression")]
                {
                    super::compression_snap::compress(uncompressed, compressed)
                }
                #[cfg(not(feature = "snappy-compression"))]
                {
                    panic!("snappy-compression feature flag not activated");
                }
            }
        }
    }

    #[inline]
    pub(crate) fn decompress(
        &self,
        compressed: &[u8],
        decompressed: &mut Vec<u8>,
    ) -> io::Result<()> {
        match self {
            Self::None => {
                decompressed.clear();
                decompressed.extend_from_slice(compressed);
                Ok(())
            }
            Self::Lz4 => {
                #[cfg(feature = "lz4-compression")]
                {
                    super::compression_lz4_block::decompress(compressed, decompressed)
                }
                #[cfg(not(feature = "lz4-compression"))]
                {
                    panic!("lz4-compression feature flag not activated");
                }
            }
            Self::Brotli => {
                #[cfg(feature = "brotli-compression")]
                {
                    super::compression_brotli::decompress(compressed, decompressed)
                }
                #[cfg(not(feature = "brotli-compression"))]
                {
                    panic!("brotli-compression feature flag not activated");
                }
            }
            Self::Snappy => {
                #[cfg(feature = "snappy-compression")]
                {
                    super::compression_snap::decompress(compressed, decompressed)
                }
                #[cfg(not(feature = "snappy-compression"))]
                {
                    panic!("snappy-compression feature flag not activated");
                }
            }
        }
    }
}

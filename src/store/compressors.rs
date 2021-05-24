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
/// The default is Lz4Block.
#[derive(Clone, Debug, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Compressor {
    /// Use the lz4 block format compressor
    Lz4Block,
    /// Use the lz4 frame format compressor
    Lz4Frame,
    /// Use the brotli compressor
    Brotli,
    /// Use the snap compressor
    Snap,
}

impl Default for Compressor {
    fn default() -> Self {
        Compressor::Lz4Block
    }
}

impl Compressor {
    pub(crate) fn from_id(id: u8) -> Compressor {
        match id {
            1 => Compressor::Lz4Block,
            2 => Compressor::Lz4Frame,
            3 => Compressor::Brotli,
            4 => Compressor::Snap,
            _ => panic!("unknown compressor id {:?}", id),
        }
    }
    pub(crate) fn get_id(&self) -> u8 {
        match self {
            &Self::Lz4Block => 1,
            &Self::Lz4Frame => 2,
            &Self::Brotli => 3,
            &Self::Snap => 4,
        }
    }
    pub(crate) fn compress(&self, uncompressed: &[u8], compressed: &mut Vec<u8>) -> io::Result<()> {
        match self {
            &Self::Lz4Block => {
                #[cfg(feature = "lz4_flex")]
                {
                    super::compression_lz4_block::compress(uncompressed, compressed)
                }
                #[cfg(not(feature = "lz4_flex"))]
                {
                    panic!("lz4_flex feature flag not activated");
                }
            }
            &Self::Lz4Frame => {
                #[cfg(feature = "lz4")]
                {
                    super::compression_lz4::compress(uncompressed, compressed)
                }
                #[cfg(not(feature = "lz4"))]
                {
                    panic!("lz4 feature flag not activated");
                }
            }
            &Self::Brotli => {
                #[cfg(feature = "brotli")]
                {
                    super::compression_brotli::compress(uncompressed, compressed)
                }
                #[cfg(not(feature = "brotli"))]
                {
                    panic!("brotli feature flag not activated");
                }
            }
            &Self::Snap => {
                #[cfg(feature = "snap")]
                {
                    super::compression_snap::compress(uncompressed, compressed)
                }
                #[cfg(not(feature = "snap"))]
                {
                    panic!("snap feature flag not activated");
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
            &Self::Lz4Block => {
                #[cfg(feature = "lz4_flex")]
                {
                    super::compression_lz4_block::decompress(compressed, decompressed)
                }
                #[cfg(not(feature = "lz4_flex"))]
                {
                    panic!("lz4_flex feature flag not activated");
                }
            }
            &Self::Lz4Frame => {
                #[cfg(feature = "lz4")]
                {
                    super::compression_lz4::decompress(compressed, decompressed)
                }
                #[cfg(not(feature = "lz4"))]
                {
                    panic!("lz4 feature flag not activated");
                }
            }
            &Self::Brotli => {
                #[cfg(feature = "brotli")]
                {
                    super::compression_brotli::decompress(compressed, decompressed)
                }
                #[cfg(not(feature = "brotli"))]
                {
                    panic!("brotli feature flag not activated");
                }
            }
            &Self::Snap => {
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

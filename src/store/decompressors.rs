use std::io;

use serde::{Deserialize, Serialize};

use super::Compressor;

pub trait StoreCompressor {
    fn compress(&self, uncompressed: &[u8], compressed: &mut Vec<u8>) -> io::Result<()>;
    fn decompress(&self, compressed: &[u8], decompressed: &mut Vec<u8>) -> io::Result<()>;
    fn get_compressor_id() -> u8;
}

/// Decompressor is deserialized from the doc store footer, when opening an index.
#[derive(Clone, Debug, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Decompressor {
    /// No compression
    None,
    /// Use the lz4 decompressor (block format)
    Lz4,
    /// Use the brotli decompressor
    Brotli,
    /// Use the snap decompressor
    Snappy,
    /// Use the zstd decompressor
    Zstd,
}

impl From<Compressor> for Decompressor {
    fn from(compressor: Compressor) -> Self {
        match compressor {
            Compressor::None => Decompressor::None,
            Compressor::Lz4 => Decompressor::Lz4,
            Compressor::Brotli => Decompressor::Brotli,
            Compressor::Snappy => Decompressor::Snappy,
            Compressor::Zstd(_) => Decompressor::Zstd,
        }
    }
}

impl Decompressor {
    pub(crate) fn from_id(id: u8) -> Decompressor {
        match id {
            0 => Decompressor::None,
            1 => Decompressor::Lz4,
            2 => Decompressor::Brotli,
            3 => Decompressor::Snappy,
            4 => Decompressor::Zstd,
            _ => panic!("unknown compressor id {id:?}"),
        }
    }

    pub(crate) fn get_id(&self) -> u8 {
        match self {
            Self::None => 0,
            Self::Lz4 => 1,
            Self::Brotli => 2,
            Self::Snappy => 3,
            Self::Zstd => 4,
        }
    }

    pub(crate) fn decompress(&self, compressed_block: &[u8]) -> io::Result<Vec<u8>> {
        let mut decompressed_block = vec![];
        self.decompress_into(compressed_block, &mut decompressed_block)?;
        Ok(decompressed_block)
    }

    #[inline]
    pub(crate) fn decompress_into(
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
            Self::Zstd => {
                #[cfg(feature = "zstd-compression")]
                {
                    super::compression_zstd_block::decompress(compressed, decompressed)
                }
                #[cfg(not(feature = "zstd-compression"))]
                {
                    panic!("zstd-compression feature flag not activated");
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::Compressor;

    #[test]
    fn compressor_decompressor_id_test() {
        assert_eq!(Decompressor::from(Compressor::None), Decompressor::None);
        assert_eq!(Decompressor::from(Compressor::Lz4), Decompressor::Lz4);
        assert_eq!(Decompressor::from(Compressor::Brotli), Decompressor::Brotli);
        assert_eq!(Decompressor::from(Compressor::Snappy), Decompressor::Snappy);
        assert_eq!(
            Decompressor::from(Compressor::Zstd(Default::default())),
            Decompressor::Zstd
        );
    }
}

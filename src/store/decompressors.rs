use std::io;

use common::OwnedBytes;
use serde::{Deserialize, Serialize};

use super::Compressor;

/// Decompressor is deserialized from the doc store footer, when opening an index.
#[derive(Clone, Debug, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Decompressor {
    /// No compression
    None,
    /// Use the lz4 decompressor (block format)
    #[cfg(feature = "lz4-compression")]
    Lz4,
    /// Use the zstd decompressor
    #[cfg(feature = "zstd-compression")]
    Zstd,
}

impl From<Compressor> for Decompressor {
    fn from(compressor: Compressor) -> Self {
        match compressor {
            Compressor::None => Decompressor::None,
            #[cfg(feature = "lz4-compression")]
            Compressor::Lz4 => Decompressor::Lz4,
            #[cfg(feature = "zstd-compression")]
            Compressor::Zstd(_) => Decompressor::Zstd,
        }
    }
}

impl Decompressor {
    pub(crate) fn from_id(id: u8) -> Decompressor {
        match id {
            0 => Decompressor::None,
            #[cfg(feature = "lz4-compression")]
            1 => Decompressor::Lz4,
            #[cfg(feature = "zstd-compression")]
            4 => Decompressor::Zstd,
            _ => panic!("unknown compressor id {id:?}"),
        }
    }

    pub(crate) fn get_id(&self) -> u8 {
        match self {
            Self::None => 0,
            #[cfg(feature = "lz4-compression")]
            Self::Lz4 => 1,
            #[cfg(feature = "zstd-compression")]
            Self::Zstd => 4,
        }
    }

    /// Uncompressed blocks are returned as is, without copying.
    pub(crate) fn decompress_block(&self, compressed_block: OwnedBytes) -> io::Result<OwnedBytes> {
        if *self == Self::None {
            return Ok(compressed_block);
        }
        let mut decompressed_block = Vec::new();
        self.decompress_into(compressed_block.as_slice(), &mut decompressed_block)?;
        Ok(OwnedBytes::new(decompressed_block))
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
            #[cfg(feature = "lz4-compression")]
            Self::Lz4 => super::compression_lz4_block::decompress(compressed, decompressed),
            #[cfg(feature = "zstd-compression")]
            Self::Zstd => super::compression_zstd_block::decompress(compressed, decompressed),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compressor_decompressor_id_test() {
        assert_eq!(Decompressor::from(Compressor::None), Decompressor::None);
        #[cfg(feature = "lz4-compression")]
        assert_eq!(Decompressor::from(Compressor::Lz4), Decompressor::Lz4);
        #[cfg(feature = "zstd-compression")]
        assert_eq!(
            Decompressor::from(Compressor::Zstd(Default::default())),
            Decompressor::Zstd
        );
    }

    #[test]
    fn test_decompress_block_none_is_zero_copy() {
        let block = OwnedBytes::new(b"hello".to_vec());
        let ptr = block.as_slice().as_ptr();
        let decompressed = Decompressor::None.decompress_block(block).unwrap();
        assert_eq!(decompressed.as_slice().as_ptr(), ptr);
    }
}

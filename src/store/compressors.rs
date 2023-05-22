use std::io;

use serde::{Deserialize, Deserializer, Serialize};

pub trait StoreCompressor {
    fn compress(&self, uncompressed: &[u8], compressed: &mut Vec<u8>) -> io::Result<()>;
    fn decompress(&self, compressed: &[u8], decompressed: &mut Vec<u8>) -> io::Result<()>;
    fn get_compressor_id() -> u8;
}

/// Compressor can be used on `IndexSettings` to choose
/// the compressor used to compress the doc store.
///
/// The default is Lz4Block, but also depends on the enabled feature flags.
#[derive(Clone, Debug, Copy, PartialEq, Eq)]
pub enum Compressor {
    /// No compression
    None,
    /// Use the lz4 compressor (block format)
    Lz4,
    /// Use the brotli compressor
    Brotli,
    /// Use the snap compressor
    Snappy,
    /// Use the zstd compressor
    Zstd(ZstdCompressor),
}

impl Serialize for Compressor {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: serde::Serializer {
        match *self {
            Compressor::None => serializer.serialize_str("none"),
            Compressor::Lz4 => serializer.serialize_str("lz4"),
            Compressor::Brotli => serializer.serialize_str("brotli"),
            Compressor::Snappy => serializer.serialize_str("snappy"),
            Compressor::Zstd(zstd) => serializer.serialize_str(&zstd.ser_to_string()),
        }
    }
}

impl<'de> Deserialize<'de> for Compressor {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: Deserializer<'de> {
        let buf = String::deserialize(deserializer)?;
        let compressor = match buf.as_str() {
            "none" => Compressor::None,
            "lz4" => Compressor::Lz4,
            "brotli" => Compressor::Brotli,
            "snappy" => Compressor::Snappy,
            _ => {
                if buf.starts_with("zstd") {
                    Compressor::Zstd(
                        ZstdCompressor::deser_from_str(&buf).map_err(serde::de::Error::custom)?,
                    )
                } else {
                    return Err(serde::de::Error::unknown_variant(
                        &buf,
                        &[
                            "none",
                            "lz4",
                            "brotli",
                            "snappy",
                            "zstd",
                            "zstd(compression_level=5)",
                        ],
                    ));
                }
            }
        };

        Ok(compressor)
    }
}

#[derive(Clone, Default, Debug, Copy, PartialEq, Eq, Serialize, Deserialize)]
/// The Zstd compressor, with optional compression level.
pub struct ZstdCompressor {
    /// The compression level, if unset defaults to zstd::DEFAULT_COMPRESSION_LEVEL = 3
    pub compression_level: Option<i32>,
}

impl ZstdCompressor {
    fn deser_from_str(val: &str) -> Result<ZstdCompressor, String> {
        if !val.starts_with("zstd") {
            return Err(format!("needs to start with zstd, but got {val}"));
        }
        if val == "zstd" {
            return Ok(ZstdCompressor::default());
        }
        let options = &val["zstd".len() + 1..val.len() - 1];

        let mut compressor = ZstdCompressor::default();
        for option in options.split(',') {
            let (opt_name, value) = options
                .split_once('=')
                .ok_or_else(|| format!("no '=' found in option {option:?}"))?;

            match opt_name {
                "compression_level" => {
                    let value = value.parse::<i32>().map_err(|err| {
                        format!("Could not parse value {value} of option {opt_name}, e: {err}")
                    })?;
                    if value >= 15 {
                        warn!(
                            "High zstd compression level detected: {:?}. High compression levels \
                             (>=15) are slow and will limit indexing speed.",
                            value
                        )
                    }
                    compressor.compression_level = Some(value);
                }
                _ => {
                    return Err(format!("unknown zstd option {opt_name:?}"));
                }
            }
        }
        Ok(compressor)
    }
    fn ser_to_string(&self) -> String {
        if let Some(compression_level) = self.compression_level {
            format!("zstd(compression_level={compression_level})")
        } else {
            "zstd".to_string()
        }
    }
}

impl Default for Compressor {
    fn default() -> Self {
        if cfg!(feature = "lz4-compression") {
            Compressor::Lz4
        } else if cfg!(feature = "brotli-compression") {
            Compressor::Brotli
        } else if cfg!(feature = "snappy-compression") {
            Compressor::Snappy
        } else if cfg!(feature = "zstd-compression") {
            Compressor::Zstd(ZstdCompressor::default())
        } else {
            Compressor::None
        }
    }
}

impl Compressor {
    #[inline]
    pub(crate) fn compress_into(
        &self,
        uncompressed: &[u8],
        compressed: &mut Vec<u8>,
    ) -> io::Result<()> {
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
            Self::Zstd(_zstd_compressor) => {
                #[cfg(feature = "zstd-compression")]
                {
                    super::compression_zstd_block::compress(
                        uncompressed,
                        compressed,
                        _zstd_compressor.compression_level,
                    )
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

    #[test]
    fn zstd_serde_roundtrip() {
        let compressor = ZstdCompressor {
            compression_level: Some(15),
        };

        assert_eq!(
            ZstdCompressor::deser_from_str(&compressor.ser_to_string()).unwrap(),
            compressor
        );

        assert_eq!(
            ZstdCompressor::deser_from_str(&ZstdCompressor::default().ser_to_string()).unwrap(),
            ZstdCompressor::default()
        );
    }

    #[test]
    fn deser_zstd_test() {
        assert_eq!(
            ZstdCompressor::deser_from_str("zstd").unwrap(),
            ZstdCompressor::default()
        );

        assert!(ZstdCompressor::deser_from_str("zzstd").is_err());
        assert!(ZstdCompressor::deser_from_str("zzstd()").is_err());
        assert_eq!(
            ZstdCompressor::deser_from_str("zstd(compression_level=15)").unwrap(),
            ZstdCompressor {
                compression_level: Some(15)
            }
        );
        assert_eq!(
            ZstdCompressor::deser_from_str("zstd(compresion_level=15)").unwrap_err(),
            "unknown zstd option \"compresion_level\""
        );
        assert_eq!(
            ZstdCompressor::deser_from_str("zstd(compression_level->2)").unwrap_err(),
            "no '=' found in option \"compression_level->2\""
        );
        assert_eq!(
            ZstdCompressor::deser_from_str("zstd(compression_level=over9000)").unwrap_err(),
            "Could not parse value over9000 of option compression_level, e: invalid digit found \
             in string"
        );
    }
}

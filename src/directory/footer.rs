use crate::common::{BinarySerializable, CountingWriter, FixedSize, HasLen, VInt};
use crate::directory::error::Incompatibility;
use crate::directory::FileSlice;
use crate::directory::{AntiCallToken, TerminatingWrite};
use crate::Version;
use crc32fast::Hasher;
use std::io;
use std::io::Write;

const FOOTER_MAX_LEN: usize = 10_000;

type CrcHashU32 = u32;

#[derive(Debug, Clone, PartialEq)]
pub struct Footer {
    pub version: Version,
    pub meta: String,
    pub versioned_footer: VersionedFooter,
}

/// Serialises the footer to a byte-array
/// - versioned_footer_len : 4 bytes
///-  versioned_footer: variable bytes
/// - meta_len: 4 bytes
/// - meta: variable bytes
/// - version_len: 4 bytes
/// - version json: variable bytes
impl BinarySerializable for Footer {
    fn serialize<W: io::Write>(&self, writer: &mut W) -> io::Result<()> {
        BinarySerializable::serialize(&self.versioned_footer, writer)?;
        BinarySerializable::serialize(&self.meta, writer)?;
        let version_string =
            serde_json::to_string(&self.version).map_err(|_err| io::ErrorKind::InvalidInput)?;
        BinarySerializable::serialize(&version_string, writer)?;
        Ok(())
    }

    fn deserialize<R: io::Read>(reader: &mut R) -> io::Result<Self> {
        let versioned_footer = VersionedFooter::deserialize(reader)?;
        let meta = String::deserialize(reader)?;
        let version_json = String::deserialize(reader)?;
        let version = serde_json::from_str(&version_json)?;
        Ok(Footer {
            version,
            meta,
            versioned_footer,
        })
    }
}

impl Footer {
    pub fn new(versioned_footer: VersionedFooter) -> Self {
        let version = crate::VERSION.clone();
        let meta = version.to_string();
        Footer {
            version,
            meta,
            versioned_footer,
        }
    }

    pub fn append_footer<W: io::Write>(&self, mut write: &mut W) -> io::Result<()> {
        let mut counting_write = CountingWriter::wrap(&mut write);
        self.serialize(&mut counting_write)?;
        let written_len = counting_write.written_bytes();
        (written_len as u32).serialize(write)?;
        Ok(())
    }

    pub fn extract_footer(file: FileSlice) -> io::Result<(Footer, FileSlice)> {
        if file.len() < 4 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!(
                    "File corrupted. The file is smaller than 4 bytes (len={}).",
                    file.len()
                ),
            ));
        }
        let (body_footer, footer_len_file) = file.split_from_end(u32::SIZE_IN_BYTES);
        let mut footer_len_bytes = footer_len_file.read_bytes()?;
        let footer_len = u32::deserialize(&mut footer_len_bytes)? as usize;
        let (body, footer) = body_footer.split_from_end(footer_len);
        let mut footer_bytes = footer.read_bytes()?;
        let footer = Footer::deserialize(&mut footer_bytes)?;
        Ok((footer, body))
    }

    /// Confirms that the index will be read correctly by this version of tantivy
    /// Has to be called after `extract_footer` to make sure it's not accessing uninitialised memory
    pub fn is_compatible(&self) -> Result<(), Incompatibility> {
        let library_version = crate::version();
        match &self.versioned_footer {
            VersionedFooter::V1 {
                crc32: _crc,
                store_compression,
            } => {
                if &library_version.store_compression != store_compression {
                    return Err(Incompatibility::CompressionMismatch {
                        library_compression_format: library_version.store_compression.to_string(),
                        index_compression_format: store_compression.to_string(),
                    });
                }
                Ok(())
            }
            VersionedFooter::V2 {
                crc32: _crc,
                store_compression,
            } => {
                if &library_version.store_compression != store_compression {
                    return Err(Incompatibility::CompressionMismatch {
                        library_compression_format: library_version.store_compression.to_string(),
                        index_compression_format: store_compression.to_string(),
                    });
                }
                Ok(())
            }
            VersionedFooter::V3 {
                crc32: _crc,
                store_compression,
            } => {
                if &library_version.store_compression != store_compression {
                    return Err(Incompatibility::CompressionMismatch {
                        library_compression_format: library_version.store_compression.to_string(),
                        index_compression_format: store_compression.to_string(),
                    });
                }
                Ok(())
            }
            VersionedFooter::UnknownVersion => Err(Incompatibility::IndexMismatch {
                library_version: library_version.clone(),
                index_version: self.version.clone(),
            }),
        }
    }
}

/// Footer that includes a crc32 hash that enables us to checksum files in the index
#[derive(Debug, Clone, PartialEq)]
pub enum VersionedFooter {
    UnknownVersion,
    V1 {
        crc32: CrcHashU32,
        store_compression: String,
    },
    // Introduction of the Block WAND information.
    V2 {
        crc32: CrcHashU32,
        store_compression: String,
    },
    // Block wand max termfred on 1 byte
    V3 {
        crc32: CrcHashU32,
        store_compression: String,
    },
}

impl BinarySerializable for VersionedFooter {
    fn serialize<W: io::Write>(&self, writer: &mut W) -> io::Result<()> {
        let mut buf = Vec::new();
        match self {
            VersionedFooter::V3 {
                crc32,
                store_compression: compression,
            } => {
                // Serializes a valid `VersionedFooter` or panics if the version is unknown
                // [   version    |   crc_hash  | compression_mode ]
                // [    0..4      |     4..8    |     variable     ]
                BinarySerializable::serialize(&3u32, &mut buf)?;
                BinarySerializable::serialize(crc32, &mut buf)?;
                BinarySerializable::serialize(compression, &mut buf)?;
            }
            VersionedFooter::V2 { .. }
            | VersionedFooter::V1 { .. }
            | VersionedFooter::UnknownVersion => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "Cannot serialize an unknown versioned footer ",
                ));
            }
        }
        BinarySerializable::serialize(&VInt(buf.len() as u64), writer)?;
        assert!(buf.len() <= FOOTER_MAX_LEN);
        writer.write_all(&buf[..])?;
        Ok(())
    }

    fn deserialize<R: io::Read>(reader: &mut R) -> io::Result<Self> {
        let len = VInt::deserialize(reader)?.0 as usize;
        if len > FOOTER_MAX_LEN {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Footer seems invalid as it suggests a footer len of {}. File is corrupted, \
            or the index was created with a different & old version of tantivy.",
                    len
                ),
            ));
        }
        let mut buf = vec![0u8; len];
        reader.read_exact(&mut buf[..])?;
        let mut cursor = &buf[..];
        let version = u32::deserialize(&mut cursor)?;
        if version > 3 {
            return Ok(VersionedFooter::UnknownVersion);
        }
        let crc32 = u32::deserialize(&mut cursor)?;
        let store_compression = String::deserialize(&mut cursor)?;
        Ok(if version == 1 {
            VersionedFooter::V1 {
                crc32,
                store_compression,
            }
        } else if version == 2 {
            VersionedFooter::V2 {
                crc32,
                store_compression,
            }
        } else {
            assert_eq!(version, 3);
            VersionedFooter::V3 {
                crc32,
                store_compression,
            }
        })
    }
}

impl VersionedFooter {
    pub fn crc(&self) -> Option<CrcHashU32> {
        match self {
            VersionedFooter::V3 { crc32, .. } => Some(*crc32),
            VersionedFooter::V2 { crc32, .. } => Some(*crc32),
            VersionedFooter::V1 { crc32, .. } => Some(*crc32),
            VersionedFooter::UnknownVersion { .. } => None,
        }
    }
}

pub(crate) struct FooterProxy<W: TerminatingWrite> {
    /// always Some except after terminate call
    hasher: Option<Hasher>,
    /// always Some except after terminate call
    writer: Option<W>,
}

impl<W: TerminatingWrite> FooterProxy<W> {
    pub fn new(writer: W) -> Self {
        FooterProxy {
            hasher: Some(Hasher::new()),
            writer: Some(writer),
        }
    }
}

impl<W: TerminatingWrite> Write for FooterProxy<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let count = self.writer.as_mut().unwrap().write(buf)?;
        self.hasher.as_mut().unwrap().update(&buf[..count]);
        Ok(count)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.writer.as_mut().unwrap().flush()
    }
}

impl<W: TerminatingWrite> TerminatingWrite for FooterProxy<W> {
    fn terminate_ref(&mut self, _: AntiCallToken) -> io::Result<()> {
        let crc32 = self.hasher.take().unwrap().finalize();
        let footer = Footer::new(VersionedFooter::V3 {
            crc32,
            store_compression: crate::store::COMPRESSION.to_string(),
        });
        let mut writer = self.writer.take().unwrap();
        footer.append_footer(&mut writer)?;
        writer.terminate()
    }
}

#[cfg(test)]
mod tests {

    use super::CrcHashU32;
    use super::FooterProxy;
    use crate::common::{BinarySerializable, VInt};
    use crate::directory::footer::{Footer, VersionedFooter};
    use crate::directory::TerminatingWrite;
    use byteorder::{ByteOrder, LittleEndian};
    use regex::Regex;
    use std::io;

    #[test]
    fn test_versioned_footer() {
        let mut vec = Vec::new();
        let footer_proxy = FooterProxy::new(&mut vec);
        assert!(footer_proxy.terminate().is_ok());
        if crate::store::COMPRESSION == "lz4" {
            assert_eq!(vec.len(), 158);
        } else {
            assert_eq!(vec.len(), 167);
        }
        let footer = Footer::deserialize(&mut &vec[..]).unwrap();
        assert!(matches!(
           footer.versioned_footer,
           VersionedFooter::V3 { store_compression, .. }
           if store_compression == crate::store::COMPRESSION
        ));
        assert_eq!(&footer.version, crate::version());
    }

    #[test]
    fn test_serialize_deserialize_footer() {
        let mut buffer = Vec::new();
        let crc32 = 123456u32;
        let footer: Footer = Footer::new(VersionedFooter::V3 {
            crc32,
            store_compression: "lz4".to_string(),
        });
        footer.serialize(&mut buffer).unwrap();
        let footer_deser = Footer::deserialize(&mut &buffer[..]).unwrap();
        assert_eq!(footer_deser, footer);
    }

    #[test]
    fn footer_length() {
        let crc32 = 1111111u32;
        let versioned_footer = VersionedFooter::V3 {
            crc32,
            store_compression: "lz4".to_string(),
        };
        let mut buf = Vec::new();
        versioned_footer.serialize(&mut buf).unwrap();
        assert_eq!(buf.len(), 13);
        let footer = Footer::new(versioned_footer);
        let regex_ptn = Regex::new(
            "tantivy v[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.{0,10}, index_format v[0-9]{1,5}",
        )
        .unwrap();
        assert!(regex_ptn.is_match(&footer.meta));
    }

    #[test]
    fn versioned_footer_from_bytes() {
        let v_footer_bytes = vec![
            // versionned footer length
            12 | 128,
            // index format version
            3,
            0,
            0,
            0,
            // crc 32
            12,
            35,
            89,
            18,
            // compression format
            3 | 128,
            b'l',
            b'z',
            b'4',
        ];
        let mut cursor = &v_footer_bytes[..];
        let versioned_footer = VersionedFooter::deserialize(&mut cursor).unwrap();
        assert!(cursor.is_empty());
        let expected_crc: u32 = LittleEndian::read_u32(&v_footer_bytes[5..9]) as CrcHashU32;
        let expected_versioned_footer: VersionedFooter = VersionedFooter::V3 {
            crc32: expected_crc,
            store_compression: "lz4".to_string(),
        };
        assert_eq!(versioned_footer, expected_versioned_footer);
        let mut buffer = Vec::new();
        assert!(versioned_footer.serialize(&mut buffer).is_ok());
        assert_eq!(&v_footer_bytes[..], &buffer[..]);
    }

    #[test]
    fn versioned_footer_panic() {
        let v_footer_bytes = vec![6u8 | 128u8, 3u8, 0u8, 0u8, 1u8, 0u8, 0u8];
        let mut b = &v_footer_bytes[..];
        let versioned_footer = VersionedFooter::deserialize(&mut b).unwrap();
        assert!(b.is_empty());
        let expected_versioned_footer = VersionedFooter::UnknownVersion;
        assert_eq!(versioned_footer, expected_versioned_footer);
        let mut buf = Vec::new();
        assert!(versioned_footer.serialize(&mut buf).is_err());
    }

    #[test]
    #[cfg(not(feature = "lz4"))]
    fn compression_mismatch() {
        let crc32 = 1111111u32;
        let versioned_footer = VersionedFooter::V1 {
            crc32,
            store_compression: "lz4".to_string(),
        };
        let footer = Footer::new(versioned_footer);
        let res = footer.is_compatible();
        assert!(res.is_err());
    }

    #[test]
    fn test_deserialize_too_large_footer() {
        let mut buf = vec![];
        assert!(FooterProxy::new(&mut buf).terminate().is_ok());
        let mut long_len_buf = [0u8; 10];
        let num_bytes = VInt(super::FOOTER_MAX_LEN as u64 + 1u64).serialize_into(&mut long_len_buf);
        buf[0..num_bytes].copy_from_slice(&long_len_buf[..num_bytes]);
        let err = Footer::deserialize(&mut &buf[..]).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert_eq!(
            err.to_string(),
            "Footer seems invalid as it suggests a footer len of 10001. File is corrupted, \
            or the index was created with a different & old version of tantivy."
        );
    }
}

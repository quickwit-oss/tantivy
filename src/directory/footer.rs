use crate::common::{BinarySerializable, CountingWriter, FixedSize, VInt};
use crate::directory::read_only_source::ReadOnlySource;
use crate::directory::{AntiCallToken, TerminatingWrite};
use crate::error::Incompatibility;
use crate::error::TantivyError::IncompatibleIndex;
use crate::error::TantivyError;
use crate::Version;
use byteorder::{ByteOrder, LittleEndian, WriteBytesExt};
use crc32fast::Hasher;
use std::io;
use std::io::Write;

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
        write.write_u32::<LittleEndian>(written_len as u32)?;
        Ok(())
    }

    pub fn extract_footer(source: ReadOnlySource) -> Result<(Footer, ReadOnlySource), io::Error> {
        if source.len() < 4 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!(
                    "File corrupted. The file is smaller than 4 bytes (len={}).",
                    source.len()
                ),
            ));
        }
        let (body_footer, footer_len_bytes) = source.split_from_end(u32::SIZE_IN_BYTES);
        let footer_len = LittleEndian::read_u32(footer_len_bytes.as_slice()) as usize;
        let body_len = body_footer.len() - footer_len;
        let (body, footer_data) = body_footer.split(body_len);
        let mut cursor = footer_data.as_slice();
        let footer = Footer::deserialize(&mut cursor)?;
        Ok((footer, body))
    }

    /// Confirms that the index will be read correctly by this version of tantivy
    /// Has to be called after `extract_footer` to make sure it's not accessing uninitialised memory
    pub fn is_compatible(&self) -> Result<bool, TantivyError> {
        let library = &*crate::VERSION;
        match &self.versioned_footer {
            VersionedFooter::V1 {
                crc32: _,
                compression,
            } => {
                // V1 means should be equal with 1
                if library.index_format_version != 1u32 {
                    if &library.store_compression != compression {
                        return Err(IncompatibleIndex(
                            Incompatibility::CompressionAndIndexMismatch {
                                library: library.clone(),
                                index: self.version.clone(),
                            },
                        ));
                    } else {
                        return Err(IncompatibleIndex(Incompatibility::IndexMismatch {
                            library: library.clone(),
                            index: self.version.clone(),
                        }));
                    }
                }

                if &library.store_compression != compression {
                    return Err(IncompatibleIndex(Incompatibility::CompressionMismatch {
                        library: library.clone(),
                        index: self.version.clone(),
                    }));
                }

                Ok(true)
            }
            // TODO - discuss if we should add another error type
            VersionedFooter::UnknownVersion { version: _ } => return Ok(false),
        }
    }
}

/// Footer that includes a crc32 hash that enables us to checksum files in the index
#[derive(Debug, Clone, PartialEq)]
pub enum VersionedFooter {
    UnknownVersion {
        version: u32,
    },
    V1 {
        crc32: CrcHashU32,
        compression: String,
    },
}

impl BinarySerializable for VersionedFooter {
    fn serialize<W: io::Write>(&self, writer: &mut W) -> io::Result<()> {
        let mut buf = Vec::new();
        BinarySerializable::serialize(&self.version(), &mut buf)?;
        match self {
            VersionedFooter::V1 { crc32, compression } => {
                // Serializes a valid `VersionedFooter` or panics if the version is unknown
                // [   version    |   crc_hash  | compression_mode ]
                // [    0..4      |     4..8    |       8.. 12     ]
                BinarySerializable::serialize(crc32, &mut buf)?;
                BinarySerializable::serialize(compression, &mut buf)?;
            }
            VersionedFooter::UnknownVersion { version: _ } => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "Cannot serialize an unknown versionned footer ",
                ));
            }
        }
        BinarySerializable::serialize(&VInt(buf.len() as u64), writer)?;
        writer.write_all(&buf[..])?;
        Ok(())
    }

    fn deserialize<R: io::Read>(reader: &mut R) -> io::Result<Self> {
        let len = VInt::deserialize(reader)?.0 as usize;
        let mut buf = vec![0u8; len];
        reader.read_exact(&mut buf[..])?;
        let mut cursor = &buf[..];
        let version = u32::deserialize(&mut cursor)?;
        if version == 1 {
            let crc32 = u32::deserialize(&mut cursor)?;
            let compression = String::deserialize(&mut cursor)?;
            Ok(VersionedFooter::V1 { crc32, compression })
        } else {
            Ok(VersionedFooter::UnknownVersion { version })
        }
    }
}

impl VersionedFooter {
    pub fn version(&self) -> u32 {
        match self {
            VersionedFooter::V1 {
                crc32: _,
                compression: _,
            } => 1u32,
            VersionedFooter::UnknownVersion { version, .. } => *version,
        }
    }

    pub fn crc(&self) -> Option<CrcHashU32> {
        match self {
            VersionedFooter::V1 {
                crc32,
                compression: _,
            } => Some(*crc32),
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
        let footer = Footer::new(VersionedFooter::V1 {
            crc32,
            compression: crate::store::COMPRESSION.to_string(),
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
    use crate::common::BinarySerializable;
    use crate::directory::footer::{Footer, VersionedFooter};
    use crate::directory::TerminatingWrite;
    use byteorder::{ByteOrder, LittleEndian};
    use regex::Regex;

    #[test]
    fn test_footer_version() {
        let mut vec = Vec::new();
        let footer_proxy = FooterProxy::new(&mut vec);
        assert!(footer_proxy.terminate().is_ok());
        assert_eq!(vec.len(), 161);
        let footer = Footer::deserialize(&mut &vec[..]).unwrap();
        assert_eq!(
            footer.versioned_footer.version(),
            crate::INDEX_FORMAT_VERSION
        );
        assert_eq!(&footer.version, crate::version());
    }

    #[test]
    fn test_serialize_deserialize_footer() {
        let mut buffer = Vec::new();
        let crc32 = 123456u32;
        let footer: Footer = Footer::new(VersionedFooter::V1 {
            crc32,
            compression: "lz4 ".to_string(),
        });
        footer.serialize(&mut buffer).unwrap();
        let footer_deser = Footer::deserialize(&mut &buffer[..]).unwrap();
        assert_eq!(footer_deser, footer);
    }

    #[test]
    fn footer_length() {
        // test to make sure the ascii art in the doc-strings is correct
        let crc32 = 1111111u32;
        let versioned_footer = VersionedFooter::V1 {
            crc32,
            compression: "lz4 ".to_string(),
        };
        let mut buf = Vec::new();
        versioned_footer.serialize(&mut buf).unwrap();
        assert_eq!(buf.len(), 14);
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
            1,
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
        let expected_versioned_footer: VersionedFooter = VersionedFooter::V1 {
            crc32: expected_crc,
            compression: "lz4".to_string(),
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
        let expected_versioned_footer = VersionedFooter::UnknownVersion {
            version: 16_777_219u32,
        };
        assert_eq!(versioned_footer, expected_versioned_footer);
        let mut buf = Vec::new();
        assert!(versioned_footer.serialize(&mut buf).is_err());
    }
}

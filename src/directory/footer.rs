use crate::directory::read_only_source::ReadOnlySource;
use crate::directory::{AntiCallToken, TerminatingWrite};
use crate::Error;
use byteorder::{ByteOrder, LittleEndian};
use crc32fast::Hasher;
use std::io;
use std::io::Write;

const COMMON_FOOTER_SIZE: usize = 4 * 5;

type CrcHashU32 = u32;

#[derive(Debug, Clone, PartialEq)]
pub struct Footer {
    pub tantivy_version: (u32, u32, u32),
    pub meta: String,
    pub versioned_footer: VersionedFooter,
}

impl Footer {
    pub fn new(versioned_footer: VersionedFooter) -> Self {
        let tantivy_version = (
            env!("CARGO_PKG_VERSION_MAJOR").parse().unwrap(),
            env!("CARGO_PKG_VERSION_MINOR").parse().unwrap(),
            env!("CARGO_PKG_VERSION_PATCH").parse().unwrap(),
        );
        Footer {
            tantivy_version,
            meta: format!(
                "tantivy v{}.{}.{}, index_format v{}",
                tantivy_version.0,
                tantivy_version.1,
                tantivy_version.2,
                versioned_footer.version()
            ),
            versioned_footer,
        }
    }

    /// Serialises the footer to a byte-array
    /// [      versioned_footer     |     meta      |    common_footer ]
    /// [           0..8            |     8..32     |        32..52    ]
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut res = self.versioned_footer.to_bytes();
        res.extend_from_slice(self.meta.as_bytes());
        let len = res.len();
        res.resize(len + COMMON_FOOTER_SIZE, 0);
        let mut common_footer = &mut res[len..];
        LittleEndian::write_u32(&mut common_footer, self.meta.len() as u32);
        LittleEndian::write_u32(&mut common_footer[4..], self.tantivy_version.0);
        LittleEndian::write_u32(&mut common_footer[8..], self.tantivy_version.1);
        LittleEndian::write_u32(&mut common_footer[12..], self.tantivy_version.2);
        LittleEndian::write_u32(&mut common_footer[16..], (len + COMMON_FOOTER_SIZE) as u32);
        res
    }

    fn from_bytes(data: &[u8]) -> Result<Self, io::Error> {
        let len = data.len();
        if len < COMMON_FOOTER_SIZE + 4 {
            // 4 bytes for index version, stored in versioned footer
            return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    format!("File corrupted. The footer len must be over 24, while the entire file len is {}", len)
                    )
            );
        }

        let size = LittleEndian::read_u32(&data[len - 4..]) as usize;
        if len < size as usize {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!(
                    "The footer len is {}, while the entire file len is {}. \
                     Your index is either corrupted or was built using a tantivy version\
                     anterior to 0.11.",
                    size, len
                ),
            ));
        }
        let footer = &data[len - size as usize..];
        let meta_len = LittleEndian::read_u32(&footer[size - COMMON_FOOTER_SIZE..]) as usize;
        let tantivy_major = LittleEndian::read_u32(&footer[size - 16..]);
        let tantivy_minor = LittleEndian::read_u32(&footer[size - 12..]);
        let tantivy_patch = LittleEndian::read_u32(&footer[size - 8..]);
        Ok(Footer {
            tantivy_version: (tantivy_major, tantivy_minor, tantivy_patch),
            meta: String::from_utf8_lossy(
                &footer[size - meta_len - COMMON_FOOTER_SIZE..size - COMMON_FOOTER_SIZE],
            )
            .into_owned(),
            versioned_footer: VersionedFooter::from_bytes(
                &footer[..size - meta_len - COMMON_FOOTER_SIZE],
            )?,
        })
    }

    pub fn extract_footer(source: ReadOnlySource) -> Result<(Footer, ReadOnlySource), io::Error> {
        let footer = Footer::from_bytes(source.as_slice())?;
        let reader = source.slice_to(source.as_slice().len() - footer.size());
        Ok((footer, reader))
    }

    pub fn size(&self) -> usize {
        self.versioned_footer.size() as usize + self.meta.len() + COMMON_FOOTER_SIZE
    }

    /// Confirms that the index will be read correctly by this version of tantivy
    /// Has to be called after `extract_footer` to make sure it's not accessing uninitialised memory
    pub fn is_compatible(&self) -> Result<bool, Error> {
        // Leaving this here to defensively catch when we add a new version of VersionedFooter
        // which will make the hardcoded logic below obsolete
        #[allow(unused_variables)]
        {
            match &self.versioned_footer {
                VersionedFooter::V0(_) => {}
                VersionedFooter::UnknownVersion { version, size } => {}
            }
        }
        // TODO: currently hardcoded, because we only support one version - V0, which always returns 0
        let index_version_supported_by_library = 0;

        let version_in_footer = self.versioned_footer.version();
        if version_in_footer == index_version_supported_by_library {
            Ok(true)
        } else {
            Err(Error::IncompatibleIndex(format!("The found index version: {} is incompatible with this library that can only support {}", version_in_footer, index_version_supported_by_library)))
        }
    }
}

/// Footer that includes a crc32 hash that enables us to checksum files in the index
#[derive(Debug, Clone, PartialEq)]
pub enum VersionedFooter {
    UnknownVersion { version: u32, size: u32 },
    V0(CrcHashU32), // crc
}

impl VersionedFooter {
    /// Serializes a valid `VersionedFooter` or panics if the version is unknown
    /// [   version    |   crc_hash  ]
    /// [    0..4      |     4..8    ]
    // TODO: add a byte-flag to mark compression used
    // 1st byte = 1 if compressed with `snap` (default), 0 - if compressed with lz4
    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            VersionedFooter::V0(crc) => {
                let mut buf = [0u8; 8];
                LittleEndian::write_u32(&mut buf[0..4], 0);
                LittleEndian::write_u32(&mut buf[4..8], *crc);
                buf.to_vec()
            }
            VersionedFooter::UnknownVersion { .. } => {
                panic!("Unsupported index should never get serialized");
            }
        }
    }

    pub(crate) fn from_bytes(footer: &[u8]) -> Result<Self, io::Error> {
        if footer.len() < 4 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Footer should be more than 4 bytes.",
            ));
        }
        let version = LittleEndian::read_u32(footer);
        match version {
            0 => {
                if footer.len() != 8 {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        format!(
                            "File corrupted. The versioned footer len is {}, while it should be 8",
                            footer.len()
                        ),
                    ));
                }
                Ok(VersionedFooter::V0(LittleEndian::read_u32(&footer[4..])))
            }
            version => Ok(VersionedFooter::UnknownVersion {
                version,
                size: footer.len() as u32,
            }),
        }
    }

    pub fn size(&self) -> u32 {
        match self {
            VersionedFooter::V0(_) => 8,
            VersionedFooter::UnknownVersion { size, .. } => *size,
        }
    }

    pub fn version(&self) -> u32 {
        match self {
            VersionedFooter::V0(_) => 0,
            VersionedFooter::UnknownVersion { version, .. } => *version,
        }
    }

    pub fn crc(&self) -> Option<CrcHashU32> {
        match self {
            VersionedFooter::V0(crc) => Some(*crc),
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
        let crc = self.hasher.take().unwrap().finalize();
        let footer = Footer::new(VersionedFooter::V0(crc)).to_bytes();
        let mut writer = self.writer.take().unwrap();
        writer.write_all(&footer)?;
        writer.terminate()
    }
}

#[cfg(test)]
mod tests {

    use super::CrcHashU32;
    use crate::directory::footer::{Footer, VersionedFooter};
    use byteorder::{ByteOrder, LittleEndian};
    use regex::Regex;

    #[test]
    fn test_serialize_deserialize_footer() {
        let crc = 123456;
        let footer = Footer::new(VersionedFooter::V0(crc));
        let footer_bytes = footer.to_bytes();
        assert_eq!(Footer::from_bytes(&footer_bytes).unwrap(), footer);
    }

    #[test]
    fn footer_length() {
        // test to make sure the ascii art in the doc-strings is correct
        let crc = 1111111 as u32;
        let versioned_footer = VersionedFooter::V0(crc);
        assert_eq!(versioned_footer.size(), 8);
        let footer = Footer::new(versioned_footer);
        let regex_ptn = Regex::new(
            "tantivy v[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.{0,10}, index_format v[0-9]{1,5}",
        )
        .unwrap();
        assert!(regex_ptn.find(&footer.meta).is_some());
    }

    #[test]
    fn versioned_footer_from_bytes() {
        let v_footer_bytes = vec![0, 0, 0, 0, 12, 35, 89, 18];
        let versioned_footer = VersionedFooter::from_bytes(&v_footer_bytes).unwrap();
        let expected_crc = LittleEndian::read_u32(&v_footer_bytes[4..]) as CrcHashU32;
        let expected_versioned_footer =
            VersionedFooter::V0(LittleEndian::read_u32(&[12, 35, 89, 18]));

        assert_eq!(versioned_footer, expected_versioned_footer);
        assert_eq!(versioned_footer.crc(), Some(expected_crc));

        assert_eq!(versioned_footer.to_bytes(), v_footer_bytes);
    }

    #[should_panic(expected = "Unsupported index should never get serialized")]
    #[test]
    fn versioned_footer_panic() {
        let v_footer_bytes = vec![1; 8];
        let versioned_footer = VersionedFooter::from_bytes(&v_footer_bytes).unwrap();
        let expected_version = LittleEndian::read_u32(&[1, 1, 1, 1]);
        let expected_versioned_footer = VersionedFooter::UnknownVersion {
            version: expected_version,
            size: v_footer_bytes.len() as u32,
        };
        assert_eq!(versioned_footer, expected_versioned_footer);
        versioned_footer.to_bytes();
    }
}

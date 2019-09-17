use crate::directory::read_only_source::ReadOnlySource;
use crate::directory::{AntiCallToken, TerminatingWrite};
use byteorder::{ByteOrder, LittleEndian};
use crc32fast::Hasher;
use std::io;
use std::io::Write;

const COMMON_FOOTER_SIZE: usize = 4 * 5;

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
                "tantivy {}.{}.{}, index v{}",
                tantivy_version.0,
                tantivy_version.1,
                tantivy_version.2,
                versioned_footer.version()
            ),
            versioned_footer,
        }
    }

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

    pub fn from_bytes(data: &[u8]) -> Result<Self, io::Error> {
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
                    "File corrupted. The footer len is {}, while the entire file len is {}",
                    size, len
                ),
            ));
        }
        let footer = &data[len - size as usize..];
        let meta_len = LittleEndian::read_u32(&footer[size - 20..]) as usize;
        let tantivy_major = LittleEndian::read_u32(&footer[size - 16..]);
        let tantivy_minor = LittleEndian::read_u32(&footer[size - 12..]);
        let tantivy_patch = LittleEndian::read_u32(&footer[size - 8..]);
        Ok(Footer {
            tantivy_version: (tantivy_major, tantivy_minor, tantivy_patch),
            meta: String::from_utf8_lossy(&footer[size - meta_len - 20..size - 20]).into_owned(),
            versioned_footer: VersionedFooter::from_bytes(&footer[..size - meta_len - 20])?,
        })
    }

    pub fn extract_footer(source: ReadOnlySource) -> Result<(Footer, ReadOnlySource), io::Error> {
        let footer = Footer::from_bytes(source.as_slice())?;
        let reader = source.slice_to(source.as_slice().len() - footer.size());
        Ok((footer, reader))
    }

    pub fn size(&self) -> usize {
        self.versioned_footer.size() as usize + self.meta.len() + 20
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum VersionedFooter {
    UnknownVersion { version: u32, size: u32 },
    V0(u32), // crc
}

impl VersionedFooter {
    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            Self::V0(crc) => {
                let mut res = vec![0; 8];
                LittleEndian::write_u32(&mut res, 0);
                LittleEndian::write_u32(&mut res[4..], *crc);
                res
            }
            Self::UnknownVersion { .. } => {
                panic!("Unsupported index should never get serialized");
            }
        }
    }

    pub fn from_bytes(footer: &[u8]) -> Result<Self, io::Error> {
        assert!(footer.len() >= 4);
        let version = LittleEndian::read_u32(footer);
        match version {
            0 => {
                if footer.len() == 8 {
                    Ok(Self::V0(LittleEndian::read_u32(&footer[4..])))
                } else {
                    Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        format!(
                            "File corrupted. The versioned footer len is {}, while it should be 8",
                            footer.len()
                        ),
                    ))
                }
            }
            version => Ok(Self::UnknownVersion {
                version,
                size: footer.len() as u32,
            }),
        }
    }

    pub fn size(&self) -> u32 {
        match self {
            Self::V0(_) => 8,
            Self::UnknownVersion { size, .. } => *size,
        }
    }

    pub fn version(&self) -> u32 {
        match self {
            Self::V0(_) => 0,
            Self::UnknownVersion { version, .. } => *version,
        }
    }

    pub fn crc(&self) -> Option<u32> {
        match self {
            Self::V0(crc) => Some(*crc),
            Self::UnknownVersion { .. } => None,
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
    use crate::directory::footer::{Footer, VersionedFooter};

    #[test]
    fn test_serialize_deserialize_footer() {
        let crc = 123456;
        let footer = Footer::new(VersionedFooter::V0(crc));
        let footer_bytes = footer.to_bytes();

        assert_eq!(Footer::from_bytes(&footer_bytes).unwrap(), footer);
    }
}

use std::io;
use std::io::Write;

use common::{BinarySerializable, HasLen};
use crc32fast::Hasher;

use crate::directory::error::Incompatibility;
use crate::directory::{AntiCallToken, FileSlice, TerminatingWrite};
use crate::{Version, INDEX_FORMAT_OLDEST_SUPPORTED_VERSION, INDEX_FORMAT_VERSION};

pub const FOOTER_LEN: usize = 24;

/// The magic byte of the footer to identify corruption
/// or an old version of the footer.
const FOOTER_MAGIC_NUMBER: u32 = 1337;

type CrcHashU32 = u32;

/// A Footer is appended to every file
#[derive(Debug, Clone, PartialEq)]
pub struct Footer {
    pub version: Version,
    pub crc: CrcHashU32,
}

impl Footer {
    pub fn new(crc: CrcHashU32) -> Self {
        let version = crate::VERSION.clone();
        Footer { version, crc }
    }

    pub fn crc(&self) -> CrcHashU32 {
        self.crc
    }
    pub fn append_footer<W: io::Write>(&self, write: &mut W) -> io::Result<()> {
        // 24 bytes
        BinarySerializable::serialize(&self.version.major, write)?;
        BinarySerializable::serialize(&self.version.minor, write)?;
        BinarySerializable::serialize(&self.version.patch, write)?;
        BinarySerializable::serialize(&self.version.index_format_version, write)?;
        BinarySerializable::serialize(&self.crc, write)?;
        BinarySerializable::serialize(&FOOTER_MAGIC_NUMBER, write)?;
        Ok(())
    }

    pub fn extract_footer(file: FileSlice) -> io::Result<(Footer, FileSlice)> {
        if file.len() < FOOTER_LEN {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!(
                    "File corrupted. The file is too small to contain the {FOOTER_LEN} byte \
                     footer (len={}).",
                    file.len()
                ),
            ));
        }

        let (body_slice, footer_slice) = file.split_from_end(FOOTER_LEN);
        let footer_bytes = footer_slice.read_bytes()?;
        let mut footer_bytes = footer_bytes.as_slice();

        let footer = Footer {
            version: Version {
                major: u32::deserialize(&mut footer_bytes)?,
                minor: u32::deserialize(&mut footer_bytes)?,
                patch: u32::deserialize(&mut footer_bytes)?,
                index_format_version: u32::deserialize(&mut footer_bytes)?,
            },
            crc: u32::deserialize(&mut footer_bytes)?,
        };

        let footer_magic_byte = u32::deserialize(&mut footer_bytes)?;
        if footer_magic_byte != FOOTER_MAGIC_NUMBER {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Footer magic byte mismatch. File corrupted or index was created using old an \
                 tantivy version which is not supported anymore. Please use tantivy 0.15 or above \
                 to recreate the index.",
            ));
        }

        Ok((footer, body_slice))
    }

    /// Confirms that the index will be read correctly by this version of tantivy
    /// Has to be called after `extract_footer` to make sure it's not accessing uninitialised memory
    #[allow(dead_code)]
    pub fn is_compatible(&self) -> Result<(), Incompatibility> {
        const SUPPORTED_INDEX_FORMAT_VERSION_RANGE: std::ops::RangeInclusive<u32> =
            INDEX_FORMAT_OLDEST_SUPPORTED_VERSION..=INDEX_FORMAT_VERSION;

        let library_version = crate::version();
        if !SUPPORTED_INDEX_FORMAT_VERSION_RANGE.contains(&self.version.index_format_version) {
            return Err(Incompatibility::IndexMismatch {
                library_version: library_version.clone(),
                index_version: self.version.clone(),
            });
        }
        Ok(())
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
        let footer = Footer::new(crc32);
        let mut writer = self.writer.take().unwrap();
        footer.append_footer(&mut writer)?;
        writer.terminate()
    }
}

#[cfg(test)]
mod tests {

    use std::io;
    use std::sync::Arc;

    use common::BinarySerializable;

    use crate::directory::footer::{Footer, FOOTER_MAGIC_NUMBER};
    use crate::directory::{FileSlice, OwnedBytes};

    #[test]
    fn test_deserialize_footer() {
        let mut buf: Vec<u8> = vec![];
        let footer = Footer::new(123);
        footer.append_footer(&mut buf).unwrap();
        let owned_bytes = OwnedBytes::new(buf);
        let fileslice = FileSlice::new(Arc::new(owned_bytes));
        let (footer_deser, _body) = Footer::extract_footer(fileslice).unwrap();
        assert_eq!(footer_deser.crc(), footer.crc());
    }
    #[test]
    fn test_deserialize_footer_missing_magic_byte() {
        let mut buf: Vec<u8> = vec![];
        BinarySerializable::serialize(&0_u32, &mut buf).unwrap();
        BinarySerializable::serialize(&0_u32, &mut buf).unwrap();
        BinarySerializable::serialize(&0_u32, &mut buf).unwrap();
        BinarySerializable::serialize(&0_u32, &mut buf).unwrap();
        BinarySerializable::serialize(&0_u32, &mut buf).unwrap();
        let wrong_magic_byte: u32 = 5555;
        BinarySerializable::serialize(&wrong_magic_byte, &mut buf).unwrap();

        let owned_bytes = OwnedBytes::new(buf);

        let fileslice = FileSlice::new(Arc::new(owned_bytes));
        let err = Footer::extract_footer(fileslice).unwrap_err();
        assert_eq!(
            err.to_string(),
            "Footer magic byte mismatch. File corrupted or index was created using old an tantivy \
             version which is not supported anymore. Please use tantivy 0.15 or above to recreate \
             the index."
        );
    }
    #[test]
    fn test_deserialize_footer_wrong_filesize() {
        let mut buf: Vec<u8> = vec![];
        BinarySerializable::serialize(&FOOTER_MAGIC_NUMBER, &mut buf).unwrap();

        let owned_bytes = OwnedBytes::new(buf);

        let fileslice = FileSlice::new(Arc::new(owned_bytes));
        let err = Footer::extract_footer(fileslice).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
        assert_eq!(
            err.to_string(),
            "File corrupted. The file is too small to contain the 24 byte footer (len=4)."
        );
    }
}

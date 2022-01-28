use std::io;

use common::{BinarySerializable, FixedSize, HasLen};

use crate::directory::FileSlice;
use crate::store::Compressor;

#[derive(Debug, Clone, PartialEq)]
pub struct DocStoreFooter {
    pub offset: u64,
    pub compressor: Compressor,
}

/// Serialises the footer to a byte-array
/// - offset : 8 bytes
/// - compressor id: 1 byte
/// - reserved for future use: 15 bytes
impl BinarySerializable for DocStoreFooter {
    fn serialize<W: io::Write>(&self, writer: &mut W) -> io::Result<()> {
        BinarySerializable::serialize(&self.offset, writer)?;
        BinarySerializable::serialize(&self.compressor.get_id(), writer)?;
        writer.write_all(&[0; 15])?;
        Ok(())
    }

    fn deserialize<R: io::Read>(reader: &mut R) -> io::Result<Self> {
        let offset = u64::deserialize(reader)?;
        let compressor_id = u8::deserialize(reader)?;
        let mut skip_buf = [0; 15];
        reader.read_exact(&mut skip_buf)?;
        Ok(DocStoreFooter {
            offset,
            compressor: Compressor::from_id(compressor_id),
        })
    }
}

impl FixedSize for DocStoreFooter {
    const SIZE_IN_BYTES: usize = 24;
}

impl DocStoreFooter {
    pub fn new(offset: u64, compressor: Compressor) -> Self {
        DocStoreFooter { offset, compressor }
    }

    pub fn extract_footer(file: FileSlice) -> io::Result<(DocStoreFooter, FileSlice)> {
        if file.len() < DocStoreFooter::SIZE_IN_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!(
                    "File corrupted. The file is smaller than Footer::SIZE_IN_BYTES (len={}).",
                    file.len()
                ),
            ));
        }
        let (body, footer_slice) = file.split_from_end(DocStoreFooter::SIZE_IN_BYTES);
        let mut footer_bytes = footer_slice.read_bytes()?;
        let footer = DocStoreFooter::deserialize(&mut footer_bytes)?;
        Ok((footer, body))
    }
}

#[test]
fn doc_store_footer_test() {
    // This test is just to safe guard changes on the footer.
    // When the doc store footer is updated, make sure to update also the serialize/deserialize
    // methods
    assert_eq!(core::mem::size_of::<DocStoreFooter>(), 16);
}

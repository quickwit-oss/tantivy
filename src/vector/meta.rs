use std::io::{self, Write};

use common::HasLen;

use crate::directory::FileSlice;

pub(crate) const VECMETA_EXT: &str = "vecmeta";

const FORMAT_FLAT: u8 = 0;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum VectorStorageFormat {
    Flat,
}

impl VectorStorageFormat {
    pub(crate) fn serialize<W: Write + ?Sized>(self, writer: &mut W) -> io::Result<()> {
        let code = match self {
            Self::Flat => FORMAT_FLAT,
        };
        writer.write_all(&[code])
    }

    fn from_code(code: u8) -> io::Result<Self> {
        match code {
            FORMAT_FLAT => Ok(Self::Flat),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "unsupported vector storage format",
            )),
        }
    }
}

pub(crate) struct VectorSegmentMeta {
    pub(crate) format: VectorStorageFormat,
    pub(crate) payload: FileSlice,
}

impl VectorSegmentMeta {
    pub(crate) fn open(file_slice: FileSlice) -> io::Result<Self> {
        if file_slice.len() == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "vector metadata is empty",
            ));
        }
        let format = VectorStorageFormat::from_code(file_slice.slice(0..1).read_bytes()?[0])?;
        let payload = file_slice.slice_from(1);
        if format == VectorStorageFormat::Flat && payload.len() != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "flat vector metadata has trailing bytes",
            ));
        }
        Ok(Self { format, payload })
    }
}

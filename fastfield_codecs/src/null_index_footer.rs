use std::io::{self, Write};
use std::ops::Range;

use common::{BinarySerializable, FixedSize};

#[derive(Debug, Clone, Copy)]
pub(crate) enum FastFieldCardinality {
    Single = 1,
}

impl BinarySerializable for FastFieldCardinality {
    fn serialize<W: Write>(&self, wrt: &mut W) -> io::Result<()> {
        self.to_code().serialize(wrt)
    }

    fn deserialize<R: io::Read>(reader: &mut R) -> io::Result<Self> {
        let code = u8::deserialize(reader)?;
        let codec_type: Self = Self::from_code(code)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "Unknown code `{code}.`"))?;
        Ok(codec_type)
    }
}

impl FastFieldCardinality {
    pub(crate) fn to_code(self) -> u8 {
        self as u8
    }

    pub(crate) fn from_code(code: u8) -> Option<Self> {
        match code {
            1 => Some(Self::Single),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum NullIndexCodec {
    Full = 1,
}

impl BinarySerializable for NullIndexCodec {
    fn serialize<W: Write>(&self, wrt: &mut W) -> io::Result<()> {
        self.to_code().serialize(wrt)
    }

    fn deserialize<R: io::Read>(reader: &mut R) -> io::Result<Self> {
        let code = u8::deserialize(reader)?;
        let codec_type: Self = Self::from_code(code)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "Unknown code `{code}.`"))?;
        Ok(codec_type)
    }
}

impl NullIndexCodec {
    pub(crate) fn to_code(self) -> u8 {
        self as u8
    }

    pub(crate) fn from_code(code: u8) -> Option<Self> {
        match code {
            1 => Some(Self::Full),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct NullIndexFooter {
    pub(crate) cardinality: FastFieldCardinality,
    pub(crate) null_index_codec: NullIndexCodec,
    // Unused for NullIndexCodec::Full
    pub(crate) null_index_byte_range: Range<u64>,
}

impl FixedSize for NullIndexFooter {
    const SIZE_IN_BYTES: usize = 18;
}

impl BinarySerializable for NullIndexFooter {
    fn serialize<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        self.cardinality.serialize(writer)?;
        self.null_index_codec.serialize(writer)?;
        self.null_index_byte_range.start.serialize(writer)?;
        self.null_index_byte_range.end.serialize(writer)?;
        Ok(())
    }

    fn deserialize<R: io::Read>(reader: &mut R) -> io::Result<Self> {
        let cardinality = FastFieldCardinality::deserialize(reader)?;
        let null_index_codec = NullIndexCodec::deserialize(reader)?;
        let null_index_byte_range_start = u64::deserialize(reader)?;
        let null_index_byte_range_end = u64::deserialize(reader)?;
        Ok(Self {
            cardinality,
            null_index_codec,
            null_index_byte_range: null_index_byte_range_start..null_index_byte_range_end,
        })
    }
}

use std::io::{self, Write};
use std::ops::Range;

use common::{BinarySerializable, CountingWriter, OwnedBytes, VInt};

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) enum FastFieldCardinality {
    Single = 1,
    Multi = 2,
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
            2 => Some(Self::Multi),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct NullIndexFooter {
    pub(crate) cardinality: FastFieldCardinality,
    pub(crate) null_index_codec: NullIndexCodec,
    // Unused for NullIndexCodec::Full
    pub(crate) null_index_byte_range: Range<u64>,
}

impl BinarySerializable for NullIndexFooter {
    fn serialize<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        self.cardinality.serialize(writer)?;
        self.null_index_codec.serialize(writer)?;
        VInt(self.null_index_byte_range.start).serialize(writer)?;
        VInt(self.null_index_byte_range.end - self.null_index_byte_range.start)
            .serialize(writer)?;
        Ok(())
    }

    fn deserialize<R: io::Read>(reader: &mut R) -> io::Result<Self> {
        let cardinality = FastFieldCardinality::deserialize(reader)?;
        let null_index_codec = NullIndexCodec::deserialize(reader)?;
        let null_index_byte_range_start = VInt::deserialize(reader)?.0;
        let null_index_byte_range_end = VInt::deserialize(reader)?.0 + null_index_byte_range_start;
        Ok(Self {
            cardinality,
            null_index_codec,
            null_index_byte_range: null_index_byte_range_start..null_index_byte_range_end,
        })
    }
}

pub(crate) fn append_null_index_footer(
    output: &mut impl io::Write,
    null_index_footer: NullIndexFooter,
) -> io::Result<()> {
    let mut counting_write = CountingWriter::wrap(output);
    null_index_footer.serialize(&mut counting_write)?;
    let footer_payload_len = counting_write.written_bytes();
    BinarySerializable::serialize(&(footer_payload_len as u16), &mut counting_write)?;

    Ok(())
}

pub(crate) fn read_null_index_footer(
    data: OwnedBytes,
) -> io::Result<(OwnedBytes, NullIndexFooter)> {
    let (data, null_footer_length_bytes) = data.rsplit(2);

    let footer_length = u16::deserialize(&mut null_footer_length_bytes.as_slice())?;
    let (data, null_index_footer_bytes) = data.rsplit(footer_length as usize);
    let null_index_footer = NullIndexFooter::deserialize(&mut null_index_footer_bytes.as_ref())?;

    Ok((data, null_index_footer))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn null_index_footer_deser_test() {
        let null_index_footer = NullIndexFooter {
            cardinality: FastFieldCardinality::Single,
            null_index_codec: NullIndexCodec::Full,
            null_index_byte_range: 100..120,
        };

        let mut out = vec![];
        null_index_footer.serialize(&mut out).unwrap();

        assert_eq!(
            null_index_footer,
            NullIndexFooter::deserialize(&mut &out[..]).unwrap()
        );
    }
}

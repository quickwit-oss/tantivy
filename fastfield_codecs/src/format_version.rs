use std::io;

use common::BinarySerializable;
use ownedbytes::OwnedBytes;

const MAGIC_NUMBER: u16 = 4335u16;
const FASTFIELD_FORMAT_VERSION: u8 = 1;

pub(crate) fn append_format_version(output: &mut impl io::Write) -> io::Result<()> {
    FASTFIELD_FORMAT_VERSION.serialize(output)?;
    MAGIC_NUMBER.serialize(output)?;

    Ok(())
}

pub(crate) fn read_format_version(data: OwnedBytes) -> io::Result<(OwnedBytes, u8)> {
    let (data, magic_number_bytes) = data.rsplit(2);

    let magic_number = u16::deserialize(&mut magic_number_bytes.as_slice())?;
    if magic_number != MAGIC_NUMBER {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("magic number mismatch {} != {}", magic_number, MAGIC_NUMBER),
        ));
    }
    let (data, format_version_bytes) = data.rsplit(1);
    let format_version = u8::deserialize(&mut format_version_bytes.as_slice())?;
    if format_version > FASTFIELD_FORMAT_VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "Unsupported fastfield format version: {}. Max supported version: {}",
                format_version, FASTFIELD_FORMAT_VERSION
            ),
        ));
    }

    Ok((data, format_version))
}

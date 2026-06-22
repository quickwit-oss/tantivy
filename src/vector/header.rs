//! Format version for the per-segment `.vec` file.
//!
//! A fixed 4-byte header (a `u32` version) is prepended to every `.vec` file,
//! ahead of the [`CompositeFile`](crate::directory::CompositeFile) body. The
//! version is the wire-layout *generation* — bump it when the framing changes
//! incompatibly. It is orthogonal to the
//! [`IdMap`](super::flat::id_map) variant, which selects the storage *mode*
//! (flat vs IVF) within a generation.

use std::io::{self, Read, Write};

use common::{BinarySerializable, HasLen};

use crate::directory::FileSlice;

/// Length of the `.vec` header in bytes (a single `u32`).
pub(crate) const HEADER_LEN: usize = 4;

/// On-disk format version of the `.vec` file.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum VectorFileVersion {
    V1 = 1,
}

/// Version stamped into newly written `.vec` files.
pub(crate) const CURRENT: VectorFileVersion = VectorFileVersion::V1;

impl BinarySerializable for VectorFileVersion {
    fn serialize<W: Write + ?Sized>(&self, writer: &mut W) -> io::Result<()> {
        (*self as u32).serialize(writer)
    }

    fn deserialize<R: Read>(reader: &mut R) -> io::Result<Self> {
        match u32::deserialize(reader)? {
            1 => Ok(VectorFileVersion::V1),
            other => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unsupported .vec format version: {other}"),
            )),
        }
    }
}

/// Write the current version header. Call before wrapping the writer in a
/// [`CompositeWrite`](crate::directory::CompositeWrite); the composite's
/// offsets are self-relative, so the header does not perturb them.
pub(crate) fn write_header<W: Write + ?Sized>(writer: &mut W) -> io::Result<()> {
    CURRENT.serialize(writer)
}

/// Parse the version header and return it alongside the composite body (the
/// file slice past the header). Errors if the version is unknown or newer than
/// [`CURRENT`].
pub(crate) fn read_header(file: &FileSlice) -> io::Result<(VectorFileVersion, FileSlice)> {
    if file.len() < HEADER_LEN {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "`.vec` file is smaller than its header",
        ));
    }
    let header_bytes = file.slice_to(HEADER_LEN).read_bytes()?;
    let version = VectorFileVersion::deserialize(&mut header_bytes.as_slice())?;
    Ok((version, file.slice_from(HEADER_LEN)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_header_round_trip() {
        let mut buf = Vec::new();
        write_header(&mut buf).unwrap();
        assert_eq!(buf.len(), HEADER_LEN);
        assert_eq!(buf, vec![1, 0, 0, 0]);

        let (version, body) = read_header(&FileSlice::from(buf)).unwrap();
        assert_eq!(version, VectorFileVersion::V1);
        assert_eq!(body.len(), 0);
    }

    #[test]
    fn test_header_preserves_body() {
        let mut buf = Vec::new();
        write_header(&mut buf).unwrap();
        buf.extend_from_slice(b"composite-bytes");

        let (version, body) = read_header(&FileSlice::from(buf)).unwrap();
        assert_eq!(version, VectorFileVersion::V1);
        assert_eq!(body.read_bytes().unwrap().as_slice(), b"composite-bytes");
    }

    #[test]
    fn test_future_version_rejected() {
        let buf = 2u32.to_le_bytes().to_vec();
        let err = read_header(&FileSlice::from(buf)).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn test_truncated_header_rejected() {
        let buf = vec![1u8, 0];
        let err = read_header(&FileSlice::from(buf)).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
    }
}

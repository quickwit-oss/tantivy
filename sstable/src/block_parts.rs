use std::io::{self, Read};

/// Appends a flag-2 block body with independently encoded metadata, payload, and keys.
///
/// Each part is `[decoded_len: u32 LE, stored_len: u32 LE, codec: u8, bytes]`.
/// Metadata and keys use zstd level 3 when the `zstd-compression` feature is enabled
/// and compression saves space. The payload is always stored verbatim, allowing
/// callers to supply values that are already compressed. The outer block length
/// is written by the caller.
pub fn encode_block_parts(
    metadata: &[u8],
    payload: &[u8],
    keys: &[u8],
    output: &mut Vec<u8>,
) -> io::Result<()> {
    output.push(2);
    for (part, compress) in [(metadata, true), (payload, false), (keys, true)] {
        let decoded_len = u32::try_from(part.len()).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidInput, "block part exceeds u32 length")
        })?;
        let compressed: Option<Vec<u8>> = match compress {
            #[cfg(feature = "zstd-compression")]
            true => Some(zstd::bulk::compress(part, 3)?),
            _ => None,
        };
        let (stored, codec) = compressed
            .as_deref()
            .filter(|bytes| bytes.len() < part.len())
            .map_or((part, 0), |bytes| (bytes, 1));
        output.extend_from_slice(&decoded_len.to_le_bytes());
        // Compression is used only when smaller, so this length also fits u32.
        output.extend_from_slice(&(stored.len() as u32).to_le_bytes());
        output.push(codec);
        output.extend_from_slice(stored);
    }
    Ok(())
}

pub(crate) fn decode_block_parts(mut data: &[u8], output: &mut Vec<u8>) -> io::Result<()> {
    for part_index in 0..3 {
        let mut header = [0u8; 9];
        data.read_exact(&mut header)?;
        let decoded_len = u32::from_le_bytes(header[..4].try_into().unwrap()) as usize;
        let stored_len = u32::from_le_bytes(header[4..8].try_into().unwrap()) as usize;
        let codec = header[8];
        let stored = data
            .get(..stored_len)
            .ok_or_else(|| io::Error::new(io::ErrorKind::UnexpectedEof, "truncated block part"))?;
        data = &data[stored_len..];
        let end = output.len().checked_add(decoded_len).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "decoded block length overflow")
        })?;
        match codec {
            0 => {
                if stored_len != decoded_len {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "raw part length mismatch",
                    ));
                }
                output.try_reserve(decoded_len).map_err(io::Error::other)?;
                output.extend_from_slice(stored);
            }
            1 if part_index != 1 => {
                #[cfg(feature = "zstd-compression")]
                {
                    // Read at most one byte beyond the advertised size. Streaming avoids
                    // allocating a large buffer solely on the basis of an untrusted header.
                    zstd::stream::read::Decoder::new(stored)?
                        .take(decoded_len as u64 + 1)
                        .read_to_end(output)?;
                    if output.len() != end {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "decoded part length mismatch",
                        ));
                    }
                }
                #[cfg(not(feature = "zstd-compression"))]
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "zstd-compression feature is not enabled",
                ));
            }
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "invalid block part codec",
                ));
            }
        }
        debug_assert_eq!(output.len(), end);
    }
    if !data.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "trailing block part bytes",
        ));
    }
    Ok(())
}

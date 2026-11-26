use std::io;

mod merger;

use std::iter::ExactSizeIterator;

use common::VInt;
use sstable::SSTable;
use sstable::value::{ValueReader, ValueWriter};
use tantivy_fst::automaton::AlwaysMatch;

pub use self::merger::TermMerger;
use crate::postings::TermInfo;

/// The term dictionary contains all of the terms in
/// `tantivy index` in a sorted manner.
///
/// The `Fst` crate is used to associate terms to their
/// respective `TermOrdinal`. The `TermInfoStore` then makes it
/// possible to fetch the associated `TermInfo`.
pub type TermDictionary = sstable::Dictionary<TermSSTable>;

/// Builder for the new term dictionary.
pub type TermDictionaryBuilder<W> = sstable::Writer<W, TermInfoValueWriter>;

/// `TermStreamer` acts as a cursor over a range of terms of a segment.
/// Terms are guaranteed to be sorted.
pub type TermStreamer<'a, A = AlwaysMatch, K = Vec<u8>> = sstable::Streamer<'a, TermSSTable, A, K>;

/// SSTable used to store TermInfo objects.
#[derive(Clone)]
pub struct TermSSTable;

pub type TermStreamerBuilder<'a, A = AlwaysMatch, K = Vec<u8>> =
    sstable::StreamerBuilder<'a, TermSSTable, A, K>;

impl SSTable for TermSSTable {
    type Value = TermInfo;
    type ValueReader = TermInfoValueReader;
    type ValueWriter = TermInfoValueWriter;
}

#[derive(Default)]
pub struct TermInfoValueReader {
    term_infos: Vec<TermInfo>,
}

#[inline(always)]
fn read_vint_u64(data: &mut &[u8]) -> io::Result<u64> {
    let mut result = 0u64;
    let mut shift = 0u32;
    while let Some((&b, rest)) = data.split_first() {
        *data = rest;
        if shift >= 64 {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "VInt overflow"));
        }
        result |= u64::from(b & 127) << shift;
        if b >= 128 {
            return Ok(result);
        }
        shift += 7;
    }
    Err(io::Error::new(
        io::ErrorKind::InvalidData,
        "Reach end of buffer while reading VInt",
    ))
}

/// Advances past `remaining` VInts by counting bytes with the stop bit (`0x80`) set.
fn skip_vints(data: &[u8], mut remaining: usize) -> io::Result<usize> {
    let mut pos = 0usize;
    while remaining >= 8 && pos + 8 <= data.len() {
        let word = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
        remaining -= (word & 0x8080_8080_8080_8080).count_ones() as usize;
        pos += 8;
    }
    while remaining > 0 {
        let Some(&b) = data.get(pos) else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Reach end of buffer while skipping VInts",
            ));
        };
        pos += 1;
        if b >= 128 {
            remaining -= 1;
        }
    }
    Ok(pos)
}

impl ValueReader for TermInfoValueReader {
    type Value = TermInfo;

    #[inline(always)]
    fn value(&self, idx: usize) -> &TermInfo {
        &self.term_infos[idx]
    }

    fn load(&mut self, mut data: &[u8]) -> io::Result<usize> {
        let len_before = data.len();
        self.term_infos.clear();
        let num_els = read_vint_u64(&mut data)?;
        if num_els == 0 {
            return Ok(len_before - data.len());
        }
        self.term_infos
            .reserve((num_els as usize).min(data.len() / 3));
        let mut postings_start = read_vint_u64(&mut data)? as usize;
        let mut positions_start = read_vint_u64(&mut data)? as usize;
        for _ in 0..num_els {
            let doc_freq = read_vint_u64(&mut data)? as u32;
            let postings_num_bytes = read_vint_u64(&mut data)?;
            let positions_num_bytes = read_vint_u64(&mut data)?;
            let postings_end = postings_start + postings_num_bytes as usize;
            let positions_end = positions_start + positions_num_bytes as usize;
            let term_info = TermInfo {
                doc_freq,
                postings_range: postings_start..postings_end,
                positions_range: positions_start..positions_end,
            };
            self.term_infos.push(term_info);
            postings_start = postings_end;
            positions_start = positions_end;
        }
        let consumed_len = len_before - data.len();
        Ok(consumed_len)
    }

    fn skip(&self, mut data: &[u8]) -> io::Result<Option<usize>> {
        let len_before = data.len();
        let num_els = read_vint_u64(&mut data)? as usize;
        let header_len = len_before - data.len();
        if num_els == 0 {
            return Ok(Some(header_len));
        }
        let num_vints = num_els
            .checked_mul(3)
            .and_then(|n| n.checked_add(2))
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "TermInfo count overflow"))?;
        let body_len = skip_vints(data, num_vints)?;
        Ok(Some(header_len + body_len))
    }

    fn load_single(&mut self, mut data: &[u8], idx: usize) -> io::Result<Option<TermInfo>> {
        let num_els = read_vint_u64(&mut data)? as usize;
        if idx >= num_els {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "TermInfo index out of bounds",
            ));
        }
        let mut postings_start = read_vint_u64(&mut data)? as usize;
        let mut positions_start = read_vint_u64(&mut data)? as usize;
        for _ in 0..idx {
            read_vint_u64(&mut data)?;
            postings_start += read_vint_u64(&mut data)? as usize;
            positions_start += read_vint_u64(&mut data)? as usize;
        }
        let doc_freq = read_vint_u64(&mut data)? as u32;
        let postings_num_bytes = read_vint_u64(&mut data)? as usize;
        let positions_num_bytes = read_vint_u64(&mut data)? as usize;
        Ok(Some(TermInfo {
            doc_freq,
            postings_range: postings_start..postings_start + postings_num_bytes,
            positions_range: positions_start..positions_start + positions_num_bytes,
        }))
    }
}

#[derive(Default)]
pub struct TermInfoValueWriter {
    term_infos: Vec<TermInfo>,
}

impl ValueWriter for TermInfoValueWriter {
    type Value = TermInfo;

    fn write(&mut self, term_info: &TermInfo) {
        self.term_infos.push(term_info.clone());
    }

    fn serialize_block(&self, buffer: &mut Vec<u8>) {
        VInt(self.term_infos.len() as u64).serialize_into_vec(buffer);
        if self.term_infos.is_empty() {
            return;
        }
        VInt(self.term_infos[0].postings_range.start as u64).serialize_into_vec(buffer);
        VInt(self.term_infos[0].positions_range.start as u64).serialize_into_vec(buffer);
        for term_info in &self.term_infos {
            VInt(term_info.doc_freq as u64).serialize_into_vec(buffer);
            VInt(term_info.postings_range.len() as u64).serialize_into_vec(buffer);
            VInt(term_info.positions_range.len() as u64).serialize_into_vec(buffer);
        }
    }

    fn clear(&mut self) {
        self.term_infos.clear();
    }
}

#[cfg(test)]
mod tests {
    use sstable::value::{ValueReader, ValueWriter};

    use crate::postings::TermInfo;
    use crate::termdict::sstable_termdict::TermInfoValueReader;

    #[test]
    fn test_block_terminfos() {
        let mut term_info_writer = super::TermInfoValueWriter::default();
        term_info_writer.write(&TermInfo {
            doc_freq: 120u32,
            postings_range: 17..45,
            positions_range: 10..122,
        });
        term_info_writer.write(&TermInfo {
            doc_freq: 10u32,
            postings_range: 45..450,
            positions_range: 122..1100,
        });
        term_info_writer.write(&TermInfo {
            doc_freq: 17u32,
            postings_range: 450..462,
            positions_range: 1100..1302,
        });
        let mut buffer = Vec::new();
        term_info_writer.serialize_block(&mut buffer);
        let mut term_info_reader = TermInfoValueReader::default();
        let num_bytes: usize = term_info_reader.load(&buffer[..]).unwrap();
        assert_eq!(
            term_info_reader.value(0),
            &TermInfo {
                doc_freq: 120u32,
                postings_range: 17..45,
                positions_range: 10..122
            }
        );
        assert_eq!(buffer.len(), num_bytes);
    }

    #[test]
    fn test_skip_and_load_single_terminfos() {
        let mut writer = super::TermInfoValueWriter::default();
        let mut expected = Vec::new();
        let mut postings_off = 17usize;
        let mut positions_off = 10usize;
        // Include >100 entries with a mix of 1-byte and multi-byte VInts to exercise the 8-byte
        // SWAR loop and scalar tail in skip_vints.
        for i in 0..130usize {
            let doc_freq = (i * i * 3 + 1) as u32;
            let postings_len = (i * 19 + 5) % 4096;
            let positions_len = (i * 37 + 11) % 8192;
            let ti = TermInfo {
                doc_freq,
                postings_range: postings_off..postings_off + postings_len,
                positions_range: positions_off..positions_off + positions_len,
            };
            writer.write(&ti);
            expected.push(ti);
            postings_off += postings_len;
            positions_off += positions_len;
        }
        let mut buffer = Vec::new();
        writer.serialize_block(&mut buffer);
        let value_len = buffer.len();
        // Append trailing key bytes to make sure skip() stops at the exact boundary.
        buffer.extend_from_slice(&[0xFF, 0x01, 0x80, 0x42]);

        let mut reader = TermInfoValueReader::default();
        assert_eq!(reader.skip(&buffer).unwrap(), Some(value_len));
        for (idx, exp) in expected.iter().enumerate() {
            assert_eq!(
                reader.load_single(&buffer[..value_len], idx).unwrap(),
                Some(exp.clone())
            );
        }
        assert!(
            reader
                .load_single(&buffer[..value_len], expected.len())
                .is_err()
        );

        // Empty block
        let empty_writer = super::TermInfoValueWriter::default();
        let mut empty_buf = Vec::new();
        empty_writer.serialize_block(&mut empty_buf);
        assert_eq!(reader.skip(&empty_buf).unwrap(), Some(empty_buf.len()));
        assert_eq!(reader.load(&empty_buf).unwrap(), empty_buf.len());
        assert!(reader.load_single(&empty_buf, 0).is_err());
    }
}

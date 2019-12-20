use crate::common::bitpacker::BitPacker;
use crate::common::compute_num_bits;
use crate::common::Endianness;
use crate::common::{BinarySerializable, FixedSize};
use crate::directory::ReadOnlySource;
use crate::postings::TermInfo;
use crate::termdict::TermOrdinal;
use byteorder::{ByteOrder, LittleEndian, ReadBytesExt};
use std::cmp;
use std::io::{self, Read, Write};

const BLOCK_LEN: usize = 256;

#[derive(Debug, Eq, PartialEq, Default)]
struct TermInfoBlockMeta {
    offset: u64,
    ref_term_info: TermInfo,
    doc_freq_nbits: u8,
    postings_offset_nbits: u8,
    positions_idx_nbits: u8,
}

impl BinarySerializable for TermInfoBlockMeta {
    fn serialize<W: Write>(&self, write: &mut W) -> io::Result<()> {
        self.offset.serialize(write)?;
        self.ref_term_info.serialize(write)?;
        write.write_all(&[
            self.doc_freq_nbits,
            self.postings_offset_nbits,
            self.positions_idx_nbits,
        ])?;
        Ok(())
    }

    fn deserialize<R: Read>(reader: &mut R) -> io::Result<Self> {
        let offset = u64::deserialize(reader)?;
        let ref_term_info = TermInfo::deserialize(reader)?;
        let mut buffer = [0u8; 3];
        reader.read_exact(&mut buffer)?;
        Ok(TermInfoBlockMeta {
            offset,
            ref_term_info,
            doc_freq_nbits: buffer[0],
            postings_offset_nbits: buffer[1],
            positions_idx_nbits: buffer[2],
        })
    }
}

impl FixedSize for TermInfoBlockMeta {
    const SIZE_IN_BYTES: usize =
        u64::SIZE_IN_BYTES + TermInfo::SIZE_IN_BYTES + 3 * u8::SIZE_IN_BYTES;
}

impl TermInfoBlockMeta {
    fn num_bits(&self) -> u8 {
        self.doc_freq_nbits + self.postings_offset_nbits + self.positions_idx_nbits
    }

    fn deserialize_term_info(&self, data: &[u8], inner_offset: usize) -> TermInfo {
        let num_bits = self.num_bits() as usize;
        let mut cursor = num_bits * inner_offset;

        let doc_freq = extract_bits(data, cursor, self.doc_freq_nbits) as u32;
        cursor += self.doc_freq_nbits as usize;

        let postings_offset = extract_bits(data, cursor, self.postings_offset_nbits);
        cursor += self.postings_offset_nbits as usize;

        let positions_idx = extract_bits(data, cursor, self.positions_idx_nbits);

        TermInfo {
            doc_freq,
            postings_offset: postings_offset + self.ref_term_info.postings_offset,
            positions_idx: positions_idx + self.ref_term_info.positions_idx,
        }
    }
}

pub struct TermInfoStore {
    num_terms: usize,
    block_meta_source: ReadOnlySource,
    term_info_source: ReadOnlySource,
}

fn extract_bits(data: &[u8], addr_bits: usize, num_bits: u8) -> u64 {
    assert!(num_bits <= 56);
    let addr_byte = addr_bits / 8;
    let bit_shift = (addr_bits % 8) as u64;
    let val_unshifted_unmasked: u64 = if data.len() >= addr_byte + 8 {
        LittleEndian::read_u64(&data[addr_byte..][..8])
    } else {
        // the buffer is not large enough.
        // Let's copy the few remaining bytes to a 8 byte buffer
        // padded with 0s.
        let mut buf = [0u8; 8];
        let data_to_copy = &data[addr_byte..];
        let nbytes = data_to_copy.len();
        buf[..nbytes].copy_from_slice(data_to_copy);
        LittleEndian::read_u64(&buf)
    };
    let val_shifted_unmasked = val_unshifted_unmasked >> bit_shift;
    let mask = (1u64 << u64::from(num_bits)) - 1;
    val_shifted_unmasked & mask
}

impl TermInfoStore {
    pub fn open(data: &ReadOnlySource) -> TermInfoStore {
        let mut metadata_slice = data.clone();
        let len = metadata_slice
            .read_u64::<Endianness>()
            .expect("Can't read terminfo lenght") as usize;
        let num_terms = metadata_slice
            .read_u64::<Endianness>()
            .expect("Can't read number of terminfo terms") as usize;
        let block_meta_source = data.slice(16, 16 + len);
        let term_info_source = data.slice_from(16 + len);
        TermInfoStore {
            num_terms,
            block_meta_source,
            term_info_source,
        }
    }

    pub fn get(&self, term_ord: TermOrdinal) -> TermInfo {
        let block_id = (term_ord as usize) / BLOCK_LEN;
        let mut block_data = self.block_meta_source.slice_from(block_id * TermInfoBlockMeta::SIZE_IN_BYTES);
        let term_info_block_data = TermInfoBlockMeta::deserialize(&mut block_data)
            .expect("Failed to deserialize terminfoblockmeta");
        let inner_offset = (term_ord as usize) % BLOCK_LEN;
        if inner_offset == 0 {
            term_info_block_data.ref_term_info
        } else {
            let mut term_info_data = self.term_info_source.slice_from(term_info_block_data.offset as usize);
            term_info_block_data.deserialize_term_info(
                &term_info_data.read_all().expect("Can't read term info data"),
                inner_offset - 1,
            )
        }
    }

    pub fn num_terms(&self) -> usize {
        self.num_terms
    }
}

pub struct TermInfoStoreWriter {
    buffer_block_metas: Vec<u8>,
    buffer_term_infos: Vec<u8>,
    term_infos: Vec<TermInfo>,
    num_terms: u64,
}

fn bitpack_serialize<W: Write>(
    write: &mut W,
    bit_packer: &mut BitPacker,
    term_info_block_meta: &TermInfoBlockMeta,
    term_info: &TermInfo,
) -> io::Result<()> {
    bit_packer.write(
        u64::from(term_info.doc_freq),
        term_info_block_meta.doc_freq_nbits,
        write,
    )?;
    bit_packer.write(
        term_info.postings_offset,
        term_info_block_meta.postings_offset_nbits,
        write,
    )?;
    bit_packer.write(
        term_info.positions_idx,
        term_info_block_meta.positions_idx_nbits,
        write,
    )?;
    Ok(())
}

impl TermInfoStoreWriter {
    pub fn new() -> TermInfoStoreWriter {
        TermInfoStoreWriter {
            buffer_block_metas: Vec::new(),
            buffer_term_infos: Vec::new(),
            term_infos: Vec::with_capacity(BLOCK_LEN),
            num_terms: 0u64,
        }
    }

    fn flush_block(&mut self) -> io::Result<()> {
        if self.term_infos.is_empty() {
            return Ok(());
        }
        let mut bit_packer = BitPacker::new();
        let ref_term_info = self.term_infos[0].clone();
        for term_info in &mut self.term_infos[1..] {
            term_info.postings_offset -= ref_term_info.postings_offset;
            term_info.positions_idx -= ref_term_info.positions_idx;
        }

        let mut max_doc_freq: u32 = 0u32;
        let mut max_postings_offset: u64 = 0u64;
        let mut max_positions_idx: u64 = 0u64;
        for term_info in &self.term_infos[1..] {
            max_doc_freq = cmp::max(max_doc_freq, term_info.doc_freq);
            max_postings_offset = cmp::max(max_postings_offset, term_info.postings_offset);
            max_positions_idx = cmp::max(max_positions_idx, term_info.positions_idx);
        }

        let max_doc_freq_nbits: u8 = compute_num_bits(u64::from(max_doc_freq));
        let max_postings_offset_nbits = compute_num_bits(max_postings_offset);
        let max_positions_idx_nbits = compute_num_bits(max_positions_idx);

        let term_info_block_meta = TermInfoBlockMeta {
            offset: self.buffer_term_infos.len() as u64,
            ref_term_info,
            doc_freq_nbits: max_doc_freq_nbits,
            postings_offset_nbits: max_postings_offset_nbits,
            positions_idx_nbits: max_positions_idx_nbits,
        };

        term_info_block_meta.serialize(&mut self.buffer_block_metas)?;
        for term_info in self.term_infos[1..].iter().cloned() {
            bitpack_serialize(
                &mut self.buffer_term_infos,
                &mut bit_packer,
                &term_info_block_meta,
                &term_info,
            )?;
        }

        // Block need end up at the end of a byte.
        bit_packer.flush(&mut self.buffer_term_infos)?;
        self.term_infos.clear();

        Ok(())
    }

    pub fn write_term_info(&mut self, term_info: &TermInfo) -> io::Result<()> {
        self.num_terms += 1u64;
        self.term_infos.push(term_info.clone());
        if self.term_infos.len() >= BLOCK_LEN {
            self.flush_block()?;
        }
        Ok(())
    }

    pub fn serialize<W: io::Write>(&mut self, write: &mut W) -> io::Result<()> {
        if !self.term_infos.is_empty() {
            self.flush_block()?;
        }
        let len = self.buffer_block_metas.len() as u64;
        len.serialize(write)?;
        self.num_terms.serialize(write)?;
        write.write_all(&self.buffer_block_metas)?;
        write.write_all(&self.buffer_term_infos)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use super::extract_bits;
    use super::TermInfoBlockMeta;
    use super::{TermInfoStore, TermInfoStoreWriter};
    use crate::common;
    use crate::common::bitpacker::BitPacker;
    use crate::common::compute_num_bits;
    use crate::common::BinarySerializable;
    use crate::directory::ReadOnlySource;
    use crate::postings::TermInfo;

    #[test]
    fn test_term_info_block() {
        common::test::fixed_size_test::<TermInfoBlockMeta>();
    }

    #[test]
    fn test_bitpacked() {
        let mut buffer = Vec::new();
        let mut bitpack = BitPacker::new();
        bitpack.write(321u64, 9, &mut buffer).unwrap();
        assert_eq!(compute_num_bits(321u64), 9);
        bitpack.write(2u64, 2, &mut buffer).unwrap();
        assert_eq!(compute_num_bits(2u64), 2);
        bitpack.write(51, 6, &mut buffer).unwrap();
        assert_eq!(compute_num_bits(51), 6);
        bitpack.close(&mut buffer).unwrap();
        assert_eq!(buffer.len(), 3 + 7);
        assert_eq!(extract_bits(&buffer[..], 0, 9), 321u64);
        assert_eq!(extract_bits(&buffer[..], 9, 2), 2u64);
        assert_eq!(extract_bits(&buffer[..], 11, 6), 51u64);
    }

    #[test]
    fn test_term_info_block_meta_serialization() {
        let term_info_block_meta = TermInfoBlockMeta {
            offset: 2009,
            ref_term_info: TermInfo {
                doc_freq: 512,
                postings_offset: 51,
                positions_idx: 3584,
            },
            doc_freq_nbits: 10,
            postings_offset_nbits: 5,
            positions_idx_nbits: 11,
        };
        let mut buffer: Vec<u8> = Vec::new();
        term_info_block_meta.serialize(&mut buffer).unwrap();
        let mut cursor: &[u8] = &buffer[..];
        let term_info_block_meta_serde = TermInfoBlockMeta::deserialize(&mut cursor).unwrap();
        assert_eq!(term_info_block_meta_serde, term_info_block_meta);
    }

    #[test]
    fn test_pack() {
        let mut store_writer = TermInfoStoreWriter::new();
        let mut term_infos = vec![];
        for i in 0..1000 {
            let term_info = TermInfo {
                doc_freq: i as u32,
                postings_offset: (i / 10) as u64,
                positions_idx: (i * 7) as u64,
            };
            store_writer.write_term_info(&term_info).unwrap();
            term_infos.push(term_info);
        }
        let mut buffer = Vec::new();
        store_writer.serialize(&mut buffer).unwrap();
        let term_info_store = TermInfoStore::open(&ReadOnlySource::from(buffer));
        for i in 0..1000 {
            assert_eq!(term_info_store.get(i as u64), term_infos[i]);
        }
    }
}

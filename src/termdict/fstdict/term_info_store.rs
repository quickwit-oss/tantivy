use std::io;
use std::cmp;
use postings::TermInfo;
use common::BinarySerializable;
use common::compute_num_bits;
use directory::ReadOnlySource;
use termdict::TermOrdinal;

const BLOCK_LEN: usize = 256;

pub struct TermInfoStore {
    data: ReadOnlySource
}

impl TermInfoStore {
    pub fn open(data: ReadOnlySource) -> TermInfoStore {
        TermInfoStore {
            data
        }
    }

    pub fn get(&self, term_ord: TermOrdinal) -> TermInfo {
        let buffer = self.data.as_slice();
        let offset = term_ord as usize * TermInfo::SIZE_IN_BYTES;
        let mut cursor = &buffer[offset..];
        TermInfo::deserialize(&mut cursor)
            .expect("The fst is corrupted. Failed to deserialize a value.")
    }

    pub fn num_terms(&self) -> usize {
        self.data.len() / TermInfo::SIZE_IN_BYTES
    }
}



struct BitpackWriter<W: io::Write> {
    write: W,
    mini_buffer: u64,
    mini_buffer_len: u8
}

impl<W: io::Write> BitpackWriter<W> {
    fn new(write: W) -> BitpackWriter<W> {
        BitpackWriter {
            write,
            mini_buffer: 0u64
        }
    }

    fn write(&mut self, mut val: u64, mut num_bits: usize) -> io::Result<()> {
        let mini_buffer_capacity = 64 - self.mini_buffer_len;
        if num_bits > self. {

        }
        Ok(())
    }

    fn finalize(&mut self) -> io::Result<()> {
        self.min
        Ok(())
    }
}

pub struct TermInfoStoreWriter {
    buffer: Vec<u8>,
    block_idx: Vec<(TermInfo, u64)>,
    term_infos: Vec<TermInfo>
}

impl TermInfoStoreWriter {
    pub fn new() -> TermInfoStoreWriter {
        TermInfoStoreWriter {
            buffer: Vec::new(),
            block_idx: Vec::new(),
            term_infos: Vec::with_capacity(BLOCK_LEN)
        }
    }

    fn flush_block(&mut self) -> io::Result<()> {
        if !self.buffer.is_empty() {
            self.block_idx.push((self.term_infos[0].clone(), self.buffer.len()));
        }
        let ref_term_info = self.term_infos[0].clone();
        for term_info in &mut self.term_infos[1..] {
            term_info.postings_offset -= ref_term_info.postings_offset;
            term_info.positions_offset -= ref_term_info.positions_offset;
            // term_infos.serialize(&mut self.buffer)?
        }
        let mut max_doc_freq: u32 = 0u32;
        let mut max_postings_offset: u64 = 0u64;
        let mut max_positions_offset: u64 = 0u64;
        for term_info in self.term_infos[1..] {
            max_doc_freq = cmp::max(max_doc_freq, term_info.doc_freq);
            max_postings_offset = cmp::max(max_postings_offset, term_info.postings_offset);
            max_positions_offset = cmp::max(max_positions_offset, term_info.positions_offset);
        }
        let max_doc_freq_nbits: u8 = compute_num_bits(max_doc_freq as u64);
        let max_postings_offset_nbits = compute_num_bits(max_postings_offset);
        let max_positions_offset_nbits = compute_num_bits(max_positions_offset);

        let term_info_num_bits: u8 = max_doc_freq_nbits + postings_offset_nbits + positions_offset_nbits + 7;

        for term_infos in &self.term_infos {
            term_infos.serialize(&mut self.buffer)?;
        }
        self.term_infos.clear();
        Ok(())
    }

    pub fn write_term_info(&mut self, term_info: &TermInfo) -> io::Result<()> {
        self.term_infos.push(term_info.clone());
        if self.term_infos.len() % BLOCK_LEN == 0 {
            self.flush_block()?;
        }
        Ok(())
    }

    pub fn serialize<W: io::Write>(&mut self, write: &mut W) -> io::Result<u64> {
        if !self.term_infos.is_empty() {
            self.flush_block()?;
        }
        let len = self.buffer.len() as u64;
        write.write_all(&self.buffer)?;
        Ok(len)
    }
}


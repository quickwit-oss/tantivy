use std::io;
use postings::TermInfo;
use common::BinarySerializable;
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


pub struct TermInfoStoreWriter {
    buffer: Vec<u8>,
    term_infos: Vec<TermInfo>
}

impl TermInfoStoreWriter {
    pub fn new() -> TermInfoStoreWriter {
        TermInfoStoreWriter {
            buffer: Vec::new(),
            term_infos: Vec::with_capacity(BLOCK_LEN)
        }
    }

    fn flush_block(&mut self) -> io::Result<()> {
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


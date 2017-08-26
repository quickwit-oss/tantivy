use postings::TermInfo;
use common::VInt;
use common::BinarySerializable;
use std::io::{self, Write};
use std::mem;

/// Returns the len of the longest
/// common prefix of `s1` and `s2`.
///
/// ie: the greatest `L` such that
/// for all `0 <= i < L`, `s1[i] == s2[i]`
fn common_prefix_len(s1: &[u8], s2: &[u8]) -> usize {
    s1.iter()
        .zip(s2.iter())
        .take_while(|&(a, b)| a==b)
        .count()
}


#[derive(Default)]
pub struct TermDeltaEncoder {
    last_term: Vec<u8>,
}

impl TermDeltaEncoder {
    pub fn encode<'a, W: Write>(&mut self, term: &'a [u8], write: &mut W) -> io::Result<()> {
        let prefix_len = common_prefix_len(term, &self.last_term);
        self.last_term.truncate(prefix_len);
        self.last_term.extend_from_slice(&term[prefix_len..]);
        let suffix = &term[prefix_len..];
        VInt(prefix_len as u64).serialize(write)?;
        VInt(suffix.len() as u64).serialize(write)?;
        write.write_all(suffix)?;
        Ok(())
    }

    pub fn term(&self) -> &[u8] {
        &self.last_term[..]
    }
}

#[derive(Default)]
pub struct TermDeltaDecoder {
    term: Vec<u8>,
}

impl TermDeltaDecoder {
    pub fn with_previous_term(term: Vec<u8>) -> TermDeltaDecoder {
        TermDeltaDecoder {
            term: Vec::from(term)
        }
    }

    pub fn decode(&mut self, cursor: &mut &[u8]) {
        let prefix_len: usize = deserialize_vint(cursor) as usize;
        let suffix_length: usize = deserialize_vint(cursor) as usize;
        let suffix = &cursor[..suffix_length];
        *cursor = &cursor[suffix_length..];
        self.term.truncate(prefix_len);
        self.term.extend_from_slice(suffix);
    }

    pub fn term(&self) -> &[u8]  {
        &self.term[..]
    }
}



pub struct TermInfoDeltaEncoder {
    term_info: TermInfo,
    has_positions: bool,
}

impl TermInfoDeltaEncoder {

    pub fn new(has_positions: bool) -> Self {
        TermInfoDeltaEncoder {
            term_info: TermInfo::default(),
            has_positions: has_positions,
        }
    }

    pub fn encode<W: Write>(&mut self, term_info: TermInfo, write: &mut W) -> io::Result<()> {
        VInt(term_info.doc_freq as u64).serialize(write)?;
        let delta_postings_offset = term_info.postings_offset - self.term_info.postings_offset;
        VInt(delta_postings_offset as u64).serialize(write)?;
        if self.has_positions {
            let delta_positions_offset =  term_info.positions_offset - self.term_info.positions_offset;
            VInt(delta_positions_offset as u64).serialize(write)?;
            write.write(&[term_info.positions_inner_offset])?;
        }
        mem::replace(&mut self.term_info, term_info);
        Ok(())
    }
}

fn deserialize_vint(data: &mut &[u8]) -> u64 {
    let mut res = 0;
    let mut shift = 0;
    for i in 0.. {
        let b = data[i];
        res |= ((b % 128u8) as u64) << shift;
        if b & 128u8 != 0u8 {
            *data = &data[(i + 1)..];
            break;
        }
        shift += 7;
    }
    res
}

pub struct TermInfoDeltaDecoder {
    term_info: TermInfo,
    has_positions: bool,
}

impl TermInfoDeltaDecoder {
    pub fn new(has_positions: bool) -> TermInfoDeltaDecoder {
        TermInfoDeltaDecoder {
            term_info: TermInfo::default(),
            has_positions: has_positions,
        }
    }

    pub fn decode(&mut self, cursor: &mut &[u8]) {
        let doc_freq = deserialize_vint(cursor) as u32;
        self.term_info.doc_freq = doc_freq;
        let delta_postings = deserialize_vint(cursor) as u32;
        self.term_info.postings_offset += delta_postings;
        if self.has_positions {
            let delta_positions = deserialize_vint(cursor) as u32;
            self.term_info.positions_offset += delta_positions;
            let position_inner_offset = cursor[0];
            *cursor = &cursor[1..];
            self.term_info.positions_inner_offset = position_inner_offset;
        }
    }

    pub fn term_info(&self) -> &TermInfo {
        &self.term_info
    }
}


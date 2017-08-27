use postings::TermInfo;
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
    prefix_len: usize,
}

impl TermDeltaEncoder {
    pub fn encode<'a>(&mut self, term: &'a [u8]) {
        self.prefix_len = common_prefix_len(term, &self.last_term);
        self.last_term.truncate(self.prefix_len);
        self.last_term.extend_from_slice(&term[self.prefix_len..]);
    }

    pub fn term(&self) -> &[u8] {
        &self.last_term[..]
    }

    pub fn prefix_suffix(&mut self) -> (usize, &[u8]) {
        (self.prefix_len, &self.last_term[self.prefix_len..])
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

    pub fn decode(&mut self, prefix_len: usize, suffix: &[u8]) {
        self.term.truncate(prefix_len);
        self.term.extend_from_slice(suffix);
    }

    pub fn term(&self) -> &[u8]  {
        &self.term[..]
    }
}

#[derive(Default)]
pub struct DeltaTermInfo {
    pub doc_freq: u32,
    pub delta_postings_offset: u32,
    pub delta_positions_offset: u32,
    pub positions_inner_offset: u8,
}

pub struct TermInfoDeltaEncoder {
    term_info: TermInfo,
    pub has_positions: bool,
}

impl TermInfoDeltaEncoder {

    pub fn new(has_positions: bool) -> Self {
        TermInfoDeltaEncoder {
            term_info: TermInfo::default(),
            has_positions: has_positions,
        }
    }

    pub fn encode(&mut self, term_info: TermInfo) -> DeltaTermInfo {
        let mut delta_term_info = DeltaTermInfo {
            doc_freq: term_info.doc_freq,
            delta_postings_offset: term_info.postings_offset - self.term_info.postings_offset,
            delta_positions_offset: 0,
            positions_inner_offset: 0,
        };
        if self.has_positions {
            delta_term_info.delta_positions_offset = term_info.positions_offset - self.term_info.positions_offset;
            delta_term_info.positions_inner_offset = term_info.positions_inner_offset;
        }
        mem::replace(&mut self.term_info, term_info);
        delta_term_info
    }
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

    pub fn decode(&mut self, code: u8, cursor: &mut &[u8]) {
        let num_bytes_docfreq: usize = ((code >> 1) & 3) as usize;
        let num_bytes_postings_offset: usize = ((code >> 3) & 3) as usize;
        const MASK: [u32; 4] = [
            0xffu32,
            0xffffu32,
            0xffffffu32,
            0xffffffffu32,
        ];
        let doc_freq: u32 = unsafe { *(cursor.as_ptr() as *const u32) } & MASK[num_bytes_docfreq];
        *cursor = &cursor[num_bytes_docfreq + 1 ..];
        let delta_postings_offset: u32 = unsafe { *(cursor.as_ptr() as *const u32) } & MASK[num_bytes_postings_offset];
        *cursor = &cursor[num_bytes_postings_offset + 1..];
        self.term_info.doc_freq = doc_freq;
        self.term_info.postings_offset += delta_postings_offset;
        if self.has_positions {
            let num_bytes_positions_offset = ((code >> 5) & 3) as usize;
            let delta_positions_offset: u32 = unsafe { *(cursor.as_ptr() as *const u32) } & MASK[num_bytes_positions_offset];
            self.term_info.positions_offset += delta_positions_offset;
            self.term_info.positions_inner_offset = cursor[num_bytes_positions_offset + 1];
            *cursor = &cursor[num_bytes_positions_offset + 2..];
        }
    }

    pub fn term_info(&self) -> &TermInfo {
        &self.term_info
    }
}


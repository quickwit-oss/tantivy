pub fn common_prefix_len(s1: &[u8], s2: &[u8]) -> usize {
    s1.iter()
        .zip(s2.iter())
        .take_while(|&(a, b)| a==b)
        .count()
}


#[derive(Default)]
pub struct DeltaEncoder {
    last_term: Vec<u8>,
}

impl DeltaEncoder {
    pub fn encode<'a>(&mut self, term: &'a [u8]) -> (usize, &'a [u8]) {
        let prefix_len = common_prefix_len(term, &self.last_term);
        self.last_term.truncate(prefix_len);
        self.last_term.extend_from_slice(&term[prefix_len..]);
        (prefix_len, &term[prefix_len..])
    }

    pub fn term(&self) -> &[u8] {
        &self.last_term[..]
    }
}

#[derive(Default)]
pub struct DeltaDecoder {
    term: Vec<u8>,
}

impl DeltaDecoder {
    pub fn with_previous_term(term: Vec<u8>) -> DeltaDecoder {
        DeltaDecoder {
            term: Vec::from(term)
        }
    }

    pub fn decode(&mut self, prefix_len: usize, suffix: &[u8]) -> &[u8] {
        self.term.truncate(prefix_len);
        self.term.extend_from_slice(suffix);
        &self.term[..]
    }

    pub fn term(&self) -> &[u8]  {
        &self.term[..]
    }
}

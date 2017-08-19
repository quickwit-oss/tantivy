use compression::{BlockEncoder, BlockDecoder, NUM_DOCS_PER_BLOCK};
use std::io::{self, Write};

fn compute_common_prefix_length(left: &[u8], right: &[u8]) -> usize {
    left.iter()
        .cloned()
        .zip(right.iter().cloned())
        .take_while(|&(b1, b2)| b1 == b2)
        .count()
}


pub struct TermBlockEncoder {
    block_encoder: BlockEncoder,

    pop_lens: [u32; NUM_DOCS_PER_BLOCK],
    push_lens: [u32; NUM_DOCS_PER_BLOCK],
    suffixes: Vec<u8>,

    previous_key: Vec<u8>,
    count: usize,
}

impl TermBlockEncoder {
    pub fn new() -> TermBlockEncoder {
        TermBlockEncoder {
            block_encoder: BlockEncoder::new(),
            pop_lens: [0u32; NUM_DOCS_PER_BLOCK],
            push_lens: [0u32; NUM_DOCS_PER_BLOCK],
            suffixes: Vec::with_capacity(NUM_DOCS_PER_BLOCK*5),

            previous_key: Vec::with_capacity(30),

            count: 0,
        }
    }

    pub fn encode(&mut self, key: &[u8]) {
        let common_prefix_len = compute_common_prefix_length(&self.previous_key, key);
        self.pop_lens[self.count] = (self.previous_key.len() - common_prefix_len) as u32;
        self.push_lens[self.count] = (key.len() - common_prefix_len) as u32;
        self.previous_key.clear();
        let suffix = &key[common_prefix_len..];
        self.suffixes.extend_from_slice(suffix);
        self.previous_key.extend_from_slice(key);
        self.count += 1;
    }

    pub fn len(&self) -> usize {
        self.count
    }

    pub fn flush<W: Write>(&mut self, output: &mut W) -> io::Result<()> {
        for i in self.count..NUM_DOCS_PER_BLOCK {
            self.pop_lens[i] = 0u32;
            self.push_lens[i] = 0u32;
        }
        output.write_all(self.block_encoder.compress_block_unsorted(&self.pop_lens))?;
        output.write_all(self.block_encoder.compress_block_unsorted(&self.push_lens))?;
        output.write_all(&self.suffixes[..])?;
        self.suffixes.clear();
        self.count = 0;
        Ok(())
    }
}



pub struct TermBlockDecoder<'a> {
    pop_lens_decoder: BlockDecoder,
    push_lens_decoder: BlockDecoder,
    suffixes: &'a [u8],
    current_key: Vec<u8>,
    cursor: usize,
}


impl<'a> TermBlockDecoder<'a> {
    pub fn new() -> TermBlockDecoder<'a> {
        TermBlockDecoder::given_previous_term(&[])
    }

    pub fn cursor(&self) -> usize {
        self.cursor
    }

    pub fn given_previous_term(previous_term: &[u8]) -> TermBlockDecoder<'a> {
        let mut current_key = Vec::with_capacity(30);
        current_key.extend_from_slice(previous_term);
        TermBlockDecoder {
            pop_lens_decoder: BlockDecoder::new(),
            push_lens_decoder: BlockDecoder::new(),
            current_key: current_key,
            suffixes: &[],
            cursor: 0,
        }
    }

    pub fn term(&self) -> &[u8] {
        &self.current_key
    }

    pub fn decode_block(&mut self, mut compressed_data: &'a [u8]) -> &'a [u8] {
        {
            let consumed_data_len = self.pop_lens_decoder.uncompress_block_unsorted(compressed_data);
            compressed_data = &compressed_data[consumed_data_len..];
        }
        {
            let consumed_data_len = self.push_lens_decoder.uncompress_block_unsorted(compressed_data);
            compressed_data = &compressed_data[consumed_data_len..];
        }
        let suffix_len: u32 = self.push_lens_decoder.output_array()[0..].iter().cloned().sum();
        let suffix_len: usize = suffix_len as usize;
        self.suffixes = &compressed_data[..suffix_len];
        self.cursor = 0;
        &compressed_data[suffix_len..]
    }

    pub fn advance(&mut self) {
        assert!(self.cursor < NUM_DOCS_PER_BLOCK);
        let pop_len = self.pop_lens_decoder.output(self.cursor) as usize;
        let push_len = self.push_lens_decoder.output(self.cursor) as usize;
        let previous_len = self.current_key.len();
        self.current_key.truncate(previous_len - pop_len);
        self.current_key.extend_from_slice(&self.suffixes[..push_len]);
        self.suffixes = &self.suffixes[push_len..];
        self.cursor += 1;
    }
}



#[cfg(test)]
mod tests {
    use super::{TermBlockEncoder, TermBlockDecoder};

    #[test]
    fn test_encoding_terms() {
        let mut buffer: Vec<u8> = vec!();
        let mut terms = vec!();
        {
            let mut term_block_encoder = TermBlockEncoder::new();
            for i in 0..128 {
                terms.push(format!("term{}", i * 7231));
            }
            for term in &terms {
                term_block_encoder.encode(term.as_bytes());
            }
            term_block_encoder.flush(&mut buffer).unwrap();
        }
        assert_eq!(buffer.len(), 711);

        let mut block_decoder = TermBlockDecoder::new();
        assert_eq!(block_decoder.decode_block(&buffer[..]).len(), 0);
        for i in 0..128 {
            block_decoder.advance();
            assert_eq!(block_decoder.term(), terms[i].as_bytes());
        }

    }
}




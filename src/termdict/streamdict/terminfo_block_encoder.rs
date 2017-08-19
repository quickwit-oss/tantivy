use compression::{BlockEncoder, BlockDecoder, VIntEncoder, VIntDecoder, NUM_DOCS_PER_BLOCK};
use postings::TermInfo;
use std::io::{self, Write};

pub struct TermInfoBlockEncoder {
    block_encoder: BlockEncoder,

    doc_freqs: [u32; NUM_DOCS_PER_BLOCK],
    postings_offsets: [u32; NUM_DOCS_PER_BLOCK],
    positions_offsets: [u32; NUM_DOCS_PER_BLOCK],
    positions_inner_offset: [u8; NUM_DOCS_PER_BLOCK],

    cursor: usize,
    encode_positions: bool,
}

impl TermInfoBlockEncoder {
    pub fn new(encode_positions: bool) -> TermInfoBlockEncoder {
        TermInfoBlockEncoder {
            block_encoder: BlockEncoder::new(),

            doc_freqs: [0u32; NUM_DOCS_PER_BLOCK],
            postings_offsets: [0u32; NUM_DOCS_PER_BLOCK],
            positions_offsets: [0u32; NUM_DOCS_PER_BLOCK],
            positions_inner_offset: [0u8; NUM_DOCS_PER_BLOCK],

            cursor: 0,
            encode_positions: encode_positions,
        }
    }

    pub fn encode(&mut self, term_info: &TermInfo) {
        self.doc_freqs[self.cursor] = term_info.doc_freq;
        self.postings_offsets[self.cursor] = term_info.postings_offset;
        self.positions_offsets[self.cursor] = term_info.positions_offset;
        self.positions_inner_offset[self.cursor] = term_info.positions_inner_offset;
        self.cursor += 1;
    }

    pub fn flush<W: Write>(&mut self, output: &mut W) -> io::Result<()> {
        output.write_all(self.block_encoder.compress_vint_unsorted(&self.doc_freqs[..self.cursor]))?;
        output.write_all(self.block_encoder.compress_vint_sorted(&self.postings_offsets[..self.cursor], 0u32))?;
        if self.encode_positions {
            output.write_all(self.block_encoder.compress_vint_sorted(&self.positions_offsets[..self.cursor], 0u32))?;
            output.write_all(&self.positions_inner_offset[..self.cursor])?;
        }
        self.cursor = 0;
        Ok(())
    }
}



pub struct  TermInfoBlockDecoder<'a> {
    doc_freq_decoder: BlockDecoder,
    postings_decoder: BlockDecoder,
    positions_decoder: BlockDecoder,
    positions_inner_offset: &'a [u8],
    current_term_info: TermInfo,

    cursor: usize,
    has_positions: bool,
}


impl<'a> TermInfoBlockDecoder<'a> {
    pub fn new(has_positions: bool) -> TermInfoBlockDecoder<'a> {
        TermInfoBlockDecoder {
            doc_freq_decoder: BlockDecoder::new(),
            postings_decoder: BlockDecoder::new(),
            positions_decoder: BlockDecoder::new(),
            positions_inner_offset: &[],

            current_term_info: TermInfo::default(),
            cursor: 0,
            has_positions: has_positions,
        }
    }


    pub fn term_info(&self) -> &TermInfo {
        &self.current_term_info
    }

    pub fn decode_block(&mut self, mut compressed_data: &'a [u8], num_els: usize) -> &'a [u8] {
        self.cursor = 0;
        {
            let consumed_size = self.doc_freq_decoder.uncompress_vint_unsorted(compressed_data, num_els);
            compressed_data = &compressed_data[consumed_size..];
        }
        {
            let consumed_size = self.postings_decoder.uncompress_vint_sorted(compressed_data, 0u32, num_els);
            compressed_data = &compressed_data[consumed_size..];
        }
        if self.has_positions {
            let consumed_size = self.positions_decoder.uncompress_vint_sorted(compressed_data, 0u32, num_els);
            compressed_data = &compressed_data[consumed_size..];
            self.positions_inner_offset = &compressed_data[..num_els];
            &compressed_data[num_els..]
        }
        else {
            compressed_data
        }
    }

    pub fn advance(&mut self) {
        assert!(self.cursor < NUM_DOCS_PER_BLOCK);
        self.current_term_info.doc_freq = self.doc_freq_decoder.output(self.cursor);
        self.current_term_info.postings_offset = self.postings_decoder.output(self.cursor);
        if self.has_positions {
            self.current_term_info.positions_offset = self.positions_decoder.output(self.cursor);
            self.current_term_info.positions_inner_offset = self.positions_inner_offset[self.cursor];
        }
        self.cursor += 1;
    }

}
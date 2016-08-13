use compression::SIMDBlockDecoder;
use std::io::Cursor;
use common::VInt;
use common::BinarySerializable;
use compression::CompositeDecoder;
use compression::NUM_DOCS_PER_BLOCK;


enum Option {
    NoFreq,
    Freq,
    FreqAndPositions,
}

pub struct FreqHandler {
    freq_decoder: SIMDBlockDecoder,
    positions: Vec<u32>,
    option: Option,
    positions_offsets: [usize; NUM_DOCS_PER_BLOCK + 1],
}


fn read_positions(data: &[u8]) -> Vec<u32> {
    let mut composite_reader = CompositeDecoder::new();   
    let mut cursor = Cursor::new(data);
    // TODO error
    let uncompressed_len = VInt::deserialize(&mut cursor).unwrap().0 as usize;
    let offset_data = &data[cursor.position() as usize..];
    composite_reader.uncompress_unsorted(offset_data, uncompressed_len);
    composite_reader.into()
}



impl FreqHandler {
    
    pub fn new() -> FreqHandler {
        FreqHandler {
            freq_decoder: SIMDBlockDecoder::with_val(1u32),
            positions: Vec::new(), 
            option: Option::NoFreq,
            positions_offsets: [0; NUM_DOCS_PER_BLOCK + 1],
        }
    }
    
    pub fn new_with_freq() -> FreqHandler {
        FreqHandler {
            freq_decoder: SIMDBlockDecoder::new(),
            positions: Vec::new(),
            option: Option::Freq,
            positions_offsets: [0; NUM_DOCS_PER_BLOCK + 1],
        }
    }

    pub fn new_with_freq_and_position(position_data: &[u8]) -> FreqHandler {
        let positions = read_positions(position_data);
        FreqHandler {
            freq_decoder: SIMDBlockDecoder::new(),
            positions: positions, 
            option: Option::FreqAndPositions,
            positions_offsets: [0; NUM_DOCS_PER_BLOCK + 1],
        }
    }
    
    fn fill_positions_offset(&mut self,) {
        let mut cur_position: usize = self.positions_offsets[NUM_DOCS_PER_BLOCK];
        let mut i: usize  = 0;
        self.positions_offsets[i] = cur_position;
        let mut last_cur_position = 0;
        for &doc_freq in self.freq_decoder.output_array() {
            i += 1;
            let mut cumulated_pos = 0u32;
            for j in last_cur_position..(last_cur_position + (doc_freq as usize)) {
                cumulated_pos += self.positions[j];
                self.positions[j] = cumulated_pos;
            } 
            cur_position += doc_freq as usize;
            self.positions_offsets[i] = cur_position;
            last_cur_position = cur_position;
        }
    }
    
    pub fn positions(&self, idx: usize) -> &[u32] {
        let start = self.positions_offsets[idx];
        let stop = self.positions_offsets[idx + 1];
        &self.positions[start..stop]        
    }
    
    pub fn read_freq_block<'a>(&mut self, data: &'a [u8]) -> &'a [u8] {
        match self.option {
            Option::NoFreq => {
                data
            }
            Option::Freq => {
                self.freq_decoder.uncompress_block_unsorted(data)
            }
            Option::FreqAndPositions => {
                let remaining: &'a [u8] = self.freq_decoder.uncompress_block_unsorted(data);
                self.fill_positions_offset();
                remaining
            }
        }
    }

    pub fn read_freq_vint(&mut self, data: &[u8], num_els: usize) {
        match self.option {
            Option::NoFreq => {}
            Option::Freq => {
                self.freq_decoder.uncompress_vint_unsorted(data, num_els);
            }
            Option::FreqAndPositions => {
                self.freq_decoder.uncompress_vint_unsorted(data, num_els);
                self.fill_positions_offset();
            }
        }
    }
    
    #[inline(always)]
    pub fn freq(&self, idx: usize)-> u32 {
        self.freq_decoder.output(idx)
    }
}
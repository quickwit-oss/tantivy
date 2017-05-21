use compression::BlockDecoder;
use common::VInt;
use common::BinarySerializable;
use compression::{CompositeDecoder, VIntDecoder};
use postings::SegmentPostingsOption;
use compression::NUM_DOCS_PER_BLOCK;


/// `FreqHandler`  is in charge of decompressing
/// frequencies and/or positions.
pub struct FreqHandler {
    freq_decoder: BlockDecoder,
    positions: Vec<u32>,
    option: SegmentPostingsOption,
    positions_offsets: [usize; NUM_DOCS_PER_BLOCK + 1],
}


fn read_positions(data: &[u8]) -> Vec<u32> {
    let mut composite_reader = CompositeDecoder::new();
    let mut readable: &[u8] = data;
    let uncompressed_len = VInt::deserialize(&mut readable).unwrap().0 as usize;
    composite_reader.uncompress_unsorted(readable, uncompressed_len);
    composite_reader.into()
}



impl FreqHandler {
    /// Returns a `FreqHandler` that just decodes `DocId`s.
    pub fn new_without_freq() -> FreqHandler {
        FreqHandler {
            freq_decoder: BlockDecoder::with_val(1u32),
            positions: Vec::new(),
            option: SegmentPostingsOption::NoFreq,
            positions_offsets: [0; NUM_DOCS_PER_BLOCK + 1],
        }
    }

    /// Returns a `FreqHandler` that decodes `DocId`s and term frequencies.
    pub fn new_with_freq() -> FreqHandler {
        FreqHandler {
            freq_decoder: BlockDecoder::new(),
            positions: Vec::new(),
            option: SegmentPostingsOption::Freq,
            positions_offsets: [0; NUM_DOCS_PER_BLOCK + 1],
        }
    }

    /// Returns a `FreqHandler` that decodes `DocId`s, term frequencies, and term positions.
    pub fn new_with_freq_and_position(position_data: &[u8]) -> FreqHandler {
        let positions = read_positions(position_data);
        FreqHandler {
            freq_decoder: BlockDecoder::new(),
            positions: positions,
            option: SegmentPostingsOption::FreqAndPositions,
            positions_offsets: [0; NUM_DOCS_PER_BLOCK + 1],
        }
    }

    fn fill_positions_offset(&mut self) {
        let mut cur_position: usize = self.positions_offsets[NUM_DOCS_PER_BLOCK];
        let mut i: usize = 0;
        self.positions_offsets[i] = cur_position;
        let mut last_cur_position = cur_position;
        for &doc_freq in self.freq_decoder.output_array() {
            i += 1;
            let mut cumulated_pos = 0u32;
            // this next loop decodes delta positions into normal positions.
            for j in last_cur_position..(last_cur_position + (doc_freq as usize)) {
                cumulated_pos += self.positions[j];
                self.positions[j] = cumulated_pos;
            }
            cur_position += doc_freq as usize;
            self.positions_offsets[i] = cur_position;
            last_cur_position = cur_position;
        }
    }


    /// Accessor to term frequency
    ///
    /// idx is the offset of the current doc in the block.
    /// It takes value between 0 and 128.
    pub fn freq(&self, idx: usize) -> u32 {
        self.freq_decoder.output(idx)
    }

    /// Accessor to the positions
    ///
    /// idx is the offset of the current doc in the block.
    /// It takes value between 0 and 128.
    pub fn positions(&self, idx: usize) -> &[u32] {
        let start = self.positions_offsets[idx];
        let stop = self.positions_offsets[idx + 1];
        &self.positions[start..stop]
    }

    /// Decompresses a complete frequency block
    pub fn read_freq_block<'a>(&mut self, data: &'a [u8]) -> &'a [u8] {
        match self.option {
            SegmentPostingsOption::NoFreq => data,
            SegmentPostingsOption::Freq => self.freq_decoder.uncompress_block_unsorted(data),
            SegmentPostingsOption::FreqAndPositions => {
                let remaining: &'a [u8] = self.freq_decoder.uncompress_block_unsorted(data);
                self.fill_positions_offset();
                remaining
            }
        }
    }

    /// Decompresses an incomplete frequency block
    pub fn read_freq_vint(&mut self, data: &[u8], num_els: usize) {
        match self.option {
            SegmentPostingsOption::NoFreq => {}
            SegmentPostingsOption::Freq => {
                self.freq_decoder.uncompress_vint_unsorted(data, num_els);
            }
            SegmentPostingsOption::FreqAndPositions => {
                self.freq_decoder.uncompress_vint_unsorted(data, num_els);
                self.fill_positions_offset();
            }
        }
    }
}

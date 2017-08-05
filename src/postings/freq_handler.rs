use compression::BlockDecoder;
use compression::VIntDecoder;
use postings::SegmentPostingsOption;
use compression::NUM_DOCS_PER_BLOCK;
use std::cell::UnsafeCell;

/// `FreqHandler`  is in charge of decompressing
/// frequencies and/or positions.
pub struct FreqHandler {
    freq_decoder: BlockDecoder,
    positions: UnsafeCell<Vec<u32>>,
    option: SegmentPostingsOption,
}



impl FreqHandler {
    /// Returns a `FreqHandler` that just decodes `DocId`s.
    pub fn new_without_freq() -> FreqHandler {
        FreqHandler {
            freq_decoder: BlockDecoder::with_val(1u32),
            positions: UnsafeCell::new(Vec::with_capacity(0)),
            option: SegmentPostingsOption::NoFreq,
        }
    }

    /// Returns a `FreqHandler` that decodes `DocId`s and term frequencies.
    pub fn new_with_freq() -> FreqHandler {
        FreqHandler {
            freq_decoder: BlockDecoder::new(),
            positions: UnsafeCell::new(Vec::with_capacity(0)),
            option: SegmentPostingsOption::Freq,
        }
    }

    /// Returns a `FreqHandler` that decodes `DocId`s, term frequencies, and term positions.
    pub fn new_with_freq_and_position(position_data: &[u8], within_block_offset: u8) -> FreqHandler {
        FreqHandler {
            freq_decoder: BlockDecoder::new(),
            positions: UnsafeCell::new(Vec::with_capacity(NUM_DOCS_PER_BLOCK)),
            option: SegmentPostingsOption::FreqAndPositions,
        }
    }

    /*
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
    }*/


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
        //unsafe { &self.positions.get() }
        println!("fix positions");
        self.delta_positions(idx)

    }

    /// Accessor to the delta positions.
    /// Delta positions is simply the difference between
    /// two consecutive positions.
    /// The first delta position is the first position of the
    /// term in the document.
    ///
    /// For instance, if positions are `[7,13,17]`
    /// then delta positions `[7, 6, 4]`
    ///
    /// idx is the offset of the current doc in the docid/freq block.
    /// It takes value between 0 and 128.
    pub fn delta_positions(&self, idx: usize) -> &[u32] {
        let freq = self.freq(idx);
        let positions: &mut Vec<u32> = unsafe { &mut *self.positions.get() };
        positions.resize(freq as usize, 0u32);
        &positions[..]
    }


    /// Decompresses a complete frequency block
    pub fn read_freq_block<'a>(&mut self, data: &'a [u8]) -> &'a [u8] {
        match self.option {
            SegmentPostingsOption::NoFreq => data,
            SegmentPostingsOption::Freq => self.freq_decoder.uncompress_block_unsorted(data),
            SegmentPostingsOption::FreqAndPositions => {
                let remaining: &'a [u8] = self.freq_decoder.uncompress_block_unsorted(data);
                // self.fill_positions_offset();
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
                // self.fill_positions_offset();
            }
        }
    }
}

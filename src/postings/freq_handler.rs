use compression::SIMDBlockDecoder;

pub enum FreqHandler {
    FreqReader(SIMDBlockDecoder),
    // SkipFreq,
    NoFreq,
}

const EMPTY: [u32; 0] = [];

impl FreqHandler {

    pub fn new_freq_reader() -> FreqHandler {
        FreqHandler::FreqReader(SIMDBlockDecoder::new())
    }

    pub fn read_freq_block<'a>(&mut self, data: &'a [u8]) -> &'a [u8] {
        match *self {
            FreqHandler::FreqReader(ref mut block_decoder) => {
                block_decoder.uncompress_block_unsorted(data)
            }
            FreqHandler::NoFreq => {
                data
            }
        }
    }

    pub fn read_freq_vint(&mut self, data: &[u8], num_els: usize) {
        match *self {
            FreqHandler::FreqReader(ref mut block_decoder) => {
                block_decoder.uncompress_vint_unsorted(data, num_els);
            }
            FreqHandler::NoFreq => {
            }
        }

    }

    pub fn output_array(&self,)-> &[u32] {
        match *self {
            FreqHandler::FreqReader(ref block_decoder) => {
                block_decoder.output_array()
            }
            FreqHandler::NoFreq => {
                &EMPTY
            }
        }
    }

    #[inline(always)]
    pub fn output(&self, idx: usize)-> u32 {
        match *self {
            FreqHandler::FreqReader(ref block_decoder) => {
                block_decoder.output(idx)
            }
            FreqHandler::NoFreq => {
                1u32
            }
        }
    }
}
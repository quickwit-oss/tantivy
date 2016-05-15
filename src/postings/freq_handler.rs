use compression::SIMDBlockDecoder;
use DocId;

pub enum FreqHandler {
    FreqReader(SIMDBlockDecoder),
    // SkipFreq,
    NoFreq,
}

impl FreqHandler {
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

}
//
//
// pub struct FreqReader {
//     block_decoder: SIMDBlockDecoder,
// }
//
// impl FreqReader {
//     fn read_freq_block<'a>(&mut self, data: &'a [u8]) -> &'a [u8] {
//         self.block_decoder.uncompress_block_unsorted(data)
//     }
//
//     fn term_freq_block(&self, doc: DocId) -> u32 {
//         self.block_decoder.output()[doc as usize]
//     }
//
//     fn read_freq_vint(&mut self, data: &[u8], num_els: usize) {
//         self.block_decoder.uncompress_vint_unsorted(data, num_els)
//     }
// }
//
//
// pub struct NoFreqReader;
//
// impl FreqHandler for NoFreqReader {
//     fn read_freq_block<'a>(&mut self, data: &'a [u8]) -> &'a [u8] {
//         data
//     }
//
//     fn term_freq_block(&self, _doc: DocId) -> u32 {
//         0
//     }
//
//     fn read_freq_vint(&mut self, _data: &[u8], _num_els: usize) {
//     }
// }

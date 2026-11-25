use crate::directory::OwnedBytes;
use crate::postings::compression::{compressed_block_size, COMPRESSION_BLOCK_SIZE};
use crate::query::Bm25Weight;
use crate::schema::IndexRecordOption;
use crate::{DocId, Score, TERMINATED};

// doc num bits uses the following encoding:
// given 0b a b cdefgh
//         |1|2|3|  4  |
// - 1: unused
// - 2: is delta-1 encoded. 0 if not, 1, if yes
// - 3: unused
// - 4: a 5 bit number in 0..32, the actual bitwidth. Bitpacking could in theory say this is 32
//   (requiring a 6th bit), but the biggest doc_id we can want to encode is TERMINATED-1, which can
//   be represented on 31b without delta encoding.
fn encode_bitwidth(bitwidth: u8, delta_1: bool) -> u8 {
    assert!(
        bitwidth < 32,
        "bitwidth needs to be less than 32, but got {}",
        bitwidth
    );
    bitwidth | ((delta_1 as u8) << 6)
}

fn decode_bitwidth(raw_bitwidth: u8) -> (u8, bool) {
    let delta_1 = ((raw_bitwidth >> 6) & 1) != 0;
    let bitwidth = raw_bitwidth & 0x1f;
    (bitwidth, delta_1)
}

#[inline]
fn encode_block_wand_max_tf(max_tf: u32) -> u8 {
    max_tf.min(u8::MAX as u32) as u8
}

#[inline]
fn decode_block_wand_max_tf(max_tf_code: u8) -> u32 {
    if max_tf_code == u8::MAX {
        u32::MAX
    } else {
        max_tf_code as u32
    }
}

#[inline]
fn read_u32(data: &[u8]) -> u32 {
    u32::from_le_bytes(data[..4].try_into().unwrap())
}

#[inline]
fn write_u32(val: u32, buf: &mut Vec<u8>) {
    buf.extend_from_slice(&val.to_le_bytes());
}

pub struct SkipSerializer {
    buffer: Vec<u8>,
}

impl SkipSerializer {
    pub fn new() -> SkipSerializer {
        SkipSerializer { buffer: Vec::new() }
    }

    pub fn write_doc(&mut self, last_doc: DocId, doc_num_bits: u8) {
        write_u32(last_doc, &mut self.buffer);
        self.buffer.push(encode_bitwidth(doc_num_bits, true));
    }

    pub fn write_term_freq(&mut self, tf_num_bits: u8) {
        self.buffer.push(tf_num_bits);
    }

    pub fn write_total_term_freq(&mut self, tf_sum: u32) {
        write_u32(tf_sum, &mut self.buffer);
    }

    pub fn write_blockwand_max(&mut self, fieldnorm_id: u8, term_freq: u32) {
        let block_wand_tf = encode_block_wand_max_tf(term_freq);
        self.buffer
            .extend_from_slice(&[fieldnorm_id, block_wand_tf]);
    }

    pub fn data(&self) -> &[u8] {
        &self.buffer[..]
    }

    pub fn clear(&mut self) {
        self.buffer.clear();
    }
}

#[derive(Clone)]
pub(crate) struct SkipReader {
    last_doc_in_block: DocId,
    pub(crate) last_doc_in_previous_block: DocId,
    owned_read: OwnedBytes,
    skip_info: IndexRecordOption,
    byte_offset: usize,
    remaining_docs: u32, // number of docs remaining, including the
    // documents in the current block.
    block_info: BlockInfo,

    position_offset: u64,
}

#[derive(Clone, Eq, PartialEq, Copy, Debug)]
pub(crate) enum BlockInfo {
    BitPacked {
        doc_num_bits: u8,
        strict_delta_encoded: bool,
        tf_num_bits: u8,
        tf_sum: u32,
        block_wand_fieldnorm_id: u8,
        block_wand_term_freq: u32,
    },
    VInt {
        num_docs: u32,
    },
}

impl Default for BlockInfo {
    fn default() -> Self {
        BlockInfo::VInt { num_docs: 0u32 }
    }
}

impl SkipReader {
    pub fn new(data: OwnedBytes, doc_freq: u32, skip_info: IndexRecordOption) -> SkipReader {
        let mut skip_reader = SkipReader {
            last_doc_in_block: if doc_freq >= COMPRESSION_BLOCK_SIZE as u32 {
                0
            } else {
                TERMINATED
            },
            last_doc_in_previous_block: 0u32,
            owned_read: data,
            skip_info,
            block_info: BlockInfo::VInt { num_docs: doc_freq },
            byte_offset: 0,
            remaining_docs: doc_freq,
            position_offset: 0u64,
        };
        if doc_freq >= COMPRESSION_BLOCK_SIZE as u32 {
            skip_reader.read_block_info();
        }
        skip_reader
    }

    #[inline(always)]
    pub fn has_remaining_docs(&self) -> bool {
        self.remaining_docs != 0
    }

    pub fn reset(&mut self, data: OwnedBytes, doc_freq: u32) {
        self.last_doc_in_block = if doc_freq >= COMPRESSION_BLOCK_SIZE as u32 {
            0
        } else {
            TERMINATED
        };
        self.last_doc_in_previous_block = 0u32;
        self.owned_read = data;
        self.block_info = BlockInfo::VInt { num_docs: doc_freq };
        self.byte_offset = 0;
        self.remaining_docs = doc_freq;
        self.position_offset = 0u64;
        if doc_freq >= COMPRESSION_BLOCK_SIZE as u32 {
            self.read_block_info();
        }
    }

    // Returns the block max score for this block if available.
    //
    // The block max score is available for all full bitpacked block,
    // but no available for the last VInt encoded incomplete block.
    pub fn block_max_score(&self, bm25_weight: &Bm25Weight) -> Option<Score> {
        match self.block_info {
            BlockInfo::BitPacked {
                block_wand_fieldnorm_id,
                block_wand_term_freq,
                ..
            } => Some(bm25_weight.score(block_wand_fieldnorm_id, block_wand_term_freq)),
            BlockInfo::VInt { .. } => None,
        }
    }

    pub(crate) fn last_doc_in_block(&self) -> DocId {
        self.last_doc_in_block
    }

    /// Number of docs from the start of the current block to the end of the postings
    /// (i.e. the current block plus every block after it).
    pub(crate) fn remaining_docs(&self) -> u32 {
        self.remaining_docs
    }

    pub fn position_offset(&self) -> u64 {
        self.position_offset
    }

    #[inline]
    pub fn byte_offset(&self) -> usize {
        self.byte_offset
    }

    fn read_block_info(&mut self) {
        let bytes = self.owned_read.as_slice();
        self.last_doc_in_block = read_u32(bytes);
        let advance_len = match self.skip_info {
            IndexRecordOption::Basic => {
                self.block_info = Self::decode_entry::<5>(bytes);
                5
            }
            IndexRecordOption::WithFreqs => {
                self.block_info = Self::decode_entry::<8>(bytes);
                8
            }
            IndexRecordOption::WithFreqsAndPositions => {
                self.block_info = Self::decode_entry::<12>(bytes);
                12
            }
        };
        self.owned_read.advance(advance_len);
    }

    pub fn block_info(&self) -> BlockInfo {
        self.block_info
    }

    /// Advance the skip reader to the block that may contain the target.
    ///
    /// If the target is larger than all documents, the skip_reader
    /// then advance to the last Variable In block.
    pub fn seek(&mut self, target: DocId) -> bool {
        if self.last_doc_in_block() >= target {
            return false;
        }
        match self.skip_info {
            IndexRecordOption::Basic => self.seek_blocks::<5>(target),
            IndexRecordOption::WithFreqs => self.seek_blocks::<8>(target),
            IndexRecordOption::WithFreqsAndPositions => self.seek_blocks::<12>(target),
        }
    }

    fn seek_blocks<const ENTRY_LEN: usize>(&mut self, target: DocId) -> bool {
        let entries = self.owned_read.as_slice();
        debug_assert_eq!(entries.len() % ENTRY_LEN, 0);
        let entry_count = entries.len() / ENTRY_LEN;
        let mut left = 0;
        let mut right = 0;
        let mut step = 1;
        while right < entry_count && read_u32(&entries[right * ENTRY_LEN..]) < target {
            left = right + 1;
            right = (right + step).min(entry_count);
            step *= 2;
        }
        while left < right {
            let middle = left + (right - left) / 2;
            let last_doc = read_u32(&entries[middle * ENTRY_LEN..]);
            if last_doc < target {
                left = middle + 1;
            } else {
                right = middle;
            }
        }
        let target_entry_index = left;
        let BlockInfo::BitPacked {
            doc_num_bits,
            tf_num_bits,
            tf_sum,
            ..
        } = self.block_info
        else {
            unreachable!();
        };
        let mut byte_offset = self.byte_offset + compressed_block_size(doc_num_bits + tf_num_bits);
        let mut position_offset = self.position_offset + tf_sum as u64;
        for entry in entries[..target_entry_index * ENTRY_LEN].chunks_exact(ENTRY_LEN) {
            let doc_num_bits = entry[4] & 0x1f;
            let tf_num_bits = if ENTRY_LEN >= 8 { entry[5] } else { 0 };
            byte_offset += compressed_block_size(doc_num_bits + tf_num_bits);
            if ENTRY_LEN == 12 {
                position_offset += read_u32(&entry[6..10]) as u64;
            }
        }
        let current_last_doc = self.last_doc_in_block;
        let advances = target_entry_index + 1;
        self.remaining_docs -= advances as u32 * COMPRESSION_BLOCK_SIZE as u32;
        self.last_doc_in_previous_block = if target_entry_index == 0 {
            current_last_doc
        } else {
            read_u32(&entries[(target_entry_index - 1) * ENTRY_LEN..])
        };
        let (last_doc_in_block, block_info) = if target_entry_index == entry_count {
            (
                TERMINATED,
                BlockInfo::VInt {
                    num_docs: self.remaining_docs,
                },
            )
        } else {
            let entry = &entries[target_entry_index * ENTRY_LEN..];
            (read_u32(entry), Self::decode_entry::<ENTRY_LEN>(entry))
        };
        self.byte_offset = byte_offset;
        self.position_offset = position_offset;
        self.last_doc_in_block = last_doc_in_block;
        self.block_info = block_info;
        let entries_consumed = advances.min(entry_count);
        self.owned_read.advance(entries_consumed * ENTRY_LEN);
        true
    }

    fn decode_entry<const ENTRY_LEN: usize>(entry: &[u8]) -> BlockInfo {
        let (doc_num_bits, strict_delta_encoded) = decode_bitwidth(entry[4]);
        let tf_num_bits = if ENTRY_LEN >= 8 { entry[5] } else { 0 };
        let tf_sum = if ENTRY_LEN == 12 {
            read_u32(&entry[6..10])
        } else {
            0
        };
        let (block_wand_fieldnorm_id, block_wand_term_freq) = match ENTRY_LEN {
            5 => (0, 0),
            8 => (entry[6], decode_block_wand_max_tf(entry[7])),
            12 => (entry[10], decode_block_wand_max_tf(entry[11])),
            _ => unreachable!(),
        };
        BlockInfo::BitPacked {
            doc_num_bits,
            strict_delta_encoded,
            tf_num_bits,
            tf_sum,
            block_wand_fieldnorm_id,
            block_wand_term_freq,
        }
    }

    pub fn advance(&mut self) {
        match self.block_info {
            BlockInfo::BitPacked {
                doc_num_bits,
                tf_num_bits,
                tf_sum,
                ..
            } => {
                self.remaining_docs -= COMPRESSION_BLOCK_SIZE as u32;
                self.byte_offset += compressed_block_size(doc_num_bits + tf_num_bits);
                self.position_offset += tf_sum as u64;
            }
            BlockInfo::VInt { num_docs } => {
                debug_assert_eq!(num_docs, self.remaining_docs);
                self.remaining_docs = 0;
                self.byte_offset = usize::MAX;
            }
        }
        self.last_doc_in_previous_block = self.last_doc_in_block;
        if self.remaining_docs >= COMPRESSION_BLOCK_SIZE as u32 {
            self.read_block_info();
        } else {
            self.last_doc_in_block = TERMINATED;
            self.block_info = BlockInfo::VInt {
                num_docs: self.remaining_docs,
            };
        }
    }
}

#[cfg(test)]
mod tests {

    use super::{
        decode_bitwidth, encode_bitwidth, BlockInfo, IndexRecordOption, SkipReader, SkipSerializer,
    };
    use crate::directory::OwnedBytes;
    use crate::postings::compression::COMPRESSION_BLOCK_SIZE;

    #[test]
    fn test_encode_block_wand_max_tf() {
        for tf in 0..255 {
            assert_eq!(super::encode_block_wand_max_tf(tf), tf as u8);
        }
        for &tf in &[255, 256, 1_000_000, u32::MAX] {
            assert_eq!(super::encode_block_wand_max_tf(tf), 255);
        }
    }

    #[test]
    fn test_decode_block_wand_max_tf() {
        for tf in 0..255 {
            assert_eq!(super::decode_block_wand_max_tf(tf), tf as u32);
        }
        assert_eq!(super::decode_block_wand_max_tf(255), u32::MAX);
    }

    #[test]
    fn test_skip_with_freq() {
        let buf = {
            let mut skip_serializer = SkipSerializer::new();
            skip_serializer.write_doc(1u32, 2u8);
            skip_serializer.write_term_freq(3u8);
            skip_serializer.write_blockwand_max(13u8, 3u32);
            skip_serializer.write_doc(5u32, 5u8);
            skip_serializer.write_term_freq(2u8);
            skip_serializer.write_blockwand_max(8u8, 2u32);
            skip_serializer.data().to_owned()
        };
        let doc_freq = 3u32 + (COMPRESSION_BLOCK_SIZE * 2) as u32;
        let mut skip_reader =
            SkipReader::new(OwnedBytes::new(buf), doc_freq, IndexRecordOption::WithFreqs);
        assert_eq!(skip_reader.last_doc_in_block(), 1u32);
        assert_eq!(
            skip_reader.block_info,
            BlockInfo::BitPacked {
                doc_num_bits: 2u8,
                strict_delta_encoded: true,
                tf_num_bits: 3u8,
                tf_sum: 0,
                block_wand_fieldnorm_id: 13,
                block_wand_term_freq: 3
            }
        );
        skip_reader.advance();
        assert_eq!(skip_reader.last_doc_in_block(), 5u32);
        assert_eq!(
            skip_reader.block_info(),
            BlockInfo::BitPacked {
                doc_num_bits: 5u8,
                strict_delta_encoded: true,
                tf_num_bits: 2u8,
                tf_sum: 0,
                block_wand_fieldnorm_id: 8,
                block_wand_term_freq: 2
            }
        );
        skip_reader.advance();
        assert_eq!(skip_reader.block_info(), BlockInfo::VInt { num_docs: 3u32 });
        skip_reader.advance();
        assert_eq!(skip_reader.block_info(), BlockInfo::VInt { num_docs: 0u32 });
        skip_reader.advance();
        assert_eq!(skip_reader.block_info(), BlockInfo::VInt { num_docs: 0u32 });
    }

    #[test]
    fn test_skip_no_freq() {
        let buf = {
            let mut skip_serializer = SkipSerializer::new();
            skip_serializer.write_doc(1u32, 2u8);
            skip_serializer.write_doc(5u32, 5u8);
            skip_serializer.data().to_owned()
        };
        let doc_freq = 3u32 + (COMPRESSION_BLOCK_SIZE * 2) as u32;
        let mut skip_reader =
            SkipReader::new(OwnedBytes::new(buf), doc_freq, IndexRecordOption::Basic);
        assert_eq!(skip_reader.last_doc_in_block(), 1u32);
        assert_eq!(
            skip_reader.block_info(),
            BlockInfo::BitPacked {
                doc_num_bits: 2u8,
                strict_delta_encoded: true,
                tf_num_bits: 0,
                tf_sum: 0u32,
                block_wand_fieldnorm_id: 0,
                block_wand_term_freq: 0
            }
        );
        skip_reader.advance();
        assert_eq!(skip_reader.last_doc_in_block(), 5u32);
        assert_eq!(
            skip_reader.block_info(),
            BlockInfo::BitPacked {
                doc_num_bits: 5u8,
                strict_delta_encoded: true,
                tf_num_bits: 0,
                tf_sum: 0u32,
                block_wand_fieldnorm_id: 0,
                block_wand_term_freq: 0
            }
        );
        skip_reader.advance();
        assert_eq!(skip_reader.block_info(), BlockInfo::VInt { num_docs: 3u32 });
        skip_reader.advance();
        assert_eq!(skip_reader.block_info(), BlockInfo::VInt { num_docs: 0u32 });
        skip_reader.advance();
        assert_eq!(skip_reader.block_info(), BlockInfo::VInt { num_docs: 0u32 });
    }

    #[test]
    fn test_skip_multiple_of_block_size() {
        let buf = {
            let mut skip_serializer = SkipSerializer::new();
            skip_serializer.write_doc(1u32, 2u8);
            skip_serializer.data().to_owned()
        };
        let doc_freq = COMPRESSION_BLOCK_SIZE as u32;
        let mut skip_reader =
            SkipReader::new(OwnedBytes::new(buf), doc_freq, IndexRecordOption::Basic);
        assert_eq!(skip_reader.last_doc_in_block(), 1u32);
        assert_eq!(
            skip_reader.block_info(),
            BlockInfo::BitPacked {
                doc_num_bits: 2u8,
                strict_delta_encoded: true,
                tf_num_bits: 0,
                tf_sum: 0u32,
                block_wand_fieldnorm_id: 0,
                block_wand_term_freq: 0
            }
        );
        skip_reader.advance();
        assert_eq!(skip_reader.block_info(), BlockInfo::VInt { num_docs: 0u32 });
    }

    fn assert_seek_matches_advance(
        actual: &mut SkipReader,
        expected: &mut SkipReader,
        target: u32,
    ) {
        let changed = actual.seek(target);
        let expected_changed = if expected.last_doc_in_block() >= target {
            false
        } else {
            loop {
                expected.advance();
                if expected.last_doc_in_block() >= target {
                    break true;
                }
            }
        };
        assert_eq!(changed, expected_changed, "target {target}");
        assert_eq!(
            actual.last_doc_in_block, expected.last_doc_in_block,
            "target {target}"
        );
        assert_eq!(
            actual.last_doc_in_previous_block, expected.last_doc_in_previous_block,
            "target {target}"
        );
        assert_eq!(actual.block_info, expected.block_info, "target {target}");
        assert_eq!(actual.byte_offset, expected.byte_offset, "target {target}");
        assert_eq!(
            actual.position_offset, expected.position_offset,
            "target {target}"
        );
        assert_eq!(
            actual.remaining_docs, expected.remaining_docs,
            "target {target}"
        );
        assert_eq!(
            actual.owned_read.as_slice(),
            expected.owned_read.as_slice(),
            "target {target}"
        );
    }

    fn seek_fixture(skip_info: IndexRecordOption) -> (Vec<u8>, u32) {
        let mut serializer = SkipSerializer::new();
        for block in 0..8u32 {
            serializer.write_doc(100 + block * 128, 1 + block as u8);
            match skip_info {
                IndexRecordOption::Basic => {}
                IndexRecordOption::WithFreqs => {
                    serializer.write_term_freq(2 + block as u8);
                    serializer.write_blockwand_max(10 + block as u8, 3 + block);
                }
                IndexRecordOption::WithFreqsAndPositions => {
                    serializer.write_term_freq(2 + block as u8);
                    serializer.write_total_term_freq(200 + block * 13);
                    serializer.write_blockwand_max(10 + block as u8, 3 + block);
                }
            }
        }
        let doc_freq = 3 + COMPRESSION_BLOCK_SIZE as u32 * 8;
        (serializer.data().to_owned(), doc_freq)
    }

    #[test]
    fn test_seek_matches_advance() {
        for skip_info in [
            IndexRecordOption::Basic,
            IndexRecordOption::WithFreqs,
            IndexRecordOption::WithFreqsAndPositions,
        ] {
            let (data, doc_freq) = seek_fixture(skip_info);
            for target in [
                0,
                1,
                99,
                100,
                101,
                227,
                228,
                355,
                500,
                900,
                996,
                997,
                2000,
                crate::TERMINATED,
            ] {
                let mut actual =
                    SkipReader::new(OwnedBytes::new(data.clone()), doc_freq, skip_info);
                let mut expected = actual.clone();
                assert_seek_matches_advance(&mut actual, &mut expected, target);
            }
            let mut actual = SkipReader::new(OwnedBytes::new(data), doc_freq, skip_info);
            let mut expected = actual.clone();
            for target in [1, 101, 300, 600, 997, 2000, crate::TERMINATED] {
                assert_seek_matches_advance(&mut actual, &mut expected, target);
            }
        }
    }

    #[test]
    fn test_encode_decode_bitwidth() {
        for bitwidth in 0..32 {
            for delta_1 in [false, true] {
                assert_eq!(
                    (bitwidth, delta_1),
                    decode_bitwidth(encode_bitwidth(bitwidth, delta_1))
                );
            }
        }
        assert_eq!(0b01000010, encode_bitwidth(0b10, true));
        assert_eq!(0b00000010, encode_bitwidth(0b10, false));
    }
}

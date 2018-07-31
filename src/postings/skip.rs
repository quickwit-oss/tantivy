use DocId;
use common::BinarySerializable;
use owned_read::OwnedRead;
use postings::compression::COMPRESSION_BLOCK_SIZE;
use schema::IndexRecordOption;

pub struct SkipSerializer {
    buffer: Vec<u8>,
    prev_doc: DocId,
}

impl SkipSerializer {
    pub fn new() -> SkipSerializer {
        SkipSerializer {
            buffer: Vec::new(),
            prev_doc: 0u32,
        }
    }

    pub fn write_doc(&mut self, last_doc: DocId, doc_num_bits: u8) {
        assert!(last_doc > self.prev_doc, "write_doc(...) called with non-increasing doc ids. \
                Did you forget to call clear maybe?");
        let delta_doc = last_doc - self.prev_doc;
        self.prev_doc = last_doc;
        delta_doc.serialize(&mut self.buffer).unwrap();
        self.buffer.push(doc_num_bits);
    }

    pub fn write_term_freq(&mut self, tf_num_bits: u8) {
        self.buffer.push(tf_num_bits);
    }


    pub fn write_total_term_freq(&mut self, tf_sum: u32) {
        tf_sum.serialize(&mut self.buffer).expect("Should never fail");
    }

    pub fn data(&self) -> &[u8] {
        &self.buffer[..]
    }

    pub fn clear(&mut self) {
        self.prev_doc = 0u32;
        self.buffer.clear();
    }
}

pub(crate) struct SkipReader {
    doc: DocId,
    owned_read: OwnedRead,
    doc_num_bits: u8,
    tf_num_bits: u8,
    tf_sum: u32,
    skip_info: IndexRecordOption,
}

impl SkipReader {
    pub fn new(data: OwnedRead, skip_info: IndexRecordOption) -> SkipReader {
        SkipReader {
            doc: 0u32,
            owned_read: data,
            skip_info,
            doc_num_bits: 0u8,
            tf_num_bits: 0u8,
            tf_sum: 0u32,
        }
    }

    pub fn reset(&mut self, data: OwnedRead) {
        self.doc = 0u32;
        self.owned_read = data;
        self.doc_num_bits = 0u8;
        self.tf_num_bits = 0u8;
        self.tf_sum = 0u32;
    }

    pub fn total_block_len(&self) -> usize {
        (self.doc_num_bits + self.tf_num_bits) as usize * COMPRESSION_BLOCK_SIZE / 8
    }

    pub fn doc(&self) -> DocId {
        self.doc
    }

    pub fn doc_num_bits(&self) -> u8 {
        self.doc_num_bits
    }

    /// Number of bits used to encode term frequencies
    ///
    /// 0 if term frequencies are not enabled.
    pub fn tf_num_bits(&self) -> u8 {
        self.tf_num_bits
    }

    pub fn tf_sum(&self) -> u32 {
        self.tf_sum
    }

    pub fn advance(&mut self) -> bool {
        if self.owned_read.as_ref().is_empty() {
            false
        } else {
            let doc_delta = u32::deserialize(&mut self.owned_read).expect("Skip data corrupted");
            self.doc += doc_delta as DocId;
            self.doc_num_bits =  self.owned_read.get(0);
            match self.skip_info {
                IndexRecordOption::Basic => {
                    self.owned_read.advance(1);
                }
                IndexRecordOption::WithFreqs=> {
                    self.tf_num_bits = self.owned_read.get(1);
                    self.owned_read.advance(2);
                }
                IndexRecordOption::WithFreqsAndPositions => {
                    self.tf_num_bits = self.owned_read.get(1);
                    self.owned_read.advance(2);
                    self.tf_sum = u32::deserialize(&mut self.owned_read)
                        .expect("Failed reading tf_sum");
                }
            }
            true
        }

    }
}

#[cfg(test)]
mod tests {

    use super::{SkipReader, SkipSerializer};
    use super::IndexRecordOption;
    use owned_read::OwnedRead;

    #[test]
    fn test_skip_with_freq() {
        let buf = {
            let mut skip_serializer = SkipSerializer::new();
            skip_serializer.write_doc(1u32, 2u8);
            skip_serializer.write_term_freq(3u8);
            skip_serializer.write_doc(5u32, 5u8);
            skip_serializer.write_term_freq(2u8);
            skip_serializer.data().to_owned()
        };
        let mut skip_reader = SkipReader::new(OwnedRead::new(buf), IndexRecordOption::WithFreqs);
        assert!(skip_reader.advance());
        assert_eq!(skip_reader.doc(), 1u32);
        assert_eq!(skip_reader.doc_num_bits(), 2u8);
        assert_eq!(skip_reader.tf_num_bits(), 3u8);
        assert!(skip_reader.advance());
        assert_eq!(skip_reader.doc(), 5u32);
        assert_eq!(skip_reader.doc_num_bits(), 5u8);
        assert_eq!(skip_reader.tf_num_bits(), 2u8);
        assert!(!skip_reader.advance());
    }

    #[test]
    fn test_skip_no_freq() {
        let buf = {
            let mut skip_serializer = SkipSerializer::new();
            skip_serializer.write_doc(1u32, 2u8);
            skip_serializer.write_doc(5u32, 5u8);
            skip_serializer.data().to_owned()
        };
        let mut skip_reader = SkipReader::new(OwnedRead::new(buf), IndexRecordOption::Basic);
        assert!(skip_reader.advance());
        assert_eq!(skip_reader.doc(), 1u32);
        assert_eq!(skip_reader.doc_num_bits(), 2u8);
        assert!(skip_reader.advance());
        assert_eq!(skip_reader.doc(), 5u32);
        assert_eq!(skip_reader.doc_num_bits(), 5u8);
        assert!(!skip_reader.advance());
    }
}
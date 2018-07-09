use DocId;
use common::{BinarySerializable, VInt};
use owned_read::OwnedRead;

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
        VInt(delta_doc as u64).serialize_into_vec(&mut self.buffer);
        self.buffer.push(doc_num_bits);
    }

    pub fn write_term_freq(&mut self, tf_num_bits: u8) {
        self.buffer.push(tf_num_bits);
    }

    pub fn data(&self) -> &[u8] {
        &self.buffer[..]
    }

    pub fn clear(&mut self) {
        self.prev_doc = 0u32;
        self.buffer.clear();
    }
}

pub struct SkipReader {
    doc: DocId,
    owned_read: OwnedRead,
    termfreq_enabled: bool,
    doc_num_bits: u8,
    tf_num_bits: u8,
}

impl SkipReader {
    pub fn new(data: OwnedRead, termfreq_enabled: bool) -> SkipReader {
        SkipReader {
            doc: 0u32,
            owned_read: data,
            termfreq_enabled,
            doc_num_bits: 0u8,
            tf_num_bits: 0u8,
        }
    }

    pub fn doc(&self) -> DocId {
        self.doc
    }

    pub fn doc_num_bits(&self) -> u8 {
        self.doc_num_bits
    }

    pub fn tf_num_bits(&self) -> u8 {
        self.tf_num_bits
    }

    pub fn advance(&mut self) -> bool {
        if self.owned_read.as_ref().is_empty() {
            false
        } else {
            let doc_delta = VInt::deserialize(&mut self.owned_read).expect("Skip data corrupted");
            self.doc += doc_delta.0 as DocId;
            self.doc_num_bits =  self.owned_read.get(0);
            if self.termfreq_enabled {
                self.tf_num_bits = self.owned_read.get(1);
                self.owned_read.advance(2);
            } else {
                self.owned_read.advance(1);
            }
            true
        }

    }
}

#[cfg(test)]
mod tests {

    use super::{SkipReader, SkipSerializer};
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
        let mut skip_reader = SkipReader::new(OwnedRead::new(buf), true);
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
        let mut skip_reader = SkipReader::new(OwnedRead::new(buf), false);
        assert!(skip_reader.advance());
        assert_eq!(skip_reader.doc(), 1u32);
        assert_eq!(skip_reader.doc_num_bits(), 2u8);
        assert!(skip_reader.advance());
        assert_eq!(skip_reader.doc(), 5u32);
        assert_eq!(skip_reader.doc_num_bits(), 5u8);
        assert!(!skip_reader.advance());
    }
}
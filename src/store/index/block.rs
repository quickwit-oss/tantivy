use crate::common::VInt;
use crate::store::index::{Checkpoint, CHECKPOINT_PERIOD};
use crate::DocId;
use std::io;

/// Represents a block of checkpoints.
///
/// The DocStore index checkpoints are organized into block
/// for code-readability and compression purpose.
///
/// A block can be of any size.
pub struct CheckpointBlock {
    pub checkpoints: Vec<Checkpoint>,
}

impl Default for CheckpointBlock {
    fn default() -> CheckpointBlock {
        CheckpointBlock {
            checkpoints: Vec::with_capacity(2 * CHECKPOINT_PERIOD),
        }
    }
}

impl CheckpointBlock {
    /// If non-empty returns [start_doc, end_doc)
    /// for the overall block.
    pub fn doc_interval(&self) -> Option<(DocId, DocId)> {
        let start_doc_opt = self
            .checkpoints
            .first()
            .cloned()
            .map(|checkpoint| checkpoint.start_doc);
        let end_doc_opt = self
            .checkpoints
            .last()
            .cloned()
            .map(|checkpoint| checkpoint.end_doc);
        match (start_doc_opt, end_doc_opt) {
            (Some(start_doc), Some(end_doc)) => Some((start_doc, end_doc)),
            _ => None,
        }
    }

    /// Adding another checkpoint in the block.
    pub fn push(&mut self, checkpoint: Checkpoint) {
        if let Some(prev_checkpoint) = self.checkpoints.last() {
            assert!(checkpoint.follows(prev_checkpoint));
        }
        self.checkpoints.push(checkpoint);
    }

    /// Returns the number of checkpoints in the block.
    pub fn len(&self) -> usize {
        self.checkpoints.len()
    }

    pub fn get(&self, idx: usize) -> Checkpoint {
        self.checkpoints[idx]
    }

    pub fn clear(&mut self) {
        self.checkpoints.clear();
    }

    pub fn serialize(&mut self, buffer: &mut Vec<u8>) {
        VInt(self.checkpoints.len() as u64).serialize_into_vec(buffer);
        if self.checkpoints.is_empty() {
            return;
        }
        VInt(self.checkpoints[0].start_doc as u64).serialize_into_vec(buffer);
        VInt(self.checkpoints[0].start_offset as u64).serialize_into_vec(buffer);
        for checkpoint in &self.checkpoints {
            let delta_doc = checkpoint.end_doc - checkpoint.start_doc;
            VInt(delta_doc as u64).serialize_into_vec(buffer);
            VInt(checkpoint.end_offset - checkpoint.start_offset).serialize_into_vec(buffer);
        }
    }

    pub fn deserialize(&mut self, data: &mut &[u8]) -> io::Result<()> {
        if data.is_empty() {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, ""));
        }
        self.checkpoints.clear();
        let len = VInt::deserialize_u64(data)? as usize;
        if len == 0 {
            return Ok(());
        }
        let mut doc = VInt::deserialize_u64(data)? as DocId;
        let mut start_offset = VInt::deserialize_u64(data)?;
        for _ in 0..len {
            let num_docs = VInt::deserialize_u64(data)? as DocId;
            let block_num_bytes = VInt::deserialize_u64(data)?;
            self.checkpoints.push(Checkpoint {
                start_doc: doc,
                end_doc: doc + num_docs,
                start_offset,
                end_offset: start_offset + block_num_bytes,
            });
            doc += num_docs;
            start_offset += block_num_bytes;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::store::index::block::CheckpointBlock;
    use crate::store::index::Checkpoint;
    use crate::DocId;
    use std::io;

    fn test_aux_ser_deser(checkpoints: &[Checkpoint]) -> io::Result<()> {
        let mut block = CheckpointBlock::default();
        for &checkpoint in checkpoints {
            block.push(checkpoint);
        }
        let mut buffer = Vec::new();
        block.serialize(&mut buffer);
        let mut block_deser = CheckpointBlock::default();
        let checkpoint = Checkpoint {
            start_doc: 0,
            end_doc: 1,
            start_offset: 2,
            end_offset: 3,
        };
        block_deser.push(checkpoint); // < check that value is erased before deser
        let mut data = &buffer[..];
        block_deser.deserialize(&mut data)?;
        assert!(data.is_empty());
        assert_eq!(checkpoints, &block_deser.checkpoints[..]);
        Ok(())
    }

    #[test]
    fn test_block_serialize_empty() -> io::Result<()> {
        test_aux_ser_deser(&[])
    }

    #[test]
    fn test_block_serialize_simple() -> io::Result<()> {
        let checkpoints = vec![Checkpoint {
            start_doc: 10,
            end_doc: 12,
            start_offset: 100,
            end_offset: 120,
        }];
        test_aux_ser_deser(&checkpoints)
    }

    #[test]
    fn test_block_serialize() -> io::Result<()> {
        let offsets: Vec<u64> = (0..11).map(|i| i * i * i).collect();
        let mut checkpoints = vec![];
        let mut start_doc = 0;
        for i in 0..10 {
            let end_doc = (i * i) as DocId;
            checkpoints.push(Checkpoint {
                start_doc,
                end_doc,
                start_offset: offsets[i],
                end_offset: offsets[i + 1],
            });
            start_doc = end_doc;
        }
        test_aux_ser_deser(&checkpoints)
    }
}

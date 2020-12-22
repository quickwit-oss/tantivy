use crate::common::{BinarySerializable, VInt};
use crate::directory::OwnedBytes;
use crate::store::index::block::CheckpointBlock;
use crate::store::index::Checkpoint;
use crate::DocId;

pub struct LayerCursor<'a> {
    remaining: &'a [u8],
    block: CheckpointBlock,
    cursor: usize,
}

impl<'a> Iterator for LayerCursor<'a> {
    type Item = Checkpoint;

    fn next(&mut self) -> Option<Checkpoint> {
        if self.cursor == self.block.len() {
            if self.remaining.is_empty() {
                return None;
            }
            let (block_mut, remaining_mut) = (&mut self.block, &mut self.remaining);
            if block_mut.deserialize(remaining_mut).is_err() {
                return None;
            }
            self.cursor = 0;
        }
        let res = Some(self.block.get(self.cursor));
        self.cursor += 1;
        res
    }
}

struct Layer {
    data: OwnedBytes,
}

impl Layer {
    fn cursor(&self) -> impl Iterator<Item = Checkpoint> + '_ {
        self.cursor_at_offset(0u64)
    }

    fn cursor_at_offset(&self, start_offset: u64) -> impl Iterator<Item = Checkpoint> + '_ {
        let data = &self.data.as_slice();
        LayerCursor {
            remaining: &data[start_offset as usize..],
            block: CheckpointBlock::default(),
            cursor: 0,
        }
    }

    fn seek_start_at_offset(&self, target: DocId, offset: u64) -> Option<Checkpoint> {
        self.cursor_at_offset(offset)
            .find(|checkpoint| checkpoint.end_doc > target)
    }
}

pub struct SkipIndex {
    layers: Vec<Layer>,
}

impl SkipIndex {
    pub(crate) fn checkpoints(&self) -> impl Iterator<Item = Checkpoint> + '_ {
        self.layers
            .last()
            .into_iter()
            .flat_map(|layer| layer.cursor())
    }

    pub fn seek(&self, target: DocId) -> Option<Checkpoint> {
        let first_layer_len = self
            .layers
            .first()
            .map(|layer| layer.data.len() as u64)
            .unwrap_or(0u64);
        let mut cur_checkpoint = Checkpoint {
            start_doc: 0u32,
            end_doc: 1u32,
            start_offset: 0u64,
            end_offset: first_layer_len,
        };
        for layer in &self.layers {
            if let Some(checkpoint) =
                layer.seek_start_at_offset(target, cur_checkpoint.start_offset)
            {
                cur_checkpoint = checkpoint;
            } else {
                return None;
            }
        }
        Some(cur_checkpoint)
    }
}

impl From<OwnedBytes> for SkipIndex {
    fn from(mut data: OwnedBytes) -> SkipIndex {
        let offsets: Vec<u64> = Vec::<VInt>::deserialize(&mut data)
            .unwrap()
            .into_iter()
            .map(|el| el.0)
            .collect();
        let mut start_offset = 0;
        let mut layers = Vec::new();
        for end_offset in offsets {
            layers.push(Layer {
                data: data.slice(start_offset as usize, end_offset as usize),
            });
            start_offset = end_offset;
        }
        SkipIndex { layers }
    }
}

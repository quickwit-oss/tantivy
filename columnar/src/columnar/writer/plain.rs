use std::mem::size_of;

use stacker::{ArenaHashMap, MemoryArena};

use super::column_operation::ColumnOperation;
use super::column_writers::ColumnWriter;
use crate::RowId;

#[derive(Default)]
pub(super) struct PlainColumnsWriter {
    pub(super) bytes_columns: ArenaHashMap,
    pub(super) str_columns: ArenaHashMap,
    pub(super) value_stores: Vec<PlainValueStore>,
}

impl PlainColumnsWriter {
    pub(super) fn mem_usage(&self) -> usize {
        self.bytes_columns.mem_usage()
            + self.str_columns.mem_usage()
            + self
                .value_stores
                .iter()
                .map(PlainValueStore::mem_usage)
                .sum::<usize>()
    }
}

#[derive(Copy, Clone)]
pub(super) struct PlainColumnWriter {
    pub(super) column_writer: ColumnWriter,
    pub(super) value_store_id: u32,
}

impl PlainColumnWriter {
    pub(super) fn new(value_store_id: u32) -> Self {
        Self {
            column_writer: ColumnWriter::default(),
            value_store_id,
        }
    }

    pub(super) fn record_bytes(
        &mut self,
        doc: RowId,
        bytes: &[u8],
        value_stores: &mut [PlainValueStore],
        arena: &mut MemoryArena,
    ) {
        let value_id = value_stores[self.value_store_id as usize].push(bytes);
        self.column_writer.record(doc, value_id, arena);
    }

    pub(super) fn operation_iterator<'a>(
        self,
        arena: &MemoryArena,
        old_to_new_ids: Option<&[RowId]>,
        byte_buffer: &'a mut Vec<u8>,
    ) -> impl Iterator<Item = ColumnOperation<u32>> + 'a + use<'a> {
        self.column_writer
            .operation_iterator(arena, old_to_new_ids, byte_buffer)
    }
}

#[derive(Default)]
pub(super) struct PlainValueStore {
    concatenated_payloads: Vec<u8>,
    end_offsets: Vec<usize>,
}

impl PlainValueStore {
    fn push(&mut self, value: &[u8]) -> u32 {
        let value_id = self.end_offsets.len() as u32;
        self.concatenated_payloads.extend_from_slice(value);
        self.end_offsets.push(self.concatenated_payloads.len());
        value_id
    }

    pub(super) fn get(&self, value_id: u32) -> &[u8] {
        let value_id = value_id as usize;
        let end = self.end_offsets[value_id];
        let start = if value_id == 0 {
            0
        } else {
            self.end_offsets[value_id - 1]
        };
        &self.concatenated_payloads[start..end]
    }

    fn mem_usage(&self) -> usize {
        self.concatenated_payloads.capacity() + self.end_offsets.capacity() * size_of::<usize>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_plain_value_store() {
        let mut store = PlainValueStore::default();
        let first = store.push(b"same");
        let empty = store.push(b"");
        let duplicate = store.push(b"same");

        assert_eq!((first, empty, duplicate), (0, 1, 2));
        assert_eq!(store.get(first), b"same");
        assert_eq!(store.get(empty), b"");
        assert_eq!(store.get(duplicate), b"same");
        assert!(store.mem_usage() >= 8);
    }
}

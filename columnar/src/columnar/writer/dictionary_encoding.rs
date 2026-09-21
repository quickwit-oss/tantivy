use stacker::{ArenaHashMap, MemoryArena};

use super::column_operation::ColumnOperation;
use super::column_writers::ColumnWriter;
use crate::RowId;
use crate::dictionary::DictionaryBuilder;

#[derive(Default)]
pub(super) struct DictionaryEncodedColumnsWriter {
    pub(super) bytes_columns: ArenaHashMap,
    pub(super) str_columns: ArenaHashMap,
    pub(super) dictionaries: Vec<DictionaryBuilder>,
}

impl DictionaryEncodedColumnsWriter {
    pub(super) fn mem_usage(&self) -> usize {
        self.bytes_columns.mem_usage()
            + self.str_columns.mem_usage()
            + self
                .dictionaries
                .iter()
                .map(DictionaryBuilder::mem_usage)
                .sum::<usize>()
    }
}

#[derive(Copy, Clone)]
pub(super) struct DictionaryEncodedColumnWriter {
    pub(super) dictionary_id: u32,
    pub(super) column_writer: ColumnWriter,
    // This is used in facets aggregation
    pub(super) sort_values_within_row: bool,
}

impl DictionaryEncodedColumnWriter {
    pub(super) fn new(dictionary_id: u32) -> Self {
        Self {
            dictionary_id,
            column_writer: ColumnWriter::default(),
            sort_values_within_row: false,
        }
    }

    pub(super) fn record_bytes(
        &mut self,
        doc: RowId,
        bytes: &[u8],
        dictionaries: &mut [DictionaryBuilder],
        arena: &mut MemoryArena,
    ) {
        let unordered_id =
            dictionaries[self.dictionary_id as usize].get_or_allocate_id(bytes, arena);
        self.column_writer.record(doc, unordered_id.0, arena);
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

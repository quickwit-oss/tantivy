use std::io;
use std::io::Write;
use std::ops::Range;
use std::sync::Arc;

use common::OwnedBytes;

use crate::column_values::u64_based::CodecType;
use crate::column_values::ColumnValues;
use crate::iterable::Iterable;
use crate::RowId;

pub fn serialize_multivalued_index(
    multivalued_index: &dyn Iterable<RowId>,
    output: &mut impl Write,
) -> io::Result<()> {
    crate::column_values::u64_based::serialize_u64_based_column_values(
        multivalued_index,
        &[CodecType::Bitpacked, CodecType::Linear],
        output,
    )?;
    Ok(())
}

pub fn open_multivalued_index(bytes: OwnedBytes) -> io::Result<MultiValueIndex> {
    let start_index_column: Arc<dyn ColumnValues<RowId>> =
        crate::column_values::u64_based::load_u64_based_column_values(bytes)?;
    Ok(MultiValueIndex { start_index_column })
}

#[derive(Clone)]
/// Index to resolve value range for given doc_id.
/// Starts at 0.
pub struct MultiValueIndex {
    pub start_index_column: Arc<dyn crate::ColumnValues<RowId>>,
}

impl MultiValueIndex {
    pub fn for_test(start_offsets: &[RowId]) -> MultiValueIndex {
        let mut buffer = Vec::new();
        serialize_multivalued_index(&start_offsets, &mut buffer).unwrap();
        let bytes = OwnedBytes::new(buffer);
        open_multivalued_index(bytes).unwrap()
    }

    /// Returns `[start, end)`, such that the values associated with
    /// the given document are `start..end`.
    #[inline]
    pub(crate) fn range(&self, row_id: RowId) -> Range<RowId> {
        let start = self.start_index_column.get_val(row_id);
        let end = self.start_index_column.get_val(row_id + 1);
        start..end
    }

    /// Returns the number of documents in the index.
    #[inline]
    pub fn num_rows(&self) -> u32 {
        self.start_index_column.num_vals() - 1
    }

    pub fn select_cursor(&self) -> MultiValueIndexCursor {
        MultiValueIndexCursor {
            multivalued_index: self.clone(),
            row_cursor: 0u32,
        }
    }
}

pub struct MultiValueIndexCursor {
    multivalued_index: MultiValueIndex,
    row_cursor: RowId,
}

impl MultiValueIndexCursor {
    /// See contract in `ColumnIndexSelectCursor`.
    ///
    /// Multi valued cardinality is special for two different
    /// ranks `rank_left` and `rank_right`, we can end up with
    /// the same `select(rank_left)` and `select(rank_right)`.
    ///
    /// For this reason, this function includes extra complexity
    /// to prevent the cursor from emitting the same row_id.
    /// - From a last call, by skipping ranks mapping to
    /// the same row_id
    /// - With the batch, by simply deduplicating the output.
    pub fn select_batch_in_place(&mut self, ranks: &mut Vec<RowId>) {
        if ranks.is_empty() {
            return;
        }
        let mut row_cursor = self.row_cursor;

        let mut write_cursor_id = usize::MAX;
        let mut last_written_row_id = u32::MAX;

        // We skip all of the ranks that we already passed.
        //
        // It is possible in the case of multivalued, for a the first
        // few rank to belong to the same row_id as the last rank
        // of the previous call.
        let start_bound = self
            .multivalued_index
            .start_index_column
            .get_val(row_cursor);

        let mut skip = 0;
        while ranks[skip] < start_bound {
            skip += 1;
            if skip == ranks.len() {
                ranks.clear();
                return;
            }
        }

        for i in skip..ranks.len() {
            let rank = ranks[i];
            let row_id = loop {
                // TODO See if we can find a way to introduce a function in
                // ColumnValue to remove dynamic dispatch.
                // This is tricky however... because it only applies to T=u32.
                //
                // TODO consider using exponential search.
                let end = self
                    .multivalued_index
                    .start_index_column
                    .get_val(row_cursor + 1) as u32;
                if end > rank {
                    break row_cursor;
                }
                row_cursor += 1;
            };
            // We remove duplicates in a branchless fashion: we only advance
            // the write cursor when we are writing a value different from
            // the last written value.
            write_cursor_id =
                write_cursor_id.wrapping_add(if row_id == last_written_row_id { 0 } else { 1 });
            ranks[write_cursor_id] = row_id;
            last_written_row_id = row_id;
        }

        self.row_cursor = row_cursor + 1;
        ranks.truncate(write_cursor_id + 1);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::MultiValueIndex;
    use crate::column_values::IterColumn;
    use crate::{ColumnValues, RowId};
    use proptest::prelude::*;

    fn index_to_pos_helper(index: &MultiValueIndex, positions: &[u32]) -> Vec<u32> {
        let mut positions = positions.to_vec();
        let mut cursor = index.select_cursor();
        cursor.select_batch_in_place(&mut positions);
        positions
    }

    // Value row id ranges are [0..10, 10..12, 12..15, etc.]
    const START_OFFSETS: &[RowId] = &[0, 10, 12, 15, 22, 23];

    #[track_caller]
    fn test_multivalue_select_cursor_aux(
        start_offsets: &'static [RowId],
        ranks: &[RowId],
        expected: &[RowId],
    ) {
        let column: Arc<dyn ColumnValues<RowId>> =
            Arc::new(IterColumn::from(start_offsets.iter().copied()));
        let index = MultiValueIndex {
            start_index_column: column,
        };
        assert_eq!(&index_to_pos_helper(&index, &ranks), expected);
    }

    #[test]
    fn test_multivalue_select_cursor_empty() {
        test_multivalue_select_cursor_aux(START_OFFSETS, &[], &[]);
    }

    #[test]
    fn test_multivalue_select_cursor_single() {
        test_multivalue_select_cursor_aux(START_OFFSETS, &[9], &[0]);
        test_multivalue_select_cursor_aux(START_OFFSETS, &[10], &[1]);
        test_multivalue_select_cursor_aux(START_OFFSETS, &[11], &[1]);
        test_multivalue_select_cursor_aux(START_OFFSETS, &[11], &[1]);
        test_multivalue_select_cursor_aux(START_OFFSETS, &[12], &[2]);
    }

    #[test]
    fn test_multivalue_select_cursor_duplicates() {
        test_multivalue_select_cursor_aux(START_OFFSETS, &[12, 14], &[2]);
    }

    #[test]
    fn test_multivalue_select_cursor_complex() {
        test_multivalue_select_cursor_aux(START_OFFSETS, &[10, 11, 15, 20, 21, 22], &[1, 3, 4])
    }


    #[test]
    fn test_multivalue_select_corner_case_skip_all() {
        let column: Arc<dyn ColumnValues<RowId>> =
            Arc::new(IterColumn::from([0, 10].into_iter()));
        let index = MultiValueIndex {
            start_index_column: column,
        };
        let mut cursor = index.select_cursor();
        {
            let mut ranks = vec![0];
            cursor.select_batch_in_place(&mut ranks);
            assert_eq!(ranks, &[0]);
        }
        {
            let mut ranks = vec![5];
            cursor.select_batch_in_place(&mut ranks);
            assert_eq!(ranks, &[]);
        }
    }

    #[test]
    fn test_multi_value_index_cursor_bug() {
        let column: Arc<dyn ColumnValues<RowId>> =
            Arc::new(IterColumn::from([0, 10].into_iter()));
        let index = MultiValueIndex {
            start_index_column: column,
        };
        let mut cursor = index.select_cursor();
        {
            let mut ranks = vec![0];
            cursor.select_batch_in_place(&mut ranks);
            assert_eq!(ranks, &[0]);
        }
        {
            let mut ranks = vec![4];
            cursor.select_batch_in_place(&mut ranks);
            assert_eq!(ranks, &[]);
        }
        {
            let mut ranks = vec![9];
            cursor.select_batch_in_place(&mut ranks);
            assert_eq!(ranks, &[]);
        }
    }

    #[test]
    fn test_multivalue_select_cursor_skip_already_emitted() {
        let column: Arc<dyn ColumnValues<RowId>> =
            Arc::new(IterColumn::from(START_OFFSETS.iter().copied()));
        let index = MultiValueIndex {
            start_index_column: column,
        };
        let mut cursor = index.select_cursor();
        {
            let mut ranks = vec![1, 10];
            cursor.select_batch_in_place(&mut ranks);
            assert_eq!(ranks, &[0, 1]);
        }
        {
            // Here we skip row_id = 1.
            let mut ranks = vec![11, 12];
            cursor.select_batch_in_place(&mut ranks);
            assert_eq!(ranks, &[2]);
        }
    }

    fn start_index_strategy() -> impl Strategy<Value = Vec<RowId>> {
        proptest::collection::vec(0u32..3u32, 1..6)
            .prop_map(|deltas: Vec<u32>| {
                let mut start_offsets: Vec<RowId> = Vec::with_capacity(deltas.len() + 1);
                let mut cumul = 0u32;
                start_offsets.push(cumul);
                for delta in deltas {
                    cumul += delta;
                    if cumul >= 10 {
                        break;
                    }
                    start_offsets.push(cumul);
                }
                start_offsets.push(10);
                start_offsets
            })
   }

    fn query_strategy() -> impl Strategy<Value = Vec<Vec<RowId>> > {
        proptest::collection::btree_set(0u32..10u32, 1..=10)
            .prop_flat_map(|els| {
                let els: Vec<RowId> = els.into_iter().collect();
                proptest::collection::btree_set(0..els.len(), 0..els.len())
                    .prop_map(move |mut split_positions| {
                        split_positions.insert(els.len());
                        let mut queries: Vec<Vec<RowId>> = Vec::with_capacity(split_positions.len() + 1);
                        let mut cursor = 0;
                        for split_position in split_positions {
                            queries.push(els[cursor..split_position].to_vec());
                            cursor = split_position;
                        }
                        queries
                    })
            })
    }


    /// Simple inefficient implementation used for reference.
    struct SimpleSelectCursor {
        start_indexes: Vec<RowId>,
        last_emitted_row_id: Option<RowId>,
    }

    impl SimpleSelectCursor {
        fn select(&self, rank: u32) -> RowId {
            for i in 0..self.start_indexes.len() - 1 {
                if self.start_indexes[i] <= rank && self.start_indexes[i + 1] > rank{
                    return i as u32;
                }
            }
            panic!();
        }

        fn select_batch_in_place(&mut self, ranks: &mut Vec<RowId>) {
            if ranks.is_empty() {
                return;
            }
            for rank in ranks.iter_mut() {
                *rank = self.select(*rank);
            }
            ranks.dedup();
            if ranks.first().copied() == self.last_emitted_row_id {
                ranks.remove(0);
            }
            if let Some(last_emitted) = ranks.last().copied() {
                self.last_emitted_row_id = Some(last_emitted);
            }
        }
    }


    proptest! {
        #[test]
        fn test_multi_value_index_cursor_proptest(start_indexes in start_index_strategy(), mut queries in query_strategy()) {
            let mut simple_select_cursor = SimpleSelectCursor {
                start_indexes: start_indexes.clone(),
                last_emitted_row_id: None
            };
            let column: Arc<dyn ColumnValues<RowId>> =
                Arc::new(IterColumn::from(start_indexes.into_iter()));
            let index = MultiValueIndex { start_index_column: column };
            let mut select_cursor = index.select_cursor();
            for query in queries.iter_mut() {
                let mut query_clone = query.clone();
                select_cursor.select_batch_in_place(query);
                simple_select_cursor.select_batch_in_place(&mut query_clone);
                assert_eq!(&query[..], &query_clone[..]);
            }
        }
    }
}

use std::ops::Range;

use crate::{column, ColumnarReader, RowId};

pub struct StackMergeOrder {
    // This does not start at 0. The first row is the number of
    // rows in the first columnar.
    cumulated_row_ids: Vec<RowId>,
}

impl StackMergeOrder {
    pub fn stack(columnars: &[&ColumnarReader]) -> StackMergeOrder {
        let mut cumulated_row_ids: Vec<RowId> = Vec::with_capacity(columnars.len());
        let mut cumulated_row_id = 0;
        for columnar in columnars {
            cumulated_row_id += columnar.num_rows();
            cumulated_row_ids.push(cumulated_row_id);
        }
        StackMergeOrder { cumulated_row_ids }
    }

    pub fn num_rows(&self) -> RowId {
        self.cumulated_row_ids.last().copied().unwrap_or(0)
    }

    pub fn offset(&self, columnar_id: usize) -> RowId {
        if columnar_id == 0 {
            return 0;
        }
        self.cumulated_row_ids[columnar_id - 1]
    }

    pub fn columnar_range(&self, columnar_id: usize) -> Range<RowId> {
        self.offset(columnar_id)..self.offset(columnar_id + 1)
    }
}

pub enum MergeRowOrder {
    /// Columnar tables are simply stacked one above the other.
    /// If the i-th columnar_readers has n_rows_i rows, then
    /// in the resulting columnar,
    /// rows [r0..n_row_0) contains the row of columnar_readers[0], in ordder
    /// rows [n_row_0..n_row_0 + n_row_1 contains the row of columnar_readers[1], in order.
    /// ..
    Stack(StackMergeOrder),
    /// Some more complex mapping, that can interleaves rows from the different readers and
    /// possibly drop rows.
    Complex(()),
}

impl MergeRowOrder {
    pub fn num_rows(&self) -> RowId {
        match self {
            MergeRowOrder::Stack(stack_row_order) => stack_row_order.num_rows(),
            MergeRowOrder::Complex(_) => {
                todo!()
            }
        }
    }
}

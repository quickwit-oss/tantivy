use std::io;

use crate::columnar::ColumnarReader;

pub enum MergeDocOrder {
    /// Columnar tables are simply stacked one above the other.
    /// If the i-th columnar_readers has n_rows_i rows, then
    /// in the resulting columnar,
    /// rows [r0..n_row_0) contains the row of columnar_readers[0], in ordder
    /// rows [n_row_0..n_row_0 + n_row_1 contains the row of columnar_readers[1], in order.
    /// ..
    Stack,
    /// Some more complex mapping, that can interleaves rows from the different readers and
    /// possibly drop rows.
    Complex(()),
}

pub fn merge_columnar(
    _columnar_readers: &[ColumnarReader],
    mapping: MergeDocOrder,
    _output: &mut impl io::Write,
) -> io::Result<()> {
    match mapping {
        MergeDocOrder::Stack => {
            // implement me :)
            todo!();
        }
        MergeDocOrder::Complex(_) => {
            // for later
            todo!();
        }
    }
}

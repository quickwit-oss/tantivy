// Copyright (C) 2022 Quickwit, Inc.
//
// Quickwit is offered under the AGPL v3.0 and as commercial software.
// For commercial licensing, contact us at hello@quickwit.io.
//
// AGPL:
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

mod column_type_header;
mod dictionary;
mod reader;
mod serializer;
mod value;
mod writer;

pub use column_type_header::Cardinality;
pub use reader::ColumnarReader;
pub use serializer::ColumnarSerializer;
pub use writer::ColumnarWriter;

pub type DocId = u32;

#[cfg(test)]
mod tests {
    use std::ops::Range;

    use common::file_slice::FileSlice;

    use crate::column_type_header::ColumnTypeAndCardinality;
    use crate::reader::ColumnarReader;
    use crate::serializer::ColumnarSerializer;
    use crate::value::NumericalValue;
    use crate::ColumnarWriter;

    #[test]
    fn test_dataframe_writer() {
        let mut dataframe_writer = ColumnarWriter::default();
        dataframe_writer.record_numerical(1u32, b"srical.value", NumericalValue::U64(1u64));
        dataframe_writer.record_numerical(2u32, b"srical.value", NumericalValue::U64(2u64));
        dataframe_writer.record_numerical(4u32, b"srical.value", NumericalValue::I64(2i64));
        let mut buffer: Vec<u8> = Vec::new();
        let serializer = ColumnarSerializer::new(&mut buffer);
        dataframe_writer.serialize(5, serializer).unwrap();
        let columnar_fileslice = FileSlice::from(buffer);
        let columnar = ColumnarReader::open(columnar_fileslice).unwrap();
        assert_eq!(columnar.num_columns(), 1);
        let cols: Vec<(ColumnTypeAndCardinality, Range<u64>)> =
            columnar.read_columns("srical.value").unwrap();
        assert_eq!(cols.len(), 1);
        // Right now this 31 bytes are spent as follows
        //
        // - header 14 bytes
        // - vals  8 //< due to padding? could have been 1byte?.
        // - null footer 6 bytes
        // - version footer 3 bytes // Should be file-wide
        assert_eq!(cols[0].1, 0..31);
    }
}

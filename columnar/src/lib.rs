mod column_type_header;
mod dictionary;
mod reader;
pub(crate) mod utils;
mod value;
mod writer;

pub use column_type_header::Cardinality;
pub use reader::ColumnarReader;
pub use value::{NumericalType, NumericalValue};
pub use writer::ColumnarWriter;
pub use reader::ColumnHandle;

pub type DocId = u32;

#[derive(Copy, Clone, Debug)]
pub struct InvalidData;

#[cfg(test)]
mod tests {
    use common::file_slice::FileSlice;
    use crate::column_type_header::ColumnType;
    use crate::reader::{ColumnarReader, ColumnHandle};
    use crate::value::NumericalValue;
    use crate::{Cardinality, ColumnarWriter};

    #[test]
    fn test_dataframe_writer_bytes() {
        let mut dataframe_writer = ColumnarWriter::default();
        dataframe_writer.record_str(1u32, "my_string", "hello");
        dataframe_writer.record_str(3u32, "my_string", "helloeee");
        let mut buffer: Vec<u8> = Vec::new();
        dataframe_writer.serialize(5, &mut buffer).unwrap();
        let columnar_fileslice = FileSlice::from(buffer);
        let columnar = ColumnarReader::open(columnar_fileslice).unwrap();
        assert_eq!(columnar.num_columns(), 1);
        let cols: Vec<ColumnHandle> =
            columnar.read_columns("my_string").unwrap();
        assert_eq!(cols.len(), 1);
        assert_eq!(cols[0].num_bytes(), 158);
    }

    #[test]
    fn test_dataframe_writer_bool() {
        let mut dataframe_writer = ColumnarWriter::default();
        dataframe_writer.record_bool(1u32, "bool.value", false);
        let mut buffer: Vec<u8> = Vec::new();
        dataframe_writer.serialize(5, &mut buffer).unwrap();
        let columnar_fileslice = FileSlice::from(buffer);
        let columnar = ColumnarReader::open(columnar_fileslice).unwrap();
        assert_eq!(columnar.num_columns(), 1);
        let cols: Vec<ColumnHandle> =
            columnar.read_columns("bool.value").unwrap();
        assert_eq!(cols.len(), 1);
        let col = cols.into_iter().next().unwrap();
        assert_eq!(
            col.column_type(),
            ColumnType::Bool
        );
        assert_eq!(
            col.cardinality(),
            Cardinality::Optional);
        assert_eq!(
            col.column_name(),
            "bool.value"
        );
    }

    #[test]
    fn test_dataframe_writer_numerical() {
        let mut dataframe_writer = ColumnarWriter::default();
        dataframe_writer.record_numerical(1u32, "srical.value", NumericalValue::U64(12u64));
        dataframe_writer.record_numerical(2u32, "srical.value", NumericalValue::U64(13u64));
        dataframe_writer.record_numerical(4u32, "srical.value", NumericalValue::U64(15u64));
        let mut buffer: Vec<u8> = Vec::new();
        dataframe_writer.serialize(5, &mut buffer).unwrap();
        let columnar_fileslice = FileSlice::from(buffer);
        let columnar = ColumnarReader::open(columnar_fileslice).unwrap();
        assert_eq!(columnar.num_columns(), 1);
        let cols: Vec<ColumnHandle> =
            columnar.read_columns("srical.value").unwrap();
        assert_eq!(cols.len(), 1);
        // Right now this 31 bytes are spent as follows
        //
        // - header 14 bytes
        // - vals  8 //< due to padding? could have been 1byte?.
        // - null footer 6 bytes
        // - version footer 3 bytes // Should be file-wide
        assert_eq!(cols[0].num_bytes(), 31);
    }
}

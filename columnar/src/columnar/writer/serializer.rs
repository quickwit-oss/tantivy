use std::io;
use std::io::Write;

use common::json_path_writer::JSON_END_OF_PATH;
use common::{BinarySerializable, CountingWriter};
use sstable::RangeSSTable;
use sstable::value::RangeValueWriter;

use crate::RowId;
use crate::columnar::format_version::{CURRENT_VERSION, PLAIN_CODEC_VERSION, footer_for_version};
use crate::columnar::{ColumnType, StrColumnCodec, column_key_code};

/// Serializes the columns of a columnar.
///
/// Contract: columns must be serialized in the order of their keys (`column_name \0 key_code`,
/// see `column_key_code`). The column sstable stores the byte ranges of the columns as a
/// partition of the column data, in key order.
pub struct ColumnarSerializer<W: io::Write> {
    wrt: CountingWriter<W>,
    sstable_range: sstable::Writer<Vec<u8>, RangeValueWriter>,
    prepare_key_buffer: Vec<u8>,
    /// True if at least one column uses the plain codec.
    has_plain_column: bool,
}

/// Returns a key consisting of the concatenation of the key and the column key code.
fn prepare_key(key: &[u8], column_type: ColumnType, codec: StrColumnCodec, buffer: &mut Vec<u8>) {
    buffer.clear();
    buffer.extend_from_slice(key);
    buffer.push(JSON_END_OF_PATH);
    buffer.push(column_key_code(column_type, codec));
}

impl<W: io::Write> ColumnarSerializer<W> {
    pub(crate) fn new(wrt: W) -> ColumnarSerializer<W> {
        let sstable_range: sstable::Writer<Vec<u8>, RangeValueWriter> =
            sstable::Dictionary::<RangeSSTable>::builder(Vec::with_capacity(100_000)).unwrap();
        ColumnarSerializer {
            wrt: CountingWriter::wrap(wrt),
            sstable_range,
            prepare_key_buffer: Vec::new(),
            has_plain_column: false,
        }
    }

    /// Creates a ColumnSerializer for a dictionary encoded or a non-text column.
    pub fn start_serialize_column<'a>(
        &'a mut self,
        column_name: &[u8],
        column_type: ColumnType,
    ) -> ColumnSerializer<'a, W> {
        self.start_serialize_column_with_codec(column_name, column_type, StrColumnCodec::Dictionary)
    }

    /// Creates a ColumnSerializer.
    ///
    /// Precondition: `codec` is `Dictionary` unless `column_type` is `Str` or `Bytes`.
    pub fn start_serialize_column_with_codec<'a>(
        &'a mut self,
        column_name: &[u8],
        column_type: ColumnType,
        codec: StrColumnCodec,
    ) -> ColumnSerializer<'a, W> {
        let start_offset = self.wrt.written_bytes();
        prepare_key(
            column_name,
            column_type,
            codec,
            &mut self.prepare_key_buffer,
        );
        self.has_plain_column |= codec == StrColumnCodec::Plain;
        ColumnSerializer {
            columnar_serializer: self,
            start_offset,
        }
    }

    pub(crate) fn finalize(mut self, num_rows: RowId) -> io::Result<()> {
        let sstable_bytes: Vec<u8> = self.sstable_range.finish()?;
        let sstable_num_bytes: u64 = sstable_bytes.len() as u64;
        self.wrt.write_all(&sstable_bytes)?;
        self.wrt.write_all(&sstable_num_bytes.to_le_bytes()[..])?;
        num_rows.serialize(&mut self.wrt)?;
        // Columnars without plain columns keep the version they had before the plain codec
        // existed, so that older readers can still open them.
        let version = if self.has_plain_column {
            PLAIN_CODEC_VERSION
        } else {
            CURRENT_VERSION
        };
        self.wrt.write_all(&footer_for_version(version))?;
        self.wrt.flush()?;
        Ok(())
    }
}

pub struct ColumnSerializer<'a, W: io::Write> {
    columnar_serializer: &'a mut ColumnarSerializer<W>,
    start_offset: u64,
}

impl<W: io::Write> ColumnSerializer<'_, W> {
    pub fn finalize(self) -> io::Result<()> {
        let end_offset: u64 = self.columnar_serializer.wrt.written_bytes();
        let byte_range = self.start_offset..end_offset;
        self.columnar_serializer.sstable_range.insert(
            &self.columnar_serializer.prepare_key_buffer[..],
            &byte_range,
        )?;
        self.columnar_serializer.prepare_key_buffer.clear();
        Ok(())
    }
}

impl<W: io::Write> io::Write for ColumnSerializer<'_, W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.columnar_serializer.wrt.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.columnar_serializer.wrt.flush()
    }

    fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        self.columnar_serializer.wrt.write_all(buf)
    }
}

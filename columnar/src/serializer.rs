use std::io;
use std::io::Write;
use std::ops::Range;

use common::CountingWriter;
use sstable::value::RangeWriter;
use sstable::SSTableRange;

pub struct ColumnarSerializer<W: io::Write> {
    wrt: CountingWriter<W>,
    sstable_range: sstable::Writer<Vec<u8>, RangeWriter>,
}

impl<W: io::Write> ColumnarSerializer<W> {
    pub fn new(wrt: W) -> ColumnarSerializer<W> {
        let sstable_range: sstable::Writer<Vec<u8>, RangeWriter> =
            sstable::Dictionary::<SSTableRange>::builder(Vec::with_capacity(100_000)).unwrap();
        ColumnarSerializer {
            wrt: CountingWriter::wrap(wrt),
            sstable_range,
        }
    }

    pub fn record_column_offsets(&mut self, key: &[u8], byte_range: Range<u64>) -> io::Result<()> {
        self.sstable_range.insert(key, &byte_range)
    }

    pub fn wrt(&mut self) -> &mut CountingWriter<W> {
        &mut self.wrt
    }

    pub fn finalize(mut self) -> io::Result<()> {
        let sstable_bytes: Vec<u8> = self.sstable_range.finish()?;
        let sstable_num_bytes: u64 = sstable_bytes.len() as u64;
        self.wrt.write_all(&sstable_bytes)?;
        self.wrt.write_all(&sstable_num_bytes.to_le_bytes()[..])?;
        Ok(())
    }
}

use crate::common::BinarySerializable;
use crate::common::CountingWriter;
use crate::common::VInt;
use crate::directory::FileSlice;
use crate::directory::{TerminatingWrite, WritePtr};
use crate::schema::Field;
use crate::space_usage::FieldUsage;
use crate::space_usage::PerFieldSpaceUsage;
use std::collections::HashMap;
use std::io::{self, Read, Write};

use super::HasLen;

#[derive(Eq, PartialEq, Hash, Copy, Ord, PartialOrd, Clone, Debug)]
pub struct FileAddr {
    field: Field,
    idx: usize,
}

impl FileAddr {
    fn new(field: Field, idx: usize) -> FileAddr {
        FileAddr { field, idx }
    }
}

impl BinarySerializable for FileAddr {
    fn serialize<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        self.field.serialize(writer)?;
        VInt(self.idx as u64).serialize(writer)?;
        Ok(())
    }

    fn deserialize<R: Read>(reader: &mut R) -> io::Result<Self> {
        let field = Field::deserialize(reader)?;
        let idx = VInt::deserialize(reader)?.0 as usize;
        Ok(FileAddr { field, idx })
    }
}

/// A `CompositeWrite` is used to write a `CompositeFile`.
pub struct CompositeWrite<W = WritePtr> {
    write: CountingWriter<W>,
    offsets: HashMap<FileAddr, u64>,
}

impl<W: TerminatingWrite + Write> CompositeWrite<W> {
    /// Crate a new API writer that writes a composite file
    /// in a given write.
    pub fn wrap(w: W) -> CompositeWrite<W> {
        CompositeWrite {
            write: CountingWriter::wrap(w),
            offsets: HashMap::new(),
        }
    }

    /// Start writing a new field.
    pub fn for_field(&mut self, field: Field) -> &mut CountingWriter<W> {
        self.for_field_with_idx(field, 0)
    }

    /// Start writing a new field.
    pub fn for_field_with_idx(&mut self, field: Field, idx: usize) -> &mut CountingWriter<W> {
        let offset = self.write.written_bytes();
        let file_addr = FileAddr::new(field, idx);
        assert!(!self.offsets.contains_key(&file_addr));
        self.offsets.insert(file_addr, offset);
        &mut self.write
    }

    /// Close the composite file
    ///
    /// An index of the different field offsets
    /// will be written as a footer.
    pub fn close(mut self) -> io::Result<()> {
        let footer_offset = self.write.written_bytes();
        VInt(self.offsets.len() as u64).serialize(&mut self.write)?;

        let mut offset_fields: Vec<_> = self
            .offsets
            .iter()
            .map(|(file_addr, offset)| (*offset, *file_addr))
            .collect();

        offset_fields.sort();

        let mut prev_offset = 0;
        for (offset, file_addr) in offset_fields {
            VInt((offset - prev_offset) as u64).serialize(&mut self.write)?;
            file_addr.serialize(&mut self.write)?;
            prev_offset = offset;
        }

        let footer_len = (self.write.written_bytes() - footer_offset) as u32;
        footer_len.serialize(&mut self.write)?;
        self.write.terminate()
    }
}

/// A composite file is an abstraction to store a
/// file partitioned by field.
///
/// The file needs to be written field by field.
/// A footer describes the start and stop offsets
/// for each field.
#[derive(Clone)]
pub struct CompositeFile {
    data: FileSlice,
    offsets_index: HashMap<FileAddr, (usize, usize)>,
}

impl CompositeFile {
    /// Opens a composite file stored in a given
    /// `FileSlice`.
    pub fn open(data: &FileSlice) -> io::Result<CompositeFile> {
        let end = data.len();
        let footer_len_data = data.slice_from(end - 4).read_bytes()?;
        let footer_len = u32::deserialize(&mut footer_len_data.as_slice())? as usize;
        let footer_start = end - 4 - footer_len;
        let footer_data = data
            .slice(footer_start, footer_start + footer_len)
            .read_bytes()?;
        let mut footer_buffer = footer_data.as_slice();
        let num_fields = VInt::deserialize(&mut footer_buffer)?.0 as usize;

        let mut file_addrs = vec![];
        let mut offsets = vec![];
        let mut field_index = HashMap::new();

        let mut offset = 0;
        for _ in 0..num_fields {
            offset += VInt::deserialize(&mut footer_buffer)?.0 as usize;
            let file_addr = FileAddr::deserialize(&mut footer_buffer)?;
            offsets.push(offset);
            file_addrs.push(file_addr);
        }
        offsets.push(footer_start);
        for i in 0..num_fields {
            let file_addr = file_addrs[i];
            let start_offset = offsets[i];
            let end_offset = offsets[i + 1];
            field_index.insert(file_addr, (start_offset, end_offset));
        }

        Ok(CompositeFile {
            data: data.slice_to(footer_start),
            offsets_index: field_index,
        })
    }

    /// Returns a composite file that stores
    /// no fields.
    pub fn empty() -> CompositeFile {
        CompositeFile {
            offsets_index: HashMap::new(),
            data: FileSlice::empty(),
        }
    }

    /// Returns the `FileSlice` associated
    /// to a given `Field` and stored in a `CompositeFile`.
    pub fn open_read(&self, field: Field) -> Option<FileSlice> {
        self.open_read_with_idx(field, 0)
    }

    /// Returns the `FileSlice` associated
    /// to a given `Field` and stored in a `CompositeFile`.
    pub fn open_read_with_idx(&self, field: Field, idx: usize) -> Option<FileSlice> {
        self.offsets_index
            .get(&FileAddr { field, idx })
            .map(|&(from, to)| self.data.slice(from, to))
    }

    pub fn space_usage(&self) -> PerFieldSpaceUsage {
        let mut fields = HashMap::new();
        for (&field_addr, &(start, end)) in self.offsets_index.iter() {
            fields
                .entry(field_addr.field)
                .or_insert_with(|| FieldUsage::empty(field_addr.field))
                .add_field_idx(field_addr.idx, end - start);
        }
        PerFieldSpaceUsage::new(fields)
    }
}

#[cfg(test)]
mod test {

    use super::{CompositeFile, CompositeWrite};
    use crate::common::BinarySerializable;
    use crate::common::VInt;
    use crate::directory::{Directory, RAMDirectory};
    use crate::schema::Field;
    use std::io::Write;
    use std::path::Path;

    #[test]
    fn test_composite_file() -> crate::Result<()> {
        let path = Path::new("test_path");
        let directory = RAMDirectory::create();
        {
            let w = directory.open_write(path).unwrap();
            let mut composite_write = CompositeWrite::wrap(w);
            let mut write_0 = composite_write.for_field(Field::from_field_id(0u32));
            VInt(32431123u64).serialize(&mut write_0)?;
            write_0.flush()?;
            let mut write_4 = composite_write.for_field(Field::from_field_id(4u32));
            VInt(2).serialize(&mut write_4)?;
            write_4.flush()?;
            composite_write.close()?;
        }
        {
            let r = directory.open_read(path)?;
            let composite_file = CompositeFile::open(&r)?;
            {
                let file0 = composite_file
                    .open_read(Field::from_field_id(0u32))
                    .unwrap()
                    .read_bytes()?;
                let mut file0_buf = file0.as_slice();
                let payload_0 = VInt::deserialize(&mut file0_buf)?.0;
                assert_eq!(file0_buf.len(), 0);
                assert_eq!(payload_0, 32431123u64);
            }
            {
                let file4 = composite_file
                    .open_read(Field::from_field_id(4u32))
                    .unwrap()
                    .read_bytes()?;
                let mut file4_buf = file4.as_slice();
                let payload_4 = VInt::deserialize(&mut file4_buf)?.0;
                assert_eq!(file4_buf.len(), 0);
                assert_eq!(payload_4, 2u64);
            }
        }
        Ok(())
    }
}

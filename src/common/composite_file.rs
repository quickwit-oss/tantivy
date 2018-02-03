use std::io::Write;
use common::CountingWriter;
use std::collections::HashMap;
use schema::Field;
use common::VInt;
use directory::WritePtr;
use std::io::{self, Read};
use directory::ReadOnlySource;
use common::BinarySerializable;

#[derive(Eq, PartialEq, Hash, Copy, Ord, PartialOrd, Clone, Debug)]
pub struct FileAddr {
    field: Field,
    idx: usize,
}

impl FileAddr {
    fn new(field: Field, idx: usize) -> FileAddr {
        FileAddr {
            field: field,
            idx: idx,
        }
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
        Ok(FileAddr {
            field: field,
            idx: idx,
        })
    }
}

/// A `CompositeWrite` is used to write a `CompositeFile`.
pub struct CompositeWrite<W = WritePtr> {
    write: CountingWriter<W>,
    offsets: HashMap<FileAddr, usize>,
}

impl<W: Write> CompositeWrite<W> {
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

    /// Close the composite file.
    ///
    /// An index of the different field offsets
    /// will be written as a footer.
    pub fn close(mut self) -> io::Result<()> {
        let footer_offset = self.write.written_bytes();
        VInt(self.offsets.len() as u64).serialize(&mut self.write)?;

        let mut offset_fields: Vec<_> = self.offsets
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
        self.write.flush()?;
        Ok(())
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
    data: ReadOnlySource,
    offsets_index: HashMap<FileAddr, (usize, usize)>,
}

impl CompositeFile {
    /// Opens a composite file stored in a given
    /// `ReadOnlySource`.
    pub fn open(data: &ReadOnlySource) -> io::Result<CompositeFile> {
        let end = data.len();
        let footer_len_data = data.slice_from(end - 4);
        let footer_len = u32::deserialize(&mut footer_len_data.as_slice())? as usize;

        let footer_start = end - 4 - footer_len;
        let footer_data = data.slice(footer_start, footer_start + footer_len);
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
            data: ReadOnlySource::empty(),
        }
    }

    /// Returns the `ReadOnlySource` associated
    /// to a given `Field` and stored in a `CompositeFile`.
    pub fn open_read(&self, field: Field) -> Option<ReadOnlySource> {
        self.open_read_with_idx(field, 0)
    }

    /// Returns the `ReadOnlySource` associated
    /// to a given `Field` and stored in a `CompositeFile`.
    pub fn open_read_with_idx(&self, field: Field, idx: usize) -> Option<ReadOnlySource> {
        self.offsets_index
            .get(&FileAddr {
                field: field,
                idx: idx,
            })
            .map(|&(from, to)| self.data.slice(from, to))
    }
}

#[cfg(test)]
mod test {

    use std::io::Write;
    use super::{CompositeFile, CompositeWrite};
    use directory::{Directory, RAMDirectory};
    use schema::Field;
    use common::VInt;
    use common::BinarySerializable;
    use std::path::Path;

    #[test]
    fn test_composite_file() {
        let path = Path::new("test_path");
        let mut directory = RAMDirectory::create();
        {
            let w = directory.open_write(path).unwrap();
            let mut composite_write = CompositeWrite::wrap(w);
            {
                let mut write_0 = composite_write.for_field(Field(0u32));
                VInt(32431123u64).serialize(&mut write_0).unwrap();
                write_0.flush().unwrap();
            }

            {
                let mut write_4 = composite_write.for_field(Field(4u32));
                VInt(2).serialize(&mut write_4).unwrap();
                write_4.flush().unwrap();
            }
            composite_write.close().unwrap();
        }
        {
            let r = directory.open_read(path).unwrap();
            let composite_file = CompositeFile::open(&r).unwrap();
            {
                let file0 = composite_file.open_read(Field(0u32)).unwrap();
                let mut file0_buf = file0.as_slice();
                let payload_0 = VInt::deserialize(&mut file0_buf).unwrap().0;
                assert_eq!(file0_buf.len(), 0);
                assert_eq!(payload_0, 32431123u64);
            }
            {
                let file4 = composite_file.open_read(Field(4u32)).unwrap();
                let mut file4_buf = file4.as_slice();
                let payload_4 = VInt::deserialize(&mut file4_buf).unwrap().0;
                assert_eq!(file4_buf.len(), 0);
                assert_eq!(payload_4, 2u64);
            }
        }
    }

}

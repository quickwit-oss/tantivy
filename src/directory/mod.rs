mod mmap_directory;
mod ram_directory;
mod directory;

use std::ops::Deref;
use std::io::{Seek, Write, Cursor};
use fst::raw::MmapReadOnly;

pub use self::directory::Directory;
pub use self::ram_directory::RAMDirectory;
pub use self::mmap_directory::MmapDirectory;
pub use self::ram_directory::SharedVec;

////////////////////////////////////////
// WritePtr


pub trait SeekableWrite: Seek + Write {}
impl<T: Seek + Write> SeekableWrite for T {}
pub type WritePtr = Box<SeekableWrite>;


////////////////////////////////////////
// Read only source.


pub enum ReadOnlySource {
    Mmap(MmapReadOnly),
    Anonymous(Vec<u8>),
}

impl Deref for ReadOnlySource {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl ReadOnlySource {

    pub fn len(&self,) -> usize {
        self.as_slice().len()
    }

    pub fn as_slice(&self,) -> &[u8] {
        match *self {
            ReadOnlySource::Mmap(ref mmap_read_only) => unsafe { 
                mmap_read_only.as_slice()
            },
            ReadOnlySource::Anonymous(ref shared_vec) => shared_vec.as_slice(),
        }
    }

    pub fn cursor<'a>(&'a self) -> Cursor<&'a [u8]> {
        Cursor::new(&self.deref())
    }

    pub fn slice(&self, from_offset:usize, to_offset:usize) -> ReadOnlySource {
        match *self {
            ReadOnlySource::Mmap(ref mmap_read_only) => {
                let sliced_mmap = mmap_read_only.range(from_offset, to_offset - from_offset);
                ReadOnlySource::Mmap(sliced_mmap)
            }
            ReadOnlySource::Anonymous(ref shared_vec) => {
                let sliced_data: Vec<u8> = Vec::from(&shared_vec[from_offset..to_offset]);
                ReadOnlySource::Anonymous(sliced_data)
            },
        }
    }
}

impl Clone for ReadOnlySource {
    fn clone(&self) -> Self {
        self.slice(0, self.len())
    }
}


#[cfg(test)]
mod tests {

    use super::*;
    use std::path::Path;

    #[test]
    fn test_ram_directory() {
        let mut ram_directory = RAMDirectory::create();
        test_directory(&mut ram_directory);
    }

    #[test]
    fn test_mmap_directory() {
        let mut mmap_directory = MmapDirectory::create_from_tempdir().unwrap();
        test_directory(&mut mmap_directory);
    }

    fn test_directory(directory: &mut Directory) {
        {
            let mut write_file = directory.open_write(Path::new("toto")).unwrap();
            write_file.write_all(&[4]).unwrap();
            write_file.write_all(&[3]).unwrap();
            write_file.write_all(&[7,3,5]).unwrap();
        }
        let read_file = directory.open_read(Path::new("toto")).unwrap();
        let data: &[u8] = &*read_file;
        assert_eq!(data.len(), 5);
        assert_eq!(data[0], 4);
        assert_eq!(data[1], 3);
        assert_eq!(data[2], 7);
        assert_eq!(data[3], 3);
        assert_eq!(data[4], 5);
    }

}

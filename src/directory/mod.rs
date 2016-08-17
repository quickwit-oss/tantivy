mod mmap_directory;
mod ram_directory;
mod directory;
mod read_only_source;
mod shared_vec_slice;

use std::io::{Seek, Write};
use std::io;

use std::path::PathBuf;
pub use self::shared_vec_slice::SharedVecSlice;

pub use self::read_only_source::ReadOnlySource;
pub use self::directory::Directory;
pub use self::ram_directory::RAMDirectory;
pub use self::mmap_directory::MmapDirectory;


////////////////////////////////////////
// WritePtr

pub trait SeekableWrite: Seek + Write {}
impl<T: Seek + Write> SeekableWrite for T {}
pub type WritePtr = Box<SeekableWrite>;

#[derive(Debug)]
pub enum OpenError {
    FileDoesNotExist(PathBuf),
    IOError(io::Error),
}




#[cfg(test)]
mod tests {

    use super::*;
    use std::path::Path;
    use std::io::SeekFrom;

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

    fn test_directory_simple(directory: &mut Directory) {
        {
            let mut write_file = directory.open_write(Path::new("toto")).unwrap();
            write_file.write_all(&[4]).unwrap();
            write_file.write_all(&[3]).unwrap();
            write_file.write_all(&[7,3,5]).unwrap();
            write_file.flush().unwrap();
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


    fn test_directory_seek(directory: &mut Directory) {
        {
            let mut write_file = directory.open_write(Path::new("toto_seek")).unwrap();
            write_file.write_all(&[4]).unwrap();
            write_file.write_all(&[3]).unwrap();
            write_file.write_all(&[7,3,5]).unwrap();
            write_file.seek(SeekFrom::Start(0)).unwrap();
            write_file.write_all(&[3,1]).unwrap();
            write_file.flush().unwrap();
        }
        let read_file = directory.open_read(Path::new("toto_seek")).unwrap();
        let data: &[u8] = &*read_file;
        assert_eq!(data.len(), 5);
        assert_eq!(data[0], 3);
        assert_eq!(data[1], 1);
        assert_eq!(data[2], 7);
        assert_eq!(data[3], 3);
        assert_eq!(data[4], 5);
    }

    fn test_directory(directory: &mut Directory) {
        test_directory_simple(directory);
        test_directory_seek(directory);
    }

}

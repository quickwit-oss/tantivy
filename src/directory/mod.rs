/*!

WORM directory abstraction.

*/

#[cfg(feature = "mmap")]
mod mmap_directory;

mod directory;
mod managed_directory;
mod ram_directory;
mod read_only_source;
mod shared_vec_slice;

/// Errors specific to the directory module.
pub mod error;

use std::io::{BufWriter, Seek, Write};

pub use self::directory::{Directory, DirectoryClone};
pub use self::ram_directory::RAMDirectory;
pub use self::read_only_source::ReadOnlySource;

#[cfg(feature = "mmap")]
pub use self::mmap_directory::MmapDirectory;

pub(crate) use self::managed_directory::ManagedDirectory;

/// Synonym of Seek + Write
pub trait SeekableWrite: Seek + Write {}
impl<T: Seek + Write> SeekableWrite for T {}

/// Write object for Directory.
///
/// `WritePtr` are required to implement both Write
/// and Seek.
pub type WritePtr = BufWriter<Box<SeekableWrite>>;

#[cfg(test)]
mod tests {

    use super::*;
    use std::io::{Seek, SeekFrom, Write};
    use std::path::Path;

    lazy_static! {
        static ref TEST_PATH: &'static Path = Path::new("some_path_for_test");
    }

    #[test]
    fn test_ram_directory() {
        let mut ram_directory = RAMDirectory::create();
        test_directory(&mut ram_directory);
    }

    #[test]
    #[cfg(feature = "mmap")]
    fn test_mmap_directory() {
        let mut mmap_directory = MmapDirectory::create_from_tempdir().unwrap();
        test_directory(&mut mmap_directory);
    }

    #[test]
    #[should_panic]
    fn ram_directory_panics_if_flush_forgotten() {
        let mut ram_directory = RAMDirectory::create();
        let mut write_file = ram_directory.open_write(*TEST_PATH).unwrap();
        assert!(write_file.write_all(&[4]).is_ok());
    }

    fn test_simple(directory: &mut Directory) {
        {
            {
                let mut write_file = directory.open_write(*TEST_PATH).unwrap();
                assert!(directory.exists(*TEST_PATH));
                write_file.write_all(&[4]).unwrap();
                write_file.write_all(&[3]).unwrap();
                write_file.write_all(&[7, 3, 5]).unwrap();
                write_file.flush().unwrap();
            }
            let read_file = directory.open_read(*TEST_PATH).unwrap();
            let data: &[u8] = &*read_file;
            assert_eq!(data, &[4u8, 3u8, 7u8, 3u8, 5u8]);
        }

        assert!(directory.delete(*TEST_PATH).is_ok());
        assert!(!directory.exists(*TEST_PATH));
    }

    fn test_seek(directory: &mut Directory) {
        {
            {
                let mut write_file = directory.open_write(*TEST_PATH).unwrap();
                write_file.write_all(&[4, 3, 7, 3, 5]).unwrap();
                write_file.seek(SeekFrom::Start(0)).unwrap();
                write_file.write_all(&[3, 1]).unwrap();
                write_file.flush().unwrap();
            }
            let read_file = directory.open_read(*TEST_PATH).unwrap();
            let data: &[u8] = &*read_file;
            assert_eq!(data, &[3u8, 1u8, 7u8, 3u8, 5u8]);
        }

        assert!(directory.delete(*TEST_PATH).is_ok());
    }

    fn test_rewrite_forbidden(directory: &mut Directory) {
        {
            directory.open_write(*TEST_PATH).unwrap();
            assert!(directory.exists(*TEST_PATH));
        }
        {
            assert!(directory.open_write(*TEST_PATH).is_err());
        }
        assert!(directory.delete(*TEST_PATH).is_ok());
    }

    fn test_write_create_the_file(directory: &mut Directory) {
        {
            assert!(directory.open_read(*TEST_PATH).is_err());
            let _w = directory.open_write(*TEST_PATH).unwrap();
            assert!(directory.exists(*TEST_PATH));
            assert!(directory.open_read(*TEST_PATH).is_ok());
            assert!(directory.delete(*TEST_PATH).is_ok());
        }
    }

    fn test_directory_delete(directory: &mut Directory) {
        assert!(directory.open_read(*TEST_PATH).is_err());
        let mut write_file = directory.open_write(*TEST_PATH).unwrap();
        write_file.write_all(&[1, 2, 3, 4]).unwrap();
        write_file.flush().unwrap();
        {
            let read_handle = directory.open_read(*TEST_PATH).unwrap();
            {
                assert_eq!(&*read_handle, &[1u8, 2u8, 3u8, 4u8]);

                // Mapped files can't be deleted on Windows
                if !cfg!(windows) {
                    assert!(directory.delete(*TEST_PATH).is_ok());
                    assert_eq!(&*read_handle, &[1u8, 2u8, 3u8, 4u8]);
                }

                assert!(directory.delete(Path::new("SomeOtherPath")).is_err());
            }
        }

        if cfg!(windows) {
            assert!(directory.delete(*TEST_PATH).is_ok());
        }

        assert!(directory.open_read(*TEST_PATH).is_err());
        assert!(directory.delete(*TEST_PATH).is_err());
    }

    fn test_directory(directory: &mut Directory) {
        test_simple(directory);
        test_seek(directory);
        test_rewrite_forbidden(directory);
        test_write_create_the_file(directory);
        test_directory_delete(directory);
    }

}

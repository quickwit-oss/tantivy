use crate::core::{MANAGED_FILEPATH, META_FILEPATH};
use crate::directory::error::{DeleteError, IOError, LockError, OpenReadError, OpenWriteError};
use crate::directory::DirectoryLock;
use crate::directory::Lock;
use crate::directory::META_LOCK;
use crate::directory::TerminatingWrite;
use crate::directory::{ReadOnlySource, WritePtr};
use crate::directory::{WatchCallback, WatchHandle};
use crate::error::DataCorruption;
use crate::Directory;
use crate::Result;
use crc32fast::Hasher;
use serde_json;
use std::collections::HashSet;
use std::io;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::result;
use std::sync::RwLockWriteGuard;
use std::sync::{Arc, RwLock};

/// Returns true iff the file is "managed".
/// Non-managed file are not subject to garbage collection.
///
/// Filenames that starts by a "." -typically locks-
/// are not managed.
fn is_managed(path: &Path) -> bool {
    path.to_str()
        .map(|p_str| !p_str.starts_with('.'))
        .unwrap_or(true)
}

/// Returns true iff the file require a footer.
fn require_footer(path: &Path) -> bool {
    is_managed(path) &
        ![*MANAGED_FILEPATH, *META_FILEPATH].contains(&path)
}


/// Wrapper of directories that keeps track of files created by Tantivy.
///
/// A managed directory is just a wrapper of a directory
/// that keeps a (persisted) list of the files that
/// have been created (and not deleted) by tantivy so far.
///
/// Thanks to this list, it implements a `garbage_collect` method
/// that removes the files that were created by tantivy and are not
/// useful anymore.
#[derive(Debug)]
pub struct ManagedDirectory {
    directory: Box<dyn Directory>,
    meta_informations: Arc<RwLock<MetaInformation>>,
}

#[derive(Debug, Default)]
struct MetaInformation {
    managed_paths: HashSet<PathBuf>,
}

/// Saves the file containing the list of existing files
/// that were created by tantivy.
fn save_managed_paths(
    directory: &mut dyn Directory,
    wlock: &RwLockWriteGuard<'_, MetaInformation>,
) -> io::Result<()> {
    let mut w = serde_json::to_vec(&wlock.managed_paths)?;
    writeln!(&mut w)?;
    directory.atomic_write(&MANAGED_FILEPATH, &w[..])?;
    Ok(())
}

impl ManagedDirectory {
    /// Wraps a directory as managed directory.
    pub fn wrap<Dir: Directory>(directory: Dir) -> Result<ManagedDirectory> {
        match directory.atomic_read(&MANAGED_FILEPATH) {
            Ok(data) => {
                let managed_files_json = String::from_utf8_lossy(&data);
                let managed_files: HashSet<PathBuf> = serde_json::from_str(&managed_files_json)
                    .map_err(|e| {
                        DataCorruption::new(
                            MANAGED_FILEPATH.to_path_buf(),
                            format!("Managed file cannot be deserialized: {:?}. ", e),
                        )
                    })?;
                Ok(ManagedDirectory {
                    directory: Box::new(directory),
                    meta_informations: Arc::new(RwLock::new(MetaInformation {
                        managed_paths: managed_files,
                    })),
                })
            }
            Err(OpenReadError::FileDoesNotExist(_)) => Ok(ManagedDirectory {
                directory: Box::new(directory),
                meta_informations: Arc::default(),
            }),
            Err(OpenReadError::IOError(e)) => Err(From::from(e)),
        }
    }

    /// Garbage collect unused files.
    ///
    /// Removes the files that were created by `tantivy` and are not
    /// used by any segment anymore.
    ///
    /// * `living_files` - List of files that are still used by the index.
    ///
    /// The use a callback ensures that the list of living_files is computed
    /// while we hold the lock on meta.
    ///
    /// This method does not panick nor returns errors.
    /// If a file cannot be deleted (for permission reasons for instance)
    /// an error is simply logged, and the file remains in the list of managed
    /// files.
    pub fn garbage_collect<L: FnOnce() -> HashSet<PathBuf>>(&mut self, get_living_files: L) {
        info!("Garbage collect");
        let mut files_to_delete = vec![];

        // It is crucial to get the living files after acquiring the
        // read lock of meta informations. That way, we
        // avoid the following scenario.
        //
        // 1) we get the list of living files.
        // 2) someone creates a new file.
        // 3) we start garbage collection and remove this file
        // even though it is a living file.
        //
        // releasing the lock as .delete() will use it too.
        {
            let meta_informations_rlock = self
                .meta_informations
                .read()
                .expect("Managed directory rlock poisoned in garbage collect.");

            // The point of this second "file" lock is to enforce the following scenario
            // 1) process B tries to load a new set of searcher.
            // The list of segments is loaded
            // 2) writer change meta.json (for instance after a merge or a commit)
            // 3) gc kicks in.
            // 4) gc removes a file that was useful for process B, before process B opened it.
            if let Ok(_meta_lock) = self.acquire_lock(&META_LOCK) {
                let living_files = get_living_files();
                for managed_path in &meta_informations_rlock.managed_paths {
                    if !living_files.contains(managed_path) {
                        files_to_delete.push(managed_path.clone());
                    }
                }
            } else {
                error!("Failed to acquire lock for GC");
            }
        }

        let mut deleted_files = vec![];
        for file_to_delete in files_to_delete {
            match self.delete(&file_to_delete) {
                Ok(_) => {
                    info!("Deleted {:?}", file_to_delete);
                    deleted_files.push(file_to_delete);
                }
                Err(file_error) => {
                    match file_error {
                        DeleteError::FileDoesNotExist(_) => {
                            deleted_files.push(file_to_delete);
                        }
                        DeleteError::IOError(_) => {
                            if !cfg!(target_os = "windows") {
                                // On windows, delete is expected to fail if the file
                                // is mmapped.
                                error!("Failed to delete {:?}", file_to_delete);
                            }
                        }
                    }
                }
            }
        }

        if !deleted_files.is_empty() {
            // update the list of managed files by removing
            // the file that were removed.
            let mut meta_informations_wlock = self
                .meta_informations
                .write()
                .expect("Managed directory wlock poisoned (2).");
            let managed_paths_write = &mut meta_informations_wlock.managed_paths;
            for delete_file in &deleted_files {
                managed_paths_write.remove(delete_file);
            }
            if save_managed_paths(self.directory.as_mut(), &meta_informations_wlock).is_err() {
                error!("Failed to save the list of managed files.");
            }
        }
    }

    /// Registers a file as managed
    ///
    /// This method must be called before the file is
    /// actually created to ensure that a failure between
    /// registering the filepath and creating the file
    /// will not lead to garbage files that will
    /// never get removed.
    ///
    /// File starting by "." are reserved to locks.
    /// They are not managed and cannot be subjected
    /// to garbage collection.
    fn register_file_as_managed(&mut self, filepath: &Path) -> io::Result<()> {
        // Files starting by "." (e.g. lock files) are not managed.
        if !is_managed(filepath) {
            return Ok(());
        }
        let mut meta_wlock = self
            .meta_informations
            .write()
            .expect("Managed file lock poisoned");
        let has_changed = meta_wlock.managed_paths.insert(filepath.to_owned());
        if has_changed {
            save_managed_paths(self.directory.as_mut(), &meta_wlock)?;
        }
        Ok(())
    }

    /// Verify checksum of a managed file
    pub fn validate_checksum(&self, path: &Path) -> result::Result<bool, OpenReadError> {
        let reader = self.directory.open_read(path)?;
        let data = reader.as_slice();
        let footer = Footer::from_bytes(data);
        let data = &data[..data.len() - footer.size() as usize];
        let mut hasher = Hasher::new();
        hasher.update(data);
        let crc = hasher.finalize();
        Ok(crc == footer.crc())
    }

    /// List files for which checksum does not match content
    pub fn list_damaged(&self) -> result::Result<HashSet<PathBuf>, OpenReadError> {
        let mut hashset = HashSet::new();
        let managed_paths = self
            .meta_informations
            .read()
            .expect("Managed directory rlock poisoned in list damaged.")
            .managed_paths
            .clone();

        for path in managed_paths.into_iter() {
            if !self.validate_checksum(&path)? {
                hashset.insert(path);
            }
        }
        Ok(hashset)
    }
}

#[derive(Debug, Copy, Clone)]
pub enum Footer {
    V0(V0)
}

impl Footer {
    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            Footer::V0(f) => f.to_bytes()
        }
    }

    pub fn from_bytes(data: &[u8]) -> Self {
        let len = data.len();
        assert!(len >= 2);
        let size = u16::from_be_bytes([data[len - 2], data[len - 1]]);
        assert_eq!(size, 10);
        let footer = &data[len - size as usize..];
        let index_version = footer[0];
        match index_version {
            0 => Footer::V0(V0::from_bytes(data)),
            _ => panic!("unsuported index_version")
        }
    }


    pub fn index_version(&self) -> u8 {
        match self {
            Footer::V0(_) => 0,
        }
    }

    pub fn tantivy_version(&self) -> (u8, u8, u8) {
        match self {
            Footer::V0(f) => f.tantivy_version,
        }
    }

    pub fn crc(&self) -> u32 {
        match self {
            Footer::V0(f) => f.crc,
        }
    }

    pub fn size(&self) -> usize {
        match self {
            Footer::V0(_) => V0::size() as usize,
        }
    }
}

#[derive(Debug, Copy, Clone)]
pub struct V0 {
    pub tantivy_version: (u8,u8,u8),
    pub crc: u32,
}

impl V0 {
    pub fn to_bytes(&self) -> Vec<u8> {
        let crc = self.crc.to_be_bytes();
        let size = Self::size().to_be_bytes();
        let mut res = vec![0, self.tantivy_version.0, self.tantivy_version.1, self.tantivy_version.2];
        res.extend_from_slice(&crc);
        res.extend_from_slice(&size);
        res
    }

    pub fn from_bytes(footer: &[u8]) -> Self {
        assert_eq!(footer[0], 0);
        assert_eq!(u16::from_be_bytes([footer[8], footer[9]]), 10);
        let tantivy_version = (footer[1], footer[2], footer[3]);
        let crc = u32::from_be_bytes([footer[4], footer[5], footer[6], footer[7]]);

        V0 {
            tantivy_version,
            crc
        }
    }

    pub fn from_crc(crc: u32) -> Self {
        Self {
            tantivy_version: (0,10,0),
            crc
        }
    }
    pub fn size() -> u16 {
        10
    }
}

struct FooterProxy<W: TerminatingWrite> {
    hasher: Hasher,
    writer: W,
}

impl<W: TerminatingWrite> FooterProxy<W> {
    fn new(writer: W) -> Self {
        FooterProxy {
            hasher: Hasher::new(),
            writer
        }
    }
}

impl<W: TerminatingWrite> Write for FooterProxy<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let count = self.writer.write(buf)?;
        self.hasher.update(&buf[..count]);
        Ok(count)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}

impl<W: TerminatingWrite> TerminatingWrite for FooterProxy<W> {
    fn terminate(mut self) -> io::Result<()> {
        let crc = self.hasher.finalize();
        let footer = V0::from_crc(crc).to_bytes();
        self.writer.write_all(&footer)?;
        self.writer.flush()?;//should we assuse calling terminate on inner already flush?
        self.writer.terminate()
    }
}

impl Directory for ManagedDirectory {
    fn open_read(&self, path: &Path) -> result::Result<ReadOnlySource, OpenReadError> {
        let ros = self.directory.open_read(path)?;
        if require_footer(path) {
            let footer_size = Footer::from_bytes(ros.as_slice()).size();
            Ok(ros.slice_to(ros.as_slice().len() - footer_size))
        } else {
            Ok(ros)
        }
    }

    fn open_write(&mut self, path: &Path) -> result::Result<WritePtr, OpenWriteError> {
        self.register_file_as_managed(path)
            .map_err(|e| IOError::with_path(path.to_owned(), e))?;
        if require_footer(path) {
            Ok(io::BufWriter::new(Box::new(
                FooterProxy::new(self.directory.open_write(path)?.into_inner().map_err(|_|()).expect("buffer should be empty"))
            )))
        } else {
           self.directory.open_write(path)
        }
    }

    fn atomic_write(&mut self, path: &Path, data: &[u8]) -> io::Result<()> {
        self.register_file_as_managed(path)?;
        if require_footer(path) {
            let mut with_footer = Vec::with_capacity(data.len() + V0::size() as usize);
            with_footer.extend_from_slice(&data);
            let mut hasher = Hasher::new();
            hasher.update(&data);
            let crc = hasher.finalize();
            with_footer.append(&mut V0::from_crc(crc).to_bytes());
            self.directory.atomic_write(path, &with_footer)
        } else {
            self.directory.atomic_write(path, &data)
        }
    }

    fn atomic_read(&self, path: &Path) -> result::Result<Vec<u8>, OpenReadError> {
        let mut vec = self.directory.atomic_read(path)?;
        if require_footer(path) {
            let footer_size = Footer::from_bytes(&vec).size();
            vec.resize(vec.len() - footer_size, 0);
        }
        Ok(vec)
    }

    fn delete(&self, path: &Path) -> result::Result<(), DeleteError> {
        self.directory.delete(path)
    }

    fn exists(&self, path: &Path) -> bool {
        self.directory.exists(path)
    }

    fn acquire_lock(&self, lock: &Lock) -> result::Result<DirectoryLock, LockError> {
        self.directory.acquire_lock(lock)
    }

    fn watch(&self, watch_callback: WatchCallback) -> crate::Result<WatchHandle> {
        self.directory.watch(watch_callback)
    }
}

impl Clone for ManagedDirectory {
    fn clone(&self) -> ManagedDirectory {
        ManagedDirectory {
            directory: self.directory.box_clone(),
            meta_informations: Arc::clone(&self.meta_informations),
        }
    }
}

#[cfg(feature = "mmap")]
#[cfg(test)]
mod tests_mmap_specific {

    use crate::directory::{Directory, ManagedDirectory, MmapDirectory};
    use std::collections::HashSet;
    use std::io::Write;
    use std::path::{Path, PathBuf};
    use tempfile::TempDir;

    #[test]
    fn test_managed_directory() {
        let tempdir = TempDir::new().unwrap();
        let tempdir_path = PathBuf::from(tempdir.path());

        let test_path1: &'static Path = Path::new("some_path_for_test");
        let test_path2: &'static Path = Path::new("some_path_for_test_2");
        {
            let mmap_directory = MmapDirectory::open(&tempdir_path).unwrap();
            let mut managed_directory = ManagedDirectory::wrap(mmap_directory).unwrap();
            let mut write_file = managed_directory.open_write(test_path1).unwrap();
            write_file.flush().unwrap();
            managed_directory
                .atomic_write(test_path2, &[0u8, 1u8])
                .unwrap();
            assert!(managed_directory.exists(test_path1));
            assert!(managed_directory.exists(test_path2));
            let living_files: HashSet<PathBuf> =
                [test_path1.to_owned()].into_iter().cloned().collect();
            managed_directory.garbage_collect(|| living_files);
            assert!(managed_directory.exists(test_path1));
            assert!(!managed_directory.exists(test_path2));
        }
        {
            let mmap_directory = MmapDirectory::open(&tempdir_path).unwrap();
            let mut managed_directory = ManagedDirectory::wrap(mmap_directory).unwrap();
            assert!(managed_directory.exists(test_path1));
            assert!(!managed_directory.exists(test_path2));
            let living_files: HashSet<PathBuf> = HashSet::new();
            managed_directory.garbage_collect(|| living_files);
            assert!(!managed_directory.exists(test_path1));
            assert!(!managed_directory.exists(test_path2));
        }
    }

    #[test]
    fn test_managed_directory_gc_while_mmapped() {
        let test_path1: &'static Path = Path::new("some_path_for_test");

        let tempdir = TempDir::new().unwrap();
        let tempdir_path = PathBuf::from(tempdir.path());
        let living_files = HashSet::new();

        let mmap_directory = MmapDirectory::open(&tempdir_path).unwrap();
        let mut managed_directory = ManagedDirectory::wrap(mmap_directory).unwrap();
        managed_directory
            .atomic_write(test_path1, &vec![0u8, 1u8])
            .unwrap();
        assert!(managed_directory.exists(test_path1));

        let _mmap_read = managed_directory.open_read(test_path1).unwrap();
        managed_directory.garbage_collect(|| living_files.clone());
        if cfg!(target_os = "windows") {
            // On Windows, gc should try and fail the file as it is mmapped.
            assert!(managed_directory.exists(test_path1));
            // unmap should happen here.
            drop(_mmap_read);
            // The file should still be in the list of managed file and
            // eventually be deleted once mmap is released.
            managed_directory.garbage_collect(|| living_files);
            assert!(!managed_directory.exists(test_path1));
        } else {
            assert!(!managed_directory.exists(test_path1));
        }
    }

}

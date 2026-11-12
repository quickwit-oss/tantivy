use std::collections::HashSet;
use std::fmt;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use crate::core::META_FILEPATH;
use crate::directory::error::{DeleteError, OpenReadError, OpenWriteError};
use crate::directory::{Directory, DirectoryLock, FileHandle, FileSlice, Lock, TerminatingWrite,
                       WatchCallback, WatchHandle, WritePtr};
use crate::directory::RamDirectory;

/// A Directory that overlays a `RamDirectory` on top of a base `Directory`.
///
/// - Writes (open_write and atomic_write) go to the in-memory overlay.
/// - Reads first check the overlay, then fallback to the base directory.
/// - sync_directory() persists overlay files that do not yet exist in the base directory.
#[derive(Clone)]
pub struct NrtDirectory {
    base: Box<dyn Directory>,
    overlay: RamDirectory,
    /// Tracks files written into the overlay to decide what to persist on sync.
    overlay_paths: Arc<RwLock<HashSet<PathBuf>>>,
}

impl NrtDirectory {
    /// Wraps a base directory with an NRT overlay.
    pub fn wrap(base: Box<dyn Directory>) -> NrtDirectory {
        NrtDirectory {
            base,
            overlay: RamDirectory::default(),
            overlay_paths: Arc::new(RwLock::new(HashSet::new())),
        }
    }

    /// Persist overlay files into the base directory if missing there.
    fn persist_overlay_into_base(&self) -> crate::Result<()> {
        let snapshot_paths: Vec<PathBuf> = {
            let guard = self.overlay_paths.read().unwrap();
            guard.iter().cloned().collect()
        };
        // First copy all non-meta files. `meta.json` must be written last atomically.
        for path in snapshot_paths {
            if path == *META_FILEPATH {
                continue;
            }
            // Skip if base already has the file
            if self.base.exists(&path).unwrap_or(false) {
                continue;
            }
            // Read bytes from overlay
            let file_slice: FileSlice = match self.overlay.open_read(&path) {
                Ok(slice) => slice,
                Err(OpenReadError::FileDoesNotExist(_)) => continue, // was removed meanwhile
                Err(e) => return Err(e.into()),
            };
            let bytes = file_slice
                .read_bytes()
                .map_err(|io_err| OpenReadError::IoError {
                    io_error: Arc::new(io_err),
                    filepath: path.clone(),
                })?;
            // Write to base
            let mut dest_wrt: WritePtr = self.base.open_write(&path)?;
            dest_wrt.write_all(bytes.as_slice())?;
            dest_wrt.terminate()?;
        }
        // Then, if present, write `meta.json` atomically to the base directory.
        if self.overlay.exists(&*META_FILEPATH).unwrap_or(false) {
            // Read meta from overlay atomically to a buffer and then write to base atomically.
            if let Ok(meta_bytes) = self.overlay.atomic_read(&*META_FILEPATH) {
                self.base.atomic_write(&*META_FILEPATH, &meta_bytes)?;
            }
        }
        Ok(())
    }
}

impl fmt::Debug for NrtDirectory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "NrtDirectory")
    }
}

impl Directory for NrtDirectory {
    fn get_file_handle(&self, path: &Path) -> Result<Arc<dyn FileHandle>, OpenReadError> {
        if self.overlay.exists(path).unwrap_or(false) {
            return self.overlay.get_file_handle(path);
        }
        self.base.get_file_handle(path)
    }

    fn open_read(&self, path: &Path) -> Result<FileSlice, OpenReadError> {
        if self.overlay.exists(path).unwrap_or(false) {
            return self.overlay.open_read(path);
        }
        self.base.open_read(path)
    }

    fn delete(&self, path: &Path) -> Result<(), DeleteError> {
        let _ = self.overlay.delete(path); // best-effort
        self.base.delete(path)
    }

    fn exists(&self, path: &Path) -> Result<bool, OpenReadError> {
        if self.overlay.exists(path).unwrap_or(false) {
            return Ok(true);
        }
        self.base.exists(path)
    }

    fn open_write(&self, path: &Path) -> Result<WritePtr, OpenWriteError> {
        {
            let mut guard = self.overlay_paths.write().unwrap();
            guard.insert(path.to_path_buf());
        }
        self.overlay.open_write(path)
    }

    fn atomic_read(&self, path: &Path) -> Result<Vec<u8>, OpenReadError> {
        if self.overlay.exists(path).unwrap_or(false) {
            return self.overlay.atomic_read(path);
        }
        self.base.atomic_read(path)
    }

    fn atomic_write(&self, path: &Path, data: &[u8]) -> io::Result<()> {
        {
            let mut guard = self.overlay_paths.write().unwrap();
            guard.insert(path.to_path_buf());
        }
        // Always write to the overlay first. We do not write meta.json to base here,
        // to ensure meta is published only after all files are persisted in sync_directory().
        self.overlay.atomic_write(path, data)?;
        Ok(())
    }

    fn acquire_lock(&self, lock: &Lock) -> Result<DirectoryLock, crate::directory::error::LockError> {
        self.base.acquire_lock(lock)
    }

    fn watch(&self, watch_callback: WatchCallback) -> crate::Result<WatchHandle> {
        // Watch meta.json changes on the base directory
        self.base.watch(watch_callback)
    }

    fn sync_directory(&self) -> io::Result<()> {
        // Best effort: persist overlay, then sync base directory
        if let Err(err) = self.persist_overlay_into_base() {
            return Err(io::Error::new(io::ErrorKind::Other, format!("{err}")));
        }
        self.base.sync_directory()
    }
}



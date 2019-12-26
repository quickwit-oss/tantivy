use crate::directory::directory::ReadOnlyDirectory;
use crate::directory::error::OpenReadError;
use crate::directory::ReadOnlySource;
use crate::error::DataCorruption;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[derive(Clone)]
struct BundleDirectory {
    source_map: Arc<HashMap<PathBuf, ReadOnlySource>>,
}

impl BundleDirectory {
    pub fn from_source(source: ReadOnlySource) -> Result<BundleDirectory, DataCorruption> {
        let mut index_offset_buf = [0u8; 8];
        let (body_idx, footer_offset) = source.split_from_end(8);
        index_offset_buf.copy_from_slice(footer_offset.as_slice());
        let offset = u64::from_le_bytes(index_offset_buf);
        let (body_source, idx_source) = body_idx.split(offset as usize);
        let idx: HashMap<PathBuf, (u64, u64)> = serde_json::from_slice(idx_source.as_slice())
            .map_err(|err| {
                let msg = format!("Failed to read index from bundle. {:?}", err);
                DataCorruption::comment_only(msg)
            })?;
        let source_map: HashMap<PathBuf, ReadOnlySource> = idx
            .into_iter()
            .map(|(path, (start, stop))| {
                let source = body_source.slice(start as usize, stop as usize);
                (path, source)
            })
            .collect();
        Ok(BundleDirectory {
            source_map: Arc::new(source_map),
        })
    }
}

impl ReadOnlyDirectory for BundleDirectory {
    fn open_read(&self, path: &Path) -> Result<ReadOnlySource, OpenReadError> {
        self.source_map
            .get(path)
            .cloned()
            .ok_or_else(|| OpenReadError::FileDoesNotExist(path.to_path_buf()))
    }

    fn exists(&self, path: &Path) -> bool {
        self.source_map.contains_key(path)
    }

    fn atomic_read(&self, path: &Path) -> Result<Vec<u8>, OpenReadError> {
        let source = self
            .source_map
            .get(path)
            .ok_or_else(|| OpenReadError::FileDoesNotExist(path.to_path_buf()))?;
        Ok(source.as_slice().to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::BundleDirectory;
    use crate::directory::{RAMDirectory, ReadOnlyDirectory, TerminatingWrite};
    use crate::Directory;
    use std::io::Write;
    use std::path::Path;

    #[test]
    fn test_bundle_directory() {
        let mut ram_directory = RAMDirectory::default();
        let test_path_atomic = Path::new("testpath_atomic");
        let test_path_wrt = Path::new("testpath_wrt");
        assert!(ram_directory
            .atomic_write(test_path_atomic, b"titi")
            .is_ok());
        {
            let mut test_wrt = ram_directory.open_write(test_path_wrt).unwrap();
            assert!(test_wrt.write_all(b"toto").is_ok());
            assert!(test_wrt.terminate().is_ok());
        }
        let mut dest_directory = RAMDirectory::default();
        let bundle_path = Path::new("bundle");
        let mut wrt = dest_directory.open_write(bundle_path).unwrap();
        assert!(ram_directory.serialize_bundle(&mut wrt).is_ok());
        assert!(wrt.terminate().is_ok());
        let source = dest_directory.open_read(bundle_path).unwrap();
        let bundle_directory = BundleDirectory::from_source(source).unwrap();
        assert_eq!(
            &bundle_directory.atomic_read(test_path_atomic).unwrap()[..],
            b"titi"
        );
        assert_eq!(
            &bundle_directory.open_read(test_path_wrt).unwrap()[..],
            b"toto"
        );
    }
}

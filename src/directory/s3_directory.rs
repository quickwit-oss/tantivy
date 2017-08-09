use common::make_io_err;
use directory::Directory;
use directory::error::{IOError, OpenWriteError, OpenReadError, DeleteError, OpenDirectoryError};
use directory::ReadOnlySource;
use directory::shared_vec_slice::SharedVecSlice;
use directory::WritePtr;
use std::collections::HashMap;
use std::convert::From;
use std::default::Default;
use std::error::Error;
use std::fmt;
use std::io;
use std::path::{Path, PathBuf};
use std::result;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::RwLock;
use rusoto_core::{DefaultCredentialsProvider, Region, default_tls_client};
use rusoto_s3::{S3, S3Client, HeadBucketRequest, GetObjectRequest};

fn get_client(region: Region) -> Result<Box<S3>, Box<Error>> {
    // TODO: handle missing creds
    let client = default_tls_client()?;
    let provider = DefaultCredentialsProvider::new()?;

    Ok(Box::new(S3Client::new(client, provider, region)))
}

#[derive(Clone)]
struct InnerDirectory {
    bucket: String,
    cache: Arc<RwLock<HashMap<PathBuf, Arc<Vec<u8>>>>>,
}

impl InnerDirectory {
    fn new(bucket: String) -> InnerDirectory {
        InnerDirectory {
            cache: Arc::new(RwLock::new(HashMap::new())),
            bucket,
        }
    }

    fn load_missing_key(&self, client: &S3, path: &Path) -> Result<(), OpenReadError> {
        let mut map = self.cache.write().map_err(|_| {
            let msg = format!(
                "Failed to acquire write lock for the \
                                            directory when trying to read {:?}",
                path
            );
            let io_err = make_io_err(msg);
            OpenReadError::IOError(IOError::with_path(path.to_owned(), io_err))
        })?;

        // TODO: this is comical and I'm more than likely over thinking it
        let key = path.as_os_str().to_os_string().into_string().map_err(|_| {
            let msg = format!("Could not build key path");
            let io_err = make_io_err(msg);
            OpenReadError::IOError(IOError::with_path(path.to_owned(), io_err))
        })?;

        let obj = client
            .get_object(&GetObjectRequest {
                bucket: self.bucket.clone(),
                key,
                ..Default::default()
            })
            .map_err(|_| {
                let msg = format!("No key found for {:?}", path);
                let io_err = make_io_err(msg);
                OpenReadError::IOError(IOError::with_path(path.to_owned(), io_err))
            })?;

        map.insert(PathBuf::from(path), Arc::new(obj.body.unwrap()));

        Ok(())
    }

    fn open_read(&self, path: &Path, client: &S3) -> result::Result<ReadOnlySource, OpenReadError> {
        debug!("Open Read {:?}", path);

        let cache = self.cache.read().map_err(|_| {
            let msg = format!(
                "Failed to acquire read lock for the \
                                            directory when trying to read {:?}",
                path
            );
            let io_err = make_io_err(msg);
            OpenReadError::IOError(IOError::with_path(path.to_owned(), io_err))
        })?;

        if !cache.contains_key(path) {
            self.load_missing_key(client, path)?;
        }

        //TODO: map_err
        let data = cache.get(path).unwrap();

        Ok(ReadOnlySource::Anonymous(SharedVecSlice::new(data.clone())))
    }

    fn exists(&self, path: &Path, client: &S3) -> bool {
        let cache_exist = self.cache
            .read()
            .expect("Failed to get read lock directory.")
            .contains_key(path);

        let mut exist = cache_exist;
        if !cache_exist {
            let r = self.load_missing_key(client, path);

            exist = r.is_ok();
        }

        exist
    }
}

/// Directory storing data in s3 bucket
///
/// The s3 bucket has a cache to limit the amount of
/// API calls
#[derive(Clone)]
pub struct S3Directory {
    /// the bucket to store files in
    bucket: String,

    /// the region the bucket exists in
    region: Region,

    /// Path in the s3 bucket to store objects
    root_path: PathBuf,

    fs: InnerDirectory,
}

impl fmt::Debug for S3Directory {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "S3Directory({:?} {} {:?})",
            self.region,
            self.bucket,
            self.root_path
        )
    }
}

impl S3Directory {
    /// Opens a S3Directory in a bucket.
    ///
    /// Returns an error if the `bucket` does not
    /// exist or if it is not a directory.
    pub fn open(
        region: String,
        bucket: String,
        directory_path: &Path,
    ) -> Result<S3Directory, OpenDirectoryError> {
        // TODO: should I use a different error type? probably

        // TODO: should I use the Rusoto Region type in the method call?
        let region = Region::from_str(&region).map_err(|_| {
            OpenDirectoryError::DoesNotExist(PathBuf::from("/bad/region"))
        })?;

        let s3 = get_client(region.clone()).map_err(|_| {
            OpenDirectoryError::DoesNotExist(PathBuf::from("/cant/s3"))
        })?;

        // does bucket exist?
        s3.head_bucket(&HeadBucketRequest { bucket: bucket.clone() })
            .map_err(|_| {
                OpenDirectoryError::DoesNotExist(PathBuf::from("/no/bucket"))
            })?;

        // TODO: how to store the client?
        // `S3` does not implement `std::marker::Sync` so it can't be on the struct
        Ok(S3Directory {
            bucket: bucket.clone(),
            region: region,
            root_path: PathBuf::from(directory_path),
            fs: InnerDirectory::new(bucket),
        })

    }

    fn get_client(&self) -> Result<Box<S3>, Box<Error>> {
        get_client(self.region.clone())
    }

    /// Joins a relative_path to the directory `root_path`
    /// to create a proper complete `filepath`.
    fn resolve_path(&self, relative_path: &Path) -> PathBuf {
        self.root_path.join(relative_path)
    }
}

impl Directory for S3Directory {
    fn open_read(&self, path: &Path) -> result::Result<ReadOnlySource, OpenReadError> {
        let s3 = self.get_client().map_err(|_| {
            let msg = format!("Could not get s3 client");
            let io_err = make_io_err(msg);
            OpenReadError::IOError(IOError::with_path(path.to_owned(), io_err))
        })?;

        self.fs.open_read(&self.resolve_path(path), s3.as_ref())
    }

    fn open_write(&mut self, path: &Path) -> Result<WritePtr, OpenWriteError> {
        unimplemented!()
    }

    fn delete(&self, path: &Path) -> result::Result<(), DeleteError> {
        unimplemented!()
    }

    fn exists(&self, path: &Path) -> bool {
        let s3 = self.get_client().expect("Failed to build s3 client");

        self.fs.exists(&self.resolve_path(path), s3.as_ref())
    }

    fn atomic_read(&self, path: &Path) -> Result<Vec<u8>, OpenReadError> {
        unimplemented!()
    }

    fn atomic_write(&mut self, path: &Path, data: &[u8]) -> io::Result<()> {
        unimplemented!()
    }

    fn box_clone(&self) -> Box<Directory> {
        Box::new(self.clone())
    }
}




#[cfg(test)]
mod tests {

    // There are more tests in directory/mod.rs
    // The following tests are specific to the S3Directory

    use super::*;

    #[test]
    fn bad_region() {
        // empty file is actually an edge case because those
        // cannot be mmapped.
        //
        // In that case the directory returns a SharedVecSlice.
        let mut s3dir = S3Directory::open(
            "us-nowhere-1".to_string(),
            "tantivy-test-bucket".to_string(),
            &PathBuf::from("/"),
        ).unwrap();
    }

    #[test]
    fn no_bucket() {

        let mut s3dir = S3Directory::open(
            "us-nowhere-1".to_string(),
            "tantivy-test-bucket-nope".to_string(),
            &PathBuf::from("/"),
        ).unwrap();
    }

    #[test]
    fn test_open_empty() {
        // empty file is actually an edge case because those
        // cannot be mmapped.
        //
        // In that case the directory returns a SharedVecSlice.
        let mut s3dir = S3Directory::open(
            "us-east-1".to_string(),
            "tantivy-test-bucket".to_string(),
            &PathBuf::from("/"),
        ).unwrap();
    }

    #[test]
    fn test_cache() {}

}

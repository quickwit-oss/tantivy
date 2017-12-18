use common::make_io_err;
use directory::Directory;
use directory::error::{IOError, OpenWriteError, OpenReadError, DeleteError, OpenDirectoryError};
use directory::ReadOnlySource;
use directory::shared_vec_slice::SharedVecSlice;
use directory::WritePtr;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::convert::From;
use std::default::Default;
use std::error::Error;
use std::fmt;
use std::io::{self, BufWriter, Cursor, Write, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::result;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::RwLock;
use rusoto_core::{DefaultCredentialsProvider, Region, default_tls_client};
use rusoto_s3::{S3, S3Client, HeadBucketRequest, GetObjectRequest, DeleteObjectRequest,
                PutObjectRequest};

/// Writer associated with the `RAMDirectory`
///
/// The Writer just writes a buffer.
///
/// # Panics
///
/// On drop, if the writer was left in a *dirty* state.
/// That is, if flush was not called after the last call
/// to write.
///
struct VecWriter {
    path: PathBuf,
    shared_directory: InnerDirectory,
    data: Cursor<Vec<u8>>,
    is_flushed: bool,
    s3: Box<S3>,
}

impl VecWriter {
    fn new(s3: Box<S3>, path_buf: PathBuf, shared_directory: InnerDirectory) -> VecWriter {
        VecWriter {
            path: path_buf,
            data: Cursor::new(Vec::new()),
            shared_directory: shared_directory,
            is_flushed: true,
            s3,
        }
    }
}

impl Drop for VecWriter {
    fn drop(&mut self) {
        if !self.is_flushed {
            panic!(
                "You forgot to flush {:?} before its writter got Drop. Do not rely on drop.",
                self.path
            )
        }
    }
}

impl Seek for VecWriter {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.data.seek(pos)
    }
}

impl Write for VecWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.is_flushed = false;
        try!(self.data.write_all(buf));
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.is_flushed = true;
        try!(self.shared_directory.write(
            self.s3.as_ref(),
            self.path.clone(),
            self.data.get_ref(),
        ));
        Ok(())
    }
}

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

    fn write(&self, client: &S3, path: PathBuf, data: &[u8]) -> io::Result<bool> {
        // TODO: this is comical and I'm more than likely over thinking it
        let key = path.as_os_str().to_os_string().into_string().map_err(|_| {
            let msg = format!("Could not build key path");
            let io_err = make_io_err(msg);
            io_err
        })?;

        let mut map = try!(self.cache.write().map_err(|_| {
            make_io_err(format!(
                "Failed to lock the directory, when trying to write {:?}",
                path
            ))
        }));

        let result = client
            .put_object(&PutObjectRequest {
                bucket: self.bucket.clone(),
                body: Some(data.to_vec()),
                key: key,
                ..Default::default()
            })
            .map_err(|a| {
                let msg = format!("Error writing for {:?}", path);
                make_io_err(msg)
            })?;


        Ok(true)
    }

    fn fetch(&self, client: &S3, path: &Path) -> Result<Arc<Vec<u8>>, OpenReadError> {
        println!("Fetch: {:?}", path);
        // TODO: this is comical and I'm more than likely over thinking it
        let key = path.as_os_str().to_os_string().into_string().map_err(|_| {
            let msg = format!("Could not build key path");
            let io_err = make_io_err(msg);
            OpenReadError::IOError(IOError::with_path(path.to_owned(), io_err))
        })?;

        let clean_key = key.trim_left_matches('/').to_string();

        let obj = client
            .get_object(&GetObjectRequest {
                bucket: self.bucket.clone(),
                key: clean_key,
                ..Default::default()
            })
            .map_err(|_| {
                let msg = format!("No key found for {:?}", path);
                let io_err = make_io_err(msg);
                OpenReadError::FileDoesNotExist(path.to_owned())
            })?;

        let mut body = obj.body.unwrap();
        let mut raw = Vec::new();
        body.read_to_end(&mut raw).unwrap();
        Ok(Arc::new(raw))
    }

    fn open_read(&self, path: &Path, client: &S3) -> result::Result<ReadOnlySource, OpenReadError> {
        debug!("Open Read {:?}", path);

        // TODO: I punted on this, since I'm switching to an inner `Directory` instance
        let mut cache = self.cache.write().map_err(|_| {
            let msg = format!(
                "Failed to acquire write lock for the \
                                            directory when trying to read {:?}",
                path
            );
            let io_err = make_io_err(msg);
            OpenReadError::IOError(IOError::with_path(path.to_owned(), io_err))
        })?;

        if !cache.contains_key(path) {
            let data = self.fetch(client, path)?;
            cache.insert(PathBuf::from(path), data);
        }

        let data = cache.get(path).ok_or_else(|| {
            let msg = format!("No file at this location {:?}", path);
            let io_err = make_io_err(msg);
            OpenReadError::IOError(IOError::with_path(path.to_owned(), io_err))
        })?;

        Ok(ReadOnlySource::Anonymous(SharedVecSlice::new(data.clone())))
    }

    fn delete(&self, path: &Path, client: &S3) -> result::Result<(), DeleteError> {
        let mut writable_map = self.cache.write().map_err(|_| {
            let msg = format!(
                "Failed to acquire write lock for the \
                                            directory when trying to delete {:?}",
                path
            );
            let io_err = make_io_err(msg);
            DeleteError::IOError(IOError::with_path(path.to_owned(), io_err))
        })?;

        // TODO: this is comical and I'm more than likely over thinking it
        let key = path.as_os_str().to_os_string().into_string().map_err(|_| {
            let msg = format!("Could not build key path");
            let io_err = make_io_err(msg);
            DeleteError::IOError(IOError::with_path(path.to_owned(), io_err))
        })?;
        let obj = client
            .delete_object(&DeleteObjectRequest {
                bucket: self.bucket.clone(),
                key,
                ..Default::default()
            })
            .map_err(|_| {
                let msg = format!("No key found for {:?}", path);
                let io_err = make_io_err(msg);
                DeleteError::IOError(IOError::with_path(path.to_owned(), io_err))
            })?;

        match writable_map.remove(path) {
            Some(_) => Ok(()),
            None => Err(DeleteError::FileDoesNotExist(PathBuf::from(path))),
        }
    }

    fn exists(&self, path: &Path, client: &S3) -> bool {
        let cache = self.cache.read().expect(
            "Failed to get read lock directory.",
        );

        let key = PathBuf::from(path);
        if cache.contains_key(&key) {
            true
        } else {
            let mut cache = self.cache.write().expect(
                "Failed to get write lock directory",
            );
            match cache.entry(key) {
                Entry::Occupied(_) => true,
                Entry::Vacant(entry) => {
                    match self.fetch(client, path) {
                        Ok(data) => {
                            entry.insert(data);
                            true
                        }
                        Err(_) => false,
                    }
                }
            }
        }
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
        let s3 = self.get_client().map_err(|_| {
            let msg = format!("Could not get s3 client");
            let io_err = make_io_err(msg);
            DeleteError::IOError(IOError::with_path(path.to_owned(), io_err))
        })?;

        self.fs.delete(&self.resolve_path(path), s3.as_ref())
    }

    fn exists(&self, path: &Path) -> bool {
        let s3 = self.get_client().expect("Failed to build s3 client");

        self.fs.exists(&self.resolve_path(path), s3.as_ref())
    }

    fn atomic_read(&self, path: &Path) -> Result<Vec<u8>, OpenReadError> {
        let read = self.open_read(path)?;
        Ok(read.as_slice().to_owned())
    }

    fn atomic_write(&mut self, path: &Path, data: &[u8]) -> io::Result<()> {
        let s3 = self.get_client().expect("Failed to build s3 client");
        let s3_2 = self.get_client().expect("Failed to build s3 client");

        let path_buf = PathBuf::from(path);
        let mut vec_writer = VecWriter::new(s3_2, path_buf.clone(), self.fs.clone());
        try!(self.fs.write(s3.as_ref(), path_buf, &Vec::new()));
        try!(vec_writer.write_all(data));
        try!(vec_writer.flush());
        Ok(())
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

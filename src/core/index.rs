use Result;
use Error;
use schema::Schema;
use std::sync::Arc;
use std::borrow::BorrowMut;
use std::fmt;
use rustc_serialize::json;
use core::SegmentId;
use directory::{Directory, MmapDirectory, RAMDirectory};
use indexer::IndexWriter;
use core::searcher::Searcher;
use std::convert::From;
use num_cpus;
use super::segment::Segment;
use core::SegmentReader;
use super::pool::Pool;
use core::SegmentMeta;
use super::pool::LeasedItem;
use std::path::Path;
use core::IndexMeta;
use core::META_FILEPATH;
use super::segment::create_segment;
use indexer::segment_updater::save_new_metas;
use directory::error::{FileError, OpenWriteError};

const NUM_SEARCHERS: usize = 12;



fn load_metas(directory: &Directory) -> Result<IndexMeta> {
    let meta_data = directory.atomic_read(&META_FILEPATH)?;
    let meta_string = String::from_utf8_lossy(&meta_data);
    json::decode(&meta_string)
        .map_err(|e| Error::CorruptedFile(META_FILEPATH.clone(), Box::new(e)))
}

/// Tantivy's Search Index
pub struct Index {
    directory: Box<Directory>,
    schema: Schema,
    searcher_pool: Arc<Pool<Searcher>>,
    opstamp: u64,
}




/// Deletes all of the document of the segment.
/// This is called when there is a merge or a rollback.
///
/// # Disclaimer
/// If deletion of a file fails (e.g. a file 
/// was read-only.), the method does not
/// fail and just logs an error when it fails.
pub fn delete_segment(directory: &Directory, segment_id: SegmentId) {
    info!("Deleting segment {:?}", segment_id);
    let segment_filepaths_res = directory.ls_starting_with(
        &*segment_id.uuid_string()
    );

    match segment_filepaths_res {
        Ok(segment_filepaths) => {
            for segment_filepath in &segment_filepaths {
                if let Err(err) = directory.delete(&segment_filepath) {
                    match err {
                        FileError::FileDoesNotExist(_) => {
                            // this is normal behavior.
                            // the position file for instance may not exists.
                        }
                        FileError::IOError(err) => {
                            error!("Failed to remove {:?} : {:?}", segment_id, err);
                        }
                    }
                }
            }
        }
        Err(_) => {
            error!("Failed to list files of segment {:?} for deletion.", segment_id.uuid_string());
        }
    }
}


impl Index {
    /// Creates a new index using the `RAMDirectory`.
    ///
    /// The index will be allocated in anonymous memory.
    /// This should only be used for unit tests.
    pub fn create_in_ram(schema: Schema) -> Index {
        let directory = Box::new(RAMDirectory::create());
        Index::from_directory(directory, schema).expect("Creating a RAMDirectory should never fail") // unwrap is ok here
    }

    /// Creates a new index in a given filepath.
    /// The index will use the `MMapDirectory`.
    ///
    /// If a previous index was in this directory, then its meta file will be destroyed.
    pub fn create(directory_path: &Path, schema: Schema) -> Result<Index> {
        let directory = MmapDirectory::open(directory_path)?;
        Index::from_directory(box directory, schema)
    }

    /// Creates a new index in a temp directory.
    ///
    /// The index will use the `MMapDirectory` in a newly created directory.
    /// The temp directory will be destroyed automatically when the `Index` object
    /// is destroyed.
    ///
    /// The temp directory is only used for testing the `MmapDirectory`.
    /// For other unit tests, prefer the `RAMDirectory`, see: `create_in_ram`.
    pub fn create_from_tempdir(schema: Schema) -> Result<Index> {
        let directory = Box::new(try!(MmapDirectory::create_from_tempdir()));
        Index::from_directory(directory, schema)
    }

    /// Creates a new index given a directory and an `IndexMeta`.
    fn create_from_metas(directory: Box<Directory>, metas: IndexMeta) -> Result<Index> {
        let schema = metas.schema.clone();
        let opstamp = metas.opstamp;
        // TODO log somethings is uncommitted is not empty.
        let index = Index {
            directory: directory,
            schema: schema,
            searcher_pool: Arc::new(Pool::new()),
            opstamp: opstamp,
        };
        try!(index.load_searchers());
        Ok(index)
    }

    /// Create a new index from a directory.
    pub fn from_directory(mut directory: Box<Directory>, schema: Schema) -> Result<Index> {
        save_new_metas(schema.clone(), 0, directory.borrow_mut())?;
        Index::create_from_metas(directory, IndexMeta::with_schema(schema))
    }

    /// Opens a new directory from an index path.
    pub fn open(directory_path: &Path) -> Result<Index> {
        let directory = try!(MmapDirectory::open(directory_path));
        let metas = try!(load_metas(&directory)); //< TODO does the directory already exists?
        Index::create_from_metas(directory.box_clone(), metas)
    }

    /// Returns the index opstamp.
    ///
    /// The opstamp is the number of documents that have been added
    /// from the beginning of time, and until the moment of the last commit.
    pub fn opstamp(&self) -> u64 {
        self.opstamp
    }

    /// Creates a multithreaded writer.
    /// Each writer produces an independent segment.
    ///
    /// # Errors
    /// If the lockfile already exists, returns `Error::FileAlreadyExists`.
    /// # Panics
    /// If the heap size per thread is too small, panics.
    pub fn writer_with_num_threads(&self,
                                   num_threads: usize,
                                   heap_size_in_bytes: usize)
                                   -> Result<IndexWriter> {
        IndexWriter::open(self, num_threads, heap_size_in_bytes)
    }


    /// Creates a multithreaded writer
    /// It just calls `writer_with_num_threads` with the number of cores as `num_threads`
    /// # Errors
    /// If the lockfile already exists, returns `Error::FileAlreadyExists`.
    /// # Panics
    /// If the heap size per thread is too small, panics.
    pub fn writer(&self, heap_size_in_bytes: usize) -> Result<IndexWriter> {
        self.writer_with_num_threads(num_cpus::get(), heap_size_in_bytes)
    }

    /// Accessor to the index schema
    ///
    /// The schema is actually cloned.
    pub fn schema(&self) -> Schema {
        self.schema.clone()
    }

    /// Returns the list of segments that are searchable
    pub fn searchable_segments(&self) -> Result<Vec<Segment>> {
        let metas = load_metas(self.directory())?; 
        let searchable_segment_ids = metas
            .committed_segments
            .iter()
            .map(|segment_meta| segment_meta.segment_id)
            .collect::<Vec<_>>();
        let commit_opstamp = metas.opstamp;
        Ok(searchable_segment_ids
            .into_iter()
            .map(|segment_id| self.segment(segment_id, commit_opstamp))
            .collect())
    }
    
    /// Remove all of the file associated with the segment.
    ///
    /// This method cannot fail. If a problem occurs,
    /// some files may end up never being removed.
    /// The error will only be logged.
    pub fn delete_segment(&self, segment_id: SegmentId) {
        delete_segment(self.directory(), segment_id); 
    }

    /// Return a segment object given a `segment_id`
    ///
    /// The segment may or may not exist.
    pub fn segment(&self, segment_id: SegmentId, opstamp: u64) -> Segment {
        create_segment(self.clone(), segment_id, opstamp)
    }

    /// Return a reference to the index directory.
    pub fn directory(&self) -> &Directory {
        &*self.directory
    }

    /// Return a mutable reference to the index directory.
    pub fn directory_mut(&mut self) -> &mut Directory {
        &mut *self.directory
    }

    /// Reads the meta.json and returns the list of
    /// committed segments.
    pub fn committed_segments(&self) -> Result<Vec<SegmentMeta>> {
        
        Ok(load_metas(self.directory())?.committed_segments)
    }
    
    /// Returns the list of segment ids that are searchable.
    pub fn searchable_segment_ids(&self) -> Result<Vec<SegmentId>> {
        Ok(load_metas(self.directory())?
            .committed_segments
            .iter()
            .map(|segment_meta| segment_meta.segment_id)
            .collect())           
    }

    /// Creates a new segment.
    pub fn new_segment(&self, opstamp: u64) -> Segment {
        self.segment(SegmentId::generate_random(), opstamp)
    }

    /// Creates a new generation of searchers after
    /// a change of the set of searchable indexes.
    ///
    /// This needs to be called when a new segment has been
    /// published or after a merge.
    pub fn load_searchers(&self) -> Result<()> {
        let searchable_segments = self.searchable_segments()?;
        let mut searchers = Vec::new();
        for _ in 0..NUM_SEARCHERS {
            let searchable_segments_clone = searchable_segments.clone();
            let segment_readers: Vec<SegmentReader> = try!(searchable_segments_clone.into_iter()
                .map(SegmentReader::open)
                .collect());
            let searcher = Searcher::from(segment_readers);
            searchers.push(searcher);
        }
        self.searcher_pool.publish_new_generation(searchers);
        Ok(())
    }

    /// Returns a searcher
    ///
    /// This method should be called every single time a search
    /// query is performed.
    /// The searchers are taken from a pool of `NUM_SEARCHERS` searchers.
    /// If no searcher is available
    /// this may block.
    ///
    /// The same searcher must be used for a given query, as it ensures
    /// the use of a consistent segment set.
    pub fn searcher(&self) -> LeasedItem<Searcher> {
        self.searcher_pool.acquire()
    }
}


impl fmt::Debug for Index {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Index({:?})", self.directory)
    }
}

impl Clone for Index {
    fn clone(&self) -> Index {
        Index {
            directory: self.directory.box_clone(),
            schema: self.schema.clone(),
            searcher_pool: self.searcher_pool.clone(),
            opstamp: self.opstamp,
        }
    }
}

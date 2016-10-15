use Result;
use Error;
use std::path::Path;
use schema::Schema;
use std::sync::Arc;
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
use super::pool::LeasedItem;
use indexer::SegmentManager;
use core::IndexMeta;
use core::META_FILEPATH;
use super::segment::create_segment;

const NUM_SEARCHERS: usize = 12; 

/// Accessor to the index segment manager
///
/// This method is not part of tantivy's public API 
pub fn get_segment_manager(index: &Index) -> Arc<SegmentManager> {
    index.segment_manager.clone()
}
    

fn load_metas(directory: &Directory) -> Result<IndexMeta> {
    let meta_file = try!(directory.open_read(&META_FILEPATH));
    let meta_content = String::from_utf8_lossy(meta_file.as_slice());
    let loaded_meta = try!(
        json::decode(&meta_content)
            .map_err(|e| Error::CorruptedFile(META_FILEPATH.clone(), Box::new(e)))
    );
    Ok(loaded_meta)
}

// pub fn set_metas(index: &mut Index, docstamp: u64) {
//     index.docstamp = docstamp;
// }

/// Tantivy's Search Index
pub struct Index {
    segment_manager: Arc<SegmentManager>,
    
    directory: Box<Directory>,
    schema: Schema,
    searcher_pool: Arc<Pool<Searcher>>,
    docstamp: u64,
    
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
    ///
    /// The index will use the `MMapDirectory`.
    pub fn create(directory_path: &Path, schema: Schema) -> Result<Index> {
        let directory = Box::new(try!(MmapDirectory::open(directory_path)));
        Index::from_directory(directory, schema)
    }

    /// Creates a new index in a temp directory.
    ///
    /// The index will use the `MMapDirectory` in a newly created directory.
    /// The temp directory will be destroyed automatically when the Index object
    /// is destroyed.
    ///
    /// The temp directory is only used for testing the `MmapDirectory`.
    /// For other unit tests, prefer the `RAMDirectory`, see: `create_in_ram`.
    pub fn create_from_tempdir(schema: Schema) -> Result<Index> {
        let directory = Box::new(try!(MmapDirectory::create_from_tempdir()));
        Index::from_directory(directory, schema)
    }
    
    /// Creates a new index given a directory and an IndexMeta.
    fn create_from_metas(directory: Box<Directory>, metas: IndexMeta) -> Result<Index> {
        let schema = metas.schema.clone();
        let docstamp = metas.docstamp;
        let committed_segments = metas.committed_segments;
        // TODO log somethings is uncommitted is not empty.
        let index = Index {
            segment_manager: Arc::new(SegmentManager::from_segments(committed_segments)),
            directory: directory,
            schema: schema,
            searcher_pool: Arc::new(Pool::new()),
            docstamp: docstamp,
        };
        try!(index.load_searchers());
        Ok(index)
    }
    
    /// Opens a new directory from a directory.
    pub fn from_directory(directory: Box<Directory>, schema: Schema) -> Result<Index> {
        Index::create_from_metas(
            directory,
            IndexMeta::with_schema(schema)
        )
    }

    /// Opens a new directory from an index path.
    pub fn open(directory_path: &Path) -> Result<Index> {
        let directory = try!(MmapDirectory::open(directory_path));
        let metas = try!(load_metas(&directory)); //< TODO does the directory already exists?
        Index::create_from_metas(directory.box_clone(), metas)
    }
    
    /// Returns the index docstamp.
    ///
    /// The docstamp is the number of documents that have been added
    /// from the beginning of time, and until the moment of the last commit.
    pub fn docstamp(&self,) -> u64 {
        self.docstamp
    }
    
    /// Creates a multithreaded writer.
    /// Each writer produces an independant segment.
    pub fn writer_with_num_threads(&self, num_threads: usize, heap_size_in_bytes: usize) -> Result<IndexWriter> {
        IndexWriter::open(self, num_threads, heap_size_in_bytes)
    }
    
    
    /// Creates a multithreaded writer
    /// It just calls `writer_with_num_threads` with the number of core as `num_threads` 
    pub fn writer(&self, heap_size_in_bytes: usize) -> Result<IndexWriter> {
        self.writer_with_num_threads(num_cpus::get(), heap_size_in_bytes)
    }
    
    /// Accessor to the index schema
    ///
    /// The schema is actually cloned.
    pub fn schema(&self,) -> Schema {
        self.schema.clone()
    }

    /// Returns the list of segments that are searchable
    pub fn searchable_segments(&self,) -> Vec<Segment> {
        self.searchable_segment_ids()
            .into_iter()
            .map(|segment_id| self.segment(segment_id))
            .collect()
    }

    /// Remove all of the file associated with the segment.
    /// 
    /// This method cannot fail. If a problem occurs,
    /// some files may end up never being removed.
    /// The error will only be logged. 
    pub fn delete_segment(&self, segment_id: SegmentId) {
        self.segment(segment_id).delete();
    }
    
    /// Return a segment object given a segment_id
    ///
    /// The segment may or may not exist.
    pub fn segment(&self, segment_id: SegmentId) -> Segment {
        create_segment(self.clone(), segment_id)
    }
    
    /// Return a reference to the index directory.
    pub fn directory(&self,) -> &Directory {
        &*self.directory
    }
    
    /// Return a mutable reference to the index directory.
    pub fn directory_mut(&mut self,) -> &mut Directory {
        &mut *self.directory
    }
    
    /// Returns the list of segment ids that are searchable.
    fn searchable_segment_ids(&self,) -> Vec<SegmentId> {
        self.segment_manager.committed_segments()
    }
    
    /// Creates a new segment.
    pub fn new_segment(&self,) -> Segment {
        self.segment(SegmentId::generate_random())
    }
    
    /// Creates a new generation of searchers after 
    /// a change of the set of searchable indexes.
    ///
    /// This needs to be called when a new segment has been
    /// published or after a merge.
    pub fn load_searchers(&self,) -> Result<()>{
        let searchable_segments = self.searchable_segments();
        let mut searchers = Vec::new();
        for _ in 0..NUM_SEARCHERS {
            let searchable_segments_clone = searchable_segments.clone();
            let segment_readers: Vec<SegmentReader> = try!(
                    searchable_segments_clone
                        .into_iter()
                        .map(SegmentReader::open)
                        .collect()
            );
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
    /// The searcher are taken from a pool of `NUM_SEARCHERS` searchers.
    /// If no searcher is available
    /// it may block.
    ///
    /// The same searcher must be used for a given query, as it ensures 
    /// the use of a consistent segment set. 
    pub fn searcher(&self,) -> LeasedItem<Searcher> {
        self.searcher_pool.acquire()
    }

}


impl fmt::Debug for Index {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
       write!(f, "Index({:?})", self.directory)
   }
}

impl Clone for Index {
    fn clone(&self,) -> Index {
        Index {
            segment_manager: self.segment_manager.clone(),
            
            directory: self.directory.box_clone(),
            schema: self.schema.clone(),
            searcher_pool: self.searcher_pool.clone(),
            docstamp: self.docstamp,
        }
    }
}

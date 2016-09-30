use Result;
use Error;
use std::path::{PathBuf, Path};
use schema::Schema;
use std::io::Write;
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
use core::SegmentMeta;
use indexer::SegmentManager;

const NUM_SEARCHERS: usize = 12; 


pub fn get_segment_manager(index: &Index) -> Arc<SegmentManager> {
    index.segment_manager.clone()
}

/// MetaInformation about the `Index`.
/// 
/// This object is serialized on disk in the `meta.json` file.
/// It keeps information about 
/// * the searchable segments,
/// * the index docstamp
/// * the schema
///
#[derive(Clone,Debug,RustcDecodable,RustcEncodable)]
pub struct IndexMeta {
    committed_segments: Vec<SegmentMeta>,
    uncommitted_segments: Vec<SegmentMeta>,
    schema: Schema,
    docstamp: u64,
}

impl IndexMeta {
    fn with_schema(schema: Schema) -> IndexMeta {
        IndexMeta {
            committed_segments: Vec::new(),
            uncommitted_segments: Vec::new(),
            schema: schema,
            docstamp: 0u64,
        }
    }
}

lazy_static! {
    static ref META_FILEPATH: PathBuf = PathBuf::from("meta.json");
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

pub fn commit(index: &mut Index, docstamp: u64) -> Result<()> {
    index.docstamp = docstamp;
    try!(index.segment_manager.commit());
    Ok(())
}
        

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
        let mut index = try!(Index::create_from_metas(directory, IndexMeta::with_schema(schema)));
        try!(index.save_metas());
        Ok(index)
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
    pub fn searchable_segments(&self,) -> Result<Vec<Segment>> {
        let segment_ids = try!(self.searchable_segment_ids());
        Ok(
            segment_ids
            .into_iter()
            .map(|segment_id| self.segment(segment_id))
            .collect()
        )
    }
    
    /// Return a segment object given a segment_id
    ///
    /// The segment may or may not exist.
    fn segment(&self, segment_id: SegmentId) -> Segment {
        Segment::new(self.clone(), segment_id)
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
    fn searchable_segment_ids(&self,) -> Result<Vec<SegmentId>> {
        self.segment_manager.committed_segments()
    }
    
    /// Creates a new segment.
    pub fn new_segment(&self,) -> Segment {
        self.segment(SegmentId::generate_random())
    }
    
    fn create_metas(&self,) -> Result<IndexMeta> {
        let (committed_segments, uncommitted_segments) = try!(self.segment_manager.segment_metas());
        Ok(IndexMeta {
            committed_segments: committed_segments,
            uncommitted_segments: uncommitted_segments,
            schema: self.schema.clone(),
            docstamp: self.docstamp, 
        })
    }
    
    /// Save the index meta file.
    /// This operation is atomic :
    /// Either
    //  - it fails, in which case an error is returned,
    /// and the `meta.json` remains untouched, 
    /// - it success, and `meta.json` is written 
    /// and flushed.
    pub fn save_metas(&mut self,) -> Result<()> {
        let metas = try!(self.create_metas());
        let mut w = Vec::new();
        try!(write!(&mut w, "{}\n", json::as_pretty_json(&metas)));
        self.directory
            .atomic_write(&META_FILEPATH, &w[..])
            .map_err(From::from)
    }
    
    /// Creates a new generation of searchers after 
    /// a change of the set of searchable indexes.
    ///
    /// This needs to be called when a new segment has been
    /// published or after a merge.
    pub fn load_searchers(&self,) -> Result<()>{
        let res_searchers: Result<Vec<Searcher>> = (0..NUM_SEARCHERS)
            .map(|_| {
                let segments: Vec<Segment> = try!(self.searchable_segments());
                let segment_readers: Vec<SegmentReader> = try!(
                    segments
                        .into_iter()
                        .map(SegmentReader::open)
                        .collect()
                );
                Ok(Searcher::from(segment_readers))
            })
            .collect();
        let searchers = try!(res_searchers);
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

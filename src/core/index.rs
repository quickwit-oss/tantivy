use Result;
use Error;
use std::path::{PathBuf, Path};
use schema::Schema;
use std::io::Write;
use std::sync::{Arc, RwLock};
use std::fmt;
use rustc_serialize::json;
use core::SegmentId;
use directory::{Directory, MmapDirectory, RAMDirectory};
use indexer::IndexWriter;
use core::searcher::Searcher;
use std::convert::From;
use num_cpus;
use std::collections::HashSet;
use super::segment::Segment;
use core::SegmentReader;
use super::pool::Pool;
use super::pool::LeasedItem;

#[derive(Clone,Debug,RustcDecodable,RustcEncodable)]
pub struct IndexMeta {
    segments: Vec<SegmentId>,
    schema: Schema,
    docstamp: u64,
}

impl IndexMeta {
    fn with_schema(schema: Schema) -> IndexMeta {
        IndexMeta {
            segments: Vec::new(),
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


pub struct Index {
    metas: Arc<RwLock<IndexMeta>>,
    directory: Box<Directory>,
    schema: Schema,
    searcher_pool: Arc<Pool<Searcher>>,
}

impl Index {

    pub fn create_in_ram(schema: Schema) -> Index {
        let directory = Box::new(RAMDirectory::create());
        Index::from_directory(directory, schema).expect("Creating a RAMDirectory should never fail") // unwrap is ok here 
    }

    pub fn create(directory_path: &Path, schema: Schema) -> Result<Index> {
        let directory = Box::new(try!(MmapDirectory::open(directory_path)));
        Index::from_directory(directory, schema)
    }

    pub fn create_from_tempdir(schema: Schema) -> Result<Index> {
        let directory = Box::new(try!(MmapDirectory::create_from_tempdir()));
        Index::from_directory(directory, schema)
    }

    fn create_from_metas(directory: Box<Directory>, metas: IndexMeta) -> Result<Index> {
        let schema = metas.schema.clone();
        let index = Index {
            directory: directory,
            metas: Arc::new(RwLock::new(metas)),
            schema: schema,
            searcher_pool: Arc::new(Pool::new()),
        };
        try!(index.load_searchers());
        Ok(index)
    }

    pub fn from_directory(directory: Box<Directory>, schema: Schema) -> Result<Index> {
        let mut index = try!(Index::create_from_metas(directory, IndexMeta::with_schema(schema)));
        try!(index.save_metas());
        Ok(index)
    }

    pub fn open(directory_path: &Path) -> Result<Index> {
        let directory = try!(MmapDirectory::open(directory_path));
        let metas = try!(load_metas(&directory)); //< TODO does the directory already exists?
        Index::create_from_metas(directory.box_clone(), metas)
    }

    pub fn docstamp(&self,) -> Result<u64> {
        self.metas
            .read()
            .map(|metas| metas.docstamp)
            .map_err(From::from)
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

    pub fn schema(&self,) -> Schema {
        self.schema.clone()
    }

    /// Marks the segment as published.
    // TODO find a rusty way to hide that, while keeping
    // it visible for IndexWriters.
    pub fn publish_segments(&mut self,
            segment_ids: &[SegmentId],
            docstamp: u64) -> Result<()> {
        {
            let mut meta_write = try!(self.metas.write());
            meta_write.segments.extend(segment_ids);
            meta_write.docstamp = docstamp;
        }
        try!(self.save_metas());
        try!(self.load_searchers());
        Ok(())
    }

    pub fn publish_merge_segment(&mut self, segment_merged_ids: HashSet<SegmentId>, merged_segment_id: SegmentId) -> Result<()> {
        {
            let mut meta_write = try!(self.metas.write());
            let mut new_segment_ids: Vec<SegmentId> = meta_write
                .segments
                .iter()
                .filter(|&segment_id| !segment_merged_ids.contains(segment_id))
                .cloned()
                .collect();
            new_segment_ids.push(merged_segment_id);
            meta_write.segments = new_segment_ids;
        }
        try!(self.save_metas());
        try!(self.load_searchers());
        Ok(())
    }

    pub fn segments(&self,) -> Result<Vec<Segment>> {
        let segment_ids = try!(self.segment_ids());
        Ok(
            segment_ids
            .into_iter()
            .map(|segment_id| self.segment(segment_id))
            .collect()
        )
            
    }

    pub fn segment(&self, segment_id: SegmentId) -> Segment {
        Segment::new(self.clone(), segment_id)
    }

    pub fn directory(&self,) -> &Directory {
        &*self.directory
    }

    pub fn directory_mut(&mut self,) -> &mut Directory {
        &mut *self.directory
    }

    fn segment_ids(&self,) -> Result<Vec<SegmentId>> {
        self.metas.read()
        .map_err(From::from)
        .map(|meta_read| {
            meta_read
            .segments
            .iter()
            .cloned()
            .collect()
        })
            
    }

    pub fn new_segment(&self,) -> Segment {
        self.segment(SegmentId::new())
    }
    
    pub fn save_metas(&mut self,) -> Result<()> {
        let mut w = Vec::new();
        {
            let metas_lock = try!(self.metas.read());
            try!(write!(&mut w, "{}\n", json::as_pretty_json(&*metas_lock)));
        };
        self.directory
            .atomic_write(&META_FILEPATH, &w[..])
            .map_err(From::from)
    }

    pub fn load_searchers(&self,) -> Result<()>{
        let res_searchers: Result<Vec<Searcher>> = (0..12)
            .map(|_| {
                let segments: Vec<Segment> = try!(self.segments());
                let segment_readers: Vec<SegmentReader> = try!(
                    segments
                        .into_iter()
                        .map(SegmentReader::open)
                        .collect()
                );
                Ok(Searcher::from_readers(segment_readers))
            })
            .collect();
        let searchers = try!(res_searchers);
        self.searcher_pool.publish_new_generation(searchers);
        Ok(())
    }

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
            metas: self.metas.clone(),
            directory: self.directory.box_clone(),
            schema: self.schema.clone(),
            searcher_pool: self.searcher_pool.clone(),
        }
    }
}
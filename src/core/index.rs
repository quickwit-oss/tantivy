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

impl fmt::Debug for Index {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
       write!(f, "Index({:?})", self.directory)
   }
}

pub struct Index {
    metas: Arc<RwLock<IndexMeta>>,
    directory: Box<Directory>,
    schema: Schema,
}

impl Clone for Index {
    fn clone(&self,) -> Index {
        Index {
            metas: self.metas.clone(),
            directory: self.directory.box_clone(),
            schema: self.schema.clone(),
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


impl Index {

    pub fn create_in_ram(schema: Schema) -> Index {
        let directory = Box::new(RAMDirectory::create());
        Index::from_directory(directory, schema)
    }

    pub fn create(directory_path: &Path, schema: Schema) -> Result<Index> {
        let directory = Box::new(try!(MmapDirectory::open(directory_path)));
        Ok(Index::from_directory(directory, schema))
    }

    pub fn create_from_tempdir(schema: Schema) -> Result<Index> {
        let directory = Box::new(try!(MmapDirectory::create_from_tempdir()));
        Ok(Index::from_directory(directory, schema))
    }

    pub fn open(directory_path: &Path) -> Result<Index> {
        let directory = try!(MmapDirectory::open(directory_path));
        let metas = try!(load_metas(&directory)); //< TODO does the directory already exists?
        let schema = metas.schema.clone();
        let locked_metas = Arc::new(RwLock::new(metas));
        Ok(Index {
            directory: Box::new(directory),
            metas: locked_metas,
            schema: schema,
        })
    }

    pub fn docstamp(&self,) -> Result<u64> {
        self.metas
            .read()
            .map(|metas| metas.docstamp)
            .map_err(From::from)
    }
    
    /// Creates a multithreaded writer.
    /// Each writer produces an independant segment.
    pub fn writer_with_num_threads(&self, num_threads: usize) -> Result<IndexWriter> {
        IndexWriter::open(self, num_threads)
    }
    
    
    /// Creates a multithreaded writer
    /// It just calls `writer_with_num_threads` with the number of core as `num_threads` 
    pub fn writer(&self,) -> Result<IndexWriter> {
        self.writer_with_num_threads(num_cpus::get())
    }

    pub fn searcher(&self,) -> Result<Searcher> {
        Searcher::for_index(self.clone())
    }
    
    pub fn from_directory(directory: Box<Directory>, schema: Schema) -> Index {
        Index {
            metas: Arc::new(RwLock::new(IndexMeta::with_schema(schema.clone()))),
            directory: directory,
            schema: schema,
        }
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
        Ok(())
    }

    pub fn segments(&self,) -> Vec<Segment> {
        self.segment_ids()
            .into_iter()
            .map(|segment_id| self.segment(segment_id))
            .collect()
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

    fn segment_ids(&self,) -> Vec<SegmentId> {
        self.metas
            .read()
            .unwrap()
            .segments
            .iter()
            .cloned()
            .collect()
    }

    pub fn new_segment(&self,) -> Segment {
        self.segment(SegmentId::new())
    }
    
    pub fn save_metas(&mut self,) -> Result<()> {
        let mut w = Vec::new();
        {
            let metas_lock = self.metas.read().unwrap();
            try!(write!(&mut w, "{}\n", json::as_pretty_json(&*metas_lock)));
        };
        self.directory
            .atomic_write(&META_FILEPATH, &w[..])
            .map_err(From::from)
    }
}




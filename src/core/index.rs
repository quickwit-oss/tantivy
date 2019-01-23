use super::pool::LeasedItem;
use super::pool::Pool;
use super::segment::create_segment;
use super::segment::Segment;
use core::searcher::Searcher;
use core::Executor;
use core::IndexMeta;
use core::SegmentId;
use core::SegmentMeta;
use core::SegmentReader;
use core::META_FILEPATH;
use directory::ManagedDirectory;
#[cfg(feature = "mmap")]
use directory::MmapDirectory;
use directory::{Directory, RAMDirectory};
use error::DataCorruption;
use error::TantivyError;
use indexer::index_writer::open_index_writer;
use indexer::index_writer::HEAP_SIZE_MIN;
use indexer::segment_updater::save_new_metas;
use indexer::LockType;
use num_cpus;
use schema::Field;
use schema::FieldType;
use schema::Schema;
use serde_json;
use std::borrow::BorrowMut;
use std::fmt;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokenizer::BoxedTokenizer;
use tokenizer::TokenizerManager;
use IndexWriter;
use Result;

fn load_metas(directory: &Directory) -> Result<IndexMeta> {
    let meta_data = directory.atomic_read(&META_FILEPATH)?;
    let meta_string = String::from_utf8_lossy(&meta_data);
    serde_json::from_str(&meta_string)
        .map_err(|e| {
            DataCorruption::new(
                META_FILEPATH.clone(),
                format!("Meta file cannot be deserialized. {:?}.", e),
            )
        })
        .map_err(From::from)
}

/// Search Index
pub struct Index {
    directory: ManagedDirectory,
    schema: Schema,
    num_searchers: Arc<AtomicUsize>,
    searcher_pool: Arc<Pool<Searcher>>,
    executor: Arc<Executor>,
    tokenizers: TokenizerManager,
}

impl Index {
    /// Examines the director to see if it contains an index
    pub fn exists<Dir: Directory>(dir: &Dir) -> bool {
        dir.exists(&META_FILEPATH)
    }

    /// Accessor to the search executor.
    ///
    /// This pool is used by default when calling `searcher.search(...)`
    /// to perform search on the individual segments.
    ///
    /// By default the executor is single thread, and simply runs in the calling thread.
    pub fn search_executor(&self) -> &Executor {
        self.executor.as_ref()
    }

    /// Replace the default single thread search executor pool
    /// by a thread pool with a given number of threads.
    pub fn set_multithread_executor(&mut self, num_threads: usize) {
        self.executor = Arc::new(Executor::multi_thread(num_threads, "thrd-tantivy-search-"));
    }

    /// Replace the default single thread search executor pool
    /// by a thread pool with a given number of threads.
    pub fn set_default_multithread_executor(&mut self) {
        let default_num_threads = num_cpus::get();
        self.set_multithread_executor(default_num_threads);
    }

    /// Creates a new index using the `RAMDirectory`.
    ///
    /// The index will be allocated in anonymous memory.
    /// This should only be used for unit tests.
    pub fn create_in_ram(schema: Schema) -> Index {
        let ram_directory = RAMDirectory::create();
        Index::create(ram_directory, schema).expect("Creating a RAMDirectory should never fail")
    }

    /// Creates a new index in a given filepath.
    /// The index will use the `MMapDirectory`.
    ///
    /// If a previous index was in this directory, then its meta file will be destroyed.
    #[cfg(feature = "mmap")]
    pub fn create_in_dir<P: AsRef<Path>>(directory_path: P, schema: Schema) -> Result<Index> {
        let mmap_directory = MmapDirectory::open(directory_path)?;
        if Index::exists(&mmap_directory) {
            return Err(TantivyError::IndexAlreadyExists);
        }

        Index::create(mmap_directory, schema)
    }

    /// Opens or creates a new index in the provided directory
    #[cfg(feature = "mmap")]
    pub fn open_or_create<Dir: Directory>(dir: Dir, schema: Schema) -> Result<Index> {
        if Index::exists(&dir) {
            let index = Index::open(dir)?;
            if index.schema() == schema {
                Ok(index)
            } else {
                Err(TantivyError::SchemaError(
                    "An index exists but the schema does not match.".to_string(),
                ))
            }
        } else {
            Index::create(dir, schema)
        }
    }

    /// Creates a new index in a temp directory.
    ///
    /// The index will use the `MMapDirectory` in a newly created directory.
    /// The temp directory will be destroyed automatically when the `Index` object
    /// is destroyed.
    ///
    /// The temp directory is only used for testing the `MmapDirectory`.
    /// For other unit tests, prefer the `RAMDirectory`, see: `create_in_ram`.
    #[cfg(feature = "mmap")]
    pub fn create_from_tempdir(schema: Schema) -> Result<Index> {
        let mmap_directory = MmapDirectory::create_from_tempdir()?;
        Index::create(mmap_directory, schema)
    }

    /// Creates a new index given an implementation of the trait `Directory`
    pub fn create<Dir: Directory>(dir: Dir, schema: Schema) -> Result<Index> {
        let directory = ManagedDirectory::wrap(dir)?;
        Index::from_directory(directory, schema)
    }

    /// Create a new index from a directory.
    ///
    /// This will overwrite existing meta.json
    fn from_directory(mut directory: ManagedDirectory, schema: Schema) -> Result<Index> {
        save_new_metas(schema.clone(), directory.borrow_mut())?;
        let metas = IndexMeta::with_schema(schema);
        Index::create_from_metas(directory, &metas)
    }

    /// Creates a new index given a directory and an `IndexMeta`.
    fn create_from_metas(directory: ManagedDirectory, metas: &IndexMeta) -> Result<Index> {
        let schema = metas.schema.clone();
        let n_cpus = num_cpus::get();
        let index = Index {
            directory,
            schema,
            num_searchers: Arc::new(AtomicUsize::new(n_cpus)),
            searcher_pool: Arc::new(Pool::new()),
            tokenizers: TokenizerManager::default(),
            executor: Arc::new(Executor::single_thread()),
        };
        index.load_searchers()?;
        Ok(index)
    }

    /// Accessor for the tokenizer manager.
    pub fn tokenizers(&self) -> &TokenizerManager {
        &self.tokenizers
    }

    /// Helper to access the tokenizer associated to a specific field.
    pub fn tokenizer_for_field(&self, field: Field) -> Result<Box<BoxedTokenizer>> {
        let field_entry = self.schema.get_field_entry(field);
        let field_type = field_entry.field_type();
        let tokenizer_manager: &TokenizerManager = self.tokenizers();
        let tokenizer_name_opt: Option<Box<BoxedTokenizer>> = match field_type {
            FieldType::Str(text_options) => text_options
                .get_indexing_options()
                .map(|text_indexing_options| text_indexing_options.tokenizer().to_string())
                .and_then(|tokenizer_name| tokenizer_manager.get(&tokenizer_name)),
            _ => None,
        };
        match tokenizer_name_opt {
            Some(tokenizer) => Ok(tokenizer),
            None => Err(TantivyError::SchemaError(format!(
                "{:?} is not a text field.",
                field_entry.name()
            ))),
        }
    }

    /// Opens a new directory from an index path.
    #[cfg(feature = "mmap")]
    pub fn open_in_dir<P: AsRef<Path>>(directory_path: P) -> Result<Index> {
        let mmap_directory = MmapDirectory::open(directory_path)?;
        Index::open(mmap_directory)
    }

    /// Open the index using the provided directory
    pub fn open<D: Directory>(directory: D) -> Result<Index> {
        let directory = ManagedDirectory::wrap(directory)?;
        let metas = load_metas(&directory)?;
        Index::create_from_metas(directory, &metas)
    }

    /// Reads the index meta file from the directory.
    pub fn load_metas(&self) -> Result<IndexMeta> {
        load_metas(self.directory())
    }

    /// Open a new index writer. Attempts to acquire a lockfile.
    ///
    /// The lockfile should be deleted on drop, but it is possible
    /// that due to a panic or other error, a stale lockfile will be
    /// left in the index directory. If you are sure that no other
    /// `IndexWriter` on the system is accessing the index directory,
    /// it is safe to manually delete the lockfile.
    ///
    /// - `num_threads` defines the number of indexing workers that
    /// should work at the same time.
    ///
    /// - `overall_heap_size_in_bytes` sets the amount of memory
    /// allocated for all indexing thread.
    /// Each thread will receive a budget of  `overall_heap_size_in_bytes / num_threads`.
    ///
    /// # Errors
    /// If the lockfile already exists, returns `Error::FileAlreadyExists`.
    /// # Panics
    /// If the heap size per thread is too small, panics.
    pub fn writer_with_num_threads(
        &self,
        num_threads: usize,
        overall_heap_size_in_bytes: usize,
    ) -> Result<IndexWriter> {
        let directory_lock = LockType::IndexWriterLock.acquire_lock(&self.directory)?;
        let heap_size_in_bytes_per_thread = overall_heap_size_in_bytes / num_threads;
        open_index_writer(
            self,
            num_threads,
            heap_size_in_bytes_per_thread,
            directory_lock,
        )
    }

    /// Creates a multithreaded writer
    ///
    /// Tantivy will automatically define the number of threads to use.
    /// `overall_heap_size_in_bytes` is the total target memory usage that will be split
    /// between a given number of threads.
    ///
    /// # Errors
    /// If the lockfile already exists, returns `Error::FileAlreadyExists`.
    /// # Panics
    /// If the heap size per thread is too small, panics.
    pub fn writer(&self, overall_heap_size_in_bytes: usize) -> Result<IndexWriter> {
        let mut num_threads = num_cpus::get();
        let heap_size_in_bytes_per_thread = overall_heap_size_in_bytes / num_threads;
        if heap_size_in_bytes_per_thread < HEAP_SIZE_MIN {
            num_threads = (overall_heap_size_in_bytes / HEAP_SIZE_MIN).max(1);
        }
        self.writer_with_num_threads(num_threads, overall_heap_size_in_bytes)
    }

    /// Accessor to the index schema
    ///
    /// The schema is actually cloned.
    pub fn schema(&self) -> Schema {
        self.schema.clone()
    }

    /// Returns the list of segments that are searchable
    pub fn searchable_segments(&self) -> Result<Vec<Segment>> {
        Ok(self
            .searchable_segment_metas()?
            .into_iter()
            .map(|segment_meta| self.segment(segment_meta))
            .collect())
    }

    #[doc(hidden)]
    pub fn segment(&self, segment_meta: SegmentMeta) -> Segment {
        create_segment(self.clone(), segment_meta)
    }

    /// Creates a new segment.
    pub fn new_segment(&self) -> Segment {
        let segment_meta = SegmentMeta::new(SegmentId::generate_random(), 0);
        self.segment(segment_meta)
    }

    /// Return a reference to the index directory.
    pub fn directory(&self) -> &ManagedDirectory {
        &self.directory
    }

    /// Return a mutable reference to the index directory.
    pub fn directory_mut(&mut self) -> &mut ManagedDirectory {
        &mut self.directory
    }

    /// Reads the meta.json and returns the list of
    /// `SegmentMeta` from the last commit.
    pub fn searchable_segment_metas(&self) -> Result<Vec<SegmentMeta>> {
        Ok(self.load_metas()?.segments)
    }

    /// Returns the list of segment ids that are searchable.
    pub fn searchable_segment_ids(&self) -> Result<Vec<SegmentId>> {
        Ok(self
            .searchable_segment_metas()?
            .iter()
            .map(|segment_meta| segment_meta.id())
            .collect())
    }

    /// Sets the number of searchers to use
    ///
    /// Only works after the next call to `load_searchers`
    pub fn set_num_searchers(&mut self, num_searchers: usize) {
        self.num_searchers.store(num_searchers, Ordering::Release);
    }

    /// Update searchers so that they reflect the state of the last
    /// `.commit()`.
    ///
    /// If indexing happens in the same process as searching,
    /// you most likely want to call `.load_searchers()` right after each
    /// successful call to `.commit()`.
    ///
    /// If indexing and searching happen in different processes, the way to
    /// get the freshest `index` at all time, is to watch `meta.json` and
    /// call `load_searchers` whenever a changes happen.
    pub fn load_searchers(&self) -> Result<()> {
        let _meta_lock = LockType::MetaLock.acquire_lock(self.directory())?;
        let searchable_segments = self.searchable_segments()?;
        let segment_readers: Vec<SegmentReader> = searchable_segments
            .iter()
            .map(SegmentReader::open)
            .collect::<Result<_>>()?;
        let schema = self.schema();
        let num_searchers: usize = self.num_searchers.load(Ordering::Acquire);
        let searchers = (0..num_searchers)
            .map(|_| Searcher::new(schema.clone(), self.clone(), segment_readers.clone()))
            .collect();
        self.searcher_pool.publish_new_generation(searchers);
        Ok(())
    }

    /// Returns a searcher
    ///
    /// This method should be called every single time a search
    /// query is performed.
    /// The searchers are taken from a pool of `num_searchers` searchers.
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
            directory: self.directory.clone(),
            schema: self.schema.clone(),
            num_searchers: Arc::clone(&self.num_searchers),
            searcher_pool: Arc::clone(&self.searcher_pool),
            tokenizers: self.tokenizers.clone(),
            executor: self.executor.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use directory::RAMDirectory;
    use schema::{Schema, INT_INDEXED, TEXT};
    use Index;

    #[test]
    fn test_indexer_for_field() {
        let mut schema_builder = Schema::builder();
        let num_likes_field = schema_builder.add_u64_field("num_likes", INT_INDEXED);
        let body_field = schema_builder.add_text_field("body", TEXT);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        assert!(index.tokenizer_for_field(body_field).is_ok());
        assert_eq!(
            format!("{:?}", index.tokenizer_for_field(num_likes_field).err()),
            "Some(SchemaError(\"\\\"num_likes\\\" is not a text field.\"))"
        );
    }

    #[test]
    fn test_index_exists() {
        let directory = RAMDirectory::create();
        assert!(!Index::exists(&directory));
        assert!(Index::create(directory.clone(), throw_away_schema()).is_ok());
        assert!(Index::exists(&directory));
    }

    #[test]
    fn open_or_create_should_create() {
        let directory = RAMDirectory::create();
        assert!(!Index::exists(&directory));
        assert!(Index::open_or_create(directory.clone(), throw_away_schema()).is_ok());
        assert!(Index::exists(&directory));
    }

    #[test]
    fn open_or_create_should_open() {
        let directory = RAMDirectory::create();
        assert!(Index::create(directory.clone(), throw_away_schema()).is_ok());
        assert!(Index::exists(&directory));
        assert!(Index::open_or_create(directory, throw_away_schema()).is_ok());
    }

    #[test]
    fn create_should_wipeoff_existing() {
        let directory = RAMDirectory::create();
        assert!(Index::create(directory.clone(), throw_away_schema()).is_ok());
        assert!(Index::exists(&directory));
        assert!(Index::create(directory.clone(), Schema::builder().build()).is_ok());
    }

    #[test]
    fn open_or_create_exists_but_schema_does_not_match() {
        let directory = RAMDirectory::create();
        assert!(Index::create(directory.clone(), throw_away_schema()).is_ok());
        assert!(Index::exists(&directory));
        assert!(Index::open_or_create(directory.clone(), throw_away_schema()).is_ok());
        let err = Index::open_or_create(directory, Schema::builder().build());
        assert_eq!(
            format!("{:?}", err.unwrap_err()),
            "SchemaError(\"An index exists but the schema does not match.\")"
        );
    }

    fn throw_away_schema() -> Schema {
        let mut schema_builder = Schema::builder();
        let _ = schema_builder.add_u64_field("num_likes", INT_INDEXED);
        schema_builder.build()
    }
}

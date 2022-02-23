use std::collections::HashSet;
use std::fmt;
#[cfg(feature = "mmap")]
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use super::segment::Segment;
use super::IndexSettings;
use crate::core::{
    Executor, IndexMeta, SegmentId, SegmentMeta, SegmentMetaInventory, META_FILEPATH,
};
use crate::directory::error::OpenReadError;
#[cfg(feature = "mmap")]
use crate::directory::MmapDirectory;
use crate::directory::{Directory, ManagedDirectory, RamDirectory, INDEX_WRITER_LOCK};
use crate::error::{DataCorruption, TantivyError};
use crate::indexer::index_writer::{MAX_NUM_THREAD, MEMORY_ARENA_NUM_BYTES_MIN};
use crate::indexer::segment_updater::save_new_metas;
use crate::reader::{IndexReader, IndexReaderBuilder};
use crate::schema::{Field, FieldType, Schema};
use crate::tokenizer::{TextAnalyzer, TokenizerManager};
use crate::IndexWriter;

fn load_metas(
    directory: &dyn Directory,
    inventory: &SegmentMetaInventory,
) -> crate::Result<IndexMeta> {
    let meta_data = directory.atomic_read(&META_FILEPATH)?;
    let meta_string = String::from_utf8(meta_data).map_err(|_utf8_err| {
        error!("Meta data is not valid utf8.");
        DataCorruption::new(
            META_FILEPATH.to_path_buf(),
            "Meta file does not contain valid utf8 file.".to_string(),
        )
    })?;
    IndexMeta::deserialize(&meta_string, inventory)
        .map_err(|e| {
            DataCorruption::new(
                META_FILEPATH.to_path_buf(),
                format!(
                    "Meta file cannot be deserialized. {:?}. Content: {:?}",
                    e, meta_string
                ),
            )
        })
        .map_err(From::from)
}

/// IndexBuilder can be used to create an index.
///
/// Use in conjunction with `SchemaBuilder`. Global index settings
/// can be configured with `IndexSettings`
///
/// # Examples
///
/// ```
/// use tantivy::schema::*;
/// use tantivy::{Index, IndexSettings, IndexSortByField, Order};
///
/// let mut schema_builder = Schema::builder();
/// let id_field = schema_builder.add_text_field("id", STRING);
/// let title_field = schema_builder.add_text_field("title", TEXT);
/// let body_field = schema_builder.add_text_field("body", TEXT);
/// let number_field = schema_builder.add_u64_field(
///     "number",
///     NumericOptions::default().set_fast(Cardinality::SingleValue),
/// );
///
/// let schema = schema_builder.build();
/// let settings = IndexSettings{sort_by_field: Some(IndexSortByField{field:"number".to_string(), order:Order::Asc}), ..Default::default()};
/// let index = Index::builder().schema(schema).settings(settings).create_in_ram();
/// ```
pub struct IndexBuilder {
    schema: Option<Schema>,
    index_settings: IndexSettings,
}
impl Default for IndexBuilder {
    fn default() -> Self {
        IndexBuilder::new()
    }
}
impl IndexBuilder {
    /// Creates a new `IndexBuilder`
    pub fn new() -> Self {
        Self {
            schema: None,
            index_settings: IndexSettings::default(),
        }
    }

    /// Set the settings
    #[must_use]
    pub fn settings(mut self, settings: IndexSettings) -> Self {
        self.index_settings = settings;
        self
    }

    /// Set the schema
    #[must_use]
    pub fn schema(mut self, schema: Schema) -> Self {
        self.schema = Some(schema);
        self
    }

    /// Creates a new index using the `RAMDirectory`.
    ///
    /// The index will be allocated in anonymous memory.
    /// This should only be used for unit tests.
    pub fn create_in_ram(self) -> Result<Index, TantivyError> {
        let ram_directory = RamDirectory::create();
        Ok(self
            .create(ram_directory)
            .expect("Creating a RAMDirectory should never fail"))
    }

    /// Creates a new index in a given filepath.
    /// The index will use the `MMapDirectory`.
    ///
    /// If a previous index was in this directory, it returns an `IndexAlreadyExists` error.
    #[cfg(feature = "mmap")]
    pub fn create_in_dir<P: AsRef<Path>>(self, directory_path: P) -> crate::Result<Index> {
        let mmap_directory: Box<dyn Directory> = Box::new(MmapDirectory::open(directory_path)?);
        if Index::exists(&*mmap_directory)? {
            return Err(TantivyError::IndexAlreadyExists);
        }
        self.create(mmap_directory)
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
    pub fn create_from_tempdir(self) -> crate::Result<Index> {
        let mmap_directory: Box<dyn Directory> = Box::new(MmapDirectory::create_from_tempdir()?);
        self.create(mmap_directory)
    }

    fn get_expect_schema(&self) -> crate::Result<Schema> {
        self.schema
            .as_ref()
            .cloned()
            .ok_or(TantivyError::IndexBuilderMissingArgument("schema"))
    }

    /// Opens or creates a new index in the provided directory
    pub fn open_or_create<T: Into<Box<dyn Directory>>>(self, dir: T) -> crate::Result<Index> {
        let dir = dir.into();
        if !Index::exists(&*dir)? {
            return self.create(dir);
        }
        let index = Index::open(dir)?;
        if index.schema() == self.get_expect_schema()? {
            Ok(index)
        } else {
            Err(TantivyError::SchemaError(
                "An index exists but the schema does not match.".to_string(),
            ))
        }
    }
    /// Creates a new index given an implementation of the trait `Directory`.
    ///
    /// If a directory previously existed, it will be erased.
    fn create<T: Into<Box<dyn Directory>>>(self, dir: T) -> crate::Result<Index> {
        let dir = dir.into();
        let directory = ManagedDirectory::wrap(dir)?;
        save_new_metas(
            self.get_expect_schema()?,
            self.index_settings.clone(),
            &directory,
        )?;
        let mut metas = IndexMeta::with_schema(self.get_expect_schema()?);
        metas.index_settings = self.index_settings;
        let index = Index::open_from_metas(directory, &metas, SegmentMetaInventory::default());
        Ok(index)
    }
}

/// Search Index
#[derive(Clone)]
pub struct Index {
    directory: ManagedDirectory,
    schema: Schema,
    settings: IndexSettings,
    executor: Arc<Executor>,
    tokenizers: TokenizerManager,
    inventory: SegmentMetaInventory,
}

impl Index {
    /// Creates a new builder.
    pub fn builder() -> IndexBuilder {
        IndexBuilder::new()
    }
    /// Examines the directory to see if it contains an index.
    ///
    /// Effectively, it only checks for the presence of the `meta.json` file.
    pub fn exists(dir: &dyn Directory) -> Result<bool, OpenReadError> {
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
    pub fn set_multithread_executor(&mut self, num_threads: usize) -> crate::Result<()> {
        self.executor = Arc::new(Executor::multi_thread(num_threads, "tantivy-search-")?);
        Ok(())
    }

    /// Replace the default single thread search executor pool
    /// by a thread pool with a given number of threads.
    pub fn set_default_multithread_executor(&mut self) -> crate::Result<()> {
        let default_num_threads = num_cpus::get();
        self.set_multithread_executor(default_num_threads)
    }

    /// Creates a new index using the `RamDirectory`.
    ///
    /// The index will be allocated in anonymous memory.
    /// This is useful for indexing small set of documents
    /// for instances like unit test or temporary in memory index.
    pub fn create_in_ram(schema: Schema) -> Index {
        IndexBuilder::new().schema(schema).create_in_ram().unwrap()
    }

    /// Creates a new index in a given filepath.
    /// The index will use the `MMapDirectory`.
    ///
    /// If a previous index was in this directory, then it returns  an `IndexAlreadyExists` error.
    #[cfg(feature = "mmap")]
    pub fn create_in_dir<P: AsRef<Path>>(
        directory_path: P,
        schema: Schema,
    ) -> crate::Result<Index> {
        IndexBuilder::new()
            .schema(schema)
            .create_in_dir(directory_path)
    }

    /// Opens or creates a new index in the provided directory
    pub fn open_or_create<T: Into<Box<dyn Directory>>>(
        dir: T,
        schema: Schema,
    ) -> crate::Result<Index> {
        let dir = dir.into();
        IndexBuilder::new().schema(schema).open_or_create(dir)
    }

    /// Creates a new index in a temp directory.
    ///
    /// The index will use the `MMapDirectory` in a newly created directory.
    /// The temp directory will be destroyed automatically when the `Index` object
    /// is destroyed.
    ///
    /// The temp directory is only used for testing the `MmapDirectory`.
    /// For other unit tests, prefer the `RamDirectory`, see: `create_in_ram`.
    #[cfg(feature = "mmap")]
    pub fn create_from_tempdir(schema: Schema) -> crate::Result<Index> {
        IndexBuilder::new().schema(schema).create_from_tempdir()
    }

    /// Creates a new index given an implementation of the trait `Directory`.
    ///
    /// If a directory previously existed, it will be erased.
    pub fn create<T: Into<Box<dyn Directory>>>(
        dir: T,
        schema: Schema,
        settings: IndexSettings,
    ) -> crate::Result<Index> {
        let dir: Box<dyn Directory> = dir.into();
        let mut builder = IndexBuilder::new().schema(schema);
        builder = builder.settings(settings);
        builder.create(dir)
    }

    /// Creates a new index given a directory and an `IndexMeta`.
    fn open_from_metas(
        directory: ManagedDirectory,
        metas: &IndexMeta,
        inventory: SegmentMetaInventory,
    ) -> Index {
        let schema = metas.schema.clone();
        Index {
            settings: metas.index_settings.clone(),
            directory,
            schema,
            tokenizers: TokenizerManager::default(),
            executor: Arc::new(Executor::single_thread()),
            inventory,
        }
    }

    /// Accessor for the tokenizer manager.
    pub fn tokenizers(&self) -> &TokenizerManager {
        &self.tokenizers
    }

    /// Helper to access the tokenizer associated to a specific field.
    pub fn tokenizer_for_field(&self, field: Field) -> crate::Result<TextAnalyzer> {
        let field_entry = self.schema.get_field_entry(field);
        let field_type = field_entry.field_type();
        let tokenizer_manager: &TokenizerManager = self.tokenizers();
        let tokenizer_name_opt: Option<TextAnalyzer> = match field_type {
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

    /// Create a default `IndexReader` for the given index.
    ///
    /// See [`Index.reader_builder()`](#method.reader_builder).
    pub fn reader(&self) -> crate::Result<IndexReader> {
        self.reader_builder().try_into()
    }

    /// Create a `IndexReader` for the given index.
    ///
    /// Most project should create at most one reader for a given index.
    /// This method is typically called only once per `Index` instance,
    /// over the lifetime of most problem.
    pub fn reader_builder(&self) -> IndexReaderBuilder {
        IndexReaderBuilder::new(self.clone())
    }

    /// Opens a new directory from an index path.
    #[cfg(feature = "mmap")]
    pub fn open_in_dir<P: AsRef<Path>>(directory_path: P) -> crate::Result<Index> {
        let mmap_directory = MmapDirectory::open(directory_path)?;
        Index::open(mmap_directory)
    }

    /// Returns the list of the segment metas tracked by the index.
    ///
    /// Such segments can of course be part of the index,
    /// but also they could be segments being currently built or in the middle of a merge
    /// operation.
    pub(crate) fn list_all_segment_metas(&self) -> Vec<SegmentMeta> {
        self.inventory.all()
    }

    /// Creates a new segment_meta (Advanced user only).
    ///
    /// As long as the `SegmentMeta` lives, the files associated with the
    /// `SegmentMeta` are guaranteed to not be garbage collected, regardless of
    /// whether the segment is recorded as part of the index or not.
    pub fn new_segment_meta(&self, segment_id: SegmentId, max_doc: u32) -> SegmentMeta {
        self.inventory.new_segment_meta(segment_id, max_doc)
    }

    /// Open the index using the provided directory
    pub fn open<T: Into<Box<dyn Directory>>>(directory: T) -> crate::Result<Index> {
        let directory = directory.into();
        let directory = ManagedDirectory::wrap(directory)?;
        let inventory = SegmentMetaInventory::default();
        let metas = load_metas(&directory, &inventory)?;
        let index = Index::open_from_metas(directory, &metas, inventory);
        Ok(index)
    }

    /// Reads the index meta file from the directory.
    pub fn load_metas(&self) -> crate::Result<IndexMeta> {
        load_metas(self.directory(), &self.inventory)
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
    /// - `overall_memory_arena_in_bytes` sets the amount of memory
    /// allocated for all indexing thread.
    /// Each thread will receive a budget of  `overall_memory_arena_in_bytes / num_threads`.
    ///
    /// # Errors
    /// If the lockfile already exists, returns `Error::DirectoryLockBusy` or an `Error::IoError`.
    /// If the memory arena per thread is too small or too big, returns
    /// `TantivyError::InvalidArgument`
    pub fn writer_with_num_threads(
        &self,
        num_threads: usize,
        overall_memory_arena_in_bytes: usize,
    ) -> crate::Result<IndexWriter> {
        let directory_lock = self
            .directory
            .acquire_lock(&INDEX_WRITER_LOCK)
            .map_err(|err| {
                TantivyError::LockFailure(
                    err,
                    Some(
                        "Failed to acquire index lock. If you are using a regular directory, this \
                         means there is already an `IndexWriter` working on this `Directory`, in \
                         this process or in a different process."
                            .to_string(),
                    ),
                )
            })?;
        let memory_arena_in_bytes_per_thread = overall_memory_arena_in_bytes / num_threads;
        IndexWriter::new(
            self,
            num_threads,
            memory_arena_in_bytes_per_thread,
            directory_lock,
        )
    }

    /// Helper to create an index writer for tests.
    ///
    /// That index writer only simply has a single thread and a memory arena of 10 MB.
    /// Using a single thread gives us a deterministic allocation of DocId.
    #[cfg(test)]
    pub fn writer_for_tests(&self) -> crate::Result<IndexWriter> {
        self.writer_with_num_threads(1, 10_000_000)
    }

    /// Creates a multithreaded writer
    ///
    /// Tantivy will automatically define the number of threads to use, but
    /// no more than 8 threads.
    /// `overall_memory_arena_in_bytes` is the total target memory usage that will be split
    /// between a given number of threads.
    ///
    /// # Errors
    /// If the lockfile already exists, returns `Error::FileAlreadyExists`.
    /// If the memory arena per thread is too small or too big, returns
    /// `TantivyError::InvalidArgument`
    pub fn writer(&self, memory_arena_num_bytes: usize) -> crate::Result<IndexWriter> {
        let mut num_threads = std::cmp::min(num_cpus::get(), MAX_NUM_THREAD);
        let memory_arena_num_bytes_per_thread = memory_arena_num_bytes / num_threads;
        if memory_arena_num_bytes_per_thread < MEMORY_ARENA_NUM_BYTES_MIN {
            num_threads = (memory_arena_num_bytes / MEMORY_ARENA_NUM_BYTES_MIN).max(1);
        }
        self.writer_with_num_threads(num_threads, memory_arena_num_bytes)
    }

    /// Accessor to the index settings
    pub fn settings(&self) -> &IndexSettings {
        &self.settings
    }

    /// Accessor to the index settings
    pub fn settings_mut(&mut self) -> &mut IndexSettings {
        &mut self.settings
    }

    /// Accessor to the index schema
    ///
    /// The schema is actually cloned.
    pub fn schema(&self) -> Schema {
        self.schema.clone()
    }

    /// Returns the list of segments that are searchable
    pub fn searchable_segments(&self) -> crate::Result<Vec<Segment>> {
        Ok(self
            .searchable_segment_metas()?
            .into_iter()
            .map(|segment_meta| self.segment(segment_meta))
            .collect())
    }

    #[doc(hidden)]
    pub fn segment(&self, segment_meta: SegmentMeta) -> Segment {
        Segment::for_index(self.clone(), segment_meta)
    }

    /// Creates a new segment.
    pub fn new_segment(&self) -> Segment {
        let segment_meta = self
            .inventory
            .new_segment_meta(SegmentId::generate_random(), 0);
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
    pub fn searchable_segment_metas(&self) -> crate::Result<Vec<SegmentMeta>> {
        Ok(self.load_metas()?.segments)
    }

    /// Returns the list of segment ids that are searchable.
    pub fn searchable_segment_ids(&self) -> crate::Result<Vec<SegmentId>> {
        Ok(self
            .searchable_segment_metas()?
            .iter()
            .map(SegmentMeta::id)
            .collect())
    }

    /// Returns the set of corrupted files
    pub fn validate_checksum(&self) -> crate::Result<HashSet<PathBuf>> {
        let managed_files = self.directory.list_managed_files();
        let active_segments_files: HashSet<PathBuf> = self
            .searchable_segment_metas()?
            .iter()
            .flat_map(|segment_meta| segment_meta.list_files())
            .collect();
        let active_existing_files: HashSet<&PathBuf> =
            active_segments_files.intersection(&managed_files).collect();

        let mut damaged_files = HashSet::new();
        for path in active_existing_files {
            if !self.directory.validate_checksum(path)? {
                damaged_files.insert((*path).clone());
            }
        }
        Ok(damaged_files)
    }
}

impl fmt::Debug for Index {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Index({:?})", self.directory)
    }
}

#[cfg(test)]
mod tests {
    use crate::directory::{RamDirectory, WatchCallback};
    use crate::schema::{Field, Schema, INDEXED, TEXT};
    use crate::{Directory, Index, IndexReader, IndexSettings, ReloadPolicy};

    #[test]
    fn test_indexer_for_field() {
        let mut schema_builder = Schema::builder();
        let num_likes_field = schema_builder.add_u64_field("num_likes", INDEXED);
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
        let directory: Box<dyn Directory> = Box::new(RamDirectory::create());
        assert!(!Index::exists(directory.as_ref()).unwrap());
        assert!(Index::create(
            directory.clone(),
            throw_away_schema(),
            IndexSettings::default()
        )
        .is_ok());
        assert!(Index::exists(directory.as_ref()).unwrap());
    }

    #[test]
    fn open_or_create_should_create() {
        let directory = RamDirectory::create();
        assert!(!Index::exists(&directory).unwrap());
        assert!(Index::open_or_create(directory.clone(), throw_away_schema()).is_ok());
        assert!(Index::exists(&directory).unwrap());
    }

    #[test]
    fn open_or_create_should_open() {
        let directory: Box<dyn Directory> = Box::new(RamDirectory::create());
        assert!(Index::create(
            directory.clone(),
            throw_away_schema(),
            IndexSettings::default()
        )
        .is_ok());
        assert!(Index::exists(directory.as_ref()).unwrap());
        assert!(Index::open_or_create(directory, throw_away_schema()).is_ok());
    }

    #[test]
    fn create_should_wipeoff_existing() {
        let directory: Box<dyn Directory> = Box::new(RamDirectory::create());
        assert!(Index::create(
            directory.clone(),
            throw_away_schema(),
            IndexSettings::default()
        )
        .is_ok());
        assert!(Index::exists(directory.as_ref()).unwrap());
        assert!(Index::create(
            directory,
            Schema::builder().build(),
            IndexSettings::default()
        )
        .is_ok());
    }

    #[test]
    fn open_or_create_exists_but_schema_does_not_match() {
        let directory = RamDirectory::create();
        assert!(Index::create(
            directory.clone(),
            throw_away_schema(),
            IndexSettings::default()
        )
        .is_ok());
        assert!(Index::exists(&directory).unwrap());
        assert!(Index::open_or_create(directory.clone(), throw_away_schema()).is_ok());
        let err = Index::open_or_create(directory, Schema::builder().build());
        assert_eq!(
            format!("{:?}", err.unwrap_err()),
            "SchemaError(\"An index exists but the schema does not match.\")"
        );
    }

    fn throw_away_schema() -> Schema {
        let mut schema_builder = Schema::builder();
        let _ = schema_builder.add_u64_field("num_likes", INDEXED);
        schema_builder.build()
    }

    #[test]
    fn test_index_on_commit_reload_policy() -> crate::Result<()> {
        let schema = throw_away_schema();
        let field = schema.get_field("num_likes").unwrap();
        let index = Index::create_in_ram(schema);
        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::OnCommit)
            .try_into()
            .unwrap();
        assert_eq!(reader.searcher().num_docs(), 0);
        test_index_on_commit_reload_policy_aux(field, &index, &reader)
    }

    #[cfg(feature = "mmap")]
    mod mmap_specific {

        use std::path::PathBuf;

        use tempfile::TempDir;

        use super::*;
        use crate::Directory;

        #[test]
        fn test_index_on_commit_reload_policy_mmap() -> crate::Result<()> {
            let schema = throw_away_schema();
            let field = schema.get_field("num_likes").unwrap();
            let tempdir = TempDir::new().unwrap();
            let tempdir_path = PathBuf::from(tempdir.path());
            let index = Index::create_in_dir(&tempdir_path, schema).unwrap();
            let reader = index
                .reader_builder()
                .reload_policy(ReloadPolicy::OnCommit)
                .try_into()
                .unwrap();
            assert_eq!(reader.searcher().num_docs(), 0);
            test_index_on_commit_reload_policy_aux(field, &index, &reader)
        }

        #[test]
        fn test_index_manual_policy_mmap() -> crate::Result<()> {
            let schema = throw_away_schema();
            let field = schema.get_field("num_likes").unwrap();
            let mut index = Index::create_from_tempdir(schema)?;
            let mut writer = index.writer_for_tests()?;
            writer.commit()?;
            let reader = index
                .reader_builder()
                .reload_policy(ReloadPolicy::Manual)
                .try_into()?;
            assert_eq!(reader.searcher().num_docs(), 0);
            writer.add_document(doc!(field=>1u64))?;
            let (sender, receiver) = crossbeam::channel::unbounded();
            let _handle = index.directory_mut().watch(WatchCallback::new(move || {
                let _ = sender.send(());
            }));
            writer.commit()?;
            assert!(receiver.recv().is_ok());
            assert_eq!(reader.searcher().num_docs(), 0);
            reader.reload()?;
            assert_eq!(reader.searcher().num_docs(), 1);
            Ok(())
        }

        #[test]
        fn test_index_on_commit_reload_policy_different_directories() -> crate::Result<()> {
            let schema = throw_away_schema();
            let field = schema.get_field("num_likes").unwrap();
            let tempdir = TempDir::new().unwrap();
            let tempdir_path = PathBuf::from(tempdir.path());
            let write_index = Index::create_in_dir(&tempdir_path, schema).unwrap();
            let read_index = Index::open_in_dir(&tempdir_path).unwrap();
            let reader = read_index
                .reader_builder()
                .reload_policy(ReloadPolicy::OnCommit)
                .try_into()
                .unwrap();
            assert_eq!(reader.searcher().num_docs(), 0);
            test_index_on_commit_reload_policy_aux(field, &write_index, &reader)
        }
    }
    fn test_index_on_commit_reload_policy_aux(
        field: Field,
        index: &Index,
        reader: &IndexReader,
    ) -> crate::Result<()> {
        let mut reader_index = reader.index();
        let (sender, receiver) = crossbeam::channel::unbounded();
        let _watch_handle = reader_index
            .directory_mut()
            .watch(WatchCallback::new(move || {
                let _ = sender.send(());
            }));
        let mut writer = index.writer_for_tests()?;
        assert_eq!(reader.searcher().num_docs(), 0);
        writer.add_document(doc!(field=>1u64))?;
        writer.commit().unwrap();
        // We need a loop here because it is possible for notify to send more than
        // one modify event. It was observed on CI on MacOS.
        loop {
            assert!(receiver.recv().is_ok());
            if reader.searcher().num_docs() == 1 {
                break;
            }
        }
        writer.add_document(doc!(field=>2u64))?;
        writer.commit().unwrap();
        // ... Same as above
        loop {
            assert!(receiver.recv().is_ok());
            if reader.searcher().num_docs() == 2 {
                break;
            }
        }
        Ok(())
    }

    // This test will not pass on windows, because windows
    // prevent deleting files that are MMapped.
    #[cfg(not(target_os = "windows"))]
    #[test]
    fn garbage_collect_works_as_intended() -> crate::Result<()> {
        let directory = RamDirectory::create();
        let schema = throw_away_schema();
        let field = schema.get_field("num_likes").unwrap();
        let index = Index::create(directory.clone(), schema, IndexSettings::default())?;

        let mut writer = index.writer_with_num_threads(8, 24_000_000).unwrap();
        for i in 0u64..8_000u64 {
            writer.add_document(doc!(field => i))?;
        }
        let (sender, receiver) = crossbeam::channel::unbounded();
        let _handle = directory.watch(WatchCallback::new(move || {
            let _ = sender.send(());
        }));
        writer.commit()?;
        let mem_right_after_commit = directory.total_mem_usage();
        assert!(receiver.recv().is_ok());
        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()?;

        assert_eq!(reader.searcher().num_docs(), 8_000);
        writer.wait_merging_threads()?;
        let mem_right_after_merge_finished = directory.total_mem_usage();

        reader.reload().unwrap();
        let searcher = reader.searcher();
        assert_eq!(searcher.num_docs(), 8_000);
        assert!(
            mem_right_after_merge_finished < mem_right_after_commit,
            "(mem after merge){} is expected < (mem before merge){}",
            mem_right_after_merge_finished,
            mem_right_after_commit
        );
        Ok(())
    }
}

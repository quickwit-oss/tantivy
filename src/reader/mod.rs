mod index_writer_reader;
mod meta_file_reader;
mod pool;

use self::meta_file_reader::MetaFileIndexReader;
pub use self::meta_file_reader::{IndexReaderBuilder, ReloadPolicy};
pub use self::pool::LeasedItem;

pub(crate) use crate::reader::index_writer_reader::NRTReader;

use crate::Searcher;

/// `IndexReader` is your entry point to read and search the index.
///
/// It controls when a new version of the index should be loaded and lends
/// you instances of `Searcher` for the last loaded version.
///
/// `Clone` does not clone the different pool of searcher. `IndexReader`
/// just wraps and `Arc`.
#[derive(Clone)]
pub enum IndexReader {
    FromMetaFile(MetaFileIndexReader),
    NRT(NRTReader),
}

impl IndexReader {
    /// Update searchers so that they reflect the state of the last
    /// `.commit()`.
    ///
    /// If you set up the `OnCommit` `ReloadPolicy` (which is the default)
    /// every commit should be rapidly reflected on your `IndexReader` and you should
    /// not need to call `reload()` at all.
    ///
    /// This automatic reload can take 10s of milliseconds to kick in however, and in unit tests
    /// it can be nice to deterministically force the reload of searchers.
    pub fn reload(&self) -> crate::Result<()> {
        match self {
            IndexReader::FromMetaFile(meta_file_reader) => meta_file_reader.reload(),
            IndexReader::NRT(nrt_reader) => nrt_reader.reload(),
        }
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
        match self {
            IndexReader::FromMetaFile(meta_file_reader) => meta_file_reader.searcher(),
            IndexReader::NRT(nrt_reader) => nrt_reader.searcher(),
        }
    }
}

impl From<MetaFileIndexReader> for IndexReader {
    fn from(meta_file_reader: MetaFileIndexReader) -> Self {
        IndexReader::FromMetaFile(meta_file_reader)
    }
}

impl From<NRTReader> for IndexReader {
    fn from(nrt_reader: NRTReader) -> Self {
        IndexReader::NRT(nrt_reader)
    }
}

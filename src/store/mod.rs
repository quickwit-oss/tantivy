//! Compressed/slow/row-oriented storage for documents.
//!
//! A field needs to be marked as stored in the schema in
//! order to be handled in the `Store`.
//!
//! Internally, documents (or rather their stored fields) are serialized to a buffer.
//! When the buffer exceeds 16K, the buffer is compressed using `brotli`, `LZ4` or `snappy`
//! and the resulting block is written to disk.
//!
//! One can then request for a specific `DocId`.
//! A skip list helps navigating to the right block,
//! decompresses it entirely and returns the document within it.
//!
//! If the last document requested was in the same block,
//! the reader is smart enough to avoid decompressing
//! the block a second time, but their is no real
//! uncompressed block* cache.
//!
//! A typical use case for the store is, once
//! the search result page has been computed, returning
//! the actual content of the 10 best document.
//!
//! # Usage
//!
//! Most users should not access the `StoreReader` directly
//! and should rely on either
//!
//! - at the segment level, the
//! [`SegmentReader`'s `doc` method](../struct.SegmentReader.html#method.doc)
//! - at the index level, the
//! [`Searcher`'s `doc` method](../struct.Searcher.html#method.doc)
//!
//! !

mod compressors;
mod footer;
mod index;
mod reader;
mod writer;
pub use self::compressors::Compressor;
pub use self::reader::StoreReader;
pub use self::writer::StoreWriter;

#[cfg(feature = "lz4-compression")]
mod compression_lz4_block;

#[cfg(feature = "brotli-compression")]
mod compression_brotli;

#[cfg(feature = "snappy-compression")]
mod compression_snap;

#[cfg(feature = "zstd-compression")]
mod compression_zstd_block;

#[cfg(test)]
pub mod tests {

    use std::path::Path;

    use super::*;
    use crate::directory::{Directory, RamDirectory, WritePtr};
    use crate::fastfield::AliveBitSet;
    use crate::schema::{self, Document, Schema, TextFieldIndexing, TextOptions, STORED, TEXT};
    use crate::{Index, Term};

    const LOREM: &str = "Doc Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do \
                         eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad \
                         minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip \
                         ex ea commodo consequat. Duis aute irure dolor in reprehenderit in \
                         voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur \
                         sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt \
                         mollit anim id est laborum.";

    const BLOCK_SIZE: usize = 16_384;

    pub fn write_lorem_ipsum_store(
        writer: WritePtr,
        num_docs: usize,
        compressor: Compressor,
        blocksize: usize,
    ) -> Schema {
        let mut schema_builder = Schema::builder();
        let field_body = schema_builder.add_text_field("body", TextOptions::default().set_stored());
        let field_title =
            schema_builder.add_text_field("title", TextOptions::default().set_stored());
        let schema = schema_builder.build();
        {
            let mut store_writer = StoreWriter::new(writer, compressor, blocksize, None);
            for i in 0..num_docs {
                let mut doc = Document::default();
                doc.add_field_value(field_body, LOREM.to_string());
                doc.add_field_value(field_title, format!("Doc {i}"));
                store_writer.store(&doc).unwrap();
            }
            store_writer.close().unwrap();
        }
        schema
    }

    const NUM_DOCS: usize = 1_000;
    #[test]
    fn test_doc_store_iter_with_delete_bug_1077() -> crate::Result<()> {
        // this will cover deletion of the first element in a checkpoint
        let deleted_doc_ids = (200..300).collect::<Vec<_>>();
        let alive_bitset =
            AliveBitSet::for_test_from_deleted_docs(&deleted_doc_ids, NUM_DOCS as u32);

        let path = Path::new("store");
        let directory = RamDirectory::create();
        let store_wrt = directory.open_write(path)?;
        let schema = write_lorem_ipsum_store(store_wrt, NUM_DOCS, Compressor::Lz4, BLOCK_SIZE);
        let field_title = schema.get_field("title").unwrap();
        let store_file = directory.open_read(path)?;
        let store = StoreReader::open(store_file)?;
        for i in 0..NUM_DOCS as u32 {
            assert_eq!(
                *store
                    .get(i)?
                    .get_first(field_title)
                    .unwrap()
                    .as_text()
                    .unwrap(),
                format!("Doc {}", i)
            );
        }

        for (_, doc) in store.iter(Some(&alive_bitset)).enumerate() {
            let doc = doc?;
            let title_content = doc.get_first(field_title).unwrap().as_text().unwrap();
            if !title_content.starts_with("Doc ") {
                panic!("unexpected title_content {}", title_content);
            }

            let id = title_content
                .strip_prefix("Doc ")
                .unwrap()
                .parse::<u32>()
                .unwrap();
            if alive_bitset.is_deleted(id) {
                panic!("unexpected deleted document {}", id);
            }
        }

        Ok(())
    }

    fn test_store(compressor: Compressor, blocksize: usize) -> crate::Result<()> {
        let path = Path::new("store");
        let directory = RamDirectory::create();
        let store_wrt = directory.open_write(path)?;
        let schema = write_lorem_ipsum_store(store_wrt, NUM_DOCS, compressor, blocksize);
        let field_title = schema.get_field("title").unwrap();
        let store_file = directory.open_read(path)?;
        let store = StoreReader::open(store_file)?;
        for i in 0..NUM_DOCS as u32 {
            assert_eq!(
                *store
                    .get(i)?
                    .get_first(field_title)
                    .unwrap()
                    .as_text()
                    .unwrap(),
                format!("Doc {}", i)
            );
        }
        for (i, doc) in store.iter(None).enumerate() {
            assert_eq!(
                *doc?.get_first(field_title).unwrap().as_text().unwrap(),
                format!("Doc {}", i)
            );
        }
        Ok(())
    }

    #[test]
    fn test_store_noop() -> crate::Result<()> {
        test_store(Compressor::None, BLOCK_SIZE)
    }
    #[cfg(feature = "lz4-compression")]
    #[test]
    fn test_store_lz4_block() -> crate::Result<()> {
        test_store(Compressor::Lz4, BLOCK_SIZE)
    }
    #[cfg(feature = "snappy-compression")]
    #[test]
    fn test_store_snap() -> crate::Result<()> {
        test_store(Compressor::Snappy, BLOCK_SIZE)
    }
    #[cfg(feature = "brotli-compression")]
    #[test]
    fn test_store_brotli() -> crate::Result<()> {
        test_store(Compressor::Brotli, BLOCK_SIZE)
    }

    #[cfg(feature = "zstd-compression")]
    #[test]
    fn test_store_zstd() -> crate::Result<()> {
        test_store(Compressor::Zstd, BLOCK_SIZE)
    }

    #[test]
    fn test_store_with_delete() -> crate::Result<()> {
        let mut schema_builder = schema::Schema::builder();

        let text_field_options = TextOptions::default()
            .set_indexing_options(
                TextFieldIndexing::default()
                    .set_index_option(schema::IndexRecordOption::WithFreqsAndPositions),
            )
            .set_stored();
        let text_field = schema_builder.add_text_field("text_field", text_field_options);
        let schema = schema_builder.build();
        let index_builder = Index::builder().schema(schema);

        let index = index_builder.create_in_ram()?;

        {
            let mut index_writer = index.writer_for_tests().unwrap();
            index_writer.add_document(doc!(text_field=> "deleteme"))?;
            index_writer.add_document(doc!(text_field=> "deletemenot"))?;
            index_writer.add_document(doc!(text_field=> "deleteme"))?;
            index_writer.add_document(doc!(text_field=> "deletemenot"))?;
            index_writer.add_document(doc!(text_field=> "deleteme"))?;

            index_writer.delete_term(Term::from_field_text(text_field, "deleteme"));
            index_writer.commit()?;
        }

        let searcher = index.reader()?.searcher();
        let reader = searcher.segment_reader(0);
        let store = reader.get_store_reader()?;
        for doc in store.iter(reader.alive_bitset()) {
            assert_eq!(
                *doc?.get_first(text_field).unwrap().as_text().unwrap(),
                "deletemenot".to_string()
            );
        }
        Ok(())
    }

    #[cfg(feature = "snappy-compression")]
    #[cfg(feature = "lz4-compression")]
    #[test]
    fn test_merge_with_changed_compressor() -> crate::Result<()> {
        let mut schema_builder = schema::Schema::builder();

        let text_field = schema_builder.add_text_field("text_field", TEXT | STORED);
        let schema = schema_builder.build();
        let index_builder = Index::builder().schema(schema);

        let mut index = index_builder.create_in_ram().unwrap();
        index.settings_mut().docstore_compression = Compressor::Lz4;
        {
            let mut index_writer = index.writer_for_tests().unwrap();
            // put enough data create enough blocks in the doc store to be considered for stacking
            for _ in 0..200 {
                index_writer.add_document(doc!(text_field=> LOREM))?;
            }
            assert!(index_writer.commit().is_ok());
            for _ in 0..200 {
                index_writer.add_document(doc!(text_field=> LOREM))?;
            }
            assert!(index_writer.commit().is_ok());
        }
        assert_eq!(
            index.reader().unwrap().searcher().segment_readers()[0]
                .get_store_reader()
                .unwrap()
                .compressor(),
            Compressor::Lz4
        );
        // Change compressor, this disables stacking on merging
        let index_settings = index.settings_mut();
        index_settings.docstore_compression = Compressor::Snappy;
        // Merging the segments
        {
            let segment_ids = index
                .searchable_segment_ids()
                .expect("Searchable segments failed.");
            let mut index_writer = index.writer_for_tests().unwrap();
            assert!(index_writer.merge(&segment_ids).wait().is_ok());
            assert!(index_writer.wait_merging_threads().is_ok());
        }

        let searcher = index.reader().unwrap().searcher();
        assert_eq!(searcher.segment_readers().len(), 1);
        let reader = searcher.segment_readers().iter().last().unwrap();
        let store = reader.get_store_reader().unwrap();

        for doc in store.iter(reader.alive_bitset()).take(50) {
            assert_eq!(
                *doc?.get_first(text_field).unwrap().as_text().unwrap(),
                LOREM.to_string()
            );
        }
        assert_eq!(store.compressor(), Compressor::Snappy);

        Ok(())
    }

    #[test]
    fn test_merge_of_small_segments() -> crate::Result<()> {
        let mut schema_builder = schema::Schema::builder();

        let text_field = schema_builder.add_text_field("text_field", TEXT | STORED);
        let schema = schema_builder.build();
        let index_builder = Index::builder().schema(schema);

        let index = index_builder.create_in_ram().unwrap();

        {
            let mut index_writer = index.writer_for_tests()?;
            index_writer.add_document(doc!(text_field=> "1"))?;
            index_writer.commit()?;
            index_writer.add_document(doc!(text_field=> "2"))?;
            index_writer.commit()?;
            index_writer.add_document(doc!(text_field=> "3"))?;
            index_writer.commit()?;
            index_writer.add_document(doc!(text_field=> "4"))?;
            index_writer.commit()?;
            index_writer.add_document(doc!(text_field=> "5"))?;
            index_writer.commit()?;
        }
        // Merging the segments
        {
            let segment_ids = index.searchable_segment_ids()?;
            let mut index_writer = index.writer_for_tests()?;
            index_writer.merge(&segment_ids).wait()?;
            index_writer.wait_merging_threads()?;
        }

        let searcher = index.reader()?.searcher();
        assert_eq!(searcher.segment_readers().len(), 1);
        let reader = searcher.segment_readers().iter().last().unwrap();
        let store = reader.get_store_reader()?;
        assert_eq!(store.block_checkpoints().count(), 1);
        Ok(())
    }
}

#[cfg(all(test, feature = "unstable"))]
mod bench {

    use std::path::Path;

    use test::Bencher;

    use super::tests::write_lorem_ipsum_store;
    use crate::directory::{Directory, RamDirectory};
    use crate::store::{Compressor, StoreReader};

    #[bench]
    #[cfg(feature = "mmap")]
    fn bench_store_encode(b: &mut Bencher) {
        let directory = RamDirectory::create();
        let path = Path::new("store");
        b.iter(|| {
            write_lorem_ipsum_store(
                directory.open_write(path).unwrap(),
                1_000,
                Compressor::default(),
                16_384,
            );
            directory.delete(path).unwrap();
        });
    }

    #[bench]
    fn bench_store_decode(b: &mut Bencher) {
        let directory = RamDirectory::create();
        let path = Path::new("store");
        write_lorem_ipsum_store(
            directory.open_write(path).unwrap(),
            1_000,
            Compressor::default(),
            16_384,
        );
        let store_file = directory.open_read(path).unwrap();
        let store = StoreReader::open(store_file).unwrap();
        b.iter(|| store.iter(None).collect::<Vec<_>>());
    }
}

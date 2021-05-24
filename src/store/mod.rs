/*!
Compressed/slow/row-oriented storage for documents.

A field needs to be marked as stored in the schema in
order to be handled in the `Store`.

Internally, documents (or rather their stored fields) are serialized to a buffer.
When the buffer exceeds 16K, the buffer is compressed using `brotli`, `LZ4` or `snappy`
and the resulting block is written to disk.

One can then request for a specific `DocId`.
A skip list helps navigating to the right block,
decompresses it entirely and returns the document within it.

If the last document requested was in the same block,
the reader is smart enough to avoid decompressing
the block a second time, but their is no real
*uncompressed block* cache.

A typical use case for the store is, once
the search result page has been computed, returning
the actual content of the 10 best document.

# Usage

Most users should not access the `StoreReader` directly
and should rely on either

- at the segment level, the
[`SegmentReader`'s `doc` method](../struct.SegmentReader.html#method.doc)
- at the index level, the
[`Searcher`'s `doc` method](../struct.Searcher.html#method.doc)

!*/

mod index;
mod reader;
mod writer;
pub use self::reader::StoreReader;
pub use self::writer::StoreWriter;

// compile_error doesn't scale very well, enum like feature flags would be great to have in Rust
#[cfg(all(feature = "lz4", feature = "brotli"))]
compile_error!("feature `lz4` or `brotli` must not be enabled together.");

#[cfg(all(feature = "lz4_block", feature = "brotli"))]
compile_error!("feature `lz4_block` or `brotli` must not be enabled together.");

#[cfg(all(feature = "lz4_block", feature = "lz4"))]
compile_error!("feature `lz4_block` or `lz4` must not be enabled together.");

#[cfg(all(feature = "lz4_block", feature = "snap"))]
compile_error!("feature `lz4_block` or `snap` must not be enabled together.");

#[cfg(all(feature = "lz4", feature = "snap"))]
compile_error!("feature `lz4` or `snap` must not be enabled together.");

#[cfg(all(feature = "brotli", feature = "snap"))]
compile_error!("feature `brotli` or `snap` must not be enabled together.");

#[cfg(not(any(
    feature = "lz4",
    feature = "brotli",
    feature = "lz4_flex",
    feature = "snap"
)))]
compile_error!("all compressors are deactivated via feature-flags, check Cargo.toml for available decompressors.");

#[cfg(feature = "lz4_flex")]
mod compression_lz4_block;
#[cfg(feature = "lz4_flex")]
pub use self::compression_lz4_block::COMPRESSION;
#[cfg(feature = "lz4_flex")]
use self::compression_lz4_block::{compress, decompress};

#[cfg(feature = "lz4")]
mod compression_lz4;
#[cfg(feature = "lz4")]
pub use self::compression_lz4::COMPRESSION;
#[cfg(feature = "lz4")]
use self::compression_lz4::{compress, decompress};

#[cfg(feature = "brotli")]
mod compression_brotli;
#[cfg(feature = "brotli")]
pub use self::compression_brotli::COMPRESSION;
#[cfg(feature = "brotli")]
use self::compression_brotli::{compress, decompress};

#[cfg(feature = "snap")]
mod compression_snap;
#[cfg(feature = "snap")]
pub use self::compression_snap::COMPRESSION;
#[cfg(feature = "snap")]
use self::compression_snap::{compress, decompress};

#[cfg(test)]
pub mod tests {

    use futures::executor::block_on;

    use super::*;
    use crate::schema::{self, FieldValue, TextFieldIndexing, STORED, TEXT};
    use crate::schema::{Document, TextOptions};
    use crate::{
        directory::{Directory, RamDirectory, WritePtr},
        Term,
    };
    use crate::{schema::Schema, Index};
    use std::path::Path;

    pub fn write_lorem_ipsum_store(writer: WritePtr, num_docs: usize) -> Schema {
        let mut schema_builder = Schema::builder();
        let field_body = schema_builder.add_text_field("body", TextOptions::default().set_stored());
        let field_title =
            schema_builder.add_text_field("title", TextOptions::default().set_stored());
        let schema = schema_builder.build();
        let lorem = String::from(
            "Doc Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed \
             do eiusmod tempor incididunt ut labore et dolore magna aliqua. \
             Ut enim ad minim veniam, quis nostrud exercitation ullamco \
             laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure \
             dolor in reprehenderit in voluptate velit esse cillum dolore eu \
             fugiat nulla pariatur. Excepteur sint occaecat cupidatat non \
             proident, sunt in culpa qui officia deserunt mollit anim id est \
             laborum.",
        );
        {
            let mut store_writer = StoreWriter::new(writer);
            for i in 0..num_docs {
                let mut fields: Vec<FieldValue> = Vec::new();
                {
                    let field_value = FieldValue::new(field_body, From::from(lorem.clone()));
                    fields.push(field_value);
                }
                {
                    let title_text = format!("Doc {}", i);
                    let field_value = FieldValue::new(field_title, From::from(title_text));
                    fields.push(field_value);
                }
                //let fields_refs: Vec<&FieldValue> = fields.iter().collect();
                let doc = Document::from(fields);
                store_writer.store(&doc).unwrap();
            }
            store_writer.close().unwrap();
        }
        schema
    }

    #[test]
    fn test_store() -> crate::Result<()> {
        let path = Path::new("store");
        let directory = RamDirectory::create();
        let store_wrt = directory.open_write(path)?;
        let schema = write_lorem_ipsum_store(store_wrt, 1_000);
        let field_title = schema.get_field("title").unwrap();
        let store_file = directory.open_read(path)?;
        let store = StoreReader::open(store_file)?;
        for i in 0..1_000 {
            assert_eq!(
                *store
                    .get(i)?
                    .get_first(field_title)
                    .unwrap()
                    .text()
                    .unwrap(),
                format!("Doc {}", i)
            );
        }
        for (i, doc) in store.iter(None).enumerate() {
            assert_eq!(
                *doc?.get_first(field_title).unwrap().text().unwrap(),
                format!("Doc {}", i)
            );
        }
        Ok(())
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

        let index = index_builder.create_in_ram().unwrap();

        {
            let mut index_writer = index.writer_for_tests().unwrap();

            index_writer.add_document(doc!(text_field=> "deleteme"));
            index_writer.add_document(doc!(text_field=> "deletemenot"));
            index_writer.add_document(doc!(text_field=> "deleteme"));
            index_writer.add_document(doc!(text_field=> "deletemenot"));
            index_writer.add_document(doc!(text_field=> "deleteme"));

            index_writer.delete_term(Term::from_field_text(text_field, "deleteme"));
            assert!(index_writer.commit().is_ok());
        }

        let searcher = index.reader().unwrap().searcher();
        let reader = searcher.segment_reader(0);
        let store = reader.get_store_reader().unwrap();
        for doc in store.iter(reader.delete_bitset()) {
            assert_eq!(
                *doc?.get_first(text_field).unwrap().text().unwrap(),
                "deletemenot".to_string()
            );
        }
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
            let mut index_writer = index.writer_for_tests().unwrap();

            index_writer.add_document(doc!(text_field=> "1"));
            assert!(index_writer.commit().is_ok());
            index_writer.add_document(doc!(text_field=> "2"));
            assert!(index_writer.commit().is_ok());
            index_writer.add_document(doc!(text_field=> "3"));
            assert!(index_writer.commit().is_ok());
            index_writer.add_document(doc!(text_field=> "4"));
            assert!(index_writer.commit().is_ok());
            index_writer.add_document(doc!(text_field=> "5"));
            assert!(index_writer.commit().is_ok());
        }
        // Merging the segments
        {
            let segment_ids = index
                .searchable_segment_ids()
                .expect("Searchable segments failed.");
            let mut index_writer = index.writer_for_tests().unwrap();
            assert!(block_on(index_writer.merge(&segment_ids)).is_ok());
            assert!(index_writer.wait_merging_threads().is_ok());
        }

        let searcher = index.reader().unwrap().searcher();
        assert_eq!(searcher.segment_readers().len(), 1);
        let reader = searcher.segment_readers().iter().last().unwrap();
        let store = reader.get_store_reader().unwrap();
        assert_eq!(store.block_checkpoints().count(), 1);
        Ok(())
    }
}

#[cfg(all(test, feature = "unstable"))]
mod bench {

    use super::tests::write_lorem_ipsum_store;
    use crate::directory::Directory;
    use crate::directory::RamDirectory;
    use crate::store::StoreReader;
    use std::path::Path;
    use test::Bencher;

    #[bench]
    #[cfg(feature = "mmap")]
    fn bench_store_encode(b: &mut Bencher) {
        let directory = RamDirectory::create();
        let path = Path::new("store");
        b.iter(|| {
            write_lorem_ipsum_store(directory.open_write(path).unwrap(), 1_000);
            directory.delete(path).unwrap();
        });
    }

    #[bench]
    fn bench_store_decode(b: &mut Bencher) {
        let directory = RamDirectory::create();
        let path = Path::new("store");
        write_lorem_ipsum_store(directory.open_write(path).unwrap(), 1_000);
        let store_file = directory.open_read(path).unwrap();
        let store = StoreReader::open(store_file).unwrap();
        b.iter(|| {
            store.get(12).unwrap();
        });
    }
}

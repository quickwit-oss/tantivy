/*!
Compressed/slow/row-oriented storage for documents.

A field needs to be marked as stored in the schema in
order to be handled in the `Store`.

Internally, documents (or rather their stored fields) are serialized to a buffer.
When the buffer exceeds 16K, the buffer is compressed using `LZ4`
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

mod skiplist;
mod reader;
mod writer;
pub use self::reader::StoreReader;
pub use self::writer::StoreWriter;

#[cfg(test)]
pub mod tests {

    use super::*;
    use directory::{Directory, RAMDirectory, WritePtr};
    use schema::Document;
    use schema::FieldValue;
    use schema::TextOptions;
    use schema::{Schema, SchemaBuilder};
    use std::path::Path;

    pub fn write_lorem_ipsum_store(writer: WritePtr, num_docs: usize) -> Schema {
        let mut schema_builder = SchemaBuilder::default();
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
    fn test_store() {
        let path = Path::new("store");
        let mut directory = RAMDirectory::create();
        let store_file = directory.open_write(path).unwrap();
        let schema = write_lorem_ipsum_store(store_file, 1_000);
        let field_title = schema.get_field("title").unwrap();
        let store_source = directory.open_read(path).unwrap();
        let store = StoreReader::from_source(store_source);
        for i in 0..1_000 {
            assert_eq!(
                *store.get(i).unwrap().get_first(field_title).unwrap().text(),
                format!("Doc {}", i)
            );
        }
    }

}

#[cfg(all(test, feature = "unstable"))]
mod bench {

    use super::tests::write_lorem_ipsum_store;
    use directory::Directory;
    use directory::RAMDirectory;
    use std::path::Path;
    use store::StoreReader;
    use test::Bencher;

    #[bench]
    #[cfg(feature = "mmap")]
    fn bench_store_encode(b: &mut Bencher) {
        let mut directory = RAMDirectory::create();
        let path = Path::new("store");
        b.iter(|| {
            write_lorem_ipsum_store(directory.open_write(path).unwrap(), 1_000);
            directory.delete(path).unwrap();
        });
    }

    #[bench]
    fn bench_store_decode(b: &mut Bencher) {
        let mut directory = RAMDirectory::create();
        let path = Path::new("store");
        write_lorem_ipsum_store(directory.open_write(path).unwrap(), 1_000);
        let store_source = directory.open_read(path).unwrap();
        let store = StoreReader::from_source(store_source);
        b.iter(|| {
            store.get(12).unwrap();
        });
    }
}

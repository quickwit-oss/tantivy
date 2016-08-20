mod reader;
mod writer;

use DocId;
pub use self::reader::StoreReader;
pub use self::writer::StoreWriter;

#[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd)]
pub struct OffsetIndex(DocId, u64);

#[cfg(test)]
mod tests {

    use super::*;
    use test::Bencher;
    use std::path::Path;
    use schema::Schema;
    use schema::TextOptions;
    use schema::FieldValue;
    use directory::{RAMDirectory, Directory, MmapDirectory, WritePtr};

    fn write_lorem_ipsum_store(writer: WritePtr) -> Schema {
        let mut schema = Schema::new();
        let field_body = schema.add_text_field("body", TextOptions::new().set_stored());
        let field_title = schema.add_text_field("title", TextOptions::new().set_stored());
        let lorem = String::from("Doc Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.");
        {
            let mut store_writer = StoreWriter::new(writer);
            for i in 0..1000 {
                let mut fields: Vec<FieldValue> = Vec::new();
                {
                    let field_value = FieldValue {
                        field: field_body,
                        value: From::from(lorem.clone())
                    };
                    fields.push(field_value);
                }
                {
                    let title_text = format!("Doc {}", i);
                    let field_value = FieldValue {
                        field: field_title,
                        value: From::from(title_text)
                    };
                    fields.push(field_value);
                }
                let fields_refs: Vec<&FieldValue> = fields.iter().collect();
                store_writer.store(&fields_refs).unwrap();
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
        let schema = write_lorem_ipsum_store(store_file);
        let field_title = schema.get_field("title").unwrap();
        let store_source = directory.open_read(path).unwrap();
        let store = StoreReader::new(store_source);
        for i in (0..10).map(|i| i * 3 / 2) {
            assert_eq!(*store.get(i).unwrap().get_first(field_title).unwrap().text(), format!("Doc {}", i));
        }
    }

    #[bench]
    fn bench_store_encode(b: &mut Bencher) {
        let mut directory = MmapDirectory::create_from_tempdir().unwrap();
        let path = Path::new("store");
        b.iter(|| {
            write_lorem_ipsum_store(directory.open_write(path).unwrap());
            directory.delete(path).unwrap();
        });
    }


    #[bench]
    fn bench_store_decode(b: &mut Bencher) {
        let mut directory = MmapDirectory::create_from_tempdir().unwrap();
        let path = Path::new("store");
        write_lorem_ipsum_store(directory.open_write(path).unwrap());
        let store_source = directory.open_read(path).unwrap();
        let store = StoreReader::new(store_source);
        b.iter(|| {
            store.get(12).unwrap();
        });

    }
}

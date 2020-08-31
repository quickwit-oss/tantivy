mod reader;
mod writer;

pub use self::reader::BytesFastFieldReader;
pub use self::writer::BytesFastFieldWriter;

#[cfg(test)]
mod tests {
    use crate::schema::{Schema, BytesOptions, Value};
    use crate::{Index, Document};
    use crate::query::AllQuery;
    use crate::collector::TopDocs;

    #[test]
    fn test_bytes() {
        let mut schema_builder = Schema::builder();
        let field = schema_builder.add_bytes_field("bytesfield", BytesOptions::default());
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut index_writer = index.writer_for_tests().unwrap();
        index_writer.add_document(doc!(field=>vec![0u8, 1, 2, 3]));
        index_writer.add_document(doc!(field=>vec![]));
        index_writer.add_document(doc!(field=>vec![255u8]));
        index_writer.add_document(doc!(field=>vec![1u8, 3, 5, 7, 9]));
        index_writer.add_document(doc!(field=>vec![0u8; 1000]));
        assert!(index_writer.commit().is_ok());
        let searcher = index.reader().unwrap().searcher();
        let segment_reader = searcher.segment_reader(0);
        let bytes_reader = segment_reader.fast_fields().bytes(field).unwrap();

        assert_eq!(bytes_reader.get_bytes(0), &[0u8, 1, 2, 3]);
        assert!(bytes_reader.get_bytes(1).is_empty());
        assert_eq!(bytes_reader.get_bytes(2), &[255u8]);
        assert_eq!(bytes_reader.get_bytes(3), &[1u8, 3, 5, 7, 9]);
        let long = vec![0u8; 1000];
        assert_eq!(bytes_reader.get_bytes(4), long.as_slice());
    }

    #[test]
    fn test_indexed_bytes() {
        let mut schema_builder = Schema::builder();
        let options = BytesOptions::default()
            .set_indexed()
            .set_stored();
        let field = schema_builder.add_bytes_field("string_bytes", options);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema.clone());
        let mut index_writer = index.writer_with_num_threads(1, 3_000_000).unwrap();
        let mut doc = Document::new();
        doc.add_bytes(field, "tantivy".to_string().into_bytes());
        doc.add_bytes(field, "lucene".to_string().into_bytes());
        index_writer.add_document(doc);
        assert!(index_writer.commit().is_ok());
        let searcher = index.reader().unwrap().searcher();
        let docs = searcher.search(&AllQuery, &TopDocs::with_limit(10)).unwrap();
        for (_score, doc_address) in docs {
            let retrieved_doc = searcher.doc(doc_address);
            assert!(retrieved_doc.is_ok());
            let retrieved_doc = retrieved_doc.unwrap();
            let values = retrieved_doc.get_all(field);
            assert_eq!(values.len(), 2);
            match values.get(0).unwrap() {
                Value::Bytes(string) => {
                    let string_bytes = std::str::from_utf8(string).unwrap();
                    assert_eq!(string_bytes, "tantivy");
                }
                _ => {}
            }
            match values.get(1).unwrap() {
                Value::Bytes(string) => {
                    let string_bytes = std::str::from_utf8(string).unwrap();
                    assert_eq!(string_bytes, "lucene");
                }
                _ => {}
            }
        }
    }
}

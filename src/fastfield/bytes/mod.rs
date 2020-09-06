mod reader;
mod writer;

pub use self::reader::BytesFastFieldReader;
pub use self::writer::BytesFastFieldWriter;

#[cfg(test)]
mod tests {
    use crate::schema::{BytesOptions, Schema, Value};
    use crate::{DocAddress, Index, Searcher};
    use std::ops::Deref;

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

    fn create_index_for_test(
        byte_options: BytesOptions,
    ) -> crate::Result<impl Deref<Target = Searcher>> {
        let mut schema_builder = Schema::builder();
        let field = schema_builder.add_bytes_field("string_bytes", byte_options);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut index_writer = index.writer_for_tests()?;
        index_writer.add_document(doc!(
                field => b"tantivy".as_ref(),
                field => b"lucene".as_ref()
        ));
        index_writer.commit()?;
        Ok(index.reader()?.searcher())
    }

    #[test]
    fn test_stored_bytes() -> crate::Result<()> {
        let searcher = create_index_for_test(BytesOptions::default().set_stored())?;
        assert_eq!(searcher.num_docs(), 1);
        let retrieved_doc = searcher.doc(DocAddress(0u32, 0u32))?;
        let field = searcher.schema().get_field("string_bytes").unwrap();
        let values = retrieved_doc.get_all(field);
        assert_eq!(values.len(), 2);
        let values_bytes: Vec<&[u8]> = values
            .into_iter()
            .flat_map(|value| {
                if let Value::Bytes(bytes_value) = value {
                    Some(&bytes_value[..])
                } else {
                    None
                }
            })
            .collect();
        assert_eq!(values_bytes, &[&b"tantivy"[..], &b"lucene"[..]]);
        Ok(())
    }

    #[test]
    fn test_non_stored_bytes() -> crate::Result<()> {
        let searcher = create_index_for_test(BytesOptions::default())?;
        assert_eq!(searcher.num_docs(), 1);
        let retrieved_doc = searcher.doc(DocAddress(0u32, 0u32))?;
        let field = searcher.schema().get_field("string_bytes").unwrap();
        let values = retrieved_doc.get_all(field);
        assert!(values.is_empty());
        Ok(())
    }
}

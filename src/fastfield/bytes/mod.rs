mod reader;
mod writer;

pub use self::reader::BytesFastFieldReader;
pub use self::writer::BytesFastFieldWriter;

#[cfg(test)]
mod tests {
    use schema::SchemaBuilder;
    use Index;

    #[test]
    fn test_bytes() {
        let mut schema_builder = SchemaBuilder::default();
        let field = schema_builder.add_bytes_field("bytesfield");
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut index_writer = index.writer_with_num_threads(1, 3_000_000).unwrap();
        index_writer.add_document(doc!(field=>vec![0u8, 1, 2, 3]));
        index_writer.add_document(doc!(field=>vec![]));
        index_writer.add_document(doc!(field=>vec![255u8]));
        index_writer.add_document(doc!(field=>vec![1u8, 3, 5, 7, 9]));
        index_writer.add_document(doc!(field=>vec![0u8; 1000]));
        assert!(index_writer.commit().is_ok());

        index.load_searchers().unwrap();
        let searcher = index.searcher();
        let reader = searcher.segment_reader(0);
        let bytes_reader = reader.bytes_fast_field_reader(field).unwrap();

        assert_eq!(bytes_reader.get_val(0), &[0u8, 1, 2, 3]);
        assert!(bytes_reader.get_val(1).is_empty());
        assert_eq!(bytes_reader.get_val(2), &[255u8]);
        assert_eq!(bytes_reader.get_val(3), &[1u8, 3, 5, 7, 9]);
        let long = vec![0u8; 1000];
        assert_eq!(bytes_reader.get_val(4), long.as_slice());
    }
}
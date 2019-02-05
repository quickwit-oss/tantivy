mod reader;
mod writer;

pub use self::reader::MultiValueIntFastFieldReader;
pub use self::writer::MultiValueIntFastFieldWriter;

#[cfg(test)]
mod tests {

    use schema::Cardinality;
    use schema::Facet;
    use schema::IntOptions;
    use schema::Schema;
    use Index;

    #[test]
    fn test_multivalued_u64() {
        let mut schema_builder = Schema::builder();
        let field = schema_builder.add_u64_field(
            "multifield",
            IntOptions::default().set_fast(Cardinality::MultiValues),
        );
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut index_writer = index.writer_with_num_threads(1, 3_000_000).unwrap();
        index_writer.add_document(doc!(field=>1u64, field=>3u64));
        index_writer.add_document(doc!());
        index_writer.add_document(doc!(field=>4u64));
        index_writer.add_document(doc!(field=>5u64, field=>20u64,field=>1u64));
        assert!(index_writer.commit().is_ok());

        let searcher = index.reader().searcher();
        let segment_reader = searcher.segment_reader(0);
        let mut vals = Vec::new();
        let multi_value_reader = segment_reader
            .multi_fast_field_reader::<u64>(field)
            .unwrap();
        {
            multi_value_reader.get_vals(2, &mut vals);
            assert_eq!(&vals, &[4u64]);
        }
        {
            multi_value_reader.get_vals(0, &mut vals);
            assert_eq!(&vals, &[1u64, 3u64]);
        }
        {
            multi_value_reader.get_vals(1, &mut vals);
            assert!(vals.is_empty());
        }
    }

    #[test]
    fn test_multivalued_i64() {
        let mut schema_builder = Schema::builder();
        let field = schema_builder.add_i64_field(
            "multifield",
            IntOptions::default().set_fast(Cardinality::MultiValues),
        );
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut index_writer = index.writer_with_num_threads(1, 3_000_000).unwrap();
        index_writer.add_document(doc!(field=> 1i64, field => 3i64));
        index_writer.add_document(doc!());
        index_writer.add_document(doc!(field=> -4i64));
        index_writer.add_document(doc!(field=> -5i64, field => -20i64, field=>1i64));
        assert!(index_writer.commit().is_ok());

        let searcher = index.reader().searcher();
        let reader = searcher.segment_reader(0);
        let mut vals = Vec::new();
        let multi_value_reader = reader.multi_fast_field_reader::<i64>(field).unwrap();
        {
            multi_value_reader.get_vals(2, &mut vals);
            assert_eq!(&vals, &[-4i64]);
        }
        {
            multi_value_reader.get_vals(0, &mut vals);
            assert_eq!(&vals, &[1i64, 3i64]);
        }
        {
            multi_value_reader.get_vals(1, &mut vals);
            assert!(vals.is_empty());
        }
        {
            multi_value_reader.get_vals(3, &mut vals);
            assert_eq!(&vals, &[-5i64, -20i64, 1i64]);
        }
    }
    #[test]
    #[ignore]
    fn test_many_facets() {
        let mut schema_builder = Schema::builder();
        let field = schema_builder.add_facet_field("facetfield");
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut index_writer = index.writer_with_num_threads(1, 3_000_000).unwrap();
        for i in 0..100_000 {
            index_writer.add_document(doc!(field=> Facet::from(format!("/lang/{}", i).as_str())));
        }
        assert!(index_writer.commit().is_ok());
    }
}

mod reader;
mod writer;

pub use self::reader::MultiValueIntFastFieldReader;
pub use self::writer::MultiValueIntFastFieldWriter;

#[cfg(test)]
mod tests {

    use std::time::{SystemTime, UNIX_EPOCH};
    use query::QueryParser;
    use collector::TopDocs;
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

        index.load_searchers().unwrap();
        let searcher = index.searcher();
        let reader = searcher.segment_reader(0);
        let mut vals = Vec::new();
        let multi_value_reader = reader.multi_fast_field_reader::<u64>(field).unwrap();
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

    fn get_current_time_stamp() -> i64 {
        SystemTime::now()
                               .duration_since(UNIX_EPOCH)
                               .expect("could not get time stamp")
                               .as_secs() as i64
    }

    #[test]
    fn test_multivalued_date() {
        let mut schema_builder = Schema::builder();
        let date_field = schema_builder.add_date_field(
            "multi_date_field",
            IntOptions::default()
                .set_fast(Cardinality::MultiValues)
                .set_indexed()
                .set_stored(),
        );
        let time_i = schema_builder.add_i64_field(
            "time_stamp_i",
            IntOptions::default()
                .set_stored(),
        );
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut index_writer = index.writer_with_num_threads(1, 3_000_000).unwrap();
        let first_time_stamp = get_current_time_stamp();
        index_writer.add_document(doc!(date_field=>get_current_time_stamp(), date_field=>get_current_time_stamp(), time_i=>1i64));
        index_writer.add_document(doc!(time_i=>0i64));
        // add one second
        index_writer.add_document(doc!(date_field=>first_time_stamp+1, time_i=>2i64));
        // add another second
        index_writer.add_document(doc!(date_field=>first_time_stamp+2, date_field=>first_time_stamp+2,date_field=>first_time_stamp+2, time_i=>3i64));
        assert!(index_writer.commit().is_ok());

        index.load_searchers().unwrap();
        let searcher = index.searcher();
        let reader = searcher.segment_reader(0);
        assert_eq!(reader.num_docs(), 4);

        {
            let parser = QueryParser::for_index(&index, vec![date_field]);
            let query = parser.parse_query(&first_time_stamp.to_string().to_owned())
                .expect("could not parse query");
            let results = searcher.search(&query, &TopDocs::with_limit(5))
                .expect("could not query index");

            assert_eq!(results.len(), 1);

            for (_score, doc_address) in results {
                let retrieved_doc = searcher.doc(doc_address).expect("cannot fetch doc");
                assert_eq!(retrieved_doc.get_first(date_field).expect("cannot find value").i64_value(), first_time_stamp);
                assert_eq!(retrieved_doc.get_first(time_i).expect("cannot find value").i64_value(), 1i64);
            }
        }

        {
            let parser = QueryParser::for_index(&index, vec![date_field]);
            // let query = RangeQuery::new_i64(date_field, (first_time_stamp + 1)..(first_time_stamp + 2)); // Todo: implement date range query
            let query = parser.parse_query(&format!("{}..{}", first_time_stamp+1, first_time_stamp+3))
                .expect("could not parse query");
            let results = searcher.search(&query, &TopDocs::with_limit(5))
                .expect("could not query index");

            assert_eq!(results.len(), 2); // currently fails, seems like range queries do not work yet.
            for (_score, doc_address) in results {
                let retrieved_doc = searcher.doc(doc_address).expect("cannot fetch doc");
                println!("{:?}", index.schema().to_json(&retrieved_doc));
//                assert_eq!(retrieved_doc.get_first(date_field).expect("cannot find value").i64_value(), first_time_stamp);
//                assert_eq!(retrieved_doc.get_first(time_i).expect("cannot find value").i64_value(), 1i64);
            }
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

        index.load_searchers().unwrap();
        let searcher = index.searcher();
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

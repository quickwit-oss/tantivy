use std::cmp::Eq;
use std::collections::HashMap;
use std::hash::Hash;

use collector::Collector;
use fastfield::FastFieldReader;
use schema::Field;

use DocId;
use Result;
use Score;
use SegmentReader;
use SegmentLocalId;


/// Facet collector  for i64/u64 fast field
pub struct FacetCollector<T>
where
    T: FastFieldReader,
    T::ValueType: Eq + Hash,
{
    counters: HashMap<T::ValueType, u64>,
    field: Field,
    ff_reader: Option<T>,
}


impl<T> FacetCollector<T>
where
    T: FastFieldReader,
    T::ValueType: Eq + Hash,
{
    /// Creates a new facet collector for aggregating a given field.
    pub fn new(field: Field) -> FacetCollector<T> {
        FacetCollector {
            counters: HashMap::new(),
            field: field,
            ff_reader: None,
        }
    }
}


impl<T> Collector for FacetCollector<T>
where
    T: FastFieldReader,
    T::ValueType: Eq + Hash,
{
    fn set_segment(&mut self, _: SegmentLocalId, reader: &SegmentReader) -> Result<()> {
        self.ff_reader = Some(reader.get_fast_field_reader(self.field)?);
        Ok(())
    }

    fn collect(&mut self, doc: DocId, _: Score) {
        let val = self.ff_reader
            .as_ref()
            .expect(
                "collect() was called before set_segment. This should never happen.",
            )
            .get(doc);
        *(self.counters.entry(val).or_insert(0)) += 1;
    }
}



#[cfg(test)]
mod tests {

    use collector::{chain, FacetCollector};
    use query::QueryParser;
    use fastfield::{I64FastFieldReader, U64FastFieldReader};
    use schema::{self, FAST, STRING};
    use Index;

    #[test]
    // create 10 documents, set num field value to 0 or 1 for even/odd ones
    // make sure we have facet counters correctly filled
    fn test_facet_collector_results() {

        let mut schema_builder = schema::SchemaBuilder::new();
        let num_field_i64 = schema_builder.add_i64_field("num_i64", FAST);
        let num_field_u64 = schema_builder.add_u64_field("num_u64", FAST);
        let text_field = schema_builder.add_text_field("text", STRING);
        let schema = schema_builder.build();

        let index = Index::create_in_ram(schema.clone());

        {
            let mut index_writer = index.writer_with_num_threads(1, 40_000_000).unwrap();
            {
                for i in 0u64..10u64 {
                    index_writer.add_document(doc!(
                        num_field_i64 => ((i as i64) % 3i64) as i64,
                        num_field_u64 => (i % 2u64) as u64,
                        text_field => "text"
                    ));
                }
            }
            assert_eq!(index_writer.commit().unwrap(), 10u64);
        }

        index.load_searchers().unwrap();
        let searcher = index.searcher();
        let mut ffvf_i64: FacetCollector<I64FastFieldReader> = FacetCollector::new(num_field_i64);
        let mut ffvf_u64: FacetCollector<U64FastFieldReader> = FacetCollector::new(num_field_u64);

        {
            // perform the query
            let mut facet_collectors = chain().push(&mut ffvf_i64).push(&mut ffvf_u64);
            let mut query_parser = QueryParser::for_index(index, vec![text_field]);
            let query = query_parser.parse_query("text:text").unwrap();
            query.search(&searcher, &mut facet_collectors).unwrap();
        }

        assert_eq!(ffvf_u64.counters[&0], 5);
        assert_eq!(ffvf_u64.counters[&1], 5);
        assert_eq!(ffvf_i64.counters[&0], 4);
        assert_eq!(ffvf_i64.counters[&1], 3);

    }
}

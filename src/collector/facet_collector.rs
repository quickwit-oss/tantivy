use std::cmp::Eq;
use std::collections::HashMap;
use std::hash::Hash;

use collector::Collector;
use fastfield::{FastFieldReader, I64FastFieldReader, U64FastFieldReader};
use schema::Field;

use DocId;
use Result;
use Score;
use SegmentReader;
use SegmentLocalId;


/// faceting for i64/u64 fast field
pub struct FastFieldValueFacet<T>
    where T: FastFieldReader,
          T::ValueType: Eq + Hash
{
    counters: HashMap<T::ValueType, u64>,
    field: Field,
    ff_reader: Option<T>,
}


impl<T> FastFieldValueFacet<T>
    where T: FastFieldReader,
          T::ValueType: Eq + Hash
{
    fn new(field: Field) -> FastFieldValueFacet<T> {
        FastFieldValueFacet {
            counters: HashMap::new(),
            field: field,
            ff_reader: None,
        }
    }
}


impl<T> Collector for FastFieldValueFacet<T>
    where T: FastFieldReader,
          T::ValueType: Eq + Hash
{
    fn set_segment(&mut self, _: SegmentLocalId, reader: &SegmentReader) -> Result<()> {
        self.ff_reader = Some(try!(reader.get_fast_field_reader(self.field)));
        Ok(())
    }

    fn collect(&mut self, doc: DocId, _: Score) {
        let val = self.ff_reader.as_ref().unwrap().get(doc);
        *(self.counters.entry(val).or_insert(0)) += 1;
    }
}


enum FacedType {
    FastFieldI64(FastFieldValueFacet<I64FastFieldReader>),
    FastFieldU64(FastFieldValueFacet<U64FastFieldReader>),
}


pub struct FacetCollector {
    facets: Vec<FacedType>,
}


impl FacetCollector {
    fn new(facets: Vec<FacedType>) -> FacetCollector {
        FacetCollector { facets: facets }
    }
}


impl Collector for FacetCollector {
    fn set_segment(&mut self, segment_id: SegmentLocalId, reader: &SegmentReader) -> Result<()> {
        for facet_type in self.facets.iter_mut() {
            match facet_type {
                &mut FacedType::FastFieldI64(ref mut fast_field_value_facet) => {
                    fast_field_value_facet.set_segment(segment_id, reader)
                }
                &mut FacedType::FastFieldU64(ref mut fast_field_value_facet) => {
                    fast_field_value_facet.set_segment(segment_id, reader)
                }
            };
        }
        Ok(())
    }

    fn collect(&mut self, doc: DocId, score: Score) {
        for facet_type in self.facets.iter_mut() {
            match facet_type {
                &mut FacedType::FastFieldI64(ref mut fast_field_value_facet) => {
                    fast_field_value_facet.collect(doc, score)
                }
                &mut FacedType::FastFieldU64(ref mut fast_field_value_facet) => {
                    fast_field_value_facet.collect(doc, score)
                }
            }
        }
    }
}


#[cfg(test)]
mod tests {

    use super::*;
    use collector::FacetCollector;
    use query::QueryParser;
    use schema::{self, Document};
    use Index;

    #[test]
    // create 10 documents, set num field value to 0 or 1 for even/odd ones
    // make sure we have facet counters correctly filled
    fn test_facet_collector_results() {
        let mut schema_builder = schema::SchemaBuilder::new();
        let num_field_i64 =
            schema_builder.add_i64_field("num_i64",
                                         schema::IntOptions::default().set_fast().set_indexed());
        let num_field_u64 =
            schema_builder.add_u64_field("num_u64",
                                         schema::IntOptions::default().set_fast().set_indexed());

        let text_field = schema_builder.add_text_field(
            "text",
            schema::TextOptions::default()
                .set_indexing_options(schema::TextIndexingOptions::Untokenized)
            );

        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema.clone());

        {
            let mut index_writer = index.writer_with_num_threads(1, 40_000_000).unwrap();
            {
                for i in 1..11 {
                    let mut doc = Document::default();
                    doc.add_i64(num_field_i64, i % 2);
                    doc.add_u64(num_field_u64, (i % 2) as u64);
                    doc.add_text(text_field, "text");
                    index_writer.add_document(doc);
                }
            }
            assert_eq!(index_writer.commit().unwrap(), 10u64);
        }

        index.load_searchers().unwrap();
        let searcher = index.searcher();
        let ffvf_i64 = FastFieldValueFacet::new(num_field_i64);
        let ffvf_u64 = FastFieldValueFacet::new(num_field_u64);
        let mut facet_collector = FacetCollector::new(vec![FacedType::FastFieldI64(ffvf_i64),
                                                           FacedType::FastFieldU64(ffvf_u64)]);

        let query_parser = QueryParser::new(schema, vec![text_field]);
        let query = query_parser.parse_query("text:text").unwrap();
        query.search(&searcher, &mut facet_collector).unwrap();
        for facet in facet_collector.facets {
            match facet {
                FacedType::FastFieldI64(ffvf) => {
                    assert_eq!(ffvf.counters[&0], 5);
                    assert_eq!(ffvf.counters[&1], 5);
                }
                FacedType::FastFieldU64(ffvf) => {
                    assert_eq!(ffvf.counters[&0], 5);
                    assert_eq!(ffvf.counters[&1], 5);
                }

            }
        }
    }
}

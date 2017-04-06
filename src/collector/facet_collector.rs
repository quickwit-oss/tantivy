use std::collections::HashMap;

use collector::Collector;
use fastfield::{FastFieldReader, U64FastFieldReader};
use schema::Field;

use DocId;
use Result;
use Score;
use SegmentReader;
use SegmentLocalId;


/// faceting for u64 fast field
pub struct FastFieldValueFacet {
    counters: HashMap<u64, u64>,
    field: Field,
    ff_reader: Option<U64FastFieldReader>,
}


impl FastFieldValueFacet {
    fn new(field: Field) -> FastFieldValueFacet {
        FastFieldValueFacet {
            counters: HashMap::new(),
            field: field,
            ff_reader: None,
        }
    }
}


impl Collector for FastFieldValueFacet {

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
    FastField(FastFieldValueFacet)
}


pub struct FacetCollector {
    facets: Vec<FacedType>
}


impl FacetCollector {

    fn new(facets: Vec<FacedType>) -> FacetCollector {
        FacetCollector {
            facets: facets
        }
    }

}


impl Collector for FacetCollector {

    fn set_segment(&mut self, segment_id: SegmentLocalId, reader: &SegmentReader) -> Result<()> {
       for facet_type in self.facets.iter_mut() {
            match facet_type {
                 &mut FacedType::FastField(ref mut fast_field_value_facet) => fast_field_value_facet.set_segment(segment_id, reader)
            };
        };
        Ok(())
    }

    fn collect(&mut self, doc: DocId, score: Score) {
        for facet_type in self.facets.iter_mut() {
            match facet_type {
                 &mut FacedType::FastField(ref mut fast_field_value_facet) => fast_field_value_facet.collect(doc, score)
            }
        };
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
		let num_field = schema_builder.add_u64_field(
            "num",
            schema::IntOptions::default()
                .set_fast()
                .set_indexed()
            );
		let text_field = schema_builder.add_text_field(
            "text",
            schema::TextOptions::default()
                //.set_stored()
                .set_indexing_options(schema::TextIndexingOptions::Untokenized)
            );

        let schema = schema_builder.build();
		let index = Index::create_in_ram(schema.clone());

        {
 			let mut index_writer = index.writer_with_num_threads(1, 40_000_000).unwrap();
			{
                for i in 1..11 {
				    let mut doc = Document::default();
				    doc.add_u64(num_field, i % 2);
				    doc.add_text(text_field, "text");
				    index_writer.add_document(doc);
                }
			}
			assert_eq!(index_writer.commit().unwrap(), 10u64);
        }

        index.load_searchers().unwrap();
        let searcher = index.searcher();
        let ffvf = FastFieldValueFacet::new(num_field);
        let mut facet_collector = FacetCollector::new(vec![FacedType::FastField(ffvf)]);

        let query_parser = QueryParser::new(schema, vec!(text_field));
        let query = query_parser.parse_query("text:text").unwrap();
        query.search(&searcher, &mut facet_collector).unwrap();
        for facet in facet_collector.facets {
            match facet {
                FacedType::FastField(ffvf) => {
                    assert_eq!(ffvf.counters[&0], 5);
                    assert_eq!(ffvf.counters[&1], 5);
                }
            }
        }
    }
}

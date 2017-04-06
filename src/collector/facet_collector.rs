use std::io;
use super::Collector;
use fastfield::U32FastFieldReader;
use schema::Field;
use ScoredDoc;
use SegmentReader;
use SegmentLocalId;
use std::collections::HashMap;


/// top-n-values facet for u32 fast field
pub struct FastFieldValueFacet {
    counters: HashMap<u32, u32>,
    field: Field,
    ff_reader: Option<U32FastFieldReader>,
    limit: usize,
    name: String,
}

impl FastFieldValueFacet {
    fn new(name: String, field: Field) -> FastFieldValueFacet {
        FastFieldValueFacet {
            counters: HashMap::new(),
            field: field,
            ff_reader: None,
            limit: 10,
            name: name,
        }
    }

    fn set_limit(&mut self, limit: usize) -> &mut FastFieldValueFacet {
        self.limit = limit;
        self
    }
}


impl Collector for FastFieldValueFacet {

    fn set_segment(&mut self, _: SegmentLocalId, reader: &SegmentReader) -> io::Result<()> {
        self.ff_reader = Some(try!(reader.get_fast_field_reader(self.field)));
        Ok(())
    }

    fn collect(&mut self, scored_doc: ScoredDoc) {
        let val = self.ff_reader.as_ref().unwrap().get(scored_doc.doc());
        *(self.counters.entry(val).or_insert(0)) += 1;
    }
} 

enum FacedType {
    FastField(FastFieldValueFacet)
}



pub struct FacetCollector {
    segment_id: u32,
    facets: Vec<FacedType>
}

impl FacetCollector {

    fn new(facets: Vec<FacedType>) -> FacetCollector {
        FacetCollector {
            segment_id: 0,
            facets: facets
        }
    }

}

impl Collector for FacetCollector {

    fn set_segment(&mut self, segment_id: SegmentLocalId, reader: &SegmentReader) -> io::Result<()> {
        self.segment_id = segment_id;
        for facet_type in self.facets.iter_mut() {
            match facet_type {
                 &mut FacedType::FastField(ref mut fast_field_value_facet) => fast_field_value_facet.set_segment(segment_id, reader)
            };
        };
        Ok(())
    }

    fn collect(&mut self, scored_doc: ScoredDoc) {
        for facet_type in self.facets.iter_mut() {
            match facet_type {
                 &mut FacedType::FastField(ref mut fast_field_value_facet) => fast_field_value_facet.collect(scored_doc)
            }
        };
    }
}


#[cfg(test)]
mod tests {


    use super::*;
    use collector::FacetCollector;
    use query::QueryParser;
    use query::Query;
	use schema::{self, Document};
	use Index;

    #[test]
    fn test_facet_collector_results() {
		let mut schema_builder = schema::SchemaBuilder::new();
		let num_field = schema_builder.add_u32_field(
            "num",
            schema::U32Options::new()
                .set_fast()
                .set_indexed()
            );
		let text_field = schema_builder.add_text_field("text", schema::TEXT);

        let schema = schema_builder.build();
		let index = Index::create_in_ram(schema.clone());

        {
 			let mut index_writer = index.writer_with_num_threads(1, 40_000_000).unwrap();
			{
                for i in 1..11 {
				    let mut doc = Document::new();
				    doc.add_u32(num_field, i % 2);
				    doc.add_text(text_field, "text");
				    index_writer.add_document(doc).unwrap();
                }
			}
			assert_eq!(index_writer.commit().unwrap(), 10u64);
        }

        let searcher = index.searcher();
        let ffvf = FastFieldValueFacet::new("num_facet".to_string(), num_field);
        let mut facet_collector = FacetCollector::new(vec![FacedType::FastField(ffvf)]);

        let query_parser = QueryParser::new(schema, vec!(num_field));
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

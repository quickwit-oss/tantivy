use std::io;
use super::Collector;
use fastfield::U32FastFieldReader;
use schema::{Field, FieldEntry};
use ScoredDoc;
use SegmentReader;
use SegmentLocalId;
use DocAddress;
use std::collections::{BinaryHeap, HashMap};
use std::cmp::Ordering;
use Score;
/*
// Rust heap is a max-heap and we need a min heap.
#[derive(Clone, Copy)]
struct GlobalScoredDoc(Score, DocAddress);

impl PartialOrd for GlobalScoredDoc {
    fn partial_cmp(&self, other: &GlobalScoredDoc) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for GlobalScoredDoc {
    #[inline(always)]
    fn cmp(&self, other: &GlobalScoredDoc) -> Ordering {
        other.0.partial_cmp(&self.0)
        .unwrap_or(
            other.1.cmp(&self.1)
        )
    }
}

impl PartialEq for GlobalScoredDoc {
    fn eq(&self, other: &GlobalScoredDoc) -> bool {
        self.cmp(&other) == Ordering::Equal
    }
}

impl Eq for GlobalScoredDoc {}
*/


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

    fn set_segment(&mut self, segment_id: SegmentLocalId, reader: &SegmentReader) -> io::Result<()> {
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
    //field: String,
    //heap: BinaryHeap<GlobalScoredDoc>,
    segment_id: u32,
    facets: Vec<FacedType>
}

impl FacetCollector {

    /*
    /// Creates a facet collector, with a number of values of "limit"
    ///
    /// # Panics
    /// The method panics if limit is 0
    pub fn with_limit(limit: usize) -> FacetCollector {
        if limit < 1 {
            panic!("Limit must be strictly greater than 0.");
        }
        FacetCollector {
            limit: limit,
            //heap: BinaryHeap::with_capacity(limit),
            segment_id: 0,
        }
    }*/

    /*pub fn docs(&self) -> Vec<DocAddress> {
        self.score_docs()
            .into_iter()
            .map(|score_doc| score_doc.1)
            .collect()
    }*/

   /* pub fn score_docs(&self) -> Vec<(Score, DocAddress)> {
        let mut scored_docs: Vec<GlobalScoredDoc> = self.heap
            .iter()
            .cloned()
            .collect();
        scored_docs.sort();
        scored_docs.into_iter()
            .map(|GlobalScoredDoc(score, doc_address)| (score, doc_address))
            .collect()
    }*/

    #[inline(always)]
    pub fn at_capacity(&self, ) -> bool {
        //self.heap.len() >= self.limit
        false
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

/*
#[cfg(test)]
mod tests {

    use super::*;
    use ScoredDoc;
    use DocId;
    use Score;
    use collector::Collector;

    #[test]
    fn test_top_collector_not_at_capacity() {
        let mut top_collector = TopCollector::with_limit(4);
        top_collector.collect(ScoredDoc(0.8, 1));
        top_collector.collect(ScoredDoc(0.2, 3));
        top_collector.collect(ScoredDoc(0.3, 5));
        assert!(!top_collector.at_capacity());
        let score_docs: Vec<(Score, DocId)> = top_collector.score_docs()
            .into_iter()
            .map(|(score, doc_address)| (score, doc_address.doc()))
            .collect();
        assert_eq!(score_docs, vec!(
            (0.8, 1), (0.3, 5), (0.2, 3),
        ));
    }

    #[test]
    fn test_top_collector_at_capacity() {
        let mut top_collector = TopCollector::with_limit(4);
        top_collector.collect(ScoredDoc(0.8, 1));
        top_collector.collect(ScoredDoc(0.2, 3));
        top_collector.collect(ScoredDoc(0.3, 5));
        top_collector.collect(ScoredDoc(0.9, 7));
        top_collector.collect(ScoredDoc(-0.2, 9));
        assert!(top_collector.at_capacity());
        {
            let score_docs: Vec<(Score, DocId)> = top_collector
                .score_docs()
                .into_iter()
                .map(|(score, doc_address)| (score, doc_address.doc()))
                .collect();
            assert_eq!(score_docs, vec!(
                (0.9, 7), (0.8, 1), (0.3, 5), (0.2, 3)
            ));
        }
        {
            let docs: Vec<DocId> = top_collector
                .docs()
                .into_iter()
                .map(|doc_address| doc_address.doc())
                .collect();
            assert_eq!(docs, vec!(7, 1, 5, 3));
        }
        
        
    }

    #[test]
    #[should_panic]
    fn test_top_0() {
        TopCollector::with_limit(0);
    }
}*/

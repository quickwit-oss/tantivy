use common::BitSet;

use crate::{Score, Searcher, SegmentReader, schema::Field};

use super::{BitSetDocSet, ConstScorer, Query, Weight};

#[derive(Clone, Debug)]
pub struct VectorQuery {
    field: Field,
    vector: Vec<f32>
}

impl VectorQuery {
    pub fn new(field: Field, vector: Vec<f32>) -> VectorQuery {
        VectorQuery {
            field,
            vector
        }
    }
}

impl Query for VectorQuery {
    fn weight(
        &self,
        searcher: &Searcher,
        scoring_enabled: bool,
    ) -> crate::Result<Box<dyn super::Weight>> {

        let schema = searcher.schema();
        // Check dimension of the vector.
        
        Ok(Box::new(VectorWeight {
            field: self.field,
            vector: self.vector
        }))
    }
}

pub struct VectorWeight {
    field: Field,
    vector: Vec<f32>
}

impl Weight for VectorWeight {
    fn scorer(
        &self,
        reader: &SegmentReader,
        boost: Score,
    ) -> crate::Result<Box<dyn super::Scorer>> {
        let max_doc = reader.max_doc();
        let mut doc_bitset = BitSet::with_max_value(max_doc);

        let reader = reader.vector_reader(self.field)?;
        let docs = reader.search(self.vector, 50);

        for doc in docs {
            doc_bitset.insert(doc);
        }

        let doc_bitset = BitSetDocSet::from(doc_bitset);
        Ok(Box::new(ConstScorer::new(doc_bitset, boost)))
    }

    fn explain(
        &self,
        reader: &crate::SegmentReader,
        doc: crate::DocId,
    ) -> crate::Result<super::Explanation> {


        todo!()
    }
}

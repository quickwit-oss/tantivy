use crate::{schema::Field, vector::VectorReader};

use super::{Collector, SegmentCollector};

/// Collector for vectors
///
/// The collector collects all vectors though all segments.
pub struct VectorCollector {
    field: Field
}

impl Collector for VectorCollector {
    type Fruit = Vec<f32>;
    type Child = VectorSegmentCollector;

    fn for_segment(
        &self,
        segment_local_id: crate::SegmentOrdinal,
        reader: &crate::SegmentReader,
    ) -> crate::Result<Self::Child> {
        trace!("for_segment");
        trace!("Segment local id: {}", segment_local_id);

        let vector_reader = reader.vector_reader(self.field)?;

        Ok(VectorSegmentCollector {
            reader: vector_reader
        })
    }

    fn requires_scoring(&self) -> bool {
        // TODO: Decide
        false
    }

    fn merge_fruits(
        &self,
        segment_fruits: Vec<<Self::Child as super::SegmentCollector>::Fruit>,
    ) -> crate::Result<Self::Fruit> {
        todo!()
    }

    
}

pub struct VectorSegmentCollector {
    reader: VectorReader
}

impl SegmentCollector for VectorSegmentCollector {
    type Fruit = Vec<f32>;

    fn collect(&mut self, doc: crate::DocId, score: crate::Score) {
        debug!("Calling collec on docId: {} score: {}", doc, score);
    }

    fn harvest(self) -> Self::Fruit {
        debug!("Harvest!");
        let v: Vec<f32> = vec![0.0,1.0,2.0,3.0];
        return v;
    }
}

impl VectorCollector {
    pub fn for_field(field: Field) -> VectorCollector {
        VectorCollector { field }
    }
}
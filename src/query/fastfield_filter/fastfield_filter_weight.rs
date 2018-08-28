use query::Weight;
use schema::Field;
use query::fastfield_filter::RangeU64;
use query::fastfield_filter::FastFieldFilterScorer;
use SegmentReader;
use query::Scorer;
use TantivyError;
use fastfield::FastFieldReader;

pub struct FastFieldFilterWeight {
    field: Field,
    range: RangeU64,
}

impl FastFieldFilterWeight {
    pub(crate) fn new(field: Field, range: RangeU64) -> FastFieldFilterWeight {
        FastFieldFilterWeight {
            field,
            range
        }
    }
}

impl Weight for FastFieldFilterWeight {
    fn scorer(&self, reader: &SegmentReader) -> Result<Box<Scorer>, TantivyError> {
        let fastfield_reader: FastFieldReader<u64> = reader.fast_field_reader(self.field    )?;
        Ok(Box::new(FastFieldFilterScorer::new(fastfield_reader, self.range.clone(), reader.max_doc())))
    }
}
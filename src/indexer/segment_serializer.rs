use crate::core::Segment;
use crate::core::SegmentComponent;
use crate::fastfield::FastFieldSerializer;
use crate::fieldnorm::FieldNormsSerializer;
use crate::postings::InvertedIndexSerializer;
use crate::store::StoreWriter;

/// Segment serializer is in charge of laying out on disk
/// the data accumulated and sorted by the `SegmentWriter`.
pub struct SegmentSerializer {
    segment: Segment,
    store_writer: StoreWriter,
    fast_field_serializer: FastFieldSerializer,
    fieldnorms_serializer: Option<FieldNormsSerializer>,
    postings_serializer: InvertedIndexSerializer,
}

impl SegmentSerializer {
    /// Creates a new `SegmentSerializer`.
    pub fn for_segment(mut segment: Segment) -> crate::Result<SegmentSerializer> {
        let store_write = segment.open_write(SegmentComponent::STORE)?;

        let fast_field_write = segment.open_write(SegmentComponent::FASTFIELDS)?;
        let fast_field_serializer = FastFieldSerializer::from_write(fast_field_write)?;

        let fieldnorms_write = segment.open_write(SegmentComponent::FIELDNORMS)?;
        let fieldnorms_serializer = FieldNormsSerializer::from_write(fieldnorms_write)?;

        let postings_serializer = InvertedIndexSerializer::open(&mut segment)?;
        Ok(SegmentSerializer {
            segment,
            store_writer: StoreWriter::new(store_write),
            fast_field_serializer,
            fieldnorms_serializer: Some(fieldnorms_serializer),
            postings_serializer,
        })
    }

    pub fn segment(&self) -> &Segment {
        &self.segment
    }

    /// Accessor to the `PostingsSerializer`.
    pub fn get_postings_serializer(&mut self) -> &mut InvertedIndexSerializer {
        &mut self.postings_serializer
    }

    /// Accessor to the `FastFieldSerializer`.
    pub fn get_fast_field_serializer(&mut self) -> &mut FastFieldSerializer {
        &mut self.fast_field_serializer
    }

    /// Extract the field norm serializer.
    ///
    /// Note the fieldnorms serializer can only be extracted once.
    pub fn extract_fieldnorms_serializer(&mut self) -> Option<FieldNormsSerializer> {
        self.fieldnorms_serializer.take()
    }

    /// Accessor to the `StoreWriter`.
    pub fn get_store_writer(&mut self) -> &mut StoreWriter {
        &mut self.store_writer
    }

    /// Finalize the segment serialization.
    pub fn close(mut self) -> crate::Result<()> {
        if let Some(fieldnorms_serializer) = self.extract_fieldnorms_serializer() {
            fieldnorms_serializer.close()?;
        }
        self.fast_field_serializer.close()?;
        self.postings_serializer.close()?;
        self.store_writer.close()?;
        Ok(())
    }
}

use Result;

use core::Segment;
use core::SegmentComponent;
use fastfield::FastFieldSerializer;
use store::StoreWriter;
use postings::PostingsSerializer;


/// Segment serializer is in charge of laying out on disk
/// the data accumulated and sorted by the `SegmentWriter`.
pub struct SegmentSerializer {
    store_writer: StoreWriter,
    fast_field_serializer: FastFieldSerializer,
    fieldnorms_serializer: FastFieldSerializer,
    postings_serializer: PostingsSerializer,
}

impl SegmentSerializer {
    /// Creates a new `SegmentSerializer`.
    pub fn for_segment(segment: &mut Segment) -> Result<SegmentSerializer> {
        let store_write = try!(segment.open_write(SegmentComponent::STORE));

        let fast_field_write = try!(segment.open_write(SegmentComponent::FASTFIELDS));
        let fast_field_serializer = try!(FastFieldSerializer::new(fast_field_write));

        let fieldnorms_write = try!(segment.open_write(SegmentComponent::FIELDNORMS));
        let fieldnorms_serializer = try!(FastFieldSerializer::new(fieldnorms_write));

        let postings_serializer = try!(PostingsSerializer::open(segment));
        Ok(SegmentSerializer {
               postings_serializer: postings_serializer,
               store_writer: StoreWriter::new(store_write),
               fast_field_serializer: fast_field_serializer,
               fieldnorms_serializer: fieldnorms_serializer,
           })
    }

    /// Accessor to the `PostingsSerializer`.
    pub fn get_postings_serializer(&mut self) -> &mut PostingsSerializer {
        &mut self.postings_serializer
    }

    /// Accessor to the `FastFieldSerializer`.
    pub fn get_fast_field_serializer(&mut self) -> &mut FastFieldSerializer {
        &mut self.fast_field_serializer
    }

    /// Accessor to the field norm serializer.
    pub fn get_fieldnorms_serializer(&mut self) -> &mut FastFieldSerializer {
        &mut self.fieldnorms_serializer
    }

    /// Accessor to the `StoreWriter`.
    pub fn get_store_writer(&mut self) -> &mut StoreWriter {
        &mut self.store_writer
    }

    /// Finalize the segment serialization.
    pub fn close(self) -> Result<()> {
        self.fast_field_serializer.close()?;
        self.postings_serializer.close()?;
        self.store_writer.close()?;
        self.fieldnorms_serializer.close()?;
        Ok(())
    }
}

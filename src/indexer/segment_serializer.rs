use Result;
use Error;

use std::io::Write;
use rustc_serialize::json;
use core::index::Segment;
use core::index::SegmentInfo;
use core::SegmentComponent;
use fastfield::FastFieldSerializer;
use store::StoreWriter;
use postings::PostingsSerializer;

pub struct SegmentSerializer {
    segment: Segment,
    store_writer: StoreWriter,
    fast_field_serializer: FastFieldSerializer,
    fieldnorms_serializer: FastFieldSerializer,
    postings_serializer: PostingsSerializer,
}

impl SegmentSerializer {

    pub fn for_segment(segment: &mut Segment) -> Result<SegmentSerializer>  {
        let store_write = try!(segment.open_write(SegmentComponent::STORE));

        let fast_field_write = try!(segment.open_write(SegmentComponent::FASTFIELDS));
        let fast_field_serializer = try!(FastFieldSerializer::new(fast_field_write));

        let fieldnorms_write = try!(segment.open_write(SegmentComponent::FIELDNORMS));
        let fieldnorms_serializer = try!(FastFieldSerializer::new(fieldnorms_write));

        let postings_serializer = try!(PostingsSerializer::open(segment));
        Ok(SegmentSerializer {
            segment: segment.clone(),
            postings_serializer: postings_serializer,
            store_writer: StoreWriter::new(store_write),
            fast_field_serializer: fast_field_serializer,
            fieldnorms_serializer: fieldnorms_serializer,
        })
    }

    pub fn get_postings_serializer(&mut self,) -> &mut PostingsSerializer {
        &mut self.postings_serializer
    }

    pub fn get_fast_field_serializer(&mut self,) -> &mut FastFieldSerializer {
        &mut self.fast_field_serializer
    }
    
    pub fn get_fieldnorms_serializer(&mut self,) -> &mut FastFieldSerializer {
        &mut self.fieldnorms_serializer
    }

    pub fn get_store_writer(&mut self,) -> &mut StoreWriter {
        &mut self.store_writer
    }

    pub fn write_segment_info(&mut self, segment_info: &SegmentInfo) -> Result<()> {
        let mut write = try!(self.segment.open_write(SegmentComponent::INFO));
        let json_data = try!(
            json::encode(segment_info)
            .map_err(|err| Error::Other(Box::new(err)))
        );
        try!(write.write_all(json_data.as_bytes()));
        try!(write.flush());
        Ok(())
    }

    pub fn close(mut self,) -> Result<()> {
        try!(self.fast_field_serializer.close());
        try!(self.postings_serializer.close());
        try!(self.store_writer.close());
        try!(self.fieldnorms_serializer.close());
        Ok(())
    }
}

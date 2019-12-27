use crate::Directory;

use crate::core::Segment;
use crate::core::SegmentComponent;
use crate::directory::error::OpenWriteError;
use crate::directory::{DirectoryClone, RAMDirectory, TerminatingWrite, WritePtr};
use crate::fastfield::FastFieldSerializer;
use crate::fieldnorm::FieldNormsSerializer;
use crate::postings::InvertedIndexSerializer;
use crate::schema::Schema;
use crate::store::StoreWriter;

/// Segment serializer is in charge of laying out on disk
/// the data accumulated and sorted by the `SegmentWriter`.
pub struct SegmentSerializer {
    store_writer: StoreWriter,
    fast_field_serializer: FastFieldSerializer,
    fieldnorms_serializer: FieldNormsSerializer,
    postings_serializer: InvertedIndexSerializer,
    bundle_writer: Option<(RAMDirectory, WritePtr)>,
}

pub(crate) struct SegmentSerializerWriters {
    postings_wrt: WritePtr,
    positions_skip_wrt: WritePtr,
    positions_wrt: WritePtr,
    terms_wrt: WritePtr,
    fast_field_wrt: WritePtr,
    fieldnorms_wrt: WritePtr,
    store_wrt: WritePtr,
}

impl SegmentSerializerWriters {
    pub(crate) fn for_segment(segment: &mut Segment) -> Result<Self, OpenWriteError> {
        Ok(SegmentSerializerWriters {
            postings_wrt: segment.open_write(SegmentComponent::POSTINGS)?,
            positions_skip_wrt: segment.open_write(SegmentComponent::POSITIONS)?,
            positions_wrt: segment.open_write(SegmentComponent::POSITIONSSKIP)?,
            terms_wrt: segment.open_write(SegmentComponent::TERMS)?,
            fast_field_wrt: segment.open_write(SegmentComponent::FASTFIELDS)?,
            fieldnorms_wrt: segment.open_write(SegmentComponent::FIELDNORMS)?,
            store_wrt: segment.open_write(SegmentComponent::STORE)?,
        })
    }
}

impl SegmentSerializer {
    pub(crate) fn new(schema: Schema, writers: SegmentSerializerWriters) -> crate::Result<Self> {
        let fast_field_serializer = FastFieldSerializer::from_write(writers.fast_field_wrt)?;
        let fieldnorms_serializer = FieldNormsSerializer::from_write(writers.fieldnorms_wrt)?;
        let postings_serializer = InvertedIndexSerializer::open(
            schema,
            writers.terms_wrt,
            writers.postings_wrt,
            writers.positions_wrt,
            writers.positions_skip_wrt,
        );
        Ok(SegmentSerializer {
            store_writer: StoreWriter::new(writers.store_wrt),
            fast_field_serializer,
            fieldnorms_serializer,
            postings_serializer,
            bundle_writer: None,
        })
    }

    /// Accessor to the `PostingsSerializer`.
    pub fn get_postings_serializer(&mut self) -> &mut InvertedIndexSerializer {
        &mut self.postings_serializer
    }

    /// Accessor to the `FastFieldSerializer`.
    pub fn get_fast_field_serializer(&mut self) -> &mut FastFieldSerializer {
        &mut self.fast_field_serializer
    }

    /// Accessor to the field norm serializer.
    pub fn get_fieldnorms_serializer(&mut self) -> &mut FieldNormsSerializer {
        &mut self.fieldnorms_serializer
    }

    /// Accessor to the `StoreWriter`.
    pub fn get_store_writer(&mut self) -> &mut StoreWriter {
        &mut self.store_writer
    }

    /// Finalize the segment serialization.
    pub fn close(mut self) -> crate::Result<()> {
        self.fast_field_serializer.close()?;
        self.postings_serializer.close()?;
        self.store_writer.close()?;
        self.fieldnorms_serializer.close()?;
        if let Some((ram_directory, mut bundle_wrt)) = self.bundle_writer.take() {
            ram_directory.serialize_bundle(&mut bundle_wrt)?;
            bundle_wrt.terminate()?;
        }
        Ok(())
    }
}

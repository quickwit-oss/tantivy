use super::{fieldnorm_to_id, id_to_fieldnorm};
use crate::common::CompositeFile;
use crate::directory::ReadOnlySource;
use crate::schema::Field;
use crate::space_usage::PerFieldSpaceUsage;
use crate::DocId;
use std::sync::Arc;

/// Reader for the fieldnorm (for each document, the number of tokens indexed in the
/// field) of all indexed fields in the index.
///
/// Each fieldnorm is approximately compressed over one byte. We refer to this byte as
/// `fieldnorm_id`.
/// The mapping from `fieldnorm` to `fieldnorm_id` is given by monotonic.
#[derive(Clone)]
pub struct FieldNormReaders {
    data: Arc<CompositeFile>,
}

impl FieldNormReaders {
    /// Creates a field norm reader.
    pub fn new(source: ReadOnlySource) -> crate::Result<FieldNormReaders> {
        let data = CompositeFile::open(&source)?;
        Ok(FieldNormReaders {
            data: Arc::new(data),
        })
    }

    /// Returns the FieldNormReader for a specific field.
    pub fn get_field(&self, field: Field) -> Option<FieldNormReader> {
        self.data.open_read(field).map(FieldNormReader::open)
    }

    /// Return a break down of the space usage per field.
    pub fn space_usage(&self) -> PerFieldSpaceUsage {
        self.data.space_usage()
    }
}

/// Reads the fieldnorm associated to a document.
/// The fieldnorm represents the length associated to
/// a given Field of a given document.
///
/// This metric is important to compute the score of a
/// document : a document having a query word in one its short fields
/// (e.g. title)  is likely to be more relevant than in one of its longer field
/// (e.g. body).
///
/// tantivy encodes `fieldnorm` on one byte with some precision loss,
/// using the same scheme as Lucene. Each value is place on a log-scale
/// that takes values from `0` to `255`.
///
/// A value on this scale is identified by a `fieldnorm_id`.
/// Apart from compression, this scale also makes it possible to
/// precompute computationally expensive functions of the fieldnorm
/// in a very short array.
#[derive(Clone)]
pub struct FieldNormReader {
    data: ReadOnlySource,
}

impl FieldNormReader {
    /// Opens a field norm reader given its data source.
    pub fn open(data: ReadOnlySource) -> Self {
        FieldNormReader { data }
    }

    /// Returns the number of documents in this segment.
    pub fn num_docs(&self) -> u32 {
        self.data.len() as u32
    }

    /// Returns the `fieldnorm` associated to a doc id.
    /// The fieldnorm is a value approximating the number
    /// of tokens in a given field of the `doc_id`.
    ///
    /// It is imprecise, and always lower than the actual
    /// number of tokens.
    ///
    /// The fieldnorm is effectively decoded from the
    /// `fieldnorm_id` by doing a simple table lookup.
    pub fn fieldnorm(&self, doc_id: DocId) -> u32 {
        let fieldnorm_id = self.fieldnorm_id(doc_id);
        id_to_fieldnorm(fieldnorm_id)
    }

    /// Returns the `fieldnorm_id` associated to a document.
    #[inline(always)]
    pub fn fieldnorm_id(&self, doc_id: DocId) -> u8 {
        let fielnorms_data = self.data.as_slice();
        fielnorms_data[doc_id as usize]
    }

    /// Converts a `fieldnorm_id` into a fieldnorm.
    #[inline(always)]
    pub fn id_to_fieldnorm(id: u8) -> u32 {
        id_to_fieldnorm(id)
    }

    /// Converts a `fieldnorm` into a `fieldnorm_id`.
    /// (This function is not injective).
    #[inline(always)]
    pub fn fieldnorm_to_id(fieldnorm: u32) -> u8 {
        fieldnorm_to_id(fieldnorm)
    }
}

#[cfg(test)]
impl From<&[u32]> for FieldNormReader {
    fn from(field_norms: &[u32]) -> FieldNormReader {
        let field_norms_id = field_norms
            .iter()
            .cloned()
            .map(FieldNormReader::fieldnorm_to_id)
            .collect::<Vec<u8>>();
        let field_norms_data = ReadOnlySource::from(field_norms_id);
        FieldNormReader {
            data: field_norms_data,
        }
    }
}

use super::{fieldnorm_to_id, id_to_fieldnorm};
use crate::directory::ReadOnlySource;
use crate::DocId;

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
pub struct FieldNormReader {
    data: ReadOnlySource,
}

impl FieldNormReader {
    /// Opens a field norm reader given its data source.
    pub fn open(data: ReadOnlySource) -> Self {
        FieldNormReader { data }
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
    pub fn fieldnorm(&mut self, doc_id: DocId) -> u32 {
        let fieldnorm_id = self.fieldnorm_id(doc_id);
        id_to_fieldnorm(fieldnorm_id)
    }

    /// Returns the `fieldnorm_id` associated to a document.
    #[inline(always)]
    pub fn fieldnorm_id(&mut self, doc_id: DocId) -> u8 {
        let fieldnorms_data = self.data.read_all().expect("Can't read field norm data");
        fieldnorms_data[doc_id as usize]
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
impl From<Vec<u32>> for FieldNormReader {
    fn from(field_norms: Vec<u32>) -> FieldNormReader {
        let field_norms_id = field_norms
            .into_iter()
            .map(FieldNormReader::fieldnorm_to_id)
            .collect::<Vec<u8>>();
        let field_norms_data = ReadOnlySource::from(field_norms_id);
        FieldNormReader {
            data: field_norms_data,
        }
    }
}

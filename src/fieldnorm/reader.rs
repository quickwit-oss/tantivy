use std::sync::Arc;

use common::HasLen;

use super::{fieldnorm_to_id, id_to_fieldnorm};
use crate::directory::{CompositeFile, FileSlice};
use crate::schema::{Field, Schema};
use crate::space_usage::PerFieldSpaceUsage;
use crate::DocId;

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
    pub fn open(file: FileSlice) -> crate::Result<FieldNormReaders> {
        let data = CompositeFile::open(&file)?;
        Ok(FieldNormReaders {
            data: Arc::new(data),
        })
    }

    /// Returns the FieldNormReader for a specific field.
    pub fn get_field(&self, field: Field) -> crate::Result<Option<FieldNormReader>> {
        if let Some(file) = self.data.open_read(field) {
            Ok(Some(FieldNormReader::open(file)))
        } else {
            Ok(None)
        }
    }

    /// Return a break down of the space usage per field.
    pub fn space_usage(&self, schema: &Schema) -> PerFieldSpaceUsage {
        self.data.space_usage(schema)
    }

    /// Returns a handle to inner file
    pub fn get_inner_file(&self) -> Arc<CompositeFile> {
        self.data.clone()
    }
}

/// Reads the fieldnorm associated with a document.
///
/// The [fieldnorm](FieldNormReader::fieldnorm) represents the length associated with
/// a given Field of a given document.
#[derive(Clone)]
pub struct FieldNormReader(ReaderImplEnum);

impl From<ReaderImplEnum> for FieldNormReader {
    fn from(reader_enum: ReaderImplEnum) -> FieldNormReader {
        FieldNormReader(reader_enum)
    }
}

#[derive(Clone)]
enum ReaderImplEnum {
    FromFileSlice(FileSlice),
    Const { num_docs: u32, fieldnorm_id: u8 },
}

impl FieldNormReader {
    /// Creates a `FieldNormReader` with a constant fieldnorm.
    ///
    /// The fieldnorm will be subjected to compression as if it was coming
    /// from an array-backed fieldnorm reader.
    pub fn constant(num_docs: u32, fieldnorm: u32) -> FieldNormReader {
        let fieldnorm_id = fieldnorm_to_id(fieldnorm);
        ReaderImplEnum::Const {
            num_docs,
            fieldnorm_id,
        }
        .into()
    }

    pub fn open(fieldnorm_file: FileSlice) -> Self {
        ReaderImplEnum::FromFileSlice(fieldnorm_file).into()
    }

    /// Returns the number of documents in this segment.
    pub fn num_docs(&self) -> u32 {
        match &self.0 {
            ReaderImplEnum::FromFileSlice(file_slice) => file_slice.len() as u32,
            ReaderImplEnum::Const { num_docs, .. } => *num_docs,
        }
    }

    /// Returns the `fieldnorm` associated with a doc id.
    pub fn fieldnorm(&self, doc_id: DocId) -> u32 {
        id_to_fieldnorm(self.fieldnorm_id(doc_id))
    }

    /// Returns the `fieldnorm_id` associated with a document.
    #[inline]
    pub fn fieldnorm_id(&self, doc_id: DocId) -> u8 {
        match &self.0 {
            ReaderImplEnum::FromFileSlice(file_slice) => file_slice
                .read_byte(doc_id as usize)
                .expect("failed to read fieldnorm byte"),
            ReaderImplEnum::Const { fieldnorm_id, .. } => *fieldnorm_id,
        }
    }

    /// Converts a `fieldnorm_id` into a fieldnorm.
    #[inline]
    pub fn id_to_fieldnorm(id: u8) -> u32 {
        id_to_fieldnorm(id)
    }

    /// Converts a `fieldnorm` into a `fieldnorm_id`.
    /// (This function is not injective).
    #[inline]
    pub fn fieldnorm_to_id(fieldnorm: u32) -> u8 {
        fieldnorm_to_id(fieldnorm)
    }

    #[cfg(test)]
    pub(crate) fn for_test(field_norms: &[u32]) -> FieldNormReader {
        let field_norms_id = field_norms
            .iter()
            .cloned()
            .map(FieldNormReader::fieldnorm_to_id)
            .collect::<Vec<u8>>();
        FieldNormReader::open(FileSlice::from(field_norms_id))
    }
}

#[cfg(test)]
mod tests {
    use crate::fieldnorm::FieldNormReader;

    #[test]
    fn test_from_fieldnorms_array() {
        let fieldnorms = &[1, 2, 3, 4, 1_000_000];
        let fieldnorm_reader = FieldNormReader::for_test(fieldnorms);
        assert_eq!(fieldnorm_reader.num_docs(), 5);
        assert_eq!(fieldnorm_reader.fieldnorm(0), 1);
        assert_eq!(fieldnorm_reader.fieldnorm(1), 2);
        assert_eq!(fieldnorm_reader.fieldnorm(2), 3);
        assert_eq!(fieldnorm_reader.fieldnorm(3), 4);
        assert_eq!(fieldnorm_reader.fieldnorm(4), 983_064);
    }

    #[test]
    fn test_const_fieldnorm_reader_small_fieldnorm_id() {
        let fieldnorm_reader = FieldNormReader::constant(1_000_000u32, 10u32);
        assert_eq!(fieldnorm_reader.num_docs(), 1_000_000u32);
        assert_eq!(fieldnorm_reader.fieldnorm(0u32), 10u32);
        assert_eq!(fieldnorm_reader.fieldnorm_id(0u32), 10u8);
    }

    #[test]
    fn test_const_fieldnorm_reader_large_fieldnorm_id() {
        let fieldnorm_reader = FieldNormReader::constant(1_000_000u32, 300u32);
        assert_eq!(fieldnorm_reader.num_docs(), 1_000_000u32);
        assert_eq!(fieldnorm_reader.fieldnorm(0u32), 280u32);
        assert_eq!(fieldnorm_reader.fieldnorm_id(0u32), 72u8);
    }
}

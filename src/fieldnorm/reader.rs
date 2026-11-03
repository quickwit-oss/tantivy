use std::collections::HashMap;
use std::sync::{Arc, OnceLock, RwLock};

use super::{fieldnorm_to_id, id_to_fieldnorm};
use crate::directory::{CompositeFile, FileSlice, OwnedBytes};
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
    readers: Arc<RwLock<HashMap<Field, FieldNormReader>>>,
}

impl FieldNormReaders {
    /// Creates a field norm reader.
    pub fn open(file: FileSlice) -> crate::Result<FieldNormReaders> {
        let data = CompositeFile::open(&file)?;
        Ok(FieldNormReaders {
            data: Arc::new(data),
            readers: Default::default(),
        })
    }

    /// Returns the FieldNormReader for a specific field.
    pub fn get_field(&self, field: Field) -> crate::Result<Option<FieldNormReader>> {
        if let Some(reader) = self
            .readers
            .read()
            .expect("Fieldnorm reader cache lock poisoned")
            .get(&field)
        {
            return Ok(Some(reader.clone()));
        }
        if let Some(file) = self.data.open_read(field) {
            let fieldnorm_reader = FieldNormReader::open(file)?;
            let reader = self
                .readers
                .write()
                .expect("Fieldnorm reader cache lock poisoned")
                .entry(field)
                .or_insert(fieldnorm_reader)
                .clone();
            Ok(Some(reader))
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
    FromData {
        data: OwnedBytes,
        minimum: Arc<OnceLock<u8>>,
    },
    Const {
        num_docs: u32,
        fieldnorm_id: u8,
        fieldnorm: u32,
    },
}

impl FieldNormReader {
    /// Creates a `FieldNormReader` with a constant fieldnorm.
    ///
    /// The fieldnorm will be subjected to compression as if it was coming
    /// from an array-backed fieldnorm reader.
    pub fn constant(num_docs: u32, fieldnorm: u32) -> FieldNormReader {
        let fieldnorm_id = fieldnorm_to_id(fieldnorm);
        let fieldnorm = id_to_fieldnorm(fieldnorm_id);
        ReaderImplEnum::Const {
            num_docs,
            fieldnorm_id,
            fieldnorm,
        }
        .into()
    }

    /// Opens a field norm reader given its file.
    pub fn open(fieldnorm_file: FileSlice) -> crate::Result<Self> {
        let data = fieldnorm_file.read_bytes()?;
        Ok(FieldNormReader::new(data))
    }

    fn new(data: OwnedBytes) -> Self {
        ReaderImplEnum::FromData {
            data,
            minimum: Arc::new(OnceLock::new()),
        }
        .into()
    }

    pub(crate) fn is_constant(&self) -> bool {
        matches!(&self.0, ReaderImplEnum::Const { .. })
    }

    pub(crate) fn min_fieldnorm_id(&self) -> u8 {
        match &self.0 {
            ReaderImplEnum::FromData { data, minimum } => {
                *minimum.get_or_init(|| data.as_slice().iter().copied().min().unwrap_or(0))
            }
            ReaderImplEnum::Const { fieldnorm_id, .. } => *fieldnorm_id,
        }
    }

    /// Returns the number of documents in this segment.
    pub fn num_docs(&self) -> u32 {
        match &self.0 {
            ReaderImplEnum::FromData { data, .. } => data.len() as u32,
            ReaderImplEnum::Const { num_docs, .. } => *num_docs,
        }
    }

    /// Returns the `fieldnorm` associated with a doc id.
    /// The fieldnorm is a value approximating the number
    /// of tokens in a given field of the `doc_id`.
    ///
    /// It is imprecise, and equal or lower than
    /// the actual number of tokens.
    ///
    /// The fieldnorm is effectively decoded from the
    /// `fieldnorm_id` by doing a simple table lookup.
    pub fn fieldnorm(&self, doc_id: DocId) -> u32 {
        match &self.0 {
            ReaderImplEnum::FromData { data, .. } => {
                let fieldnorm_id = data.as_slice()[doc_id as usize];
                id_to_fieldnorm(fieldnorm_id)
            }
            ReaderImplEnum::Const { fieldnorm, .. } => *fieldnorm,
        }
    }

    /// Returns the `fieldnorm_id` associated with a document.
    #[inline]
    pub fn fieldnorm_id(&self, doc_id: DocId) -> u8 {
        match &self.0 {
            ReaderImplEnum::FromData { data, .. } => {
                let fieldnorm_id = data.as_slice()[doc_id as usize];
                fieldnorm_id
            }
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
        let field_norms_data = OwnedBytes::new(field_norms_id);
        FieldNormReader::new(field_norms_data)
    }
}

#[cfg(test)]
mod tests {
    use crate::fieldnorm::FieldNormReader;

    #[test]
    fn test_fieldnorm_minimum_is_shared() -> crate::Result<()> {
        let mut schema = crate::schema::Schema::builder();
        let field = schema.add_text_field("status", crate::schema::STRING);
        let index = crate::Index::create_in_ram(schema.build());
        let mut writer = index.writer_with_num_threads::<crate::TantivyDocument>(1, 20_000_000)?;
        writer.add_document(crate::doc!(field => "active", field => "pending"))?;
        writer.add_document(crate::doc!(field => "active"))?;
        writer.commit()?;
        let searcher = index.reader()?.searcher();
        let readers = searcher.segment_reader(0).fieldnorms_readers();
        let first = readers.get_field(field)?.unwrap();
        let cloned_readers = readers.clone();
        let second = cloned_readers.get_field(field)?.unwrap();
        match (&first.0, &second.0) {
            (
                super::ReaderImplEnum::FromData { minimum: a, .. },
                super::ReaderImplEnum::FromData { minimum: b, .. },
            ) => {
                assert!(std::sync::Arc::ptr_eq(a, b));
                assert!(a.get().is_none());
                assert_eq!(first.min_fieldnorm_id(), 1);
                assert_eq!(b.get(), Some(&1));
            }
            _ => panic!("Expected stored fieldnorms"),
        }
        assert_eq!(second.min_fieldnorm_id(), 1);
        assert_eq!(FieldNormReader::constant(5, 10).min_fieldnorm_id(), 10);
        assert_eq!(FieldNormReader::for_test(&[]).min_fieldnorm_id(), 0);
        assert_eq!(FieldNormReader::for_test(&[4, 0, 2]).min_fieldnorm_id(), 0);
        Ok(())
    }

    #[test]
    fn test_short_postings_do_not_initialize_minimum() -> crate::Result<()> {
        let mut schema = crate::schema::Schema::builder();
        let field = schema.add_text_field("status", crate::schema::STRING);
        let index = crate::Index::create_in_ram(schema.build());
        let mut writer = index.writer_with_num_threads::<crate::TantivyDocument>(1, 20_000_000)?;
        for id in 0..1024 {
            let mut doc = crate::doc!(field => "active");
            if id < 128 {
                doc.add_text(field, "rare");
            }
            writer.add_document(doc)?;
        }
        writer.commit()?;
        let searcher = index.reader()?.searcher();
        let reader = searcher.segment_reader(0).get_fieldnorms_reader(field)?;
        let query = crate::query::TermQuery::new(
            crate::Term::from_field_text(field, "rare"),
            crate::schema::IndexRecordOption::Basic,
        );
        let hits = searcher.search(
            &query,
            &crate::collector::TopDocs::with_limit(10).order_by_score(),
        )?;
        assert_eq!(hits.len(), 10);
        match &reader.0 {
            super::ReaderImplEnum::FromData { minimum, .. } => assert!(minimum.get().is_none()),
            _ => panic!("Expected stored fieldnorms"),
        }
        Ok(())
    }

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

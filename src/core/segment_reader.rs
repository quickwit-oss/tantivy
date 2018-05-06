use common::CompositeFile;
use common::HasLen;
use core::InvertedIndexReader;
use core::Segment;
use core::SegmentComponent;
use core::SegmentId;
use core::SegmentMeta;
use error::ErrorKind;
use fastfield::DeleteBitSet;
use fastfield::FacetReader;
use fastfield::FastFieldReader;
use fastfield::{self, FastFieldNotAvailableError};
use fastfield::{FastValue, MultiValueIntFastFieldReader};
use fieldnorm::FieldNormReader;
use schema::Cardinality;
use schema::Document;
use schema::Field;
use schema::FieldType;
use schema::Schema;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::sync::RwLock;
use store::StoreReader;
use termdict::TermDictionary;
use DocId;
use Result;

/// Entry point to access all of the datastructures of the `Segment`
///
/// - term dictionary
/// - postings
/// - store
/// - fast field readers
/// - field norm reader
///
/// The segment reader has a very low memory footprint,
/// as close to all of the memory data is mmapped.
///
///
/// TODO fix not decoding docfreq
#[derive(Clone)]
pub struct SegmentReader {
    inv_idx_reader_cache: Arc<RwLock<HashMap<Field, Arc<InvertedIndexReader>>>>,

    segment_id: SegmentId,
    segment_meta: SegmentMeta,

    termdict_composite: CompositeFile,
    postings_composite: CompositeFile,
    positions_composite: CompositeFile,
    fast_fields_composite: CompositeFile,
    fieldnorms_composite: CompositeFile,

    store_reader: StoreReader,
    delete_bitset_opt: Option<DeleteBitSet>,
    schema: Schema,
}

impl SegmentReader {
    /// Returns the highest document id ever attributed in
    /// this segment + 1.
    /// Today, `tantivy` does not handle deletes, so it happens
    /// to also be the number of documents in the index.
    pub fn max_doc(&self) -> DocId {
        self.segment_meta.max_doc()
    }

    /// Returns the number of documents.
    /// Deleted documents are not counted.
    ///
    /// Today, `tantivy` does not handle deletes so max doc and
    /// num_docs are the same.
    pub fn num_docs(&self) -> DocId {
        self.segment_meta.num_docs()
    }

    /// Returns the schema of the index this segment belongs to.
    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    /// Return the number of documents that have been
    /// deleted in the segment.
    pub fn num_deleted_docs(&self) -> DocId {
        self.delete_bitset()
            .map(|delete_set| delete_set.len() as DocId)
            .unwrap_or(0u32)
    }

    /// Returns true iff some of the documents of the segment have been deleted.
    pub fn has_deletes(&self) -> bool {
        self.delete_bitset().is_some()
    }

    /// Accessor to a segment's fast field reader given a field.
    ///
    /// Returns the u64 fast value reader if the field
    /// is a u64 field indexed as "fast".
    ///
    /// Return a FastFieldNotAvailableError if the field is not
    /// declared as a fast field in the schema.
    ///
    /// # Panics
    /// May panic if the index is corrupted.
    pub fn fast_field_reader<Item: FastValue>(
        &self,
        field: Field,
    ) -> fastfield::Result<FastFieldReader<Item>> {
        let field_entry = self.schema.get_field_entry(field);
        if Item::fast_field_cardinality(field_entry.field_type()) == Some(Cardinality::SingleValue)
        {
            self.fast_fields_composite
                .open_read(field)
                .ok_or_else(|| FastFieldNotAvailableError::new(field_entry))
                .map(FastFieldReader::open)
        } else {
            Err(FastFieldNotAvailableError::new(field_entry))
        }
    }

    pub(crate) fn fast_field_reader_with_idx<Item: FastValue>(
        &self,
        field: Field,
        idx: usize
    ) -> fastfield::Result<FastFieldReader<Item>> {
        if let Some(ff_source) = self.fast_fields_composite.open_read_with_idx(field, idx) {
            Ok(FastFieldReader::open(ff_source))
        } else {
            let field_entry = self.schema.get_field_entry(field);
            Err(FastFieldNotAvailableError::new(field_entry))
        }
    }

    /// Accessor to the `MultiValueIntFastFieldReader` associated to a given `Field`.
    /// May panick if the field is not a multivalued fastfield of the type `Item`.
    pub fn multi_fast_field_reader<Item: FastValue>(
        &self,
        field: Field,
    ) -> fastfield::Result<MultiValueIntFastFieldReader<Item>> {
        let field_entry = self.schema.get_field_entry(field);
        if Item::fast_field_cardinality(field_entry.field_type()) == Some(Cardinality::MultiValues)
        {
            let idx_reader = self.fast_field_reader_with_idx(field, 0)?;
            let vals_reader = self.fast_field_reader_with_idx(field, 1)?;
            Ok(MultiValueIntFastFieldReader::open(idx_reader, vals_reader))
        } else {
            Err(FastFieldNotAvailableError::new(field_entry))
        }
    }

    /// Accessor to the `FacetReader` associated to a given `Field`.
    pub fn facet_reader(&self, field: Field) -> Result<FacetReader> {
        let field_entry = self.schema.get_field_entry(field);
        if field_entry.field_type() != &FieldType::HierarchicalFacet {
            return Err(ErrorKind::InvalidArgument(format!(
                "The field {:?} is not a \
                 hierarchical facet.",
                field_entry
            )).into());
        }
        let term_ords_reader = self.multi_fast_field_reader(field)?;
        let termdict_source = self.termdict_composite.open_read(field).ok_or_else(|| {
            ErrorKind::InvalidArgument(format!(
                "The field \"{}\" is a hierarchical \
                 but this segment does not seem to have the field term \
                 dictionary.",
                field_entry.name()
            ))
        })?;
        let termdict = TermDictionary::from_source(termdict_source);
        let facet_reader = FacetReader::new(term_ords_reader, termdict);
        Ok(facet_reader)
    }

    /// Accessor to the segment's `Field norms`'s reader.
    ///
    /// Field norms are the length (in tokens) of the fields.
    /// It is used in the computation of the [TfIdf]
    /// (https://fulmicoton.gitbooks.io/tantivy-doc/content/tfidf.html).
    ///
    /// They are simply stored as a fast field, serialized in
    /// the `.fieldnorm` file of the segment.
    pub fn get_fieldnorms_reader(&self, field: Field) -> FieldNormReader {
        if let Some(fieldnorm_source) = self.fieldnorms_composite.open_read(field) {
            FieldNormReader::open(fieldnorm_source)
        } else {
            let field_name = self.schema.get_field_name(field);
            let err_msg = format!(
                "Field norm not found for field {:?}. Was it market as indexed during indexing.",
                field_name
            );
            panic!(err_msg);
        }
    }

    /// Accessor to the segment's `StoreReader`.
    pub fn get_store_reader(&self) -> &StoreReader {
        &self.store_reader
    }

    /// Open a new segment for reading.
    pub fn open(segment: &Segment) -> Result<SegmentReader> {
        let termdict_source = segment.open_read(SegmentComponent::TERMS)?;
        let termdict_composite = CompositeFile::open(&termdict_source)?;

        let store_source = segment.open_read(SegmentComponent::STORE)?;
        let store_reader = StoreReader::from_source(store_source);

        let postings_source = segment.open_read(SegmentComponent::POSTINGS)?;
        let postings_composite = CompositeFile::open(&postings_source)?;

        let positions_composite = {
            if let Ok(source) = segment.open_read(SegmentComponent::POSITIONS) {
                CompositeFile::open(&source)?
            } else {
                CompositeFile::empty()
            }
        };

        let fast_fields_data = segment.open_read(SegmentComponent::FASTFIELDS)?;
        let fast_fields_composite = CompositeFile::open(&fast_fields_data)?;

        let fieldnorms_data = segment.open_read(SegmentComponent::FIELDNORMS)?;
        let fieldnorms_composite = CompositeFile::open(&fieldnorms_data)?;

        let delete_bitset_opt = if segment.meta().has_deletes() {
            let delete_data = segment.open_read(SegmentComponent::DELETE)?;
            Some(DeleteBitSet::open(delete_data))
        } else {
            None
        };

        let schema = segment.schema();
        Ok(SegmentReader {
            inv_idx_reader_cache: Arc::new(RwLock::new(HashMap::new())),
            segment_meta: segment.meta().clone(),
            termdict_composite,
            postings_composite,
            fast_fields_composite,
            fieldnorms_composite,
            segment_id: segment.id(),
            store_reader,
            delete_bitset_opt,
            positions_composite,
            schema,
        })
    }

    /// Returns a field reader associated to the field given in argument.
    /// If the field was not present in the index during indexing time,
    /// the InvertedIndexReader is empty.
    ///
    /// The field reader is in charge of iterating through the
    /// term dictionary associated to a specific field,
    /// and opening the posting list associated to any term.
    pub fn inverted_index(&self, field: Field) -> Arc<InvertedIndexReader> {
        if let Some(inv_idx_reader) = self.inv_idx_reader_cache
            .read()
            .expect("Lock poisoned. This should never happen")
            .get(&field)
        {
            return Arc::clone(inv_idx_reader);
        }
        let field_entry = self.schema.get_field_entry(field);
        let field_type = field_entry.field_type();
        let record_option_opt = field_type.get_index_record_option();

        if record_option_opt.is_none() {
            panic!("Field {:?} does not seem indexed.", field_entry.name());
        }

        let record_option = record_option_opt.unwrap();

        let postings_source_opt = self.postings_composite.open_read(field);

        if postings_source_opt.is_none() {
            // no documents in the segment contained this field.
            // As a result, no data is associated to the inverted index.
            //
            // Returns an empty inverted index.
            return Arc::new(InvertedIndexReader::empty(field_type.clone()));
        }

        let postings_source = postings_source_opt.unwrap();

        let termdict_source = self.termdict_composite
            .open_read(field)
            .expect("Failed to open field term dictionary in composite file. Is the field indexed");

        let positions_source = self.positions_composite
            .open_read(field)
            .expect("Index corrupted. Failed to open field positions in composite file.");

        let inv_idx_reader = Arc::new(InvertedIndexReader::new(
            TermDictionary::from_source(termdict_source),
            postings_source,
            positions_source,
            record_option,
        ));

        // by releasing the lock in between, we may end up opening the inverting index
        // twice, but this is fine.
        self.inv_idx_reader_cache
            .write()
            .expect("Field reader cache lock poisoned. This should never happen.")
            .insert(field, Arc::clone(&inv_idx_reader));

        inv_idx_reader
    }

    /// Returns the document (or to be accurate, its stored field)
    /// bearing the given doc id.
    /// This method is slow and should seldom be called from
    /// within a collector.
    pub fn doc(&self, doc_id: DocId) -> Result<Document> {
        self.store_reader.get(doc_id)
    }

    /// Returns the segment id
    pub fn segment_id(&self) -> SegmentId {
        self.segment_id
    }

    /// Returns the bitset representing
    /// the documents that have been deleted.
    pub fn delete_bitset(&self) -> Option<&DeleteBitSet> {
        self.delete_bitset_opt.as_ref()
    }

    /// Returns true iff the `doc` is marked
    /// as deleted.
    pub fn is_deleted(&self, doc: DocId) -> bool {
        self.delete_bitset()
            .map(|delete_set| delete_set.is_deleted(doc))
            .unwrap_or(false)
    }
}

impl fmt::Debug for SegmentReader {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SegmentReader({:?})", self.segment_id)
    }
}

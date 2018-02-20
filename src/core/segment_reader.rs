use Result;
use core::Segment;
use core::SegmentId;
use core::SegmentComponent;
use std::sync::RwLock;
use common::HasLen;
use core::SegmentMeta;
use fastfield::{self, FastFieldNotAvailableError};
use fastfield::DeleteBitSet;
use store::StoreReader;
use directory::ReadOnlySource;
use schema::Document;
use DocId;
use std::sync::Arc;
use std::collections::HashMap;
use common::CompositeFile;
use std::fmt;
use core::InvertedIndexReader;
use schema::Field;
use schema::FieldType;
use error::ErrorKind;
use termdict::TermDictionaryImpl;
use fastfield::FacetReader;
use fastfield::FastFieldReader;
use schema::Schema;
use termdict::TermDictionary;
use fastfield::{FastValue, MultiValueIntFastFieldReader};
use schema::Cardinality;

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
    delete_bitset: DeleteBitSet,
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

    /// Return the number of documents that have been
    /// deleted in the segment.
    pub fn num_deleted_docs(&self) -> DocId {
        self.delete_bitset.len() as DocId
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
        if Item::fast_field_cardinality(field_entry.field_type()) == Some(Cardinality::SingleValue) {
            self.fast_fields_composite
                .open_read(field)
                .ok_or_else(|| FastFieldNotAvailableError::new(field_entry))
                .map(FastFieldReader::open)
        } else {
            Err(FastFieldNotAvailableError::new(field_entry))
        }
    }

    /// Accessor to the `MultiValueIntFastFieldReader` associated to a given `Field`.
    /// May panick if the field is not a multivalued fastfield of the type `Item`.
    pub fn multi_fast_field_reader<Item: FastValue>(&self, field: Field) -> fastfield::Result<MultiValueIntFastFieldReader<Item>> {
        let field_entry = self.schema.get_field_entry(field);
        if Item::fast_field_cardinality(field_entry.field_type()) == Some(Cardinality::MultiValues) {
            let idx_reader = self.fast_fields_composite
                .open_read_with_idx(field, 0)
                .ok_or_else(|| FastFieldNotAvailableError::new(field_entry))
                .map(FastFieldReader::open)?;
            let vals_reader = self.fast_fields_composite
                .open_read_with_idx(field, 1)
                .ok_or_else(|| FastFieldNotAvailableError::new(field_entry))
                .map(FastFieldReader::open)?;
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
        let termdict = TermDictionaryImpl::from_source(termdict_source);
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
    pub fn get_fieldnorms_reader(&self, field: Field) -> Option<FastFieldReader<u64>> {
        self.fieldnorms_composite
            .open_read(field)
            .map(FastFieldReader::open)
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

        let delete_bitset = if segment.meta().has_deletes() {
            let delete_data = segment.open_read(SegmentComponent::DELETE)?;
            DeleteBitSet::open(delete_data)
        } else {
            DeleteBitSet::empty()
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
            delete_bitset,
            positions_composite,
            schema,
        })
    }

    /// Returns a field reader associated to the field given in argument.
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

        let record_option = self.schema
            .get_field_entry(field)
            .field_type()
            .get_index_record_option()
            .expect("Field does not seem indexed.");

        let termdict_source: ReadOnlySource = self.termdict_composite
            .open_read(field)
            .expect("Failed to open field term dictionary in composite file. Is the field indexed");

        let postings_source = self.postings_composite
            .open_read(field)
            .expect("Index corrupted. Failed to open field postings in composite file.");

        let positions_source = self.positions_composite
            .open_read(field)
            .expect("Index corrupted. Failed to open field positions in composite file.");

        let inv_idx_reader = Arc::new(InvertedIndexReader::new(
            termdict_source,
            postings_source,
            positions_source,
            self.delete_bitset.clone(),
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
    pub fn delete_bitset(&self) -> &DeleteBitSet {
        &self.delete_bitset
    }

    /// Returns true iff the `doc` is marked
    /// as deleted.
    pub fn is_deleted(&self, doc: DocId) -> bool {
        self.delete_bitset.is_deleted(doc)
    }
}

impl fmt::Debug for SegmentReader {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SegmentReader({:?})", self.segment_id)
    }
}

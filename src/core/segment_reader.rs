use crate::common::HasLen;
use crate::core::InvertedIndexReader;
use crate::core::Segment;
use crate::core::SegmentComponent;
use crate::core::SegmentId;
use crate::directory::FileSlice;
use crate::fastfield::DeleteBitSet;
use crate::fastfield::FacetReader;
use crate::fastfield::FastFieldReaders;
use crate::fieldnorm::{FieldNormReader, FieldNormReaders};
use crate::schema::FieldType;
use crate::schema::Schema;
use crate::schema::{Field, IndexRecordOption};
use crate::space_usage::SegmentSpaceUsage;
use crate::store::StoreReader;
use crate::termdict::TermDictionary;
use crate::DocId;
use crate::{common::CompositeFile, error::DataCorruption};
use fail::fail_point;
use std::fmt;
use std::sync::Arc;
use std::sync::RwLock;
use std::{collections::HashMap, io};

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
    max_doc: DocId,
    num_docs: DocId,

    termdict_composite: CompositeFile,
    postings_composite: CompositeFile,
    positions_composite: CompositeFile,
    positions_idx_composite: CompositeFile,
    fast_fields_readers: Arc<FastFieldReaders>,
    fieldnorm_readers: FieldNormReaders,

    store_file: FileSlice,
    delete_bitset_opt: Option<DeleteBitSet>,
    schema: Schema,
}

impl SegmentReader {
    /// Returns the highest document id ever attributed in
    /// this segment + 1.
    /// Today, `tantivy` does not handle deletes, so it happens
    /// to also be the number of documents in the index.
    pub fn max_doc(&self) -> DocId {
        self.max_doc
    }

    /// Returns the number of documents.
    /// Deleted documents are not counted.
    ///
    /// Today, `tantivy` does not handle deletes so max doc and
    /// num_docs are the same.
    pub fn num_docs(&self) -> DocId {
        self.num_docs
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
    pub fn fast_fields(&self) -> &FastFieldReaders {
        &self.fast_fields_readers
    }

    /// Accessor to the `FacetReader` associated to a given `Field`.
    pub fn facet_reader(&self, field: Field) -> crate::Result<FacetReader> {
        let field_entry = self.schema.get_field_entry(field);

        match field_entry.field_type() {
            FieldType::HierarchicalFacet(_) => {
                let term_ords_reader = self.fast_fields().u64s(field)?;
                let termdict = self
                    .termdict_composite
                    .open_read(field)
                    .map(TermDictionary::open)
                    .unwrap_or_else(|| Ok(TermDictionary::empty()))?;
                Ok(FacetReader::new(term_ords_reader, termdict))
            }
            _ => Err(crate::TantivyError::InvalidArgument(format!(
                "Field {:?} is not a facet field.",
                field_entry.name()
            ))),
        }
    }

    /// Accessor to the segment's `Field norms`'s reader.
    ///
    /// Field norms are the length (in tokens) of the fields.
    /// It is used in the computation of the [TfIdf](https://fulmicoton.gitbooks.io/tantivy-doc/content/tfidf.html).
    ///
    /// They are simply stored as a fast field, serialized in
    /// the `.fieldnorm` file of the segment.
    pub fn get_fieldnorms_reader(&self, field: Field) -> crate::Result<FieldNormReader> {
        self.fieldnorm_readers.get_field(field)?.ok_or_else(|| {
            let field_name = self.schema.get_field_name(field);
            let err_msg = format!(
                "Field norm not found for field {:?}. Was it marked as indexed during indexing?",
                field_name
            );
            crate::TantivyError::SchemaError(err_msg)
        })
    }

    /// Accessor to the segment's `StoreReader`.
    pub fn get_store_reader(&self) -> io::Result<StoreReader> {
        StoreReader::open(self.store_file.clone())
    }

    /// Open a new segment for reading.
    pub fn open(segment: &Segment) -> crate::Result<SegmentReader> {
        let termdict_file = segment.open_read(SegmentComponent::TERMS)?;
        let termdict_composite = CompositeFile::open(&termdict_file)?;

        let store_file = segment.open_read(SegmentComponent::STORE)?;

        fail_point!("SegmentReader::open#middle");

        let postings_file = segment.open_read(SegmentComponent::POSTINGS)?;
        let postings_composite = CompositeFile::open(&postings_file)?;

        let positions_composite = {
            if let Ok(positions_file) = segment.open_read(SegmentComponent::POSITIONS) {
                CompositeFile::open(&positions_file)?
            } else {
                CompositeFile::empty()
            }
        };

        let positions_idx_composite = {
            if let Ok(positions_skip_file) = segment.open_read(SegmentComponent::POSITIONSSKIP) {
                CompositeFile::open(&positions_skip_file)?
            } else {
                CompositeFile::empty()
            }
        };

        let schema = segment.schema();

        let fast_fields_data = segment.open_read(SegmentComponent::FASTFIELDS)?;
        let fast_fields_composite = CompositeFile::open(&fast_fields_data)?;
        let fast_field_readers =
            Arc::new(FastFieldReaders::new(schema.clone(), fast_fields_composite));

        let fieldnorm_data = segment.open_read(SegmentComponent::FIELDNORMS)?;
        let fieldnorm_readers = FieldNormReaders::open(fieldnorm_data)?;

        let delete_bitset_opt = if segment.meta().has_deletes() {
            let delete_data = segment.open_read(SegmentComponent::DELETE)?;
            let delete_bitset = DeleteBitSet::open(delete_data)?;
            Some(delete_bitset)
        } else {
            None
        };

        Ok(SegmentReader {
            inv_idx_reader_cache: Default::default(),
            max_doc: segment.meta().max_doc(),
            num_docs: segment.meta().num_docs(),
            termdict_composite,
            postings_composite,
            fast_fields_readers: fast_field_readers,
            fieldnorm_readers,
            segment_id: segment.id(),
            store_file,
            delete_bitset_opt,
            positions_composite,
            positions_idx_composite,
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
    ///
    /// If the field is marked as index, a warn is logged and an empty `InvertedIndexReader`
    /// is returned.
    /// Similarly if the field is marked as indexed but no term has been indexed for the given
    /// index. an empty `InvertedIndexReader` is returned (but no warning is logged).
    pub fn inverted_index(&self, field: Field) -> crate::Result<Arc<InvertedIndexReader>> {
        if let Some(inv_idx_reader) = self
            .inv_idx_reader_cache
            .read()
            .expect("Lock poisoned. This should never happen")
            .get(&field)
        {
            return Ok(Arc::clone(inv_idx_reader));
        }
        let field_entry = self.schema.get_field_entry(field);
        let field_type = field_entry.field_type();
        let record_option_opt = field_type.get_index_record_option();

        if record_option_opt.is_none() {
            warn!("Field {:?} does not seem indexed.", field_entry.name());
        }

        let postings_file_opt = self.postings_composite.open_read(field);

        if postings_file_opt.is_none() || record_option_opt.is_none() {
            // no documents in the segment contained this field.
            // As a result, no data is associated to the inverted index.
            //
            // Returns an empty inverted index.
            let record_option = record_option_opt.unwrap_or(IndexRecordOption::Basic);
            return Ok(Arc::new(InvertedIndexReader::empty(record_option)));
        }

        let record_option = record_option_opt.unwrap();
        let postings_file = postings_file_opt.unwrap();

        let termdict_file: FileSlice = self.termdict_composite.open_read(field)
            .ok_or_else(||
               DataCorruption::comment_only(format!("Failed to open field {:?}'s term dictionary in the composite file. Has the schema been modified?", field_entry.name()))
            )?;

        let positions_file = self
            .positions_composite
            .open_read(field)
            .expect("Index corrupted. Failed to open field positions in composite file.");

        let positions_idx_file = self
            .positions_idx_composite
            .open_read(field)
            .expect("Index corrupted. Failed to open field positions in composite file.");

        let inv_idx_reader = Arc::new(InvertedIndexReader::new(
            TermDictionary::open(termdict_file)?,
            postings_file,
            positions_file,
            positions_idx_file,
            record_option,
        )?);

        // by releasing the lock in between, we may end up opening the inverting index
        // twice, but this is fine.
        self.inv_idx_reader_cache
            .write()
            .expect("Field reader cache lock poisoned. This should never happen.")
            .insert(field, Arc::clone(&inv_idx_reader));

        Ok(inv_idx_reader)
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

    /// Returns an iterator that will iterate over the alive document ids
    pub fn doc_ids_alive(&self) -> impl Iterator<Item = DocId> + '_ {
        (0u32..self.max_doc).filter(move |doc| !self.is_deleted(*doc))
    }

    /// Summarize total space usage of this segment.
    pub fn space_usage(&self) -> io::Result<SegmentSpaceUsage> {
        Ok(SegmentSpaceUsage::new(
            self.num_docs(),
            self.termdict_composite.space_usage(),
            self.postings_composite.space_usage(),
            self.positions_composite.space_usage(),
            self.positions_idx_composite.space_usage(),
            self.fast_fields_readers.space_usage(),
            self.fieldnorm_readers.space_usage(),
            self.get_store_reader()?.space_usage(),
            self.delete_bitset_opt
                .as_ref()
                .map(DeleteBitSet::space_usage)
                .unwrap_or(0),
        ))
    }
}

impl fmt::Debug for SegmentReader {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SegmentReader({:?})", self.segment_id)
    }
}

#[cfg(test)]
mod test {
    use crate::core::Index;
    use crate::schema::{Schema, Term, STORED, TEXT};
    use crate::DocId;

    #[test]
    fn test_alive_docs_iterator() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        schema_builder.add_text_field("name", TEXT | STORED);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema.clone());
        let name = schema.get_field("name").unwrap();

        {
            let mut index_writer = index.writer_for_tests()?;
            index_writer.add_document(doc!(name => "tantivy"));
            index_writer.add_document(doc!(name => "horse"));
            index_writer.add_document(doc!(name => "jockey"));
            index_writer.add_document(doc!(name => "cap"));
            // we should now have one segment with two docs
            index_writer.commit()?;
        }

        {
            let mut index_writer2 = index.writer(50_000_000)?;
            index_writer2.delete_term(Term::from_field_text(name, "horse"));
            index_writer2.delete_term(Term::from_field_text(name, "cap"));

            // ok, now we should have a deleted doc
            index_writer2.commit()?;
        }
        let searcher = index.reader()?.searcher();
        let docs: Vec<DocId> = searcher.segment_reader(0).doc_ids_alive().collect();
        assert_eq!(vec![0u32, 2u32], docs);
        Ok(())
    }
}

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::{fmt, io};

use fail::fail_point;

use crate::core::{InvertedIndexReader, Segment, SegmentComponent, SegmentId};
use crate::directory::{CompositeFile, FileSlice};
use crate::error::DataCorruption;
use crate::fastfield::{intersect_alive_bitsets, AliveBitSet, FacetReader, FastFieldReaders};
use crate::fieldnorm::{FieldNormReader, FieldNormReaders};
use crate::schema::{Field, FieldType, IndexRecordOption, Schema};
use crate::space_usage::SegmentSpaceUsage;
use crate::store::StoreReader;
use crate::termdict::TermDictionary;
use crate::{DocId, Opstamp};

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
#[derive(Clone)]
pub struct SegmentReader {
    inv_idx_reader_cache: Arc<RwLock<HashMap<Field, Arc<InvertedIndexReader>>>>,

    segment_id: SegmentId,
    delete_opstamp: Option<Opstamp>,

    max_doc: DocId,
    num_docs: DocId,

    termdict_composite: CompositeFile,
    postings_composite: CompositeFile,
    positions_composite: CompositeFile,
    fast_fields_readers: Arc<FastFieldReaders>,
    fieldnorm_readers: FieldNormReaders,

    store_file: FileSlice,
    alive_bitset_opt: Option<AliveBitSet>,
    schema: Schema,
}

impl SegmentReader {
    /// Returns the highest document id ever attributed in
    /// this segment + 1.
    pub fn max_doc(&self) -> DocId {
        self.max_doc
    }

    /// Returns the number of alive documents.
    /// Deleted documents are not counted.
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
        self.max_doc - self.num_docs
    }

    /// Returns true if some of the documents of the segment have been deleted.
    pub fn has_deletes(&self) -> bool {
        self.num_deleted_docs() > 0
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
            FieldType::Facet(_) => {
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
                "Field norm not found for field {field_name:?}. Was the field set to record norm \
                 during indexing?"
            );
            crate::TantivyError::SchemaError(err_msg)
        })
    }

    pub(crate) fn fieldnorms_readers(&self) -> &FieldNormReaders {
        &self.fieldnorm_readers
    }

    /// Accessor to the segment's `StoreReader`.
    pub fn get_store_reader(&self) -> io::Result<StoreReader> {
        StoreReader::open(self.store_file.clone())
    }

    /// Open a new segment for reading.
    pub fn open(segment: &Segment) -> crate::Result<SegmentReader> {
        Self::open_with_custom_alive_set(segment, None)
    }

    /// Open a new segment for reading.
    pub fn open_with_custom_alive_set(
        segment: &Segment,
        custom_bitset: Option<AliveBitSet>,
    ) -> crate::Result<SegmentReader> {
        let termdict_file = segment.open_read(SegmentComponent::Terms)?;
        let termdict_composite = CompositeFile::open(&termdict_file)?;

        let store_file = segment.open_read(SegmentComponent::Store)?;

        fail_point!("SegmentReader::open#middle");

        let postings_file = segment.open_read(SegmentComponent::Postings)?;
        let postings_composite = CompositeFile::open(&postings_file)?;

        let positions_composite = {
            if let Ok(positions_file) = segment.open_read(SegmentComponent::Positions) {
                CompositeFile::open(&positions_file)?
            } else {
                CompositeFile::empty()
            }
        };

        let schema = segment.schema();

        let fast_fields_data = segment.open_read(SegmentComponent::FastFields)?;
        let fast_fields_composite = CompositeFile::open(&fast_fields_data)?;
        let fast_field_readers =
            Arc::new(FastFieldReaders::new(schema.clone(), fast_fields_composite));
        let fieldnorm_data = segment.open_read(SegmentComponent::FieldNorms)?;
        let fieldnorm_readers = FieldNormReaders::open(fieldnorm_data)?;

        let original_bitset = if segment.meta().has_deletes() {
            let delete_file_slice = segment.open_read(SegmentComponent::Delete)?;
            let delete_data = delete_file_slice.read_bytes()?;
            Some(AliveBitSet::open(delete_data))
        } else {
            None
        };

        let alive_bitset_opt = intersect_alive_bitset(original_bitset, custom_bitset);

        let max_doc = segment.meta().max_doc();
        let num_docs = alive_bitset_opt
            .as_ref()
            .map(|alive_bitset| alive_bitset.num_alive_docs() as u32)
            .unwrap_or(max_doc);

        Ok(SegmentReader {
            inv_idx_reader_cache: Default::default(),
            num_docs,
            max_doc,
            termdict_composite,
            postings_composite,
            fast_fields_readers: fast_field_readers,
            fieldnorm_readers,
            segment_id: segment.id(),
            delete_opstamp: segment.meta().delete_opstamp(),
            store_file,
            alive_bitset_opt,
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

        let termdict_file: FileSlice =
            self.termdict_composite.open_read(field).ok_or_else(|| {
                DataCorruption::comment_only(format!(
                    "Failed to open field {:?}'s term dictionary in the composite file. Has the \
                     schema been modified?",
                    field_entry.name()
                ))
            })?;

        let positions_file = self.positions_composite.open_read(field).ok_or_else(|| {
            let error_msg = format!(
                "Failed to open field {:?}'s positions in the composite file. Has the schema been \
                 modified?",
                field_entry.name()
            );
            DataCorruption::comment_only(error_msg)
        })?;

        let inv_idx_reader = Arc::new(InvertedIndexReader::new(
            TermDictionary::open(termdict_file)?,
            postings_file,
            positions_file,
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

    /// Returns the delete opstamp
    pub fn delete_opstamp(&self) -> Option<Opstamp> {
        self.delete_opstamp
    }

    /// Returns the bitset representing
    /// the documents that have been deleted.
    pub fn alive_bitset(&self) -> Option<&AliveBitSet> {
        self.alive_bitset_opt.as_ref()
    }

    /// Returns true if the `doc` is marked
    /// as deleted.
    pub fn is_deleted(&self, doc: DocId) -> bool {
        self.alive_bitset()
            .map(|delete_set| delete_set.is_deleted(doc))
            .unwrap_or(false)
    }

    /// Returns an iterator that will iterate over the alive document ids
    pub fn doc_ids_alive(&self) -> Box<dyn Iterator<Item = DocId> + '_> {
        if let Some(alive_bitset) = &self.alive_bitset_opt {
            Box::new(alive_bitset.iter_alive())
        } else {
            Box::new(0u32..self.max_doc)
        }
    }

    /// Summarize total space usage of this segment.
    pub fn space_usage(&self) -> io::Result<SegmentSpaceUsage> {
        Ok(SegmentSpaceUsage::new(
            self.num_docs(),
            self.termdict_composite.space_usage(),
            self.postings_composite.space_usage(),
            self.positions_composite.space_usage(),
            self.fast_fields_readers.space_usage(),
            self.fieldnorm_readers.space_usage(),
            self.get_store_reader()?.space_usage(),
            self.alive_bitset_opt
                .as_ref()
                .map(AliveBitSet::space_usage)
                .unwrap_or(0),
        ))
    }
}

fn intersect_alive_bitset(
    left_opt: Option<AliveBitSet>,
    right_opt: Option<AliveBitSet>,
) -> Option<AliveBitSet> {
    match (left_opt, right_opt) {
        (Some(left), Some(right)) => {
            assert_eq!(left.bitset().max_value(), right.bitset().max_value());
            Some(intersect_alive_bitsets(left, right))
        }
        (Some(left), None) => Some(left),
        (None, Some(right)) => Some(right),
        (None, None) => None,
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
    fn test_num_alive() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        schema_builder.add_text_field("name", TEXT | STORED);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema.clone());
        let name = schema.get_field("name").unwrap();

        {
            let mut index_writer = index.writer_for_tests()?;
            index_writer.add_document(doc!(name => "tantivy"))?;
            index_writer.add_document(doc!(name => "horse"))?;
            index_writer.add_document(doc!(name => "jockey"))?;
            index_writer.add_document(doc!(name => "cap"))?;
            // we should now have one segment with two docs
            index_writer.delete_term(Term::from_field_text(name, "horse"));
            index_writer.delete_term(Term::from_field_text(name, "cap"));

            // ok, now we should have a deleted doc
            index_writer.commit()?;
        }
        let searcher = index.reader()?.searcher();
        assert_eq!(2, searcher.segment_reader(0).num_docs());
        assert_eq!(4, searcher.segment_reader(0).max_doc());
        Ok(())
    }
    #[test]
    fn test_alive_docs_iterator() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        schema_builder.add_text_field("name", TEXT | STORED);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema.clone());
        let name = schema.get_field("name").unwrap();

        {
            let mut index_writer = index.writer_for_tests()?;
            index_writer.add_document(doc!(name => "tantivy"))?;
            index_writer.add_document(doc!(name => "horse"))?;
            index_writer.add_document(doc!(name => "jockey"))?;
            index_writer.add_document(doc!(name => "cap"))?;
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

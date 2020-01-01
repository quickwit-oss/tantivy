use crate::common::CompositeFile;
use crate::common::HasLen;
use crate::core::InvertedIndexReader;
use crate::core::Segment;
use crate::core::SegmentComponent;
use crate::core::SegmentId;
use crate::directory::ReadOnlySource;
use crate::fastfield::DeleteBitSet;
use crate::fastfield::FacetReader;
use crate::fastfield::FastFieldReaders;
use crate::fieldnorm::FieldNormReader;
use crate::schema::Field;
use crate::schema::FieldType;
use crate::schema::Schema;
use crate::space_usage::SegmentSpaceUsage;
use crate::store::StoreReader;
use crate::termdict::TermDictionary;
use crate::DocId;
use fail::fail_point;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::sync::RwLock;

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
    fieldnorms_composite: CompositeFile,

    store_source: ReadOnlySource,
    delete_bitset_opt: Option<DeleteBitSet>,
    schema: Schema,
}

impl SegmentReader {
    /// Open a new segment for reading.
    pub fn open(segment: &Segment) -> crate::Result<SegmentReader> {
        let termdict_source = segment.open_read(SegmentComponent::TERMS)?;
        let termdict_composite = CompositeFile::open(&termdict_source)?;

        let store_source = segment.open_read(SegmentComponent::STORE)?;

        fail_point!("SegmentReader::open#middle");

        let postings_source = segment.open_read(SegmentComponent::POSTINGS)?;
        let postings_composite = CompositeFile::open(&postings_source)?;

        let positions_composite = {
            if let Ok(source) = segment.open_read(SegmentComponent::POSITIONS) {
                CompositeFile::open(&source)?
            } else {
                CompositeFile::empty()
            }
        };

        let positions_idx_composite = {
            if let Ok(source) = segment.open_read(SegmentComponent::POSITIONSSKIP) {
                CompositeFile::open(&source)?
            } else {
                CompositeFile::empty()
            }
        };

        let schema = segment.schema();

        let fast_fields_data = segment.open_read(SegmentComponent::FASTFIELDS)?;
        let fast_fields_composite = CompositeFile::open(&fast_fields_data)?;
        let fast_field_readers =
            Arc::new(FastFieldReaders::load_all(&schema, &fast_fields_composite)?);

        let fieldnorms_data = segment.open_read(SegmentComponent::FIELDNORMS)?;
        let fieldnorms_composite = CompositeFile::open(&fieldnorms_data)?;

        let delete_bitset_opt = if segment.meta().has_deletes() {
            let delete_data = segment.open_read(SegmentComponent::DELETE)?;
            Some(DeleteBitSet::open(delete_data))
        } else {
            None
        };

        Ok(SegmentReader {
            inv_idx_reader_cache: Arc::new(RwLock::new(HashMap::new())),
            max_doc: segment.meta().max_doc(),
            num_docs: segment.meta().num_docs(),
            termdict_composite,
            postings_composite,
            fast_fields_readers: fast_field_readers,
            fieldnorms_composite,
            segment_id: segment.id(),
            store_source,
            delete_bitset_opt,
            positions_composite,
            positions_idx_composite,
            schema,
        })
    }

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
    pub fn facet_reader(&self, field: Field) -> Option<FacetReader> {
        let field_entry = self.schema.get_field_entry(field);
        if field_entry.field_type() != &FieldType::HierarchicalFacet {
            return None;
        }
        let term_ords_reader = self.fast_fields().u64s(field)?;
        let termdict_source = self.termdict_composite.open_read(field)?;
        let termdict = TermDictionary::from_source(&termdict_source);
        let facet_reader = FacetReader::new(term_ords_reader, termdict);
        Some(facet_reader)
    }

    /// Accessor to the segment's `Field norms`'s reader.
    ///
    /// Field norms are the length (in tokens) of the fields.
    /// It is used in the computation of the [TfIdf](https://fulmicoton.gitbooks.io/tantivy-doc/content/tfidf.html).
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
    pub fn get_store_reader(&self) -> StoreReader {
        StoreReader::from_source(self.store_source.clone())
    }
    //
    //    /// Open a new segment for reading.
    //    pub fn open(segment: &Segment) -> crate::Result<SegmentReader> {
    //        let termdict_source = segment.open_read(SegmentComponent::TERMS)?;
    //        let termdict_composite = CompositeFile::open(&termdict_source)?;
    //
    //        let store_source = segment.open_read(SegmentComponent::STORE)?;
    //
    //        fail_point!("SegmentReader::open#middle");
    //
    //        let postings_source = segment.open_read(SegmentComponent::POSTINGS)?;
    //        let postings_composite = CompositeFile::open(&postings_source)?;
    //
    //        let positions_composite = {
    //            if let Ok(source) = segment.open_read(SegmentComponent::POSITIONS) {
    //                CompositeFile::open(&source)?
    //            } else {
    //                CompositeFile::empty()
    //            }
    //        };
    //
    //        let positions_idx_composite = {
    //            if let Ok(source) = segment.open_read(SegmentComponent::POSITIONSSKIP) {
    //                CompositeFile::open(&source)?
    //            } else {
    //                CompositeFile::empty()
    //            }
    //        };
    //
    //        let schema = segment.schema();
    //
    //        let fast_fields_data = segment.open_read(SegmentComponent::FASTFIELDS)?;
    //        let fast_fields_composite = CompositeFile::open(&fast_fields_data)?;
    //        let fast_field_readers =
    //            Arc::new(FastFieldReaders::load_all(&schema, &fast_fields_composite)?);
    //
    //        let fieldnorms_data = segment.open_read(SegmentComponent::FIELDNORMS)?;
    //        let fieldnorms_composite = CompositeFile::open(&fieldnorms_data)?;
    //
    //        let delete_bitset_opt = if segment.meta().has_deletes() {
    //            let delete_data = segment.open_read(SegmentComponent::DELETE)?;
    //            Some(DeleteBitSet::open(delete_data))
    //        } else {
    //            None
    //        };
    //
    //        Ok(SegmentReader {
    //            inv_idx_reader_cache: Arc::new(RwLock::new(HashMap::new())),
    //            max_doc: segment.meta().max_doc(),
    //            num_docs: segment.meta().num_docs(),
    //            termdict_composite,
    //            postings_composite,
    //            fast_fields_readers: fast_field_readers,
    //            fieldnorms_composite,
    //            segment_id: segment.id(),
    //            store_source,
    //            delete_bitset_opt,
    //            positions_composite,
    //            positions_idx_composite,
    //            schema,
    //        })
    //    }

    /// Returns a field reader associated to the field given in argument.
    /// If the field was not present in the index during indexing time,
    /// the InvertedIndexReader is empty.
    ///
    /// The field reader is in charge of iterating through the
    /// term dictionary associated to a specific field,
    /// and opening the posting list associated to any term.
    pub fn inverted_index(&self, field: Field) -> Arc<InvertedIndexReader> {
        if let Some(inv_idx_reader) = self
            .inv_idx_reader_cache
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
            return Arc::new(InvertedIndexReader::empty(field_type));
        }

        let postings_source = postings_source_opt.unwrap();

        let termdict_source = self.termdict_composite.open_read(field).expect(
            "Failed to open field term dictionary in composite file. Is the field indexed?",
        );

        let positions_source = self
            .positions_composite
            .open_read(field)
            .expect("Index corrupted. Failed to open field positions in composite file.");

        let positions_idx_source = self
            .positions_idx_composite
            .open_read(field)
            .expect("Index corrupted. Failed to open field positions in composite file.");

        let inv_idx_reader = Arc::new(InvertedIndexReader::new(
            TermDictionary::from_source(&termdict_source),
            postings_source,
            positions_source,
            positions_idx_source,
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
    pub fn doc_ids_alive(&self) -> SegmentReaderAliveDocsIterator<'_> {
        SegmentReaderAliveDocsIterator::new(&self)
    }

    /// Summarize total space usage of this segment.
    pub fn space_usage(&self) -> SegmentSpaceUsage {
        SegmentSpaceUsage::new(
            self.num_docs(),
            self.termdict_composite.space_usage(),
            self.postings_composite.space_usage(),
            self.positions_composite.space_usage(),
            self.positions_idx_composite.space_usage(),
            self.fast_fields_readers.space_usage(),
            self.fieldnorms_composite.space_usage(),
            self.get_store_reader().space_usage(),
            self.delete_bitset_opt
                .as_ref()
                .map(DeleteBitSet::space_usage)
                .unwrap_or(0),
        )
    }
}

impl fmt::Debug for SegmentReader {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SegmentReader({:?})", self.segment_id)
    }
}

/// Implements the iterator trait to allow easy iteration
/// over non-deleted ("alive") DocIds in a SegmentReader
pub struct SegmentReaderAliveDocsIterator<'a> {
    reader: &'a SegmentReader,
    max_doc: DocId,
    current: DocId,
}

impl<'a> SegmentReaderAliveDocsIterator<'a> {
    pub fn new(reader: &'a SegmentReader) -> SegmentReaderAliveDocsIterator<'a> {
        SegmentReaderAliveDocsIterator {
            reader,
            max_doc: reader.max_doc(),
            current: 0,
        }
    }
}

impl<'a> Iterator for SegmentReaderAliveDocsIterator<'a> {
    type Item = DocId;

    fn next(&mut self) -> Option<Self::Item> {
        // TODO: Use TinySet (like in BitSetDocSet) to speed this process up
        if self.current >= self.max_doc {
            return None;
        }

        // find the next alive doc id
        while self.reader.is_deleted(self.current) {
            self.current += 1;

            if self.current >= self.max_doc {
                return None;
            }
        }

        // capture the current alive DocId
        let result = Some(self.current);

        // move down the chain
        self.current += 1;

        result
    }
}

#[cfg(test)]
mod test {
    use crate::core::Index;
    use crate::schema::{Schema, Term, STORED, TEXT};
    use crate::DocId;

    #[test]
    fn test_alive_docs_iterator() {
        let mut schema_builder = Schema::builder();
        schema_builder.add_text_field("name", TEXT | STORED);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema.clone());
        let name = schema.get_field("name").unwrap();

        {
            let mut index_writer = index.writer_with_num_threads(1, 3_000_000).unwrap();
            index_writer.add_document(doc!(name => "tantivy"));
            index_writer.add_document(doc!(name => "horse"));
            index_writer.add_document(doc!(name => "jockey"));
            index_writer.add_document(doc!(name => "cap"));

            // we should now have one segment with two docs
            index_writer.commit().unwrap();
        }

        {
            let mut index_writer2 = index.writer(50_000_000).unwrap();
            index_writer2.delete_term(Term::from_field_text(name, "horse"));
            index_writer2.delete_term(Term::from_field_text(name, "cap"));

            // ok, now we should have a deleted doc
            index_writer2.commit().unwrap();
        }
        let searcher = index.reader().unwrap().searcher();
        let docs: Vec<DocId> = searcher.segment_reader(0).doc_ids_alive().collect();
        assert_eq!(vec![0u32, 2u32], docs);
    }
}

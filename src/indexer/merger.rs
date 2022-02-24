use std::cmp;
use std::collections::HashMap;
use std::sync::Arc;

use itertools::Itertools;
use measure_time::debug_time;
use tantivy_bitpacker::minmax;

use crate::core::{Segment, SegmentReader};
use crate::docset::{DocSet, TERMINATED};
use crate::error::DataCorruption;
use crate::fastfield::{
    AliveBitSet, CompositeFastFieldSerializer, DynamicFastFieldReader, FastFieldDataAccess,
    FastFieldReader, FastFieldStats, MultiValueLength, MultiValuedFastFieldReader,
};
use crate::fieldnorm::{FieldNormReader, FieldNormReaders, FieldNormsSerializer, FieldNormsWriter};
use crate::indexer::doc_id_mapping::{expect_field_id_for_sort_field, SegmentDocIdMapping};
use crate::indexer::SegmentSerializer;
use crate::postings::{InvertedIndexSerializer, Postings, SegmentPostings};
use crate::schema::{Cardinality, Field, FieldType, Schema};
use crate::store::StoreWriter;
use crate::termdict::{TermMerger, TermOrdinal};
use crate::{
    DocId, IndexSettings, IndexSortByField, InvertedIndexReader, Order, SegmentComponent,
    SegmentOrdinal,
};

/// Segment's max doc must be `< MAX_DOC_LIMIT`.
///
/// We do not allow segments with more than
pub const MAX_DOC_LIMIT: u32 = 1 << 31;

fn estimate_total_num_tokens_in_single_segment(
    reader: &SegmentReader,
    field: Field,
) -> crate::Result<u64> {
    // There are no deletes. We can simply use the exact value saved into the posting list.
    // Note that this value is not necessarily exact as it could have been the result of a merge
    // between segments themselves containing deletes.
    if !reader.has_deletes() {
        return Ok(reader.inverted_index(field)?.total_num_tokens());
    }

    // When there are deletes, we use an approximation either
    // by using the fieldnorm.
    if let Some(fieldnorm_reader) = reader.fieldnorms_readers().get_field(field)? {
        let mut count: [usize; 256] = [0; 256];
        for doc in reader.doc_ids_alive() {
            let fieldnorm_id = fieldnorm_reader.fieldnorm_id(doc);
            count[fieldnorm_id as usize] += 1;
        }
        let total_num_tokens = count
            .iter()
            .cloned()
            .enumerate()
            .map(|(fieldnorm_ord, count)| {
                count as u64 * u64::from(FieldNormReader::id_to_fieldnorm(fieldnorm_ord as u8))
            })
            .sum::<u64>();
        return Ok(total_num_tokens);
    }

    // There are no fieldnorms available.
    // Here we just do a pro-rata with the overall number of tokens an the ratio of
    // documents alive.
    let segment_num_tokens = reader.inverted_index(field)?.total_num_tokens();
    if reader.max_doc() == 0 {
        // That supposedly never happens, but let's be a bit defensive here.
        return Ok(0u64);
    }
    let ratio = reader.num_docs() as f64 / reader.max_doc() as f64;
    Ok((segment_num_tokens as f64 * ratio) as u64)
}

fn estimate_total_num_tokens(readers: &[SegmentReader], field: Field) -> crate::Result<u64> {
    let mut total_num_tokens: u64 = 0;
    for reader in readers {
        total_num_tokens += estimate_total_num_tokens_in_single_segment(reader, field)?;
    }
    Ok(total_num_tokens)
}

pub struct IndexMerger {
    index_settings: IndexSettings,
    schema: Schema,
    pub(crate) readers: Vec<SegmentReader>,
    max_doc: u32,
}

fn compute_min_max_val(
    u64_reader: &impl FastFieldReader<u64>,
    segment_reader: &SegmentReader,
) -> Option<(u64, u64)> {
    if segment_reader.max_doc() == 0 {
        return None;
    }

    if segment_reader.alive_bitset().is_none() {
        // no deleted documents,
        // we can use the previous min_val, max_val.
        return Some((u64_reader.min_value(), u64_reader.max_value()));
    }
    // some deleted documents,
    // we need to recompute the max / min
    minmax(
        segment_reader
            .doc_ids_alive()
            .map(|doc_id| u64_reader.get(doc_id)),
    )
}

struct TermOrdinalMapping {
    per_segment_new_term_ordinals: Vec<Vec<TermOrdinal>>,
}

impl TermOrdinalMapping {
    fn new(max_term_ords: Vec<TermOrdinal>) -> TermOrdinalMapping {
        TermOrdinalMapping {
            per_segment_new_term_ordinals: max_term_ords
                .into_iter()
                .map(|max_term_ord| vec![TermOrdinal::default(); max_term_ord as usize])
                .collect(),
        }
    }

    fn register_from_to(&mut self, segment_ord: usize, from_ord: TermOrdinal, to_ord: TermOrdinal) {
        self.per_segment_new_term_ordinals[segment_ord][from_ord as usize] = to_ord;
    }

    fn get_segment(&self, segment_ord: usize) -> &[TermOrdinal] {
        &(self.per_segment_new_term_ordinals[segment_ord])[..]
    }

    fn max_term_ord(&self) -> TermOrdinal {
        self.per_segment_new_term_ordinals
            .iter()
            .flat_map(|term_ordinals| term_ordinals.iter().cloned().max())
            .max()
            .unwrap_or_default()
    }
}

struct DeltaComputer {
    buffer: Vec<u32>,
}

impl DeltaComputer {
    fn new() -> DeltaComputer {
        DeltaComputer {
            buffer: vec![0u32; 512],
        }
    }

    fn compute_delta(&mut self, positions: &[u32]) -> &[u32] {
        if positions.len() > self.buffer.len() {
            self.buffer.resize(positions.len(), 0u32);
        }
        let mut last_pos = 0u32;
        for (cur_pos, dest) in positions.iter().cloned().zip(self.buffer.iter_mut()) {
            *dest = cur_pos - last_pos;
            last_pos = cur_pos;
        }
        &self.buffer[..positions.len()]
    }
}

impl IndexMerger {
    pub fn open(
        schema: Schema,
        index_settings: IndexSettings,
        segments: &[Segment],
    ) -> crate::Result<IndexMerger> {
        let delete_bitsets = segments.iter().map(|_| None).collect_vec();
        Self::open_with_custom_alive_set(schema, index_settings, segments, delete_bitsets)
    }

    // Create merge with a custom delete set.
    // For every Segment, a delete bitset can be provided, which
    // will be merged with the existing bit set. Make sure the index
    // corresponds to the segment index.
    //
    // If `None` is provided for custom alive set, the regular alive set will be used.
    // If a delete_bitsets is provided, the union between the provided and regular
    // alive set will be used.
    //
    // This can be used to merge but also apply an additional filter.
    // One use case is demux, which is basically taking a list of
    // segments and partitions them e.g. by a value in a field.
    pub fn open_with_custom_alive_set(
        schema: Schema,
        index_settings: IndexSettings,
        segments: &[Segment],
        alive_bitset_opt: Vec<Option<AliveBitSet>>,
    ) -> crate::Result<IndexMerger> {
        let mut readers = vec![];
        for (segment, new_alive_bitset_opt) in segments.iter().zip(alive_bitset_opt.into_iter()) {
            if segment.meta().num_docs() > 0 {
                let reader =
                    SegmentReader::open_with_custom_alive_set(segment, new_alive_bitset_opt)?;
                readers.push(reader);
            }
        }
        let max_doc = readers.iter().map(|reader| reader.num_docs()).sum();
        if let Some(sort_by_field) = index_settings.sort_by_field.as_ref() {
            readers = Self::sort_readers_by_min_sort_field(readers, sort_by_field)?;
        }
        // sort segments by their natural sort setting
        if max_doc >= MAX_DOC_LIMIT {
            let err_msg = format!(
                "The segment resulting from this merge would have {} docs,which exceeds the limit \
                 {}.",
                max_doc, MAX_DOC_LIMIT
            );
            return Err(crate::TantivyError::InvalidArgument(err_msg));
        }
        Ok(IndexMerger {
            index_settings,
            schema,
            readers,
            max_doc,
        })
    }

    fn sort_readers_by_min_sort_field(
        readers: Vec<SegmentReader>,
        sort_by_field: &IndexSortByField,
    ) -> crate::Result<Vec<SegmentReader>> {
        // presort the readers by their min_values, so that when they are disjunct, we can use
        // the regular merge logic (implicitly sorted)
        let mut readers_with_min_sort_values = readers
            .into_iter()
            .map(|reader| {
                let accessor = Self::get_sort_field_accessor(&reader, sort_by_field)?;
                Ok((reader, accessor.min_value()))
            })
            .collect::<crate::Result<Vec<_>>>()?;
        if sort_by_field.order.is_asc() {
            readers_with_min_sort_values.sort_by_key(|(_, min_val)| *min_val);
        } else {
            readers_with_min_sort_values.sort_by_key(|(_, min_val)| std::cmp::Reverse(*min_val));
        }
        Ok(readers_with_min_sort_values
            .into_iter()
            .map(|(reader, _)| reader)
            .collect())
    }

    fn write_fieldnorms(
        &self,
        mut fieldnorms_serializer: FieldNormsSerializer,
        doc_id_mapping: &SegmentDocIdMapping,
    ) -> crate::Result<()> {
        let fields = FieldNormsWriter::fields_with_fieldnorm(&self.schema);
        let mut fieldnorms_data = Vec::with_capacity(self.max_doc as usize);
        for field in fields {
            fieldnorms_data.clear();

            let fieldnorms_readers: Vec<FieldNormReader> = self
                .readers
                .iter()
                .map(|reader| reader.get_fieldnorms_reader(field))
                .collect::<Result<_, _>>()?;
            for (doc_id, reader_ordinal) in doc_id_mapping.iter() {
                let fieldnorms_reader = &fieldnorms_readers[*reader_ordinal as usize];
                let fieldnorm_id = fieldnorms_reader.fieldnorm_id(*doc_id);
                fieldnorms_data.push(fieldnorm_id);
            }

            fieldnorms_serializer.serialize_field(field, &fieldnorms_data[..])?;
        }
        fieldnorms_serializer.close()?;
        Ok(())
    }

    fn write_fast_fields(
        &self,
        fast_field_serializer: &mut CompositeFastFieldSerializer,
        mut term_ord_mappings: HashMap<Field, TermOrdinalMapping>,
        doc_id_mapping: &SegmentDocIdMapping,
    ) -> crate::Result<()> {
        debug_time!("write-fast-fields");

        for (field, field_entry) in self.schema.fields() {
            let field_type = field_entry.field_type();
            match field_type {
                FieldType::Facet(_) => {
                    let term_ordinal_mapping = term_ord_mappings.remove(&field).expect(
                        "Logic Error in Tantivy (Please report). Facet field should have required \
                         a`term_ordinal_mapping`.",
                    );
                    self.write_hierarchical_facet_field(
                        field,
                        &term_ordinal_mapping,
                        fast_field_serializer,
                        doc_id_mapping,
                    )?;
                }
                FieldType::U64(ref options)
                | FieldType::I64(ref options)
                | FieldType::F64(ref options)
                | FieldType::Date(ref options) => match options.get_fastfield_cardinality() {
                    Some(Cardinality::SingleValue) => {
                        self.write_single_fast_field(field, fast_field_serializer, doc_id_mapping)?;
                    }
                    Some(Cardinality::MultiValues) => {
                        self.write_multi_fast_field(field, fast_field_serializer, doc_id_mapping)?;
                    }
                    None => {}
                },
                FieldType::Bytes(byte_options) => {
                    if byte_options.is_fast() {
                        self.write_bytes_fast_field(field, fast_field_serializer, doc_id_mapping)?;
                    }
                }
                FieldType::Str(_) | FieldType::JsonObject(_) => {
                    // We don't handle json / string fast field for the moment
                    // They can be implemented using what is done
                    // for facets in the future
                }
            }
        }
        Ok(())
    }

    // used both to merge field norms, `u64/i64` single fast fields.
    fn write_single_fast_field(
        &self,
        field: Field,
        fast_field_serializer: &mut CompositeFastFieldSerializer,
        doc_id_mapping: &SegmentDocIdMapping,
    ) -> crate::Result<()> {
        let (min_value, max_value) = self
            .readers
            .iter()
            .filter_map(|reader| {
                let u64_reader: DynamicFastFieldReader<u64> =
                    reader.fast_fields().typed_fast_field_reader(field).expect(
                        "Failed to find a reader for single fast field. This is a tantivy bug and \
                         it should never happen.",
                    );
                compute_min_max_val(&u64_reader, reader)
            })
            .reduce(|a, b| (a.0.min(b.0), a.1.max(b.1)))
            .expect("Unexpected error, empty readers in IndexMerger");

        let fast_field_readers = self
            .readers
            .iter()
            .map(|reader| {
                let u64_reader: DynamicFastFieldReader<u64> =
                    reader.fast_fields().typed_fast_field_reader(field).expect(
                        "Failed to find a reader for single fast field. This is a tantivy bug and \
                         it should never happen.",
                    );
                u64_reader
            })
            .collect::<Vec<_>>();

        let stats = FastFieldStats {
            min_value,
            max_value,
            num_vals: doc_id_mapping.len() as u64,
        };
        #[derive(Clone)]
        struct SortedDocIdFieldAccessProvider<'a> {
            doc_id_mapping: &'a SegmentDocIdMapping,
            fast_field_readers: &'a Vec<DynamicFastFieldReader<u64>>,
        }
        impl<'a> FastFieldDataAccess for SortedDocIdFieldAccessProvider<'a> {
            fn get_val(&self, doc: u64) -> u64 {
                let (doc_id, reader_ordinal) = self.doc_id_mapping[doc as usize];
                self.fast_field_readers[reader_ordinal as usize].get(doc_id)
            }
        }
        let fastfield_accessor = SortedDocIdFieldAccessProvider {
            doc_id_mapping,
            fast_field_readers: &fast_field_readers,
        };
        let iter1 = doc_id_mapping.iter().map(|(doc_id, reader_ordinal)| {
            let fast_field_reader = &fast_field_readers[*reader_ordinal as usize];
            fast_field_reader.get(*doc_id)
        });
        let iter2 = doc_id_mapping.iter().map(|(doc_id, reader_ordinal)| {
            let fast_field_reader = &fast_field_readers[*reader_ordinal as usize];
            fast_field_reader.get(*doc_id)
        });
        fast_field_serializer.create_auto_detect_u64_fast_field(
            field,
            stats,
            fastfield_accessor,
            iter1,
            iter2,
        )?;

        Ok(())
    }

    /// Checks if the readers are disjunct for their sort property and in the correct order to be
    /// able to just stack them.
    pub(crate) fn is_disjunct_and_sorted_on_sort_property(
        &self,
        sort_by_field: &IndexSortByField,
    ) -> crate::Result<bool> {
        let reader_ordinal_and_field_accessors =
            self.get_reader_with_sort_field_accessor(sort_by_field)?;

        let everything_is_in_order = reader_ordinal_and_field_accessors
            .into_iter()
            .map(|reader| reader.1)
            .tuple_windows()
            .all(|(field_accessor1, field_accessor2)| {
                if sort_by_field.order.is_asc() {
                    field_accessor1.max_value() <= field_accessor2.min_value()
                } else {
                    field_accessor1.min_value() >= field_accessor2.max_value()
                }
            });
        Ok(everything_is_in_order)
    }

    pub(crate) fn get_sort_field_accessor(
        reader: &SegmentReader,
        sort_by_field: &IndexSortByField,
    ) -> crate::Result<impl FastFieldReader<u64>> {
        let field_id = expect_field_id_for_sort_field(reader.schema(), sort_by_field)?; // for now expect fastfield, but not strictly required
        let value_accessor = reader.fast_fields().u64_lenient(field_id)?;
        Ok(value_accessor)
    }
    /// Collecting value_accessors into a vec to bind the lifetime.
    pub(crate) fn get_reader_with_sort_field_accessor(
        &self,
        sort_by_field: &IndexSortByField,
    ) -> crate::Result<Vec<(SegmentOrdinal, impl FastFieldReader<u64> + Clone)>> {
        let reader_ordinal_and_field_accessors = self
            .readers
            .iter()
            .enumerate()
            .map(|(reader_ordinal, _)| reader_ordinal as SegmentOrdinal)
            .map(|reader_ordinal: SegmentOrdinal| {
                let value_accessor = Self::get_sort_field_accessor(
                    &self.readers[reader_ordinal as usize],
                    sort_by_field,
                )?;
                Ok((reader_ordinal, value_accessor))
            })
            .collect::<crate::Result<Vec<_>>>()?;
        Ok(reader_ordinal_and_field_accessors)
    }

    /// Generates the doc_id mapping where position in the vec=new
    /// doc_id.
    /// ReaderWithOrdinal will include the ordinal position of the
    /// reader in self.readers.
    pub(crate) fn generate_doc_id_mapping(
        &self,
        sort_by_field: &IndexSortByField,
    ) -> crate::Result<SegmentDocIdMapping> {
        let reader_ordinal_and_field_accessors =
            self.get_reader_with_sort_field_accessor(sort_by_field)?;
        // Loading the field accessor on demand causes a 15x regression

        // create iterators over segment/sort_accessor/doc_id  tuple
        let doc_id_reader_pair =
            reader_ordinal_and_field_accessors
                .iter()
                .map(|reader_and_field_accessor| {
                    let reader = &self.readers[reader_and_field_accessor.0 as usize];
                    reader.doc_ids_alive().map(move |doc_id| {
                        (
                            doc_id,
                            reader_and_field_accessor.0,
                            &reader_and_field_accessor.1,
                        )
                    })
                });

        let total_num_new_docs = self
            .readers
            .iter()
            .map(|reader| reader.num_docs() as usize)
            .sum();

        let mut sorted_doc_ids = Vec::with_capacity(total_num_new_docs);

        // create iterator tuple of (old doc_id, reader) in order of the new doc_ids
        sorted_doc_ids.extend(
            doc_id_reader_pair
                .into_iter()
                .kmerge_by(|a, b| {
                    let val1 = a.2.get(a.0);
                    let val2 = b.2.get(b.0);
                    if sort_by_field.order == Order::Asc {
                        val1 < val2
                    } else {
                        val1 > val2
                    }
                })
                .map(|(doc_id, reader_with_id, _)| (doc_id, reader_with_id)),
        );
        Ok(SegmentDocIdMapping::new(sorted_doc_ids, false))
    }

    // Creating the index file to point into the data, generic over `BytesFastFieldReader` and
    // `MultiValuedFastFieldReader`
    //
    fn write_1_n_fast_field_idx_generic<T: MultiValueLength>(
        field: Field,
        fast_field_serializer: &mut CompositeFastFieldSerializer,
        doc_id_mapping: &SegmentDocIdMapping,
        reader_and_field_accessors: &[(&SegmentReader, T)],
    ) -> crate::Result<Vec<u64>> {
        let mut total_num_vals = 0u64;
        // In the first pass, we compute the total number of vals.
        //
        // This is required by the bitpacker, as it needs to know
        // what should be the bit length use for bitpacking.
        let mut num_docs = 0;
        for (reader, u64s_reader) in reader_and_field_accessors.iter() {
            if let Some(alive_bitset) = reader.alive_bitset() {
                num_docs += alive_bitset.num_alive_docs() as u64;
                for doc in reader.doc_ids_alive() {
                    let num_vals = u64s_reader.get_len(doc) as u64;
                    total_num_vals += num_vals;
                }
            } else {
                num_docs += reader.max_doc() as u64;
                total_num_vals += u64s_reader.get_total_len();
            }
        }

        let stats = FastFieldStats {
            max_value: total_num_vals,
            // The fastfield offset index contains (num_docs + 1) values.
            num_vals: num_docs + 1,
            min_value: 0,
        };
        // We can now create our `idx` serializer, and in a second pass,
        // can effectively push the different indexes.

        // copying into a temp vec is not ideal, but the fast field codec api requires random
        // access, which is used in the estimation. It's possible to 1. calculate random
        // acccess on the fly or 2. change the codec api to make random access optional, but
        // they both have also major drawbacks.

        let mut offsets = Vec::with_capacity(doc_id_mapping.len());
        let mut offset = 0;
        for (doc_id, reader) in doc_id_mapping.iter() {
            let reader = &reader_and_field_accessors[*reader as usize].1;
            offsets.push(offset);
            offset += reader.get_len(*doc_id) as u64;
        }
        offsets.push(offset);

        fast_field_serializer.create_auto_detect_u64_fast_field(
            field,
            stats,
            &offsets[..],
            offsets.iter().cloned(),
            offsets.iter().cloned(),
        )?;
        Ok(offsets)
    }
    /// Returns the fastfield index (index for the data, not the data).
    fn write_multi_value_fast_field_idx(
        &self,
        field: Field,
        fast_field_serializer: &mut CompositeFastFieldSerializer,
        doc_id_mapping: &SegmentDocIdMapping,
    ) -> crate::Result<Vec<u64>> {
        let reader_ordinal_and_field_accessors = self
            .readers
            .iter()
            .map(|reader| {
                let u64s_reader: MultiValuedFastFieldReader<u64> = reader
                    .fast_fields()
                    .typed_fast_field_multi_reader(field)
                    .expect(
                        "Failed to find index for multivalued field. This is a bug in tantivy, \
                         please report.",
                    );
                (reader, u64s_reader)
            })
            .collect::<Vec<_>>();

        Self::write_1_n_fast_field_idx_generic(
            field,
            fast_field_serializer,
            doc_id_mapping,
            &reader_ordinal_and_field_accessors,
        )
    }

    fn write_hierarchical_facet_field(
        &self,
        field: Field,
        term_ordinal_mappings: &TermOrdinalMapping,
        fast_field_serializer: &mut CompositeFastFieldSerializer,
        doc_id_mapping: &SegmentDocIdMapping,
    ) -> crate::Result<()> {
        debug_time!("write-hierarchical-facet-field");

        // Multifastfield consists of 2 fastfields.
        // The first serves as an index into the second one and is stricly increasing.
        // The second contains the actual values.

        // First we merge the idx fast field.
        self.write_multi_value_fast_field_idx(field, fast_field_serializer, doc_id_mapping)?;

        let fast_field_reader = self
            .readers
            .iter()
            .map(|reader| {
                let ff_reader: MultiValuedFastFieldReader<u64> = reader
                    .fast_fields()
                    .u64s(field)
                    .expect("Could not find multivalued u64 fast value reader.");
                ff_reader
            })
            .collect::<Vec<_>>();
        // We can now write the actual fast field values.
        // In the case of hierarchical facets, they are actually term ordinals.
        let max_term_ord = term_ordinal_mappings.max_term_ord();
        {
            let mut serialize_vals =
                fast_field_serializer.new_u64_fast_field_with_idx(field, 0u64, max_term_ord, 1)?;
            let mut vals = Vec::with_capacity(100);

            for (old_doc_id, reader_ordinal) in doc_id_mapping.iter() {
                let term_ordinal_mapping: &[TermOrdinal] =
                    term_ordinal_mappings.get_segment(*reader_ordinal as usize);

                let ff_reader = &fast_field_reader[*reader_ordinal as usize];
                ff_reader.get_vals(*old_doc_id, &mut vals);
                for &prev_term_ord in &vals {
                    let new_term_ord = term_ordinal_mapping[prev_term_ord as usize];
                    serialize_vals.add_val(new_term_ord)?;
                }
            }

            serialize_vals.close_field()?;
        }
        Ok(())
    }

    /// Creates a mapping if the segments are stacked. this is helpful to merge codelines between
    /// index sorting and the others
    pub(crate) fn get_doc_id_from_concatenated_data(&self) -> crate::Result<SegmentDocIdMapping> {
        let total_num_new_docs = self
            .readers
            .iter()
            .map(|reader| reader.num_docs() as usize)
            .sum();

        let mut mapping = Vec::with_capacity(total_num_new_docs);

        mapping.extend(
            self.readers
                .iter()
                .enumerate()
                .flat_map(|(reader_ordinal, reader)| {
                    reader
                        .doc_ids_alive()
                        .map(move |doc_id| (doc_id, reader_ordinal as SegmentOrdinal))
                }),
        );
        Ok(SegmentDocIdMapping::new(mapping, true))
    }
    fn write_multi_fast_field(
        &self,
        field: Field,
        fast_field_serializer: &mut CompositeFastFieldSerializer,
        doc_id_mapping: &SegmentDocIdMapping,
    ) -> crate::Result<()> {
        // Multifastfield consists in 2 fastfields.
        // The first serves as an index into the second one and is stricly increasing.
        // The second contains the actual values.

        // First we merge the idx fast field.
        let offsets =
            self.write_multi_value_fast_field_idx(field, fast_field_serializer, doc_id_mapping)?;

        let mut min_value = u64::max_value();
        let mut max_value = u64::min_value();
        let mut num_vals = 0;

        let mut vals = Vec::with_capacity(100);

        let mut ff_readers = Vec::new();

        // Our values are bitpacked and we need to know what should be
        // our bitwidth and our minimum value before serializing any values.
        //
        // Computing those is non-trivial if some documents are deleted.
        // We go through a complete first pass to compute the minimum and the
        // maximum value and initialize our Serializer.
        for reader in &self.readers {
            let ff_reader: MultiValuedFastFieldReader<u64> = reader
                .fast_fields()
                .typed_fast_field_multi_reader(field)
                .expect(
                    "Failed to find multivalued fast field reader. This is a bug in tantivy. \
                     Please report.",
                );
            for doc in reader.doc_ids_alive() {
                ff_reader.get_vals(doc, &mut vals);
                for &val in &vals {
                    min_value = cmp::min(val, min_value);
                    max_value = cmp::max(val, max_value);
                }
                num_vals += vals.len();
            }
            ff_readers.push(ff_reader);
            // TODO optimize when no deletes
        }

        if min_value > max_value {
            min_value = 0;
            max_value = 0;
        }

        // We can now initialize our serializer, and push it the different values
        let stats = FastFieldStats {
            max_value,
            num_vals: num_vals as u64,
            min_value,
        };

        struct SortedDocIdMultiValueAccessProvider<'a> {
            doc_id_mapping: &'a SegmentDocIdMapping,
            fast_field_readers: &'a Vec<MultiValuedFastFieldReader<u64>>,
            offsets: Vec<u64>,
        }
        impl<'a> FastFieldDataAccess for SortedDocIdMultiValueAccessProvider<'a> {
            fn get_val(&self, pos: u64) -> u64 {
                // use the offsets index to find the doc_id which will contain the position.
                // the offsets are stricly increasing so we can do a simple search on it.
                let new_doc_id = self
                    .offsets
                    .iter()
                    .position(|&offset| offset > pos)
                    .expect("pos is out of bounds")
                    - 1;

                // now we need to find the position of `pos` in the multivalued bucket
                let num_pos_covered_until_now = self.offsets[new_doc_id];
                let pos_in_values = pos - num_pos_covered_until_now;

                let (old_doc_id, reader_ordinal) = self.doc_id_mapping[new_doc_id as usize];
                let num_vals = self.fast_field_readers[reader_ordinal as usize].get_len(old_doc_id);
                assert!(num_vals >= pos_in_values);
                let mut vals = vec![];
                self.fast_field_readers[reader_ordinal as usize].get_vals(old_doc_id, &mut vals);

                vals[pos_in_values as usize]
            }
        }
        let fastfield_accessor = SortedDocIdMultiValueAccessProvider {
            doc_id_mapping,
            fast_field_readers: &ff_readers,
            offsets,
        };
        let iter1 = doc_id_mapping.iter().flat_map(|(doc_id, reader_ordinal)| {
            let ff_reader = &ff_readers[*reader_ordinal as usize];
            let mut vals = vec![];
            ff_reader.get_vals(*doc_id, &mut vals);
            vals.into_iter()
        });
        let iter2 = doc_id_mapping.iter().flat_map(|(doc_id, reader_ordinal)| {
            let ff_reader = &ff_readers[*reader_ordinal as usize];
            let mut vals = vec![];
            ff_reader.get_vals(*doc_id, &mut vals);
            vals.into_iter()
        });
        fast_field_serializer.create_auto_detect_u64_fast_field_with_idx(
            field,
            stats,
            fastfield_accessor,
            iter1,
            iter2,
            1,
        )?;

        Ok(())
    }

    fn write_bytes_fast_field(
        &self,
        field: Field,
        fast_field_serializer: &mut CompositeFastFieldSerializer,
        doc_id_mapping: &SegmentDocIdMapping,
    ) -> crate::Result<()> {
        let reader_and_field_accessors = self
            .readers
            .iter()
            .map(|reader| {
                let bytes_reader = reader.fast_fields().bytes(field).expect(
                    "Failed to find index for bytes field. This is a bug in tantivy, please \
                     report.",
                );
                (reader, bytes_reader)
            })
            .collect::<Vec<_>>();

        Self::write_1_n_fast_field_idx_generic(
            field,
            fast_field_serializer,
            doc_id_mapping,
            &reader_and_field_accessors,
        )?;
        let mut serialize_vals = fast_field_serializer.new_bytes_fast_field_with_idx(field, 1);

        for (doc_id, reader_ordinal) in doc_id_mapping.iter() {
            let bytes_reader = &reader_and_field_accessors[*reader_ordinal as usize].1;
            let val = bytes_reader.get_bytes(*doc_id);
            serialize_vals.write_all(val)?;
        }

        serialize_vals.flush()?;
        Ok(())
    }

    fn write_postings_for_field(
        &self,
        indexed_field: Field,
        field_type: &FieldType,
        serializer: &mut InvertedIndexSerializer,
        fieldnorm_reader: Option<FieldNormReader>,
        doc_id_mapping: &SegmentDocIdMapping,
    ) -> crate::Result<Option<TermOrdinalMapping>> {
        debug_time!("write-postings-for-field");
        let mut positions_buffer: Vec<u32> = Vec::with_capacity(1_000);
        let mut delta_computer = DeltaComputer::new();

        let mut max_term_ords: Vec<TermOrdinal> = Vec::new();

        let field_readers: Vec<Arc<InvertedIndexReader>> = self
            .readers
            .iter()
            .map(|reader| reader.inverted_index(indexed_field))
            .collect::<crate::Result<Vec<_>>>()?;

        let mut field_term_streams = Vec::new();
        for field_reader in &field_readers {
            let terms = field_reader.terms();
            field_term_streams.push(terms.stream()?);
            max_term_ords.push(terms.num_terms() as u64);
        }

        let mut term_ord_mapping_opt = match field_type {
            FieldType::Facet(_) => Some(TermOrdinalMapping::new(max_term_ords)),
            _ => None,
        };

        let mut merged_terms = TermMerger::new(field_term_streams);

        // map from segment doc ids to the resulting merged segment doc id.

        let mut merged_doc_id_map: Vec<Vec<Option<DocId>>> = self
            .readers
            .iter()
            .map(|reader| {
                let mut segment_local_map = vec![];
                segment_local_map.resize(reader.max_doc() as usize, None);
                segment_local_map
            })
            .collect();
        for (new_doc_id, (old_doc_id, segment_ord)) in doc_id_mapping.iter().enumerate() {
            let segment_map = &mut merged_doc_id_map[*segment_ord as usize];
            segment_map[*old_doc_id as usize] = Some(new_doc_id as DocId);
        }

        // Note that the total number of tokens is not exact.
        // It is only used as a parameter in the BM25 formula.
        let total_num_tokens: u64 = estimate_total_num_tokens(&self.readers, indexed_field)?;

        // Create the total list of doc ids
        // by stacking the doc ids from the different segment.
        //
        // In the new segments, the doc id from the different
        // segment are stacked so that :
        // - Segment 0's doc ids become doc id [0, seg.max_doc]
        // - Segment 1's doc ids become  [seg0.max_doc, seg0.max_doc + seg.max_doc]
        // - Segment 2's doc ids become  [seg0.max_doc + seg1.max_doc, seg0.max_doc + seg1.max_doc +
        //   seg2.max_doc]
        //
        // This stacking applies only when the index is not sorted, in that case the
        // doc_ids are kmerged by their sort property
        let mut field_serializer =
            serializer.new_field(indexed_field, total_num_tokens, fieldnorm_reader)?;

        let field_entry = self.schema.get_field_entry(indexed_field);

        // ... set segment postings option the new field.
        let segment_postings_option = field_entry.field_type().get_index_record_option().expect(
            "Encountered a field that is not supposed to be
                         indexed. Have you modified the schema?",
        );

        let mut segment_postings_containing_the_term: Vec<(usize, SegmentPostings)> = vec![];
        let mut doc_id_and_positions = vec![];

        while merged_terms.advance() {
            segment_postings_containing_the_term.clear();
            let term_bytes: &[u8] = merged_terms.key();

            let mut total_doc_freq = 0;

            // Let's compute the list of non-empty posting lists
            for (segment_ord, term_info) in merged_terms.current_segment_ords_and_term_infos() {
                let segment_reader = &self.readers[segment_ord];
                let inverted_index: &InvertedIndexReader = &*field_readers[segment_ord];
                let segment_postings = inverted_index
                    .read_postings_from_terminfo(&term_info, segment_postings_option)?;
                let alive_bitset_opt = segment_reader.alive_bitset();
                let doc_freq = if let Some(alive_bitset) = alive_bitset_opt {
                    segment_postings.doc_freq_given_deletes(alive_bitset)
                } else {
                    segment_postings.doc_freq()
                };
                if doc_freq > 0u32 {
                    total_doc_freq += doc_freq;
                    segment_postings_containing_the_term.push((segment_ord, segment_postings));
                }
            }

            // At this point, `segment_postings` contains the posting list
            // of all of the segments containing the given term (and that are non-empty)
            //
            // These segments are non-empty and advance has already been called.
            if total_doc_freq == 0u32 {
                // All docs that used to contain the term have been deleted. The `term` will be
                // entirely removed.
                continue;
            }

            let to_term_ord = field_serializer.new_term(term_bytes, total_doc_freq)?;

            if let Some(ref mut term_ord_mapping) = term_ord_mapping_opt {
                for (segment_ord, from_term_ord) in merged_terms.matching_segments() {
                    term_ord_mapping.register_from_to(segment_ord, from_term_ord, to_term_ord);
                }
            }

            // We can now serialize this postings, by pushing each document to the
            // postings serializer.
            for (segment_ord, mut segment_postings) in
                segment_postings_containing_the_term.drain(..)
            {
                let old_to_new_doc_id = &merged_doc_id_map[segment_ord];

                let mut doc = segment_postings.doc();
                while doc != TERMINATED {
                    // deleted doc are skipped as they do not have a `remapped_doc_id`.
                    if let Some(remapped_doc_id) = old_to_new_doc_id[doc as usize] {
                        // we make sure to only write the term if
                        // there is at least one document.
                        let term_freq = segment_postings.term_freq();
                        segment_postings.positions(&mut positions_buffer);
                        // if doc_id_mapping exists, the doc_ids are reordered, they are
                        // not just stacked. The field serializer expects monotonically increasing
                        // doc_ids, so we collect and sort them first, before writing.
                        //
                        // I think this is not strictly necessary, it would be possible to
                        // avoid the loading into a vec via some form of kmerge, but then the merge
                        // logic would deviate much more from the stacking case (unsorted index)
                        if !doc_id_mapping.is_trivial() {
                            doc_id_and_positions.push((
                                remapped_doc_id,
                                term_freq,
                                positions_buffer.to_vec(),
                            ));
                        } else {
                            let delta_positions = delta_computer.compute_delta(&positions_buffer);
                            field_serializer.write_doc(remapped_doc_id, term_freq, delta_positions);
                        }
                    }

                    doc = segment_postings.advance();
                }
            }
            if !doc_id_mapping.is_trivial() {
                doc_id_and_positions.sort_unstable_by_key(|&(doc_id, _, _)| doc_id);

                for (doc_id, term_freq, positions) in &doc_id_and_positions {
                    let delta_positions = delta_computer.compute_delta(positions);
                    field_serializer.write_doc(*doc_id, *term_freq, delta_positions);
                }
                doc_id_and_positions.clear();
            }
            // closing the term.
            field_serializer.close_term()?;
        }
        field_serializer.close()?;
        Ok(term_ord_mapping_opt)
    }

    fn write_postings(
        &self,
        serializer: &mut InvertedIndexSerializer,
        fieldnorm_readers: FieldNormReaders,
        doc_id_mapping: &SegmentDocIdMapping,
    ) -> crate::Result<HashMap<Field, TermOrdinalMapping>> {
        let mut term_ordinal_mappings = HashMap::new();
        for (field, field_entry) in self.schema.fields() {
            let fieldnorm_reader = fieldnorm_readers.get_field(field)?;
            if field_entry.is_indexed() {
                if let Some(term_ordinal_mapping) = self.write_postings_for_field(
                    field,
                    field_entry.field_type(),
                    serializer,
                    fieldnorm_reader,
                    doc_id_mapping,
                )? {
                    term_ordinal_mappings.insert(field, term_ordinal_mapping);
                }
            }
        }
        Ok(term_ordinal_mappings)
    }

    fn write_storable_fields(
        &self,
        store_writer: &mut StoreWriter,
        doc_id_mapping: &SegmentDocIdMapping,
    ) -> crate::Result<()> {
        debug_time!("write-storable-fields");
        debug!("write-storable-field");

        let store_readers: Vec<_> = self
            .readers
            .iter()
            .map(|reader| reader.get_store_reader())
            .collect::<Result<_, _>>()?;
        let mut document_iterators: Vec<_> = store_readers
            .iter()
            .enumerate()
            .map(|(i, store)| store.iter_raw(self.readers[i].alive_bitset()))
            .collect();
        if !doc_id_mapping.is_trivial() {
            debug!("non-trivial-doc-id-mapping");
            for (old_doc_id, reader_ordinal) in doc_id_mapping.iter() {
                let doc_bytes_it = &mut document_iterators[*reader_ordinal as usize];
                if let Some(doc_bytes_res) = doc_bytes_it.next() {
                    let doc_bytes = doc_bytes_res?;
                    store_writer.store_bytes(&doc_bytes)?;
                } else {
                    return Err(DataCorruption::comment_only(&format!(
                        "unexpected missing document in docstore on merge, doc id {:?}",
                        old_doc_id
                    ))
                    .into());
                }
            }
        } else {
            debug!("trivial-doc-id-mapping");
            for reader in &self.readers {
                let store_reader = reader.get_store_reader()?;
                if reader.has_deletes()
                    // If there is not enough data in the store, we avoid stacking in order to
                    // avoid creating many small blocks in the doc store. Once we have 5 full blocks,
                    // we start stacking. In the worst case 2/7 of the blocks would be very small.
                    // [segment 1 - {1 doc}][segment 2 - {fullblock * 5}{1doc}]
                    // => 5 * full blocks, 2 * 1 document blocks
                    //
                    // In a more realistic scenario the segments are of the same size, so 1/6 of
                    // the doc stores would be on average half full, given total randomness (which
                    // is not the case here, but not sure how it behaves exactly).
                    //
                    // https://github.com/quickwit-oss/tantivy/issues/1053
                    //
                    // take 7 in order to not walk over all checkpoints.
                    || store_reader.block_checkpoints().take(7).count() < 6
                    || store_reader.compressor() != store_writer.compressor()
                {
                    for doc_bytes_res in store_reader.iter_raw(reader.alive_bitset()) {
                        let doc_bytes = doc_bytes_res?;
                        store_writer.store_bytes(&doc_bytes)?;
                    }
                } else {
                    store_writer.stack(&store_reader)?;
                }
            }
        }
        Ok(())
    }

    /// Writes the merged segment by pushing information
    /// to the `SegmentSerializer`.
    ///
    /// # Returns
    /// The number of documents in the resulting segment.
    pub fn write(&self, mut serializer: SegmentSerializer) -> crate::Result<u32> {
        let doc_id_mapping = if let Some(sort_by_field) = self.index_settings.sort_by_field.as_ref()
        {
            // If the documents are already sorted and stackable, we ignore the mapping and execute
            // it as if there was no sorting
            if self.is_disjunct_and_sorted_on_sort_property(sort_by_field)? {
                self.get_doc_id_from_concatenated_data()?
            } else {
                self.generate_doc_id_mapping(sort_by_field)?
            }
        } else {
            self.get_doc_id_from_concatenated_data()?
        };
        debug!("write-fieldnorms");
        if let Some(fieldnorms_serializer) = serializer.extract_fieldnorms_serializer() {
            self.write_fieldnorms(fieldnorms_serializer, &doc_id_mapping)?;
        }
        debug!("write-postings");
        let fieldnorm_data = serializer
            .segment()
            .open_read(SegmentComponent::FieldNorms)?;
        let fieldnorm_readers = FieldNormReaders::open(fieldnorm_data)?;
        let term_ord_mappings = self.write_postings(
            serializer.get_postings_serializer(),
            fieldnorm_readers,
            &doc_id_mapping,
        )?;
        debug!("write-fastfields");
        self.write_fast_fields(
            serializer.get_fast_field_serializer(),
            term_ord_mappings,
            &doc_id_mapping,
        )?;
        debug!("write-storagefields");
        self.write_storable_fields(serializer.get_store_writer(), &doc_id_mapping)?;
        debug!("close-serializer");
        serializer.close()?;
        Ok(self.max_doc)
    }
}

#[cfg(test)]
mod tests {
    use byteorder::{BigEndian, ReadBytesExt};
    use futures::executor::block_on;
    use schema::FAST;

    use crate::collector::tests::{
        BytesFastFieldTestCollector, FastFieldTestCollector, TEST_COLLECTOR_WITH_SCORE,
    };
    use crate::collector::{Count, FacetCollector};
    use crate::core::Index;
    use crate::fastfield::FastFieldReader;
    use crate::query::{AllQuery, BooleanQuery, Scorer, TermQuery};
    use crate::schema::{
        Cardinality, Document, Facet, FacetOptions, IndexRecordOption, NumericOptions, Term,
        TextFieldIndexing, INDEXED, TEXT,
    };
    use crate::{
        assert_nearly_equals, schema, DocAddress, DocSet, IndexSettings, IndexSortByField,
        IndexWriter, Order, Searcher, SegmentId,
    };

    #[test]
    fn test_index_merger_no_deletes() -> crate::Result<()> {
        let mut schema_builder = schema::Schema::builder();
        let text_fieldtype = schema::TextOptions::default()
            .set_indexing_options(
                TextFieldIndexing::default()
                    .set_tokenizer("default")
                    .set_index_option(IndexRecordOption::WithFreqs),
            )
            .set_stored();
        let text_field = schema_builder.add_text_field("text", text_fieldtype);
        let date_field = schema_builder.add_date_field("date", INDEXED);
        let score_fieldtype = schema::NumericOptions::default().set_fast(Cardinality::SingleValue);
        let score_field = schema_builder.add_u64_field("score", score_fieldtype);
        let bytes_score_field = schema_builder.add_bytes_field("score_bytes", FAST);
        let index = Index::create_in_ram(schema_builder.build());
        let reader = index.reader()?;
        let curr_time = chrono::Utc::now();
        {
            let mut index_writer = index.writer_for_tests()?;
            // writing the segment
            index_writer.add_document(doc!(
                text_field => "af b",
                score_field => 3u64,
                date_field => curr_time,
                bytes_score_field => 3u32.to_be_bytes().as_ref()
            ))?;
            index_writer.add_document(doc!(
                text_field => "a b c",
                score_field => 5u64,
                bytes_score_field => 5u32.to_be_bytes().as_ref()
            ))?;
            index_writer.add_document(doc!(
                text_field => "a b c d",
                score_field => 7u64,
                bytes_score_field => 7u32.to_be_bytes().as_ref()
            ))?;
            index_writer.commit()?;
            // writing the segment
            index_writer.add_document(doc!(
                text_field => "af b",
                date_field => curr_time,
                score_field => 11u64,
                bytes_score_field => 11u32.to_be_bytes().as_ref()
            ))?;
            index_writer.add_document(doc!(
                text_field => "a b c g",
                score_field => 13u64,
                bytes_score_field => 13u32.to_be_bytes().as_ref()
            ))?;
            index_writer.commit()?;
        }
        {
            let segment_ids = index
                .searchable_segment_ids()
                .expect("Searchable segments failed.");
            let mut index_writer = index.writer_for_tests()?;
            block_on(index_writer.merge(&segment_ids))?;
            index_writer.wait_merging_threads()?;
        }
        {
            reader.reload()?;
            let searcher = reader.searcher();
            let get_doc_ids = |terms: Vec<Term>| {
                let query = BooleanQuery::new_multiterms_query(terms);
                searcher
                    .search(&query, &TEST_COLLECTOR_WITH_SCORE)
                    .map(|top_docs| top_docs.docs().to_vec())
            };
            {
                assert_eq!(
                    get_doc_ids(vec![Term::from_field_text(text_field, "a")])?,
                    vec![
                        DocAddress::new(0, 1),
                        DocAddress::new(0, 2),
                        DocAddress::new(0, 4)
                    ]
                );
                assert_eq!(
                    get_doc_ids(vec![Term::from_field_text(text_field, "af")])?,
                    vec![DocAddress::new(0, 0), DocAddress::new(0, 3)]
                );
                assert_eq!(
                    get_doc_ids(vec![Term::from_field_text(text_field, "g")])?,
                    vec![DocAddress::new(0, 4)]
                );
                assert_eq!(
                    get_doc_ids(vec![Term::from_field_text(text_field, "b")])?,
                    vec![
                        DocAddress::new(0, 0),
                        DocAddress::new(0, 1),
                        DocAddress::new(0, 2),
                        DocAddress::new(0, 3),
                        DocAddress::new(0, 4)
                    ]
                );
                assert_eq!(
                    get_doc_ids(vec![Term::from_field_date(date_field, &curr_time)])?,
                    vec![DocAddress::new(0, 0), DocAddress::new(0, 3)]
                );
            }
            {
                let doc = searcher.doc(DocAddress::new(0, 0))?;
                assert_eq!(doc.get_first(text_field).unwrap().as_text(), Some("af b"));
            }
            {
                let doc = searcher.doc(DocAddress::new(0, 1))?;
                assert_eq!(doc.get_first(text_field).unwrap().as_text(), Some("a b c"));
            }
            {
                let doc = searcher.doc(DocAddress::new(0, 2))?;
                assert_eq!(
                    doc.get_first(text_field).unwrap().as_text(),
                    Some("a b c d")
                );
            }
            {
                let doc = searcher.doc(DocAddress::new(0, 3))?;
                assert_eq!(doc.get_first(text_field).unwrap().as_text(), Some("af b"));
            }
            {
                let doc = searcher.doc(DocAddress::new(0, 4))?;
                assert_eq!(
                    doc.get_first(text_field).unwrap().as_text(),
                    Some("a b c g")
                );
            }
            {
                let get_fast_vals = |terms: Vec<Term>| {
                    let query = BooleanQuery::new_multiterms_query(terms);
                    searcher.search(&query, &FastFieldTestCollector::for_field(score_field))
                };
                let get_fast_vals_bytes = |terms: Vec<Term>| {
                    let query = BooleanQuery::new_multiterms_query(terms);
                    searcher.search(
                        &query,
                        &BytesFastFieldTestCollector::for_field(bytes_score_field),
                    )
                };
                assert_eq!(
                    get_fast_vals(vec![Term::from_field_text(text_field, "a")])?,
                    vec![5, 7, 13]
                );
                assert_eq!(
                    get_fast_vals_bytes(vec![Term::from_field_text(text_field, "a")])?,
                    vec![0, 0, 0, 5, 0, 0, 0, 7, 0, 0, 0, 13]
                );
            }
        }
        Ok(())
    }

    #[test]
    fn test_index_merger_with_deletes() -> crate::Result<()> {
        let mut schema_builder = schema::Schema::builder();
        let text_fieldtype = schema::TextOptions::default()
            .set_indexing_options(
                TextFieldIndexing::default().set_index_option(IndexRecordOption::WithFreqs),
            )
            .set_stored();
        let text_field = schema_builder.add_text_field("text", text_fieldtype);
        let score_fieldtype = schema::NumericOptions::default().set_fast(Cardinality::SingleValue);
        let score_field = schema_builder.add_u64_field("score", score_fieldtype);
        let bytes_score_field = schema_builder.add_bytes_field("score_bytes", FAST);
        let index = Index::create_in_ram(schema_builder.build());
        let mut index_writer = index.writer_for_tests()?;
        let reader = index.reader().unwrap();
        let search_term = |searcher: &Searcher, term: Term| {
            let collector = FastFieldTestCollector::for_field(score_field);
            let bytes_collector = BytesFastFieldTestCollector::for_field(bytes_score_field);
            let term_query = TermQuery::new(term, IndexRecordOption::Basic);
            searcher
                .search(&term_query, &(collector, bytes_collector))
                .map(|(scores, bytes)| {
                    let mut score_bytes = &bytes[..];
                    for &score in &scores {
                        assert_eq!(score as u32, score_bytes.read_u32::<BigEndian>().unwrap());
                    }
                    scores
                })
        };

        let empty_vec = Vec::<u64>::new();
        {
            // a first commit
            index_writer.add_document(doc!(
                text_field => "a b d",
                score_field => 1u64,
                bytes_score_field => vec![0u8, 0, 0, 1],
            ))?;
            index_writer.add_document(doc!(
                text_field => "b c",
                score_field => 2u64,
                bytes_score_field => vec![0u8, 0, 0, 2],
            ))?;
            index_writer.delete_term(Term::from_field_text(text_field, "c"));
            index_writer.add_document(doc!(
                text_field => "c d",
                score_field => 3u64,
                bytes_score_field => vec![0u8, 0, 0, 3],
            ))?;
            index_writer.commit()?;
            reader.reload()?;
            let searcher = reader.searcher();
            assert_eq!(searcher.num_docs(), 2);
            assert_eq!(searcher.segment_readers()[0].num_docs(), 2);
            assert_eq!(searcher.segment_readers()[0].max_doc(), 3);
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "a"))?,
                vec![1]
            );
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "b"))?,
                vec![1]
            );
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "c"))?,
                vec![3]
            );
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "d"))?,
                vec![1, 3]
            );
        }
        {
            // a second commit
            index_writer.add_document(doc!(
                text_field => "a d e",
                score_field => 4_000u64,
                bytes_score_field => vec![0u8, 0, 0, 4],
            ))?;
            index_writer.add_document(doc!(
                text_field => "e f",
                score_field => 5_000u64,
                bytes_score_field => vec![0u8, 0, 0, 5],
            ))?;
            index_writer.delete_term(Term::from_field_text(text_field, "a"));
            index_writer.delete_term(Term::from_field_text(text_field, "f"));
            index_writer.add_document(doc!(
                text_field => "f g",
                score_field => 6_000u64,
                bytes_score_field => vec![0u8, 0, 23, 112],
            ))?;
            index_writer.add_document(doc!(
                text_field => "g h",
                score_field => 7_000u64,
                bytes_score_field => vec![0u8, 0, 27, 88],
            ))?;
            index_writer.commit()?;
            reader.reload()?;
            let searcher = reader.searcher();

            assert_eq!(searcher.segment_readers().len(), 2);
            assert_eq!(searcher.num_docs(), 3);
            assert_eq!(searcher.segment_readers()[0].num_docs(), 2);
            assert_eq!(searcher.segment_readers()[0].max_doc(), 4);
            assert_eq!(searcher.segment_readers()[1].num_docs(), 1);
            assert_eq!(searcher.segment_readers()[1].max_doc(), 3);
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "a"))?,
                empty_vec
            );
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "b"))?,
                empty_vec
            );
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "c"))?,
                vec![3]
            );
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "d"))?,
                vec![3]
            );
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "e"))?,
                empty_vec
            );
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "f"))?,
                vec![6_000]
            );
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "g"))?,
                vec![6_000, 7_000]
            );

            let score_field_reader = searcher
                .segment_reader(0)
                .fast_fields()
                .u64(score_field)
                .unwrap();
            assert_eq!(score_field_reader.min_value(), 4000);
            assert_eq!(score_field_reader.max_value(), 7000);

            let score_field_reader = searcher
                .segment_reader(1)
                .fast_fields()
                .u64(score_field)
                .unwrap();
            assert_eq!(score_field_reader.min_value(), 1);
            assert_eq!(score_field_reader.max_value(), 3);
        }
        {
            // merging the segments
            let segment_ids = index.searchable_segment_ids()?;
            block_on(index_writer.merge(&segment_ids))?;
            reader.reload()?;
            let searcher = reader.searcher();
            assert_eq!(searcher.segment_readers().len(), 1);
            assert_eq!(searcher.num_docs(), 3);
            assert_eq!(searcher.segment_readers()[0].num_docs(), 3);
            assert_eq!(searcher.segment_readers()[0].max_doc(), 3);
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "a"))?,
                empty_vec
            );
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "b"))?,
                empty_vec
            );
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "c"))?,
                vec![3]
            );
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "d"))?,
                vec![3]
            );
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "e"))?,
                empty_vec
            );
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "f"))?,
                vec![6_000]
            );
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "g"))?,
                vec![6_000, 7_000]
            );
            let score_field_reader = searcher
                .segment_reader(0)
                .fast_fields()
                .u64(score_field)
                .unwrap();
            assert_eq!(score_field_reader.min_value(), 3);
            assert_eq!(score_field_reader.max_value(), 7000);
        }
        {
            // test a commit with only deletes
            index_writer.delete_term(Term::from_field_text(text_field, "c"));
            index_writer.commit()?;

            reader.reload()?;
            let searcher = reader.searcher();
            assert_eq!(searcher.segment_readers().len(), 1);
            assert_eq!(searcher.num_docs(), 2);
            assert_eq!(searcher.segment_readers()[0].num_docs(), 2);
            assert_eq!(searcher.segment_readers()[0].max_doc(), 3);
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "a"))?,
                empty_vec
            );
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "b"))?,
                empty_vec
            );
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "c"))?,
                empty_vec
            );
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "d"))?,
                empty_vec
            );
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "e"))?,
                empty_vec
            );
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "f"))?,
                vec![6_000]
            );
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "g"))?,
                vec![6_000, 7_000]
            );
            let score_field_reader = searcher
                .segment_reader(0)
                .fast_fields()
                .u64(score_field)
                .unwrap();
            assert_eq!(score_field_reader.min_value(), 3);
            assert_eq!(score_field_reader.max_value(), 7000);
        }
        {
            // Test merging a single segment in order to remove deletes.
            let segment_ids = index.searchable_segment_ids()?;
            block_on(index_writer.merge(&segment_ids))?;
            reader.reload()?;

            let searcher = reader.searcher();
            assert_eq!(searcher.segment_readers().len(), 1);
            assert_eq!(searcher.num_docs(), 2);
            assert_eq!(searcher.segment_readers()[0].num_docs(), 2);
            assert_eq!(searcher.segment_readers()[0].max_doc(), 2);
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "a"))?,
                empty_vec
            );
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "b"))?,
                empty_vec
            );
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "c"))?,
                empty_vec
            );
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "d"))?,
                empty_vec
            );
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "e"))?,
                empty_vec
            );
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "f"))?,
                vec![6_000]
            );
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "g"))?,
                vec![6_000, 7_000]
            );
            let score_field_reader = searcher
                .segment_reader(0)
                .fast_fields()
                .u64(score_field)
                .unwrap();
            assert_eq!(score_field_reader.min_value(), 6000);
            assert_eq!(score_field_reader.max_value(), 7000);
        }

        {
            // Test removing all docs
            index_writer.delete_term(Term::from_field_text(text_field, "g"));
            index_writer.commit()?;
            let segment_ids = index.searchable_segment_ids()?;
            reader.reload()?;

            let searcher = reader.searcher();
            assert!(segment_ids.is_empty());
            assert!(searcher.segment_readers().is_empty());
            assert_eq!(searcher.num_docs(), 0);
        }
        Ok(())
    }
    #[test]
    fn test_merge_facets_sort_none() {
        test_merge_facets(None, true)
    }

    #[test]
    fn test_merge_facets_sort_asc() {
        // In the merge case this will go through the doc_id mapping code
        test_merge_facets(
            Some(IndexSettings {
                sort_by_field: Some(IndexSortByField {
                    field: "intval".to_string(),
                    order: Order::Desc,
                }),
                ..Default::default()
            }),
            true,
        );
        // In the merge case this will not go through the doc_id mapping code, because the data is
        // sorted and disjunct
        test_merge_facets(
            Some(IndexSettings {
                sort_by_field: Some(IndexSortByField {
                    field: "intval".to_string(),
                    order: Order::Desc,
                }),
                ..Default::default()
            }),
            false,
        );
    }

    #[test]
    fn test_merge_facets_sort_desc() {
        // In the merge case this will go through the doc_id mapping code
        test_merge_facets(
            Some(IndexSettings {
                sort_by_field: Some(IndexSortByField {
                    field: "intval".to_string(),
                    order: Order::Desc,
                }),
                ..Default::default()
            }),
            true,
        );
        // In the merge case this will not go through the doc_id mapping code, because the data is
        // sorted and disjunct
        test_merge_facets(
            Some(IndexSettings {
                sort_by_field: Some(IndexSortByField {
                    field: "intval".to_string(),
                    order: Order::Desc,
                }),
                ..Default::default()
            }),
            false,
        );
    }
    // force_segment_value_overlap forces the int value for sorting to have overlapping min and max
    // ranges between segments so that merge algorithm can't apply certain optimizations
    fn test_merge_facets(index_settings: Option<IndexSettings>, force_segment_value_overlap: bool) {
        let mut schema_builder = schema::Schema::builder();
        let facet_field = schema_builder.add_facet_field("facet", FacetOptions::default());
        let int_options = NumericOptions::default()
            .set_fast(Cardinality::SingleValue)
            .set_indexed();
        let int_field = schema_builder.add_u64_field("intval", int_options);
        let mut index_builder = Index::builder().schema(schema_builder.build());
        if let Some(settings) = index_settings {
            index_builder = index_builder.settings(settings);
        }
        let index = index_builder.create_in_ram().unwrap();
        // let index = Index::create_in_ram(schema_builder.build());
        let reader = index.reader().unwrap();
        let mut int_val = 0;
        {
            let mut index_writer = index.writer_for_tests().unwrap();
            let index_doc =
                |index_writer: &mut IndexWriter, doc_facets: &[&str], int_val: &mut u64| {
                    let mut doc = Document::default();
                    for facet in doc_facets {
                        doc.add_facet(facet_field, Facet::from(facet));
                    }
                    doc.add_u64(int_field, *int_val);
                    *int_val += 1;
                    index_writer.add_document(doc).unwrap();
                };

            index_doc(
                &mut index_writer,
                &["/top/a/firstdoc", "/top/b"],
                &mut int_val,
            );
            index_doc(
                &mut index_writer,
                &["/top/a/firstdoc", "/top/b", "/top/c"],
                &mut int_val,
            );
            index_doc(&mut index_writer, &["/top/a", "/top/b"], &mut int_val);
            index_doc(&mut index_writer, &["/top/a"], &mut int_val);

            index_doc(&mut index_writer, &["/top/b", "/top/d"], &mut int_val);
            if force_segment_value_overlap {
                index_doc(&mut index_writer, &["/top/d"], &mut 0);
                index_doc(&mut index_writer, &["/top/e"], &mut 10);
                index_writer.commit().expect("committed");
                index_doc(&mut index_writer, &["/top/a"], &mut 5); // 5 is between 0 - 10 so the
                                                                   // segments don' have disjunct
                                                                   // ranges
            } else {
                index_doc(&mut index_writer, &["/top/d"], &mut int_val);
                index_doc(&mut index_writer, &["/top/e"], &mut int_val);
                index_writer.commit().expect("committed");
                index_doc(&mut index_writer, &["/top/a"], &mut int_val);
            }
            index_doc(&mut index_writer, &["/top/b"], &mut int_val);
            index_doc(&mut index_writer, &["/top/c"], &mut int_val);
            index_writer.commit().expect("committed");

            index_doc(&mut index_writer, &["/top/e", "/top/f"], &mut int_val);
            index_writer.commit().expect("committed");
        }

        reader.reload().unwrap();
        let test_searcher = |expected_num_docs: usize, expected: &[(&str, u64)]| {
            let searcher = reader.searcher();
            let mut facet_collector = FacetCollector::for_field(facet_field);
            facet_collector.add_facet(Facet::from("/top"));
            let (count, facet_counts) = searcher
                .search(&AllQuery, &(Count, facet_collector))
                .unwrap();
            assert_eq!(count, expected_num_docs);
            let facets: Vec<(String, u64)> = facet_counts
                .get("/top")
                .map(|(facet, count)| (facet.to_string(), count))
                .collect();
            assert_eq!(
                facets,
                expected
                    .iter()
                    .map(|&(facet_str, count)| (String::from(facet_str), count))
                    .collect::<Vec<_>>()
            );
        };
        test_searcher(
            11,
            &[
                ("/top/a", 5),
                ("/top/b", 5),
                ("/top/c", 2),
                ("/top/d", 2),
                ("/top/e", 2),
                ("/top/f", 1),
            ],
        );
        // Merging the segments
        {
            let segment_ids = index
                .searchable_segment_ids()
                .expect("Searchable segments failed.");
            let mut index_writer = index.writer_for_tests().unwrap();
            block_on(index_writer.merge(&segment_ids)).expect("Merging failed");
            index_writer.wait_merging_threads().unwrap();
            reader.reload().unwrap();
            test_searcher(
                11,
                &[
                    ("/top/a", 5),
                    ("/top/b", 5),
                    ("/top/c", 2),
                    ("/top/d", 2),
                    ("/top/e", 2),
                    ("/top/f", 1),
                ],
            );
        }

        // Deleting one term
        {
            let mut index_writer = index.writer_for_tests().unwrap();
            let facet = Facet::from_path(vec!["top", "a", "firstdoc"]);
            let facet_term = Term::from_facet(facet_field, &facet);
            index_writer.delete_term(facet_term);
            index_writer.commit().unwrap();
            reader.reload().unwrap();
            test_searcher(
                9,
                &[
                    ("/top/a", 3),
                    ("/top/b", 3),
                    ("/top/c", 1),
                    ("/top/d", 2),
                    ("/top/e", 2),
                    ("/top/f", 1),
                ],
            );
        }
    }

    #[test]
    fn test_bug_merge() -> crate::Result<()> {
        let mut schema_builder = schema::Schema::builder();
        let int_field = schema_builder.add_u64_field("intvals", INDEXED);
        let index = Index::create_in_ram(schema_builder.build());
        let mut index_writer = index.writer_for_tests().unwrap();
        index_writer.add_document(doc!(int_field => 1u64))?;
        index_writer.commit().expect("commit failed");
        index_writer.add_document(doc!(int_field => 1u64))?;
        index_writer.commit().expect("commit failed");
        let reader = index.reader()?;
        let searcher = reader.searcher();
        assert_eq!(searcher.num_docs(), 2);
        index_writer.delete_term(Term::from_field_u64(int_field, 1));
        let segment_ids = index
            .searchable_segment_ids()
            .expect("Searchable segments failed.");
        block_on(index_writer.merge(&segment_ids))?;
        reader.reload()?;
        // commit has not been called yet. The document should still be
        // there.
        assert_eq!(reader.searcher().num_docs(), 2);
        Ok(())
    }

    #[test]
    fn test_merge_multivalued_int_fields_all_deleted() -> crate::Result<()> {
        let mut schema_builder = schema::Schema::builder();
        let int_options = NumericOptions::default()
            .set_fast(Cardinality::MultiValues)
            .set_indexed();
        let int_field = schema_builder.add_u64_field("intvals", int_options);
        let index = Index::create_in_ram(schema_builder.build());
        let reader = index.reader()?;
        {
            let mut index_writer = index.writer_for_tests()?;
            let mut doc = Document::default();
            doc.add_u64(int_field, 1);
            index_writer.add_document(doc.clone())?;
            index_writer.commit()?;
            index_writer.add_document(doc)?;
            index_writer.commit()?;
            index_writer.delete_term(Term::from_field_u64(int_field, 1));
            let segment_ids = index.searchable_segment_ids()?;
            block_on(index_writer.merge(&segment_ids))?;

            // assert delete has not been committed
            reader.reload()?;
            let searcher = reader.searcher();
            assert_eq!(searcher.num_docs(), 2);

            index_writer.commit()?;

            index_writer.wait_merging_threads()?;
        }

        reader.reload()?;
        let searcher = reader.searcher();
        assert_eq!(searcher.num_docs(), 0);
        Ok(())
    }

    #[test]
    fn test_merge_multivalued_int_fields_simple() -> crate::Result<()> {
        let mut schema_builder = schema::Schema::builder();
        let int_options = NumericOptions::default()
            .set_fast(Cardinality::MultiValues)
            .set_indexed();
        let int_field = schema_builder.add_u64_field("intvals", int_options);
        let index = Index::create_in_ram(schema_builder.build());

        {
            let mut index_writer = index.writer_for_tests()?;
            let index_doc = |index_writer: &mut IndexWriter, int_vals: &[u64]| {
                let mut doc = Document::default();
                for &val in int_vals {
                    doc.add_u64(int_field, val);
                }
                index_writer.add_document(doc).unwrap();
            };
            index_doc(&mut index_writer, &[1, 2]);
            index_doc(&mut index_writer, &[1, 2, 3]);
            index_doc(&mut index_writer, &[4, 5]);
            index_doc(&mut index_writer, &[1, 2]);
            index_doc(&mut index_writer, &[1, 5]);
            index_doc(&mut index_writer, &[3]);
            index_doc(&mut index_writer, &[17]);
            assert!(index_writer.commit().is_ok());
            index_doc(&mut index_writer, &[20]);
            assert!(index_writer.commit().is_ok());
            index_doc(&mut index_writer, &[28, 27]);
            index_doc(&mut index_writer, &[1_000]);
            assert!(index_writer.commit().is_ok());
        }
        let reader = index.reader()?;
        let searcher = reader.searcher();
        let mut vals: Vec<u64> = Vec::new();

        {
            let segment = searcher.segment_reader(0u32);
            let ff_reader = segment.fast_fields().u64s(int_field).unwrap();

            ff_reader.get_vals(0, &mut vals);
            assert_eq!(&vals, &[1, 2]);

            ff_reader.get_vals(1, &mut vals);
            assert_eq!(&vals, &[1, 2, 3]);

            ff_reader.get_vals(2, &mut vals);
            assert_eq!(&vals, &[4, 5]);

            ff_reader.get_vals(3, &mut vals);
            assert_eq!(&vals, &[1, 2]);

            ff_reader.get_vals(4, &mut vals);
            assert_eq!(&vals, &[1, 5]);

            ff_reader.get_vals(5, &mut vals);
            assert_eq!(&vals, &[3]);

            ff_reader.get_vals(6, &mut vals);
            assert_eq!(&vals, &[17]);
        }

        {
            let segment = searcher.segment_reader(1u32);
            let ff_reader = segment.fast_fields().u64s(int_field).unwrap();
            ff_reader.get_vals(0, &mut vals);
            assert_eq!(&vals, &[28, 27]);

            ff_reader.get_vals(1, &mut vals);
            assert_eq!(&vals, &[1_000]);
        }

        {
            let segment = searcher.segment_reader(2u32);
            let ff_reader = segment.fast_fields().u64s(int_field).unwrap();
            ff_reader.get_vals(0, &mut vals);
            assert_eq!(&vals, &[20]);
        }

        // Merging the segments
        {
            let segment_ids = index.searchable_segment_ids()?;
            let mut index_writer = index.writer_for_tests()?;
            block_on(index_writer.merge(&segment_ids))?;
            index_writer.wait_merging_threads()?;
        }
        reader.reload()?;

        {
            let searcher = reader.searcher();
            let segment = searcher.segment_reader(0u32);
            let ff_reader = segment.fast_fields().u64s(int_field).unwrap();

            ff_reader.get_vals(0, &mut vals);
            assert_eq!(&vals, &[1, 2]);

            ff_reader.get_vals(1, &mut vals);
            assert_eq!(&vals, &[1, 2, 3]);

            ff_reader.get_vals(2, &mut vals);
            assert_eq!(&vals, &[4, 5]);

            ff_reader.get_vals(3, &mut vals);
            assert_eq!(&vals, &[1, 2]);

            ff_reader.get_vals(4, &mut vals);
            assert_eq!(&vals, &[1, 5]);

            ff_reader.get_vals(5, &mut vals);
            assert_eq!(&vals, &[3]);

            ff_reader.get_vals(6, &mut vals);
            assert_eq!(&vals, &[17]);

            ff_reader.get_vals(7, &mut vals);
            assert_eq!(&vals, &[28, 27]);

            ff_reader.get_vals(8, &mut vals);
            assert_eq!(&vals, &[1_000]);

            ff_reader.get_vals(9, &mut vals);
            assert_eq!(&vals, &[20]);
        }
        Ok(())
    }

    #[test]
    fn merges_f64_fast_fields_correctly() -> crate::Result<()> {
        let mut builder = schema::SchemaBuilder::new();

        let fast_multi = NumericOptions::default().set_fast(Cardinality::MultiValues);

        let field = builder.add_f64_field("f64", schema::FAST);
        let multi_field = builder.add_f64_field("f64s", fast_multi);

        let index = Index::create_in_ram(builder.build());

        let mut writer = index.writer_for_tests()?;

        // Make sure we'll attempt to merge every created segment
        let mut policy = crate::indexer::LogMergePolicy::default();
        policy.set_min_num_segments(2);
        writer.set_merge_policy(Box::new(policy));

        for i in 0..100 {
            let mut doc = Document::new();
            doc.add_f64(field, 42.0);
            doc.add_f64(multi_field, 0.24);
            doc.add_f64(multi_field, 0.27);
            writer.add_document(doc)?;
            if i % 5 == 0 {
                writer.commit()?;
            }
        }

        writer.commit()?;
        writer.wait_merging_threads()?;

        // If a merging thread fails, we should end up with more
        // than one segment here
        assert_eq!(1, index.searchable_segments()?.len());
        Ok(())
    }

    #[test]
    fn test_merged_index_has_blockwand() -> crate::Result<()> {
        let mut builder = schema::SchemaBuilder::new();
        let text = builder.add_text_field("text", TEXT);
        let index = Index::create_in_ram(builder.build());
        let mut writer = index.writer_for_tests()?;
        let happy_term = Term::from_field_text(text, "happy");
        let term_query = TermQuery::new(happy_term, IndexRecordOption::WithFreqs);
        for _ in 0..62 {
            writer.add_document(doc!(text=>"hello happy tax payer"))?;
        }
        writer.commit()?;
        let reader = index.reader()?;
        let searcher = reader.searcher();
        let mut term_scorer = term_query
            .specialized_weight(&searcher, true)?
            .specialized_scorer(searcher.segment_reader(0u32), 1.0)?;
        assert_eq!(term_scorer.doc(), 0);
        assert_nearly_equals!(term_scorer.block_max_score(), 0.0079681855);
        assert_nearly_equals!(term_scorer.score(), 0.0079681855);
        for _ in 0..81 {
            writer.add_document(doc!(text=>"hello happy tax payer"))?;
        }
        writer.commit()?;
        reader.reload()?;
        let searcher = reader.searcher();

        assert_eq!(searcher.segment_readers().len(), 2);
        for segment_reader in searcher.segment_readers() {
            let mut term_scorer = term_query
                .specialized_weight(&searcher, true)?
                .specialized_scorer(segment_reader, 1.0)?;
            // the difference compared to before is instrinsic to the bm25 formula. no worries
            // there.
            for doc in segment_reader.doc_ids_alive() {
                assert_eq!(term_scorer.doc(), doc);
                assert_nearly_equals!(term_scorer.block_max_score(), 0.003478312);
                assert_nearly_equals!(term_scorer.score(), 0.003478312);
                term_scorer.advance();
            }
        }

        let segment_ids: Vec<SegmentId> = searcher
            .segment_readers()
            .iter()
            .map(|reader| reader.segment_id())
            .collect();
        block_on(writer.merge(&segment_ids[..]))?;

        reader.reload()?;
        let searcher = reader.searcher();
        assert_eq!(searcher.segment_readers().len(), 1);

        let segment_reader = searcher.segment_reader(0u32);
        let mut term_scorer = term_query
            .specialized_weight(&searcher, true)?
            .specialized_scorer(segment_reader, 1.0)?;
        // the difference compared to before is instrinsic to the bm25 formula. no worries there.
        for doc in segment_reader.doc_ids_alive() {
            assert_eq!(term_scorer.doc(), doc);
            assert_nearly_equals!(term_scorer.block_max_score(), 0.003478312);
            assert_nearly_equals!(term_scorer.score(), 0.003478312);
            term_scorer.advance();
        }

        Ok(())
    }

    #[test]
    fn test_max_doc() {
        // this is the first time I write a unit test for a constant.
        assert!(((super::MAX_DOC_LIMIT - 1) as i32) >= 0);
        assert!((super::MAX_DOC_LIMIT as i32) < 0);
    }
}

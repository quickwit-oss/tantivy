mod column_operation;
mod column_writers;
mod serializer;
mod value_index;

use std::io;
use std::mem::size_of;
use std::net::Ipv6Addr;

use column_operation::ColumnOperation;
pub(crate) use column_writers::CompatibleNumericalTypes;
use common::CountingWriter;
use common::json_path_writer::JSON_END_OF_PATH;
pub(crate) use serializer::ColumnarSerializer;
use stacker::{Addr, ArenaHashMap, MemoryArena};

use crate::column_index::{
    SerializableColumnIndex, SerializableOptionalIndex, serialize_column_index,
};
use crate::column_values::{MonotonicallyMappableToU64, MonotonicallyMappableToU128};
use crate::columnar::column_type::ColumnType;
use crate::columnar::writer::column_writers::{
    ColumnWriter, NumericalColumnWriter, PayloadColumnWriter, PlainValueStore,
    StrOrBytesColumnWriter,
};
use crate::columnar::writer::value_index::{IndexBuilder, PreallocatedIndexBuilders};
use crate::dictionary::{DictionaryBuilder, TermIdMapping, UnorderedId};
use crate::value::{Coerce, NumericalType, NumericalValue};
use crate::{Cardinality, PayloadEncoding, RowId};

/// This is a set of buffers that are used to temporarily write the values into before passing them
/// to the fast field codecs.
#[derive(Default)]
struct SpareBuffers {
    value_index_builders: PreallocatedIndexBuilders,
    u64_values: Vec<u64>,
    ip_addr_values: Vec<Ipv6Addr>,
    plain_value_ids: Vec<u32>,
    plain_block_raw_values: Vec<u8>,
    plain_block_offsets: Vec<u32>,
    plain_block_end_bytes: Vec<u32>,
    plain_block_end_values: Vec<u32>,
}

impl SpareBuffers {
    fn mem_usage(&self) -> usize {
        self.value_index_builders.mem_usage()
            + self.u64_values.capacity() * size_of::<u64>()
            + self.ip_addr_values.capacity() * size_of::<Ipv6Addr>()
            + self.plain_value_ids.capacity() * size_of::<u32>()
            + self.plain_block_raw_values.capacity()
            + self.plain_block_offsets.capacity() * size_of::<u32>()
            + self.plain_block_end_bytes.capacity() * size_of::<u32>()
            + self.plain_block_end_values.capacity() * size_of::<u32>()
    }
}

/// Makes it possible to create a new columnar.
///
/// ```rust
/// use tantivy_columnar::ColumnarWriter;
///
/// let mut columnar_writer = ColumnarWriter::default();
/// columnar_writer.record_str(0u32 /* doc id */, "product_name", "Red backpack");
/// columnar_writer.record_numerical(0u32 /* doc id */, "price", 10u64);
/// columnar_writer.record_str(1u32 /* doc id */, "product_name", "Apple");
/// columnar_writer.record_numerical(0u32 /* doc id */, "price", 10.5f64); //< uh oh we ended up mixing integer and floats.
/// let mut wrt: Vec<u8> =  Vec::new();
/// columnar_writer.serialize(2u32, None, &mut wrt).unwrap();
/// ```
#[derive(Default)]
pub struct ColumnarWriter {
    numerical_field_hash_map: ArenaHashMap,
    datetime_field_hash_map: ArenaHashMap,
    bool_field_hash_map: ArenaHashMap,
    ip_addr_field_hash_map: ArenaHashMap,
    bytes_field_hash_map: ArenaHashMap,
    str_field_hash_map: ArenaHashMap,
    arena: MemoryArena,
    // Dictionaries used to store dictionary-encoded values.
    dictionaries: Vec<DictionaryBuilder>,
    // Raw stores used by plain string and byte columns.
    plain_value_stores: Vec<PlainValueStore>,
    buffers: SpareBuffers,
}

impl ColumnarWriter {
    pub fn mem_usage(&self) -> usize {
        self.arena.mem_usage()
            + self.numerical_field_hash_map.mem_usage()
            + self.bool_field_hash_map.mem_usage()
            + self.bytes_field_hash_map.mem_usage()
            + self.str_field_hash_map.mem_usage()
            + self.ip_addr_field_hash_map.mem_usage()
            + self.datetime_field_hash_map.mem_usage()
            + self
                .dictionaries
                .iter()
                .map(|dict| dict.mem_usage())
                .sum::<usize>()
            + self
                .plain_value_stores
                .iter()
                .map(PlainValueStore::mem_usage)
                .sum::<usize>()
            + self.buffers.mem_usage()
    }

    /// Returns the list of doc ids from 0..num_docs sorted by the `sort_field`
    /// column.
    ///
    /// If the column is multivalued, use the first value for scoring.
    /// If no value is associated to a specific row, the document is assigned
    /// the lowest possible score.
    ///
    /// The sort applied is stable.
    pub fn sort_order(&self, sort_field: &str, num_docs: RowId, reversed: bool) -> Vec<u32> {
        let Some(numerical_col_writer) = self
            .numerical_field_hash_map
            .get::<NumericalColumnWriter>(sort_field.as_bytes())
            .or_else(|| {
                self.datetime_field_hash_map
                    .get::<NumericalColumnWriter>(sort_field.as_bytes())
            })
        else {
            let str_or_bytes_column_opt = self
                .str_field_hash_map
                .get::<StrOrBytesColumnWriter>(sort_field.as_bytes())
                .or_else(|| {
                    self.bytes_field_hash_map
                        .get::<StrOrBytesColumnWriter>(sort_field.as_bytes())
                });
            let Some(str_or_bytes_column) = str_or_bytes_column_opt else {
                return Vec::new();
            };

            let mut symbols_buffer = Vec::new();
            return match str_or_bytes_column.payload() {
                PayloadColumnWriter::Dictionary(writer) => {
                    let dictionary_builder = &self.dictionaries[writer.dictionary_id as usize];
                    let term_id_mapping = dictionary_builder.build_term_id_mapping(&self.arena);
                    collect_sort_order_from_ops(
                        str_or_bytes_column.operation_iterator(
                            &self.arena,
                            None,
                            &mut symbols_buffer,
                        ),
                        num_docs,
                        reversed,
                        |unordered_id| Some(term_id_mapping.to_ord(UnorderedId(unordered_id)).0),
                        None,
                        |a, b| a.cmp(b),
                    )
                }
                PayloadColumnWriter::Plain(writer) => {
                    let value_store = &self.plain_value_stores[writer.value_store_id as usize];
                    collect_sort_order_from_ops(
                        str_or_bytes_column.operation_iterator(
                            &self.arena,
                            None,
                            &mut symbols_buffer,
                        ),
                        num_docs,
                        reversed,
                        Some,
                        None,
                        |left, right| compare_optional_plain_values(*left, *right, value_store),
                    )
                }
            };
        };
        let mut symbols_buffer = Vec::new();
        collect_sort_order_from_ops(
            numerical_col_writer.operation_iterator(&self.arena, None, &mut symbols_buffer),
            num_docs,
            reversed,
            // MonotonicallyMappableToU64 converts each value to u64 in an
            // order-preserving way (u64: identity, i64: XOR sign bit, f64: bit
            // manipulation). Converting once per document lets the comparator be
            // a simple u64 cmp instead of unwrapping the NumericalValue variant
            // on every comparison.
            //
            // For f64, NaN maps to a deterministic u64 via raw bit manipulation,
            // so it sorts to a consistent position. Sorting only requires total
            // ordering, not IEEE 754 equality semantics where NaN != NaN.
            |nv| {
                Some(match nv {
                    NumericalValue::U64(v) => v.to_u64(),
                    NumericalValue::I64(v) => v.to_u64(),
                    NumericalValue::F64(v) => v.to_u64(),
                })
            },
            // None for missing values. Option<u64> sorts None < Some(_),
            // placing nulls before non-null values.
            None,
            |a, b| a.cmp(b),
        )
    }

    /// Records a column type. This is useful to bypass the coercion process,
    /// makes sure the empty is present in the resulting columnar, or set
    /// the `sort_values_within_row`.
    ///
    /// `sort_values_within_row` is only allowed for `Bytes` or `Str` columns.
    pub fn record_column_type(
        &mut self,
        column_name: &str,
        column_type: ColumnType,
        sort_values_within_row: bool,
    ) {
        self.record_column_type_with_encoding(
            column_name,
            column_type,
            sort_values_within_row,
            PayloadEncoding::Dictionary,
        )
        .expect("column was already registered with a conflicting payload encoding");
    }

    /// Records a column type and the payload encoding for a string or byte column.
    ///
    /// Dictionary encoding is the only valid encoding for other column types. Re-registering a
    /// string or byte column with a different encoding returns an error.
    pub fn record_column_type_with_encoding(
        &mut self,
        column_name: &str,
        column_type: ColumnType,
        sort_values_within_row: bool,
        encoding: PayloadEncoding,
    ) -> io::Result<()> {
        if sort_values_within_row {
            match column_type {
                ColumnType::Bytes | ColumnType::Str => {}
                _ => {
                    return Err(invalid_input(
                        "sort_values_within_row is only allowed for Bytes and Str columns",
                    ));
                }
            }
        }
        match column_type {
            ColumnType::Str | ColumnType::Bytes => {
                let (hash_map, dictionaries, plain_value_stores) = (
                    if column_type == ColumnType::Str {
                        &mut self.str_field_hash_map
                    } else {
                        &mut self.bytes_field_hash_map
                    },
                    &mut self.dictionaries,
                    &mut self.plain_value_stores,
                );
                let existing_column =
                    hash_map.get::<StrOrBytesColumnWriter>(column_name.as_bytes());
                match existing_column {
                    Some(column_writer) if column_writer.encoding() != encoding => {
                        return Err(invalid_input(
                            "column was already registered with a different payload encoding",
                        ));
                    }
                    _ => {}
                }
                hash_map.mutate_or_create(
                    column_name.as_bytes(),
                    |column_opt: Option<StrOrBytesColumnWriter>| {
                        let mut column_writer = if let Some(column_writer) = column_opt {
                            column_writer
                        } else {
                            match encoding {
                                PayloadEncoding::Dictionary => {
                                    let dictionary_id = dictionaries.len() as u32;
                                    dictionaries.push(DictionaryBuilder::default());
                                    StrOrBytesColumnWriter::dictionary(dictionary_id)
                                }
                                PayloadEncoding::Plain => {
                                    let value_store_id = plain_value_stores.len() as u32;
                                    plain_value_stores.push(PlainValueStore::default());
                                    StrOrBytesColumnWriter::plain(value_store_id)
                                }
                            }
                        };
                        column_writer.sort_values_within_row = sort_values_within_row;
                        column_writer
                    },
                );
            }
            ColumnType::Bool => {
                require_dictionary_encoding(encoding)?;
                self.bool_field_hash_map.mutate_or_create(
                    column_name.as_bytes(),
                    |column_opt: Option<ColumnWriter>| column_opt.unwrap_or_default(),
                );
            }
            ColumnType::DateTime => {
                require_dictionary_encoding(encoding)?;
                self.datetime_field_hash_map.mutate_or_create(
                    column_name.as_bytes(),
                    |column_opt: Option<ColumnWriter>| column_opt.unwrap_or_default(),
                );
            }
            ColumnType::I64 | ColumnType::F64 | ColumnType::U64 => {
                require_dictionary_encoding(encoding)?;
                let numerical_type = column_type.numerical_type().unwrap();
                self.numerical_field_hash_map.mutate_or_create(
                    column_name.as_bytes(),
                    |column_opt: Option<NumericalColumnWriter>| {
                        let mut column: NumericalColumnWriter = column_opt.unwrap_or_default();
                        column.force_numerical_type(numerical_type);
                        column
                    },
                );
            }
            ColumnType::IpAddr => {
                require_dictionary_encoding(encoding)?;
                self.ip_addr_field_hash_map.mutate_or_create(
                    column_name.as_bytes(),
                    |column_opt: Option<ColumnWriter>| column_opt.unwrap_or_default(),
                );
            }
        }
        Ok(())
    }

    pub fn record_numerical<T: Into<NumericalValue> + Copy>(
        &mut self,
        doc: RowId,
        column_name: &str,
        numerical_value: T,
    ) {
        let (hash_map, arena) = (&mut self.numerical_field_hash_map, &mut self.arena);
        hash_map.mutate_or_create(
            column_name.as_bytes(),
            |column_opt: Option<NumericalColumnWriter>| {
                let mut column: NumericalColumnWriter = column_opt.unwrap_or_default();
                column.record_numerical_value(doc, numerical_value.into(), arena);
                column
            },
        );
    }

    pub fn record_ip_addr(&mut self, doc: RowId, column_name: &str, ip_addr: Ipv6Addr) {
        let (hash_map, arena) = (&mut self.ip_addr_field_hash_map, &mut self.arena);
        hash_map.mutate_or_create(
            column_name.as_bytes(),
            |column_opt: Option<ColumnWriter>| {
                let mut column: ColumnWriter = column_opt.unwrap_or_default();
                column.record(doc, ip_addr, arena);
                column
            },
        );
    }

    pub fn record_bool(&mut self, doc: RowId, column_name: &str, val: bool) {
        let (hash_map, arena) = (&mut self.bool_field_hash_map, &mut self.arena);
        hash_map.mutate_or_create(
            column_name.as_bytes(),
            |column_opt: Option<ColumnWriter>| {
                let mut column: ColumnWriter = column_opt.unwrap_or_default();
                column.record(doc, val, arena);
                column
            },
        );
    }

    pub fn record_datetime(&mut self, doc: RowId, column_name: &str, datetime: common::DateTime) {
        let (hash_map, arena) = (&mut self.datetime_field_hash_map, &mut self.arena);
        hash_map.mutate_or_create(
            column_name.as_bytes(),
            |column_opt: Option<ColumnWriter>| {
                let mut column: ColumnWriter = column_opt.unwrap_or_default();
                column.record(
                    doc,
                    NumericalValue::I64(datetime.into_timestamp_nanos()),
                    arena,
                );
                column
            },
        );
    }

    pub fn record_str(&mut self, doc: RowId, column_name: &str, value: &str) {
        let (hash_map, arena, dictionaries, plain_value_stores) = (
            &mut self.str_field_hash_map,
            &mut self.arena,
            &mut self.dictionaries,
            &mut self.plain_value_stores,
        );
        hash_map.mutate_or_create(
            column_name.as_bytes(),
            |column_opt: Option<StrOrBytesColumnWriter>| {
                let mut column: StrOrBytesColumnWriter = column_opt.unwrap_or_else(|| {
                    // Each column has its own dictionary
                    let dictionary_id = dictionaries.len() as u32;
                    dictionaries.push(DictionaryBuilder::default());
                    StrOrBytesColumnWriter::dictionary(dictionary_id)
                });
                column.record_bytes(
                    doc,
                    value.as_bytes(),
                    dictionaries,
                    plain_value_stores,
                    arena,
                );
                column
            },
        );
    }

    pub fn record_bytes(&mut self, doc: RowId, column_name: &str, value: &[u8]) {
        let (hash_map, arena, dictionaries, plain_value_stores) = (
            &mut self.bytes_field_hash_map,
            &mut self.arena,
            &mut self.dictionaries,
            &mut self.plain_value_stores,
        );
        hash_map.mutate_or_create(
            column_name.as_bytes(),
            |column_opt: Option<StrOrBytesColumnWriter>| {
                let mut column: StrOrBytesColumnWriter = column_opt.unwrap_or_else(|| {
                    // Each column has its own dictionary
                    let dictionary_id = dictionaries.len() as u32;
                    dictionaries.push(DictionaryBuilder::default());
                    StrOrBytesColumnWriter::dictionary(dictionary_id)
                });
                column.record_bytes(doc, value, dictionaries, plain_value_stores, arena);
                column
            },
        );
    }
    pub fn serialize(
        &mut self,
        num_docs: RowId,
        old_to_new_row_ids: Option<&[RowId]>,
        wrt: &mut dyn io::Write,
    ) -> io::Result<()> {
        let mut serializer = ColumnarSerializer::new(wrt);

        let mut columns: Vec<(&[u8], ColumnType, Addr)> = self
            .numerical_field_hash_map
            .iter()
            .map(|(column_name, addr)| {
                let numerical_column_writer: NumericalColumnWriter =
                    self.numerical_field_hash_map.read(addr);
                let column_type = numerical_column_writer.numerical_type().into();
                (column_name, column_type, addr)
            })
            .collect();
        columns.extend(
            self.bytes_field_hash_map
                .iter()
                .map(|(column_name, addr)| (column_name, ColumnType::Bytes, addr)),
        );
        columns.extend(
            self.str_field_hash_map
                .iter()
                .map(|(column_name, addr)| (column_name, ColumnType::Str, addr)),
        );
        columns.extend(
            self.bool_field_hash_map
                .iter()
                .map(|(column_name, addr)| (column_name, ColumnType::Bool, addr)),
        );
        columns.extend(
            self.ip_addr_field_hash_map
                .iter()
                .map(|(column_name, addr)| (column_name, ColumnType::IpAddr, addr)),
        );
        columns.extend(
            self.datetime_field_hash_map
                .iter()
                .map(|(column_name, addr)| (column_name, ColumnType::DateTime, addr)),
        );
        columns.sort_unstable_by_key(|(column_name, col_type, _)| (*column_name, *col_type));
        let (arena, buffers, dictionaries, plain_value_stores) = (
            &self.arena,
            &mut self.buffers,
            &self.dictionaries,
            &self.plain_value_stores,
        );
        let mut symbol_byte_buffer: Vec<u8> = Vec::new();
        for (column_name, column_type, addr) in columns {
            if column_name.contains(&JSON_END_OF_PATH) {
                // Tantivy uses b'0' as a separator for nested fields in JSON.
                // Column names with a b'0' are not simply ignored by the columnar (and the inverted
                // index).
                continue;
            }
            match column_type {
                ColumnType::Bool => {
                    let column_writer: ColumnWriter = self.bool_field_hash_map.read(addr);
                    let cardinality = column_writer.get_cardinality(num_docs);
                    let mut column_serializer =
                        serializer.start_serialize_column(column_name, column_type);
                    serialize_bool_column(
                        cardinality,
                        num_docs,
                        column_writer.operation_iterator(
                            arena,
                            old_to_new_row_ids,
                            &mut symbol_byte_buffer,
                        ),
                        buffers,
                        &mut column_serializer,
                    )?;
                    column_serializer.finalize()?;
                }
                ColumnType::IpAddr => {
                    let column_writer: ColumnWriter = self.ip_addr_field_hash_map.read(addr);
                    let cardinality = column_writer.get_cardinality(num_docs);
                    let mut column_serializer =
                        serializer.start_serialize_column(column_name, ColumnType::IpAddr);
                    serialize_ip_addr_column(
                        cardinality,
                        num_docs,
                        column_writer.operation_iterator(
                            arena,
                            old_to_new_row_ids,
                            &mut symbol_byte_buffer,
                        ),
                        buffers,
                        &mut column_serializer,
                    )?;
                    column_serializer.finalize()?;
                }
                ColumnType::Bytes | ColumnType::Str => {
                    let str_or_bytes_column_writer: StrOrBytesColumnWriter =
                        if column_type == ColumnType::Bytes {
                            self.bytes_field_hash_map.read(addr)
                        } else {
                            self.str_field_hash_map.read(addr)
                        };
                    let cardinality = str_or_bytes_column_writer
                        .column_writer()
                        .get_cardinality(num_docs);
                    let mut column_serializer =
                        serializer.start_serialize_column(column_name, column_type);
                    match str_or_bytes_column_writer.payload() {
                        PayloadColumnWriter::Dictionary(writer) => {
                            let dictionary_builder = &dictionaries[writer.dictionary_id as usize];
                            serialize_dictionary_bytes_or_str_column(
                                cardinality,
                                num_docs,
                                str_or_bytes_column_writer.sort_values_within_row,
                                dictionary_builder,
                                str_or_bytes_column_writer.operation_iterator(
                                    arena,
                                    old_to_new_row_ids,
                                    &mut symbol_byte_buffer,
                                ),
                                buffers,
                                arena,
                                &mut column_serializer,
                            )?;
                        }
                        PayloadColumnWriter::Plain(writer) => {
                            let value_store = &plain_value_stores[writer.value_store_id as usize];
                            serialize_plain_bytes_or_str_column(
                                cardinality,
                                num_docs,
                                str_or_bytes_column_writer.sort_values_within_row,
                                value_store,
                                str_or_bytes_column_writer.operation_iterator(
                                    arena,
                                    old_to_new_row_ids,
                                    &mut symbol_byte_buffer,
                                ),
                                buffers,
                                &mut column_serializer,
                            )?;
                        }
                    }
                    column_serializer.finalize()?;
                }
                ColumnType::F64 | ColumnType::I64 | ColumnType::U64 => {
                    let numerical_column_writer: NumericalColumnWriter =
                        self.numerical_field_hash_map.read(addr);
                    let cardinality = numerical_column_writer.cardinality(num_docs);
                    let mut column_serializer =
                        serializer.start_serialize_column(column_name, column_type);
                    let numerical_type = column_type.numerical_type().unwrap();
                    serialize_numerical_column(
                        cardinality,
                        num_docs,
                        numerical_type,
                        numerical_column_writer.operation_iterator(
                            arena,
                            old_to_new_row_ids,
                            &mut symbol_byte_buffer,
                        ),
                        buffers,
                        &mut column_serializer,
                    )?;
                    column_serializer.finalize()?;
                }
                ColumnType::DateTime => {
                    let column_writer: ColumnWriter = self.datetime_field_hash_map.read(addr);
                    let cardinality = column_writer.get_cardinality(num_docs);
                    let mut column_serializer =
                        serializer.start_serialize_column(column_name, ColumnType::DateTime);
                    serialize_numerical_column(
                        cardinality,
                        num_docs,
                        NumericalType::I64,
                        column_writer.operation_iterator(
                            arena,
                            old_to_new_row_ids,
                            &mut symbol_byte_buffer,
                        ),
                        buffers,
                        &mut column_serializer,
                    )?;
                    column_serializer.finalize()?;
                }
            };
        }
        serializer.finalize(num_docs)?;
        Ok(())
    }
}

/// Shared sorting pattern for both numeric and Str/Bytes sort fields.
///
/// Iterates column operations, fills gaps for missing docs with `default_key`, converts each value
/// to a sort key via `value_to_key`, then sorts by the key using `cmp_keys`. Returns the doc ids
/// in sorted order.
fn collect_sort_order_from_ops<V, K: Clone>(
    ops: impl Iterator<Item = ColumnOperation<V>>,
    num_docs: RowId,
    reversed: bool,
    value_to_key: impl Fn(V) -> K,
    default_key: K,
    cmp_keys: impl Fn(&K, &K) -> std::cmp::Ordering,
) -> Vec<u32> {
    let mut doc_sort_keys: Vec<(K, RowId)> = Vec::with_capacity(num_docs as usize);
    let mut start_doc_check_fill: RowId = 0;
    let mut current_doc_opt: Option<RowId> = None;

    for op in ops {
        match op {
            ColumnOperation::NewDoc(doc) => {
                current_doc_opt = Some(doc);
            }
            ColumnOperation::Value(val) => {
                if let Some(current_doc) = current_doc_opt {
                    // Fill gaps since the last doc with the default key.
                    doc_sort_keys.extend(
                        (start_doc_check_fill..current_doc).map(|doc| (default_key.clone(), doc)),
                    );
                    start_doc_check_fill = current_doc + 1;
                    // For multivalued fields, only the first value is used.
                    current_doc_opt = None;

                    doc_sort_keys.push((value_to_key(val), current_doc));
                }
            }
        }
    }
    // Fill remaining docs at the tail.
    doc_sort_keys.extend((start_doc_check_fill..num_docs).map(|doc| (default_key.clone(), doc)));

    doc_sort_keys.sort_by(|(left_key, _), (right_key, _)| {
        let cmp = cmp_keys(left_key, right_key);
        if reversed { cmp.reverse() } else { cmp }
    });
    doc_sort_keys
        .into_iter()
        .map(|(_sort_key, doc)| doc)
        .collect()
}

// V3 serialize [PayloadEncoding, Dictionary, Column, dictionary num bytes U32::LE]
// Column: [Column Index, Column Values, column index num bytes U32::LE]
#[expect(clippy::too_many_arguments)]
fn serialize_dictionary_bytes_or_str_column(
    cardinality: Cardinality,
    num_docs: RowId,
    sort_values_within_row: bool,
    dictionary_builder: &DictionaryBuilder,
    operation_it: impl Iterator<Item = ColumnOperation<u32>>,
    buffers: &mut SpareBuffers,
    arena: &MemoryArena,
    wrt: impl io::Write,
) -> io::Result<()> {
    let SpareBuffers {
        value_index_builders,
        u64_values,
        ..
    } = buffers;
    let mut wrt = wrt;
    wrt.write_all(&[PayloadEncoding::Dictionary.to_code()])?;
    let mut counting_writer = CountingWriter::wrap(wrt);
    let term_id_mapping: TermIdMapping =
        dictionary_builder.serialize(arena, &mut counting_writer)?;
    let dictionary_num_bytes: u32 = counting_writer.written_bytes() as u32;
    let mut wrt = counting_writer.finish();
    let operation_iterator = operation_it.map(|symbol: ColumnOperation<u32>| {
        // We map unordered ids to ordered ids.
        match symbol {
            ColumnOperation::Value(unordered_id) => {
                let ordered_id = term_id_mapping.to_ord(UnorderedId(unordered_id));
                ColumnOperation::Value(ordered_id.0 as u64)
            }
            ColumnOperation::NewDoc(doc) => ColumnOperation::NewDoc(doc),
        }
    });
    send_to_serialize_column_mappable_to_u64(
        operation_iterator,
        cardinality,
        num_docs,
        sort_values_within_row,
        value_index_builders,
        u64_values,
        &mut wrt,
    )?;
    wrt.write_all(&dictionary_num_bytes.to_le_bytes()[..])?;
    Ok(())
}

fn serialize_plain_bytes_or_str_column(
    cardinality: Cardinality,
    num_docs: RowId,
    sort_values_within_row: bool,
    value_store: &PlainValueStore,
    operation_it: impl Iterator<Item = ColumnOperation<u32>>,
    buffers: &mut SpareBuffers,
    mut wrt: impl io::Write,
) -> io::Result<()> {
    let SpareBuffers {
        value_index_builders,
        plain_value_ids,
        plain_block_raw_values,
        plain_block_offsets,
        plain_block_end_bytes,
        plain_block_end_values,
        ..
    } = buffers;
    plain_value_ids.clear();

    wrt.write_all(&[PayloadEncoding::Plain.to_code()])?;
    let serializable_column_index = match cardinality {
        Cardinality::Full => {
            consume_operation_iterator(
                operation_it,
                value_index_builders.borrow_required_index_builder(),
                plain_value_ids,
            );
            SerializableColumnIndex::Full
        }
        Cardinality::Optional => {
            let optional_index_builder = value_index_builders.borrow_optional_index_builder();
            consume_operation_iterator(operation_it, optional_index_builder, plain_value_ids);
            let optional_index = optional_index_builder.finish(num_docs);
            SerializableColumnIndex::Optional(SerializableOptionalIndex {
                non_null_row_ids: Box::new(optional_index),
                num_rows: num_docs,
            })
        }
        Cardinality::Multivalued => {
            let multivalued_index_builder = value_index_builders.borrow_multivalued_index_builder();
            consume_operation_iterator(operation_it, multivalued_index_builder, plain_value_ids);
            let serializable_multivalued_index = multivalued_index_builder.finish(num_docs);
            if sort_values_within_row {
                sort_plain_values_within_row(
                    serializable_multivalued_index.start_offsets.boxed_iter(),
                    plain_value_ids,
                    value_store,
                );
            }
            SerializableColumnIndex::Multivalued(serializable_multivalued_index)
        }
    };
    let column_index_num_bytes = serialize_column_index(serializable_column_index, &mut wrt)?;

    serialize_plain_blocks(
        plain_value_ids,
        value_store,
        plain_block_raw_values,
        plain_block_offsets,
        plain_block_end_bytes,
        plain_block_end_values,
        &mut wrt,
    )?;
    debug_assert_eq!(plain_block_end_bytes.len(), plain_block_end_values.len());
    // Keep the serialized layout identical to PlainBlockIndex: all byte endpoints first,
    // followed by all value endpoints.
    for &end_byte in plain_block_end_bytes.iter() {
        wrt.write_all(&end_byte.to_le_bytes())?;
    }
    for &end_value in plain_block_end_values.iter() {
        wrt.write_all(&end_value.to_le_bytes())?;
    }
    let num_blocks = u32::try_from(plain_block_end_bytes.len())
        .map_err(|_| invalid_input("plain column contains more than u32::MAX blocks"))?;
    wrt.write_all(&column_index_num_bytes.to_le_bytes())?;
    wrt.write_all(&num_blocks.to_le_bytes())?;
    Ok(())
}

fn serialize_plain_blocks(
    value_ids: &[u32],
    value_store: &PlainValueStore,
    block_raw_values: &mut Vec<u8>,
    block_offsets: &mut Vec<u32>,
    block_end_bytes: &mut Vec<u32>,
    block_end_values: &mut Vec<u32>,
    output: &mut impl io::Write,
) -> io::Result<()> {
    block_raw_values.clear();
    block_offsets.clear();
    block_offsets.push(0);
    block_end_bytes.clear();
    block_end_values.clear();
    let mut end_byte = 0u32;
    let mut end_value = 0u32;

    for &value_id in value_ids {
        let value = value_store.get(value_id);
        block_raw_values.extend_from_slice(value);
        let block_end = u32::try_from(block_raw_values.len())
            .map_err(|_| invalid_input("plain value exceeds the OnPair block size limit"))?;
        block_offsets.push(block_end);
        let block_num_values = block_offsets.len() - 1;
        if block_raw_values.len() >= crate::column::PLAIN_BLOCK_RAW_NUM_BYTES_THRESHOLD
            || block_num_values >= crate::column::PLAIN_BLOCK_MAX_NUM_VALUES
        {
            flush_plain_block(
                block_raw_values,
                block_offsets,
                &mut end_byte,
                &mut end_value,
                block_end_bytes,
                block_end_values,
                output,
            )?;
        }
    }
    if block_offsets.len() > 1 {
        flush_plain_block(
            block_raw_values,
            block_offsets,
            &mut end_byte,
            &mut end_value,
            block_end_bytes,
            block_end_values,
            output,
        )?;
    }
    Ok(())
}

fn flush_plain_block(
    block_raw_values: &mut Vec<u8>,
    block_offsets: &mut Vec<u32>,
    end_byte: &mut u32,
    end_value: &mut u32,
    block_end_bytes: &mut Vec<u32>,
    block_end_values: &mut Vec<u32>,
    output: &mut impl io::Write,
) -> io::Result<()> {
    let block_num_values = u32::try_from(block_offsets.len() - 1)
        .map_err(|_| invalid_input("plain OnPair block contains too many values"))?;
    let block_num_bytes =
        crate::column::serialize_onpair_block(block_raw_values, block_offsets, output)?;
    *end_byte = end_byte
        .checked_add(block_num_bytes)
        .ok_or_else(|| invalid_input("plain column block data exceeds u32::MAX bytes"))?;
    *end_value = end_value
        .checked_add(block_num_values)
        .ok_or_else(|| invalid_input("plain column contains more than u32::MAX values"))?;
    block_end_bytes.push(*end_byte);
    block_end_values.push(*end_value);
    block_raw_values.clear();
    block_offsets.clear();
    block_offsets.push(0);
    Ok(())
}

fn sort_plain_values_within_row(
    multivalued_index: impl Iterator<Item = RowId>,
    values: &mut [u32],
    value_store: &PlainValueStore,
) {
    let mut start_index = 0usize;
    for end_index in multivalued_index {
        let end_index = end_index as usize;
        values[start_index..end_index]
            .sort_unstable_by(|left, right| value_store.get(*left).cmp(value_store.get(*right)));
        start_index = end_index;
    }
}

fn compare_optional_plain_values(
    left: Option<u32>,
    right: Option<u32>,
    value_store: &PlainValueStore,
) -> std::cmp::Ordering {
    match (left, right) {
        (None, None) => std::cmp::Ordering::Equal,
        (None, Some(_)) => std::cmp::Ordering::Less,
        (Some(_), None) => std::cmp::Ordering::Greater,
        (Some(left), Some(right)) => value_store.get(left).cmp(value_store.get(right)),
    }
}

fn serialize_numerical_column(
    cardinality: Cardinality,
    num_docs: RowId,
    numerical_type: NumericalType,
    op_iterator: impl Iterator<Item = ColumnOperation<NumericalValue>>,
    buffers: &mut SpareBuffers,
    wrt: &mut impl io::Write,
) -> io::Result<()> {
    let SpareBuffers {
        value_index_builders,
        u64_values,
        ..
    } = buffers;
    match numerical_type {
        NumericalType::I64 => {
            send_to_serialize_column_mappable_to_u64(
                coerce_numerical_symbol::<i64>(op_iterator),
                cardinality,
                num_docs,
                false,
                value_index_builders,
                u64_values,
                wrt,
            )?;
        }
        NumericalType::U64 => {
            send_to_serialize_column_mappable_to_u64(
                coerce_numerical_symbol::<u64>(op_iterator),
                cardinality,
                num_docs,
                false,
                value_index_builders,
                u64_values,
                wrt,
            )?;
        }
        NumericalType::F64 => {
            send_to_serialize_column_mappable_to_u64(
                coerce_numerical_symbol::<f64>(op_iterator),
                cardinality,
                num_docs,
                false,
                value_index_builders,
                u64_values,
                wrt,
            )?;
        }
    };
    Ok(())
}

fn serialize_bool_column(
    cardinality: Cardinality,
    num_docs: RowId,
    column_operations_it: impl Iterator<Item = ColumnOperation<bool>>,
    buffers: &mut SpareBuffers,
    wrt: &mut impl io::Write,
) -> io::Result<()> {
    let SpareBuffers {
        value_index_builders,
        u64_values,
        ..
    } = buffers;
    send_to_serialize_column_mappable_to_u64(
        column_operations_it.map(|bool_column_operation| match bool_column_operation {
            ColumnOperation::NewDoc(doc) => ColumnOperation::NewDoc(doc),
            ColumnOperation::Value(bool_val) => ColumnOperation::Value(bool_val.to_u64()),
        }),
        cardinality,
        num_docs,
        false,
        value_index_builders,
        u64_values,
        wrt,
    )?;
    Ok(())
}

fn serialize_ip_addr_column(
    cardinality: Cardinality,
    num_docs: RowId,
    column_operations_it: impl Iterator<Item = ColumnOperation<Ipv6Addr>>,
    buffers: &mut SpareBuffers,
    wrt: &mut impl io::Write,
) -> io::Result<()> {
    let SpareBuffers {
        value_index_builders,
        ip_addr_values,
        ..
    } = buffers;
    send_to_serialize_column_mappable_to_u128(
        column_operations_it,
        cardinality,
        num_docs,
        value_index_builders,
        ip_addr_values,
        wrt,
    )?;
    Ok(())
}

fn send_to_serialize_column_mappable_to_u128<
    T: Copy + Ord + std::fmt::Debug + Send + Sync + MonotonicallyMappableToU128 + PartialOrd,
>(
    op_iterator: impl Iterator<Item = ColumnOperation<T>>,
    cardinality: Cardinality,
    num_rows: RowId,
    value_index_builders: &mut PreallocatedIndexBuilders,
    values: &mut Vec<T>,
    mut wrt: impl io::Write,
) -> io::Result<()> {
    values.clear();
    // TODO: split index and values
    let serializable_column_index = match cardinality {
        Cardinality::Full => {
            consume_operation_iterator(
                op_iterator,
                value_index_builders.borrow_required_index_builder(),
                values,
            );
            SerializableColumnIndex::Full
        }
        Cardinality::Optional => {
            let optional_index_builder = value_index_builders.borrow_optional_index_builder();
            consume_operation_iterator(op_iterator, optional_index_builder, values);
            let optional_index = optional_index_builder.finish(num_rows);
            SerializableColumnIndex::Optional(SerializableOptionalIndex {
                num_rows,
                non_null_row_ids: Box::new(optional_index),
            })
        }
        Cardinality::Multivalued => {
            let multivalued_index_builder = value_index_builders.borrow_multivalued_index_builder();
            consume_operation_iterator(op_iterator, multivalued_index_builder, values);
            let serializable_multivalued_index = multivalued_index_builder.finish(num_rows);
            SerializableColumnIndex::Multivalued(serializable_multivalued_index)
        }
    };
    crate::column::serialize_column_mappable_to_u128(
        serializable_column_index,
        &&values[..],
        &mut wrt,
    )?;
    Ok(())
}

fn send_to_serialize_column_mappable_to_u64(
    op_iterator: impl Iterator<Item = ColumnOperation<u64>>,
    cardinality: Cardinality,
    num_rows: RowId,
    sort_values_within_row: bool,
    value_index_builders: &mut PreallocatedIndexBuilders,
    values: &mut Vec<u64>,
    mut wrt: impl io::Write,
) -> io::Result<()> {
    values.clear();
    let serializable_column_index = match cardinality {
        Cardinality::Full => {
            consume_operation_iterator(
                op_iterator,
                value_index_builders.borrow_required_index_builder(),
                values,
            );
            SerializableColumnIndex::Full
        }
        Cardinality::Optional => {
            let optional_index_builder = value_index_builders.borrow_optional_index_builder();
            consume_operation_iterator(op_iterator, optional_index_builder, values);
            let optional_index = optional_index_builder.finish(num_rows);
            SerializableColumnIndex::Optional(SerializableOptionalIndex {
                non_null_row_ids: Box::new(optional_index),
                num_rows,
            })
        }
        Cardinality::Multivalued => {
            let multivalued_index_builder = value_index_builders.borrow_multivalued_index_builder();
            consume_operation_iterator(op_iterator, multivalued_index_builder, values);
            let serializable_multivalued_index = multivalued_index_builder.finish(num_rows);
            if sort_values_within_row {
                sort_values_within_row_in_place(
                    serializable_multivalued_index.start_offsets.boxed_iter(),
                    values,
                );
            }
            SerializableColumnIndex::Multivalued(serializable_multivalued_index)
        }
    };
    crate::column::serialize_column_mappable_to_u64(
        serializable_column_index,
        &&values[..],
        &mut wrt,
    )?;
    Ok(())
}

fn sort_values_within_row_in_place(
    multivalued_index: impl Iterator<Item = RowId>,
    values: &mut [u64],
) {
    let mut start_index: usize = 0;
    for end_index in multivalued_index {
        let end_index = end_index as usize;
        values[start_index..end_index].sort_unstable();
        start_index = end_index;
    }
}

fn coerce_numerical_symbol<T>(
    operation_iterator: impl Iterator<Item = ColumnOperation<NumericalValue>>,
) -> impl Iterator<Item = ColumnOperation<u64>>
where T: Coerce + MonotonicallyMappableToU64 {
    operation_iterator.map(|symbol| match symbol {
        ColumnOperation::NewDoc(doc) => ColumnOperation::NewDoc(doc),
        ColumnOperation::Value(numerical_value) => {
            ColumnOperation::Value(T::coerce(numerical_value).to_u64())
        }
    })
}

fn consume_operation_iterator<T: Ord, TIndexBuilder: IndexBuilder>(
    operation_iterator: impl Iterator<Item = ColumnOperation<T>>,
    index_builder: &mut TIndexBuilder,
    values: &mut Vec<T>,
) {
    for symbol in operation_iterator {
        match symbol {
            ColumnOperation::NewDoc(doc) => {
                index_builder.record_row(doc);
            }
            ColumnOperation::Value(value) => {
                index_builder.record_value();
                values.push(value);
            }
        }
    }
}

fn require_dictionary_encoding(encoding: PayloadEncoding) -> io::Result<()> {
    if encoding == PayloadEncoding::Dictionary {
        return Ok(());
    }
    Err(invalid_input(
        "plain payload encoding is only supported for Bytes and Str columns",
    ))
}

fn invalid_input(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

#[cfg(test)]
mod tests {
    use stacker::MemoryArena;

    use crate::columnar::writer::column_operation::ColumnOperation;
    use crate::{Cardinality, NumericalValue};

    #[test]
    fn test_column_writer_required_simple() {
        let mut arena = MemoryArena::default();
        let mut column_writer = super::ColumnWriter::default();
        column_writer.record(0u32, NumericalValue::from(14i64), &mut arena);
        column_writer.record(1u32, NumericalValue::from(15i64), &mut arena);
        column_writer.record(2u32, NumericalValue::from(-16i64), &mut arena);
        assert_eq!(column_writer.get_cardinality(3), Cardinality::Full);
        let mut buffer = Vec::new();
        let symbols: Vec<ColumnOperation<NumericalValue>> = column_writer
            .operation_iterator(&arena, None, &mut buffer)
            .collect();
        assert_eq!(symbols.len(), 6);
        assert!(matches!(symbols[0], ColumnOperation::NewDoc(0u32)));
        assert!(matches!(
            symbols[1],
            ColumnOperation::Value(NumericalValue::I64(14i64))
        ));
        assert!(matches!(symbols[2], ColumnOperation::NewDoc(1u32)));
        assert!(matches!(
            symbols[3],
            ColumnOperation::Value(NumericalValue::I64(15i64))
        ));
        assert!(matches!(symbols[4], ColumnOperation::NewDoc(2u32)));
        assert!(matches!(
            symbols[5],
            ColumnOperation::Value(NumericalValue::I64(-16i64))
        ));
    }

    #[test]
    fn test_column_writer_optional_cardinality_missing_first() {
        let mut arena = MemoryArena::default();
        let mut column_writer = super::ColumnWriter::default();
        column_writer.record(1u32, NumericalValue::from(15i64), &mut arena);
        column_writer.record(2u32, NumericalValue::from(-16i64), &mut arena);
        assert_eq!(column_writer.get_cardinality(3), Cardinality::Optional);
        let mut buffer = Vec::new();
        let symbols: Vec<ColumnOperation<NumericalValue>> = column_writer
            .operation_iterator(&arena, None, &mut buffer)
            .collect();
        assert_eq!(symbols.len(), 4);
        assert!(matches!(symbols[0], ColumnOperation::NewDoc(1u32)));
        assert!(matches!(
            symbols[1],
            ColumnOperation::Value(NumericalValue::I64(15i64))
        ));
        assert!(matches!(symbols[2], ColumnOperation::NewDoc(2u32)));
        assert!(matches!(
            symbols[3],
            ColumnOperation::Value(NumericalValue::I64(-16i64))
        ));
    }

    #[test]
    fn test_column_writer_optional_cardinality_missing_last() {
        let mut arena = MemoryArena::default();
        let mut column_writer = super::ColumnWriter::default();
        column_writer.record(0u32, NumericalValue::from(15i64), &mut arena);
        assert_eq!(column_writer.get_cardinality(2), Cardinality::Optional);
        let mut buffer = Vec::new();
        let symbols: Vec<ColumnOperation<NumericalValue>> = column_writer
            .operation_iterator(&arena, None, &mut buffer)
            .collect();
        assert_eq!(symbols.len(), 2);
        assert!(matches!(symbols[0], ColumnOperation::NewDoc(0u32)));
        assert!(matches!(
            symbols[1],
            ColumnOperation::Value(NumericalValue::I64(15i64))
        ));
    }

    #[test]
    fn test_column_writer_multivalued() {
        let mut arena = MemoryArena::default();
        let mut column_writer = super::ColumnWriter::default();
        column_writer.record(0u32, NumericalValue::from(16i64), &mut arena);
        column_writer.record(0u32, NumericalValue::from(17i64), &mut arena);
        assert_eq!(column_writer.get_cardinality(1), Cardinality::Multivalued);
        let mut buffer = Vec::new();
        let symbols: Vec<ColumnOperation<NumericalValue>> = column_writer
            .operation_iterator(&arena, None, &mut buffer)
            .collect();
        assert_eq!(symbols.len(), 3);
        assert!(matches!(symbols[0], ColumnOperation::NewDoc(0u32)));
        assert!(matches!(
            symbols[1],
            ColumnOperation::Value(NumericalValue::I64(16i64))
        ));
        assert!(matches!(
            symbols[2],
            ColumnOperation::Value(NumericalValue::I64(17i64))
        ));
    }
}

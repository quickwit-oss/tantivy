mod column_operation;
mod column_writers;
mod serializer;
mod value_index;

use std::collections::HashSet;
use std::io;
use std::net::Ipv6Addr;

use column_operation::ColumnOperation;
pub(crate) use column_writers::CompatibleNumericalTypes;
use common::CountingWriter;
use common::json_path_writer::JSON_END_OF_PATH;
pub(crate) use serializer::ColumnarSerializer;
use stacker::{Addr, ArenaHashMap, ExpUnrolledLinkedList, MemoryArena};

use crate::column_index::{SerializableColumnIndex, SerializableOptionalIndex};
use crate::column_values::{MonotonicallyMappableToU64, MonotonicallyMappableToU128};
use crate::columnar::column_type::{ColumnType, StrColumnCodec, column_key_code};
use crate::columnar::writer::column_operation::PlainValueSpan;
use crate::columnar::writer::column_writers::{
    ColumnWriter, NumericalColumnWriter, StrOrBytesColumnWriter, StrOrBytesValues,
};
use crate::columnar::writer::value_index::{IndexBuilder, PreallocatedIndexBuilders};
use crate::dictionary::{DictionaryBuilder, TermIdMapping, UnorderedId};
use crate::value::{Coerce, NumericalType, NumericalValue};
use crate::{Cardinality, RowId};

/// This is a set of buffers that are used to temporarily write the values into before passing them
/// to the fast field codecs.
#[derive(Default)]
struct SpareBuffers {
    value_index_builders: PreallocatedIndexBuilders,
    u64_values: Vec<u64>,
    ip_addr_values: Vec<Ipv6Addr>,
    #[cfg(feature = "plain_codec")]
    plain_value_spans: Vec<PlainValueSpan>,
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
    generated_tie_breaker_columns: HashSet<Vec<u8>>,
    arena: MemoryArena,
    // Dictionaries used to store dictionary-encoded values.
    dictionaries: Vec<DictionaryBuilder>,
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
    }

    /// Returns the list of doc ids from 0..num_docs sorted by the `sort_field`
    /// column.
    ///
    /// If the column is multivalued, use the first value for scoring.
    /// If no value is associated to a specific row, the document is assigned
    /// the lowest possible score.
    ///
    /// The sort applied is stable.
    ///
    /// The sort_field has to be a schema field (not a json path): we rely on the fact that
    /// all values have the same numerical type in `collect_numerical_sort_order`.
    pub fn sort_order(&self, sort_field: &str, num_docs: RowId, reversed: bool) -> Vec<u32> {
        let sort_field_bytes = sort_field.as_bytes();
        let mut symbols_buffer = Vec::new();
        if let Some(numerical_col_writer) = self
            .numerical_field_hash_map
            .get::<NumericalColumnWriter>(sort_field_bytes)
        {
            return collect_numerical_sort_order(
                numerical_col_writer.operation_iterator(&self.arena, None, &mut symbols_buffer),
                num_docs,
                reversed,
            );
        }
        // Datetime columns store a plain `ColumnWriter` (not a `NumericalColumnWriter`), with
        // `NumericalValue::I64` values.
        if let Some(datetime_col_writer) = self
            .datetime_field_hash_map
            .get::<ColumnWriter>(sort_field_bytes)
        {
            return collect_numerical_sort_order(
                datetime_col_writer.operation_iterator::<NumericalValue>(
                    &self.arena,
                    None,
                    &mut symbols_buffer,
                ),
                num_docs,
                reversed,
            );
        }
        let str_or_bytes_column_opt = self
            .str_field_hash_map
            .get::<StrOrBytesColumnWriter>(sort_field_bytes)
            .or_else(|| {
                self.bytes_field_hash_map
                    .get::<StrOrBytesColumnWriter>(sort_field_bytes)
            });
        let Some(str_or_bytes_column) = str_or_bytes_column_opt else {
            return Vec::new();
        };
        match str_or_bytes_column.values {
            StrOrBytesValues::Dictionary { dictionary_id } => {
                let dictionary_builder = &self.dictionaries[dictionary_id as usize];
                let term_id_mapping = dictionary_builder.build_term_id_mapping(&self.arena);
                collect_sort_order_from_ops(
                    str_or_bytes_column.dictionary_operation_iterator(
                        &self.arena,
                        None,
                        &mut symbols_buffer,
                    ),
                    num_docs,
                    reversed,
                    |uid| Some(term_id_mapping.to_ord(uid).0),
                    None,
                    |a, b| a.cmp(b),
                )
            }
            StrOrBytesValues::Plain {
                value_bytes,
                num_bytes,
            } => {
                // Values are sorted by bytes. This copies the bytes of the column.
                let mut all_value_bytes: Vec<u8> = Vec::with_capacity(num_bytes as usize);
                value_bytes.read_to_end(&self.arena, &mut all_value_bytes);
                collect_sort_order_from_ops(
                    str_or_bytes_column.plain_operation_iterator(
                        &self.arena,
                        None,
                        &mut symbols_buffer,
                    ),
                    num_docs,
                    reversed,
                    |span| Some(plain_value_bytes(&all_value_bytes, span)),
                    None,
                    |a, b| a.cmp(b),
                )
            }
        }
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
        if sort_values_within_row {
            assert!(
                column_type == ColumnType::Bytes || column_type == ColumnType::Str,
                "sort_values_within_row is only allowed for Bytes and Str columns",
            );
        }
        match column_type {
            ColumnType::Str | ColumnType::Bytes => {
                self.mutate_or_create_str_or_bytes_column(
                    column_name,
                    column_type,
                    |column_writer| column_writer.sort_values_within_row = sort_values_within_row,
                );
            }
            ColumnType::Bool => {
                self.bool_field_hash_map.mutate_or_create(
                    column_name.as_bytes(),
                    |column_opt: Option<ColumnWriter>| column_opt.unwrap_or_default(),
                );
            }
            ColumnType::DateTime => {
                self.datetime_field_hash_map.mutate_or_create(
                    column_name.as_bytes(),
                    |column_opt: Option<ColumnWriter>| column_opt.unwrap_or_default(),
                );
            }
            ColumnType::I64 | ColumnType::F64 | ColumnType::U64 => {
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
            ColumnType::IpAddr => self.ip_addr_field_hash_map.mutate_or_create(
                column_name.as_bytes(),
                |column_opt: Option<ColumnWriter>| column_opt.unwrap_or_default(),
            ),
        }
    }

    /// Sets the codec of a `Str` or `Bytes` column, creating the column if it does not exist.
    ///
    /// Columns are dictionary encoded by default. Values of dictionary encoded columns are
    /// deduplicated while indexing. Values of plain columns are not: each value is appended to
    /// the memory arena.
    ///
    /// Contract: the codec must be set before any value is recorded in the column. Setting the
    /// codec twice keeps the last one.
    ///
    /// Switching an existing column to the plain codec leaves its (empty) dictionary unused:
    /// call this before `record_column_type` to avoid allocating it.
    ///
    /// Panics if `column_type` is not `Str` or `Bytes`, if the column already has values with
    /// another codec, or if `codec` is `Plain` and the `plain_codec` feature is disabled.
    pub fn set_str_column_codec(
        &mut self,
        column_name: &str,
        column_type: ColumnType,
        codec: StrColumnCodec,
    ) {
        assert!(
            matches!(column_type, ColumnType::Str | ColumnType::Bytes),
            "a codec can only be set on str and bytes columns"
        );
        #[cfg(not(feature = "plain_codec"))]
        assert!(
            codec != StrColumnCodec::Plain,
            "the plain codec requires the `plain_codec` feature"
        );
        let (hash_map, dictionaries) = (
            if column_type == ColumnType::Str {
                &mut self.str_field_hash_map
            } else {
                &mut self.bytes_field_hash_map
            },
            &mut self.dictionaries,
        );
        hash_map.mutate_or_create(
            column_name.as_bytes(),
            |column_opt: Option<StrOrBytesColumnWriter>| {
                if let Some(column_writer) = column_opt {
                    if column_writer.codec() == codec {
                        return column_writer;
                    }
                    assert!(
                        column_writer.is_empty(),
                        "the codec of a column must be set before values are recorded"
                    );
                }
                let values = match codec {
                    StrColumnCodec::Dictionary => {
                        let dictionary_id = dictionaries.len() as u32;
                        dictionaries.push(DictionaryBuilder::default());
                        StrOrBytesValues::Dictionary { dictionary_id }
                    }
                    StrColumnCodec::Plain => StrOrBytesValues::Plain {
                        value_bytes: ExpUnrolledLinkedList::default(),
                        num_bytes: 0,
                    },
                };
                let mut column_writer = StrOrBytesColumnWriter::with_values(values);
                if let Some(previous_column_writer) = column_opt {
                    column_writer.sort_values_within_row =
                        previous_column_writer.sort_values_within_row;
                }
                column_writer
            },
        );
    }

    /// Applies `mutate` to the `Str` or `Bytes` column writer, creating it if it does not exist.
    ///
    /// Precondition: `column_type` is `Str` or `Bytes`.
    fn mutate_or_create_str_or_bytes_column(
        &mut self,
        column_name: &str,
        column_type: ColumnType,
        mut mutate: impl FnMut(&mut StrOrBytesColumnWriter),
    ) {
        let (hash_map, dictionaries) = (
            if column_type == ColumnType::Str {
                &mut self.str_field_hash_map
            } else {
                &mut self.bytes_field_hash_map
            },
            &mut self.dictionaries,
        );
        hash_map.mutate_or_create(
            column_name.as_bytes(),
            |column_opt: Option<StrOrBytesColumnWriter>| {
                let mut column_writer = if let Some(column_writer) = column_opt {
                    column_writer
                } else {
                    let dictionary_id = dictionaries.len() as u32;
                    dictionaries.push(DictionaryBuilder::default());
                    StrOrBytesColumnWriter::with_dictionary_id(dictionary_id)
                };
                mutate(&mut column_writer);
                column_writer
            },
        );
    }

    /// Registers a full `i64` column whose values are generated when this writer is serialized.
    pub fn record_tie_breaker_column(&mut self, column_name: &str) {
        self.record_column_type(column_name, ColumnType::I64, false);
        self.generated_tie_breaker_columns
            .insert(column_name.as_bytes().to_vec());
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
        let (hash_map, arena, dictionaries) = (
            &mut self.str_field_hash_map,
            &mut self.arena,
            &mut self.dictionaries,
        );
        hash_map.mutate_or_create(
            column_name.as_bytes(),
            |column_opt: Option<StrOrBytesColumnWriter>| {
                let mut column: StrOrBytesColumnWriter = column_opt.unwrap_or_else(|| {
                    // Each column has its own dictionary
                    let dictionary_id = dictionaries.len() as u32;
                    dictionaries.push(DictionaryBuilder::default());
                    StrOrBytesColumnWriter::with_dictionary_id(dictionary_id)
                });
                column.record_bytes(doc, value.as_bytes(), dictionaries, arena);
                column
            },
        );
    }

    pub fn record_bytes(&mut self, doc: RowId, column_name: &str, value: &[u8]) {
        let (hash_map, arena, dictionaries) = (
            &mut self.bytes_field_hash_map,
            &mut self.arena,
            &mut self.dictionaries,
        );
        hash_map.mutate_or_create(
            column_name.as_bytes(),
            |column_opt: Option<StrOrBytesColumnWriter>| {
                let mut column: StrOrBytesColumnWriter = column_opt.unwrap_or_else(|| {
                    // Each column has its own dictionary
                    let dictionary_id = dictionaries.len() as u32;
                    dictionaries.push(DictionaryBuilder::default());
                    StrOrBytesColumnWriter::with_dictionary_id(dictionary_id)
                });
                column.record_bytes(doc, value, dictionaries, arena);
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

        let mut columns: Vec<(&[u8], ColumnType, StrColumnCodec, Addr)> = self
            .numerical_field_hash_map
            .iter()
            .map(|(column_name, addr)| {
                let numerical_column_writer: NumericalColumnWriter =
                    self.numerical_field_hash_map.read(addr);
                let column_type = numerical_column_writer.numerical_type().into();
                (column_name, column_type, StrColumnCodec::Dictionary, addr)
            })
            .collect();
        columns.extend(self.bytes_field_hash_map.iter().map(|(column_name, addr)| {
            let column_writer: StrOrBytesColumnWriter = self.bytes_field_hash_map.read(addr);
            (column_name, ColumnType::Bytes, column_writer.codec(), addr)
        }));
        columns.extend(self.str_field_hash_map.iter().map(|(column_name, addr)| {
            let column_writer: StrOrBytesColumnWriter = self.str_field_hash_map.read(addr);
            (column_name, ColumnType::Str, column_writer.codec(), addr)
        }));
        for (hash_map, column_type) in [
            (&self.bool_field_hash_map, ColumnType::Bool),
            (&self.ip_addr_field_hash_map, ColumnType::IpAddr),
            (&self.datetime_field_hash_map, ColumnType::DateTime),
        ] {
            columns.extend(hash_map.iter().map(|(column_name, addr)| {
                (column_name, column_type, StrColumnCodec::Dictionary, addr)
            }));
        }
        // The serializer requires the columns in the order of their keys. The key code of
        // plain columns does not follow the order of the column types.
        columns.sort_unstable_by_key(|(column_name, column_type, codec, _)| {
            (*column_name, column_key_code(*column_type, *codec))
        });
        let (arena, buffers, dictionaries) = (&self.arena, &mut self.buffers, &self.dictionaries);
        let mut symbol_byte_buffer: Vec<u8> = Vec::new();
        for (column_name, column_type, codec, addr) in columns {
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
                        .column_writer
                        .get_cardinality(num_docs);
                    let mut column_serializer = serializer.start_serialize_column_with_codec(
                        column_name,
                        column_type,
                        codec,
                    );
                    match str_or_bytes_column_writer.values {
                        StrOrBytesValues::Dictionary { dictionary_id } => {
                            serialize_bytes_or_str_column(
                                cardinality,
                                num_docs,
                                str_or_bytes_column_writer.sort_values_within_row,
                                &dictionaries[dictionary_id as usize],
                                str_or_bytes_column_writer.dictionary_operation_iterator(
                                    arena,
                                    old_to_new_row_ids,
                                    &mut symbol_byte_buffer,
                                ),
                                buffers,
                                &self.arena,
                                &mut column_serializer,
                            )?;
                        }
                        #[cfg(feature = "plain_codec")]
                        StrOrBytesValues::Plain {
                            value_bytes,
                            num_bytes,
                        } => {
                            serialize_plain_bytes_or_str_column(
                                cardinality,
                                num_docs,
                                str_or_bytes_column_writer.sort_values_within_row,
                                old_to_new_row_ids.is_some(),
                                &value_bytes,
                                num_bytes,
                                str_or_bytes_column_writer.plain_operation_iterator(
                                    arena,
                                    old_to_new_row_ids,
                                    &mut symbol_byte_buffer,
                                ),
                                buffers,
                                &self.arena,
                                &mut column_serializer,
                            )?;
                        }
                        #[cfg(not(feature = "plain_codec"))]
                        StrOrBytesValues::Plain { .. } => {
                            unreachable!(
                                "`set_str_column_codec` rejects the plain codec without the \
                                 `plain_codec` feature"
                            )
                        }
                    }
                    column_serializer.finalize()?;
                }
                ColumnType::F64 | ColumnType::I64 | ColumnType::U64 => {
                    let mut column_serializer =
                        serializer.start_serialize_column(column_name, column_type);
                    if self.generated_tie_breaker_columns.contains(column_name) {
                        crate::column::serialize_generated_tie_breaker_column(
                            num_docs,
                            &mut column_serializer,
                        )?;
                    } else {
                        let numerical_column_writer: NumericalColumnWriter =
                            self.numerical_field_hash_map.read(addr);
                        let cardinality = numerical_column_writer.cardinality(num_docs);
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
                    }
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

/// Returns the sort order of a numerical or datetime column, given its operations.
///
/// # Hidden contract
///
/// For this to make sense, all numerical value emitted have to be of the same type.
fn collect_numerical_sort_order(
    ops: impl Iterator<Item = ColumnOperation<NumericalValue>>,
    num_docs: RowId,
    reversed: bool,
) -> Vec<u32> {
    collect_sort_order_from_ops(
        ops,
        num_docs,
        reversed,
        |numerical_value| {
            Some(match numerical_value {
                NumericalValue::U64(val_u64) => val_u64.to_u64(),
                NumericalValue::I64(val_i64) => val_i64.to_u64(),
                NumericalValue::F64(val_f64) => val_f64.to_u64(),
            })
        },
        // None for missing values. Option<u64> sorts None < Some(_),
        // placing nulls before non-null values.
        None,
        |a, b| a.cmp(b),
    )
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

// Serialize [Dictionary, Column, dictionary num bytes U32::LE]
// Column: [Column Index, Column Values, column index num bytes U32::LE]
#[expect(clippy::too_many_arguments)]
fn serialize_bytes_or_str_column(
    cardinality: Cardinality,
    num_docs: RowId,
    sort_values_within_row: bool,
    dictionary_builder: &DictionaryBuilder,
    operation_it: impl Iterator<Item = ColumnOperation<UnorderedId>>,
    buffers: &mut SpareBuffers,
    arena: &MemoryArena,
    wrt: impl io::Write,
) -> io::Result<()> {
    let SpareBuffers {
        value_index_builders,
        u64_values,
        ..
    } = buffers;
    let mut counting_writer = CountingWriter::wrap(wrt);
    let term_id_mapping: TermIdMapping =
        dictionary_builder.serialize(arena, &mut counting_writer)?;
    let dictionary_num_bytes: u32 = counting_writer.written_bytes() as u32;
    let mut wrt = counting_writer.finish();
    let operation_iterator = operation_it.map(|symbol: ColumnOperation<UnorderedId>| {
        // We map unordered ids to ordered ids.
        match symbol {
            ColumnOperation::Value(unordered_id) => {
                let ordered_id = term_id_mapping.to_ord(unordered_id);
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

/// Returns the bytes of the plain value `span`.
///
/// Precondition: `all_value_bytes` holds all of the value bytes of the column of `span`.
fn plain_value_bytes(all_value_bytes: &[u8], span: PlainValueSpan) -> &[u8] {
    let start = span.start as usize;
    &all_value_bytes[start..start + span.len as usize]
}

/// Serializes a plain-encoded `Str` or `Bytes` column with the `PlainColumnSerializer`.
///
/// `value_bytes` holds the `num_bytes` bytes of the values, in record order. If
/// `sort_values_within_row` is set, the values of a row are sorted by bytes.
///
/// Without doc remapping and without sorting, the values are streamed from the arena. Otherwise,
/// the value bytes are first copied in a buffer for random access.
#[cfg(feature = "plain_codec")]
#[expect(clippy::too_many_arguments)]
fn serialize_plain_bytes_or_str_column(
    cardinality: Cardinality,
    num_docs: RowId,
    sort_values_within_row: bool,
    is_remapped: bool,
    value_bytes: &ExpUnrolledLinkedList,
    num_bytes: u64,
    operation_it: impl Iterator<Item = ColumnOperation<PlainValueSpan>>,
    buffers: &mut SpareBuffers,
    arena: &MemoryArena,
    wrt: impl io::Write,
) -> io::Result<()> {
    let SpareBuffers {
        value_index_builders,
        plain_value_spans,
        ..
    } = buffers;
    plain_value_spans.clear();
    let column_index = build_serializable_column_index(
        operation_it,
        cardinality,
        num_docs,
        value_index_builders,
        plain_value_spans,
    );
    let mut plain_serializer = crate::column::PlainColumnSerializer::new(wrt);
    let multivalued_index_opt = match &column_index {
        SerializableColumnIndex::Multivalued(multivalued_index) if sort_values_within_row => {
            Some(multivalued_index)
        }
        _ => None,
    };
    if !is_remapped && multivalued_index_opt.is_none() {
        // The spans are contiguous and in record order: the values are streamed.
        let mut value_chunks = value_bytes.iter_chunks(arena);
        let mut num_bytes_read: u64 = 0;
        for span in plain_value_spans.iter() {
            debug_assert_eq!(span.start, num_bytes_read);
            let len = span.len as usize;
            plain_serializer.add_value_with(len, |block_bytes| {
                value_chunks.read_exact_into(len, block_bytes)
            })?;
            num_bytes_read += span.len as u64;
        }
        debug_assert_eq!(num_bytes_read, num_bytes);
        return plain_serializer.finish(column_index);
    }
    let mut all_value_bytes: Vec<u8> = Vec::with_capacity(num_bytes as usize);
    value_bytes.read_to_end(arena, &mut all_value_bytes);
    if let Some(multivalued_index) = multivalued_index_opt {
        let mut start_index: usize = 0;
        for end_index in multivalued_index.start_offsets.boxed_iter() {
            let end_index = end_index as usize;
            plain_value_spans[start_index..end_index].sort_unstable_by(|left, right| {
                plain_value_bytes(&all_value_bytes, *left)
                    .cmp(plain_value_bytes(&all_value_bytes, *right))
            });
            start_index = end_index;
        }
    }
    for span in plain_value_spans.iter() {
        plain_serializer.add_value(plain_value_bytes(&all_value_bytes, *span))?;
    }
    plain_serializer.finish(column_index)
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
    let serializable_column_index = build_serializable_column_index(
        op_iterator,
        cardinality,
        num_rows,
        value_index_builders,
        values,
    );
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
    let serializable_column_index = build_serializable_column_index(
        op_iterator,
        cardinality,
        num_rows,
        value_index_builders,
        values,
    );
    if sort_values_within_row
        && let SerializableColumnIndex::Multivalued(multivalued_index) = &serializable_column_index
    {
        sort_values_within_row_in_place(multivalued_index.start_offsets.boxed_iter(), values);
    }
    crate::column::serialize_column_mappable_to_u64(
        serializable_column_index,
        &&values[..],
        &mut wrt,
    )?;
    Ok(())
}

/// Consumes the operations: values are appended to `values`, and the returned column index maps
/// rows to them.
///
/// Contract: `cardinality` is the cardinality of the column described by `op_iterator`.
fn build_serializable_column_index<'a, T: Ord>(
    op_iterator: impl Iterator<Item = ColumnOperation<T>>,
    cardinality: Cardinality,
    num_rows: RowId,
    value_index_builders: &'a mut PreallocatedIndexBuilders,
    values: &mut Vec<T>,
) -> SerializableColumnIndex<'a> {
    match cardinality {
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
            SerializableColumnIndex::Multivalued(multivalued_index_builder.finish(num_rows))
        }
    }
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

    #[cfg(feature = "plain_codec")]
    mod plain {
        use crate::{
            ColumnType, ColumnarReader, DynamicColumn, PlainBytesColumn, RowId, StrColumnCodec,
        };

        fn serialize_and_open(
            columnar_writer: &mut crate::ColumnarWriter,
            num_docs: RowId,
            old_to_new_row_ids: Option<&[RowId]>,
        ) -> ColumnarReader {
            let mut buffer: Vec<u8> = Vec::new();
            columnar_writer
                .serialize(num_docs, old_to_new_row_ids, &mut buffer)
                .unwrap();
            ColumnarReader::open(buffer).unwrap()
        }

        fn open_single_column(columnar: &ColumnarReader, column_name: &str) -> DynamicColumn {
            let handles = columnar.read_columns(column_name).unwrap();
            assert_eq!(handles.len(), 1);
            handles[0].open().unwrap()
        }

        fn values_per_doc(column: &PlainBytesColumn, num_docs: u32) -> Vec<Vec<Vec<u8>>> {
            let mut reader = column.reader();
            let mut values_per_doc: Vec<Vec<Vec<u8>>> = Vec::with_capacity(num_docs as usize);
            for doc in 0..num_docs {
                let mut doc_values: Vec<Vec<u8>> = Vec::new();
                reader
                    .for_each_value(doc, |value| doc_values.push(value.to_vec()))
                    .unwrap();
                values_per_doc.push(doc_values);
            }
            values_per_doc
        }

        fn to_bytes(values_per_doc: &[&[&str]]) -> Vec<Vec<Vec<u8>>> {
            values_per_doc
                .iter()
                .map(|doc_values| {
                    doc_values
                        .iter()
                        .map(|value| value.as_bytes().to_vec())
                        .collect()
                })
                .collect()
        }

        #[test]
        fn test_plain_str_column() {
            let mut columnar_writer = crate::ColumnarWriter::default();
            columnar_writer.set_str_column_codec("text", ColumnType::Str, StrColumnCodec::Plain);
            columnar_writer.record_str(0, "text", "hello");
            columnar_writer.record_str(0, "text", "world");
            columnar_writer.record_str(2, "text", "hello");
            columnar_writer.record_str(3, "text", "");
            let columnar = serialize_and_open(&mut columnar_writer, 4, None);
            let DynamicColumn::PlainStr(plain_str_column) = open_single_column(&columnar, "text")
            else {
                panic!("expected a plain str column");
            };
            let plain_bytes_column = PlainBytesColumn::from(plain_str_column);
            assert_eq!(plain_bytes_column.num_values(), 4);
            assert_eq!(
                values_per_doc(&plain_bytes_column, 4),
                to_bytes(&[&["hello", "world"], &[], &["hello"], &[""]])
            );
        }

        #[test]
        fn test_plain_bytes_column_optional() {
            let mut columnar_writer = crate::ColumnarWriter::default();
            columnar_writer.set_str_column_codec("bytes", ColumnType::Bytes, StrColumnCodec::Plain);
            columnar_writer.record_bytes(1, "bytes", b"b");
            columnar_writer.record_bytes(2, "bytes", b"a");
            let columnar = serialize_and_open(&mut columnar_writer, 3, None);
            let DynamicColumn::PlainBytes(plain_bytes_column) =
                open_single_column(&columnar, "bytes")
            else {
                panic!("expected a plain bytes column");
            };
            assert_eq!(
                plain_bytes_column.get_cardinality(),
                crate::Cardinality::Optional
            );
            assert_eq!(
                values_per_doc(&plain_bytes_column, 3),
                to_bytes(&[&[], &["b"], &["a"]])
            );
        }

        #[test]
        fn test_plain_column_with_doc_remapping() {
            let mut columnar_writer = crate::ColumnarWriter::default();
            columnar_writer.set_str_column_codec("text", ColumnType::Str, StrColumnCodec::Plain);
            columnar_writer.record_str(0, "text", "doc0");
            columnar_writer.record_str(1, "text", "doc1");
            columnar_writer.record_str(2, "text", "doc2");
            let old_to_new_row_ids: Vec<RowId> = vec![2, 0, 1];
            let columnar = serialize_and_open(&mut columnar_writer, 3, Some(&old_to_new_row_ids));
            let DynamicColumn::PlainStr(plain_str_column) = open_single_column(&columnar, "text")
            else {
                panic!("expected a plain str column");
            };
            assert_eq!(
                values_per_doc(&PlainBytesColumn::from(plain_str_column), 3),
                to_bytes(&[&["doc1"], &["doc2"], &["doc0"]])
            );
        }

        #[test]
        fn test_plain_column_sort_values_within_row() {
            let mut columnar_writer = crate::ColumnarWriter::default();
            columnar_writer.record_column_type("text", ColumnType::Str, true);
            columnar_writer.set_str_column_codec("text", ColumnType::Str, StrColumnCodec::Plain);
            for value in ["c", "a", "b"] {
                columnar_writer.record_str(0, "text", value);
            }
            columnar_writer.record_str(1, "text", "z");
            columnar_writer.record_str(1, "text", "y");
            let columnar = serialize_and_open(&mut columnar_writer, 2, None);
            let DynamicColumn::PlainStr(plain_str_column) = open_single_column(&columnar, "text")
            else {
                panic!("expected a plain str column");
            };
            assert_eq!(
                values_per_doc(&PlainBytesColumn::from(plain_str_column), 2),
                to_bytes(&[&["a", "b", "c"], &["y", "z"]])
            );
        }

        #[test]
        fn test_plain_column_key_order() {
            // The key code of a plain bytes column (0x13) is greater than the key code of a
            // dictionary str column (0x04), although `Bytes < Str`.
            let mut columnar_writer = crate::ColumnarWriter::default();
            columnar_writer.set_str_column_codec("field", ColumnType::Bytes, StrColumnCodec::Plain);
            columnar_writer.record_bytes(0, "field", b"bytes");
            columnar_writer.record_str(0, "field", "str");
            columnar_writer.record_numerical(0, "field", 1u64);
            let columnar = serialize_and_open(&mut columnar_writer, 1, None);
            let mut codecs: Vec<(ColumnType, Option<StrColumnCodec>)> = columnar
                .read_columns("field")
                .unwrap()
                .iter()
                .map(|handle| (handle.column_type(), handle.str_codec()))
                .collect();
            codecs.sort_by_key(|(column_type, _)| *column_type);
            assert_eq!(
                codecs,
                vec![
                    (ColumnType::I64, None),
                    (ColumnType::Bytes, Some(StrColumnCodec::Plain)),
                    (ColumnType::Str, Some(StrColumnCodec::Dictionary)),
                ]
            );
        }

        #[test]
        fn test_plain_column_large_values() {
            // Larger than a memory arena page (1 MiB) and than an `ExpUnrolledLinkedList` block.
            let large_value: Vec<u8> = (0..3_000_000u32).map(|i| (i % 251) as u8).collect();
            let mut columnar_writer = crate::ColumnarWriter::default();
            columnar_writer.set_str_column_codec("bytes", ColumnType::Bytes, StrColumnCodec::Plain);
            columnar_writer.record_bytes(0, "bytes", b"small");
            columnar_writer.record_bytes(1, "bytes", &large_value);
            columnar_writer.record_bytes(1, "bytes", b"");
            columnar_writer.record_bytes(2, "bytes", b"small2");
            let columnar = serialize_and_open(&mut columnar_writer, 3, None);
            let DynamicColumn::PlainBytes(plain_bytes_column) =
                open_single_column(&columnar, "bytes")
            else {
                panic!("expected a plain bytes column");
            };
            assert_eq!(
                values_per_doc(&plain_bytes_column, 3),
                vec![
                    vec![b"small".to_vec()],
                    vec![large_value, Vec::new()],
                    vec![b"small2".to_vec()],
                ]
            );
        }

        #[test]
        fn test_plain_column_values_are_not_deduplicated() {
            let value: Vec<u8> = vec![b'x'; 1_000];
            let num_docs: u32 = 3_000;
            let mut columnar_writer = crate::ColumnarWriter::default();
            columnar_writer.set_str_column_codec("bytes", ColumnType::Bytes, StrColumnCodec::Plain);
            for doc in 0..num_docs {
                columnar_writer.record_bytes(doc, "bytes", &value);
            }
            // Each value is stored in the memory arena.
            assert!(columnar_writer.mem_usage() >= num_docs as usize * value.len());
            let columnar = serialize_and_open(&mut columnar_writer, num_docs, None);
            let DynamicColumn::PlainBytes(plain_bytes_column) =
                open_single_column(&columnar, "bytes")
            else {
                panic!("expected a plain bytes column");
            };
            assert_eq!(plain_bytes_column.num_values(), num_docs);
            assert_eq!(
                values_per_doc(&plain_bytes_column, num_docs),
                vec![vec![value]; num_docs as usize]
            );
        }

        #[test]
        fn test_plain_columns_interleaved() {
            let mut columnar_writer = crate::ColumnarWriter::default();
            columnar_writer.set_str_column_codec("left", ColumnType::Str, StrColumnCodec::Plain);
            columnar_writer.set_str_column_codec("right", ColumnType::Str, StrColumnCodec::Plain);
            for doc in 0..100u32 {
                columnar_writer.record_str(doc, "left", &format!("left{doc}"));
                columnar_writer.record_str(doc, "right", &format!("right{doc}"));
            }
            let columnar = serialize_and_open(&mut columnar_writer, 100, None);
            for column_name in ["left", "right"] {
                let DynamicColumn::PlainStr(plain_str_column) =
                    open_single_column(&columnar, column_name)
                else {
                    panic!("expected a plain str column");
                };
                let expected: Vec<Vec<Vec<u8>>> = (0..100u32)
                    .map(|doc| vec![format!("{column_name}{doc}").into_bytes()])
                    .collect();
                assert_eq!(
                    values_per_doc(&PlainBytesColumn::from(plain_str_column), 100),
                    expected
                );
            }
        }

        #[test]
        fn test_plain_column_sort_order() {
            let mut columnar_writer = crate::ColumnarWriter::default();
            columnar_writer.set_str_column_codec("text", ColumnType::Str, StrColumnCodec::Plain);
            columnar_writer.record_str(0, "text", "b");
            columnar_writer.record_str(2, "text", "a");
            columnar_writer.record_str(3, "text", "c");
            // Only the first value of a doc is used.
            columnar_writer.record_str(3, "text", "0");
            assert_eq!(
                columnar_writer.sort_order("text", 4, false),
                vec![1, 2, 0, 3]
            );
            assert_eq!(
                columnar_writer.sort_order("text", 4, true),
                vec![3, 0, 2, 1]
            );
        }

        #[test]
        fn test_set_str_column_codec_switch_before_values() {
            let mut columnar_writer = crate::ColumnarWriter::default();
            columnar_writer.record_column_type("text", ColumnType::Str, true);
            columnar_writer.set_str_column_codec("text", ColumnType::Str, StrColumnCodec::Plain);
            columnar_writer.set_str_column_codec("text", ColumnType::Str, StrColumnCodec::Plain);
            columnar_writer.record_str(0, "text", "b");
            columnar_writer.record_str(0, "text", "a");
            let columnar = serialize_and_open(&mut columnar_writer, 1, None);
            let DynamicColumn::PlainStr(plain_str_column) = open_single_column(&columnar, "text")
            else {
                panic!("expected a plain str column");
            };
            // `sort_values_within_row` is kept when the codec changes.
            assert_eq!(
                values_per_doc(&PlainBytesColumn::from(plain_str_column), 1),
                to_bytes(&[&["a", "b"]])
            );
        }

        #[test]
        #[should_panic(expected = "the codec of a column must be set before values are recorded")]
        fn test_set_str_column_codec_after_values_panics() {
            let mut columnar_writer = crate::ColumnarWriter::default();
            columnar_writer.record_str(0, "text", "a");
            columnar_writer.set_str_column_codec("text", ColumnType::Str, StrColumnCodec::Plain);
        }

        #[test]
        #[should_panic(expected = "a codec can only be set on str and bytes columns")]
        fn test_set_str_column_codec_numerical_panics() {
            let mut columnar_writer = crate::ColumnarWriter::default();
            columnar_writer.set_str_column_codec("num", ColumnType::U64, StrColumnCodec::Plain);
        }
    }
}

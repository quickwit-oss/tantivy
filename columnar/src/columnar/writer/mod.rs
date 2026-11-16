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
use stacker::{Addr, ArenaHashMap, MemoryArena};

use crate::column::PlainColumnSerializer;
use crate::column_index::{SerializableColumnIndex, SerializableOptionalIndex};
use crate::column_values::{MonotonicallyMappableToU64, MonotonicallyMappableToU128};
use crate::columnar::column_type::{ColumnType, StrColumnCodec, column_key_code};
use crate::columnar::writer::column_operation::ValueLen;
use crate::columnar::writer::column_writers::{
    ColumnWriter, NumericalColumnWriter, PlainColumnWriter, StrOrBytesColumnWriter,
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
    /// Bytes of a plain column, materialized when docs are remapped.
    plain_value_bytes: Vec<u8>,
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
        let StrOrBytesColumnWriter::Dictionary(dictionary_column_writer) = str_or_bytes_column
        else {
            // Sorting by a plain-encoded column is not supported, and rejected when the index is
            // created. Docs keep their order.
            return (0..num_docs).collect();
        };
        let dictionary_builder =
            &self.dictionaries[dictionary_column_writer.dictionary_id as usize];
        let term_id_mapping = dictionary_builder.build_term_id_mapping(&self.arena);
        collect_sort_order_from_ops(
            dictionary_column_writer.operation_iterator(&self.arena, None, &mut symbols_buffer),
            num_docs,
            reversed,
            |uid| Some(term_id_mapping.to_ord(uid).0),
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
        if sort_values_within_row {
            assert!(
                column_type == ColumnType::Bytes || column_type == ColumnType::Str,
                "sort_values_within_row is only allowed for Bytes and Str columns",
            );
        }
        match column_type {
            ColumnType::Str | ColumnType::Bytes => {
                self.record_str_or_bytes_column_type(
                    column_name,
                    column_type,
                    None,
                    sort_values_within_row,
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

    /// Same as `record_column_type`, with the codec of a `Str` or `Bytes` column.
    ///
    /// The codec of a column is fixed when the column is created.
    ///
    /// # Panics
    ///
    /// Panics if the column already exists with a different codec, or if `codec` is `Plain` for a
    /// column type other than `Str` or `Bytes`.
    pub fn record_column_type_with_codec(
        &mut self,
        column_name: &str,
        column_type: ColumnType,
        codec: StrColumnCodec,
    ) {
        match column_type {
            ColumnType::Str | ColumnType::Bytes => {
                self.record_str_or_bytes_column_type(column_name, column_type, Some(codec), false);
            }
            _ => {
                assert_eq!(
                    codec,
                    StrColumnCodec::Dictionary,
                    "the plain codec is only supported for str and bytes columns"
                );
                self.record_column_type(column_name, column_type, false);
            }
        }
    }

    /// Creates the `Str` or `Bytes` column if it does not exist yet.
    ///
    /// The column is created with `codec_opt`, or the dictionary codec if it is `None`.
    fn record_str_or_bytes_column_type(
        &mut self,
        column_name: &str,
        column_type: ColumnType,
        codec_opt: Option<StrColumnCodec>,
        sort_values_within_row: bool,
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
                let mut column_writer =
                    get_or_create_str_or_bytes_column_writer(column_opt, codec_opt, dictionaries);
                match &mut column_writer {
                    StrOrBytesColumnWriter::Dictionary(dictionary_column_writer) => {
                        dictionary_column_writer.sort_values_within_row = sort_values_within_row;
                    }
                    StrOrBytesColumnWriter::Plain(_) => {
                        assert!(
                            !sort_values_within_row,
                            "sort_values_within_row is not supported with the plain codec"
                        );
                    }
                }
                column_writer
            },
        );
    }

    /// Returns the code ending the key of the column of type `column_type` stored at `addr`.
    fn column_key_code(&self, column_type: ColumnType, addr: Addr) -> u8 {
        let codec = match column_type {
            ColumnType::Str => self
                .str_field_hash_map
                .read::<StrOrBytesColumnWriter>(addr)
                .codec(),
            ColumnType::Bytes => self
                .bytes_field_hash_map
                .read::<StrOrBytesColumnWriter>(addr)
                .codec(),
            _ => StrColumnCodec::Dictionary,
        };
        column_key_code(column_type, codec)
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

    /// Records a value in a `Str` column.
    ///
    /// If the column does not exist yet, it is created with the dictionary codec.
    pub fn record_str(&mut self, doc: RowId, column_name: &str, value: &str) {
        self.record_str_or_bytes(ColumnType::Str, doc, column_name, value.as_bytes(), None);
    }

    /// Records a value in a `Str` column using the codec `codec`.
    ///
    /// # Panics
    ///
    /// Panics if the column already exists with a different codec.
    pub fn record_str_with_codec(
        &mut self,
        doc: RowId,
        column_name: &str,
        value: &str,
        codec: StrColumnCodec,
    ) {
        self.record_str_or_bytes(
            ColumnType::Str,
            doc,
            column_name,
            value.as_bytes(),
            Some(codec),
        );
    }

    /// Records a value in a `Bytes` column.
    ///
    /// If the column does not exist yet, it is created with the dictionary codec.
    pub fn record_bytes(&mut self, doc: RowId, column_name: &str, value: &[u8]) {
        self.record_str_or_bytes(ColumnType::Bytes, doc, column_name, value, None);
    }

    /// Records a value in a `Bytes` column using the codec `codec`.
    ///
    /// # Panics
    ///
    /// Panics if the column already exists with a different codec.
    pub fn record_bytes_with_codec(
        &mut self,
        doc: RowId,
        column_name: &str,
        value: &[u8],
        codec: StrColumnCodec,
    ) {
        self.record_str_or_bytes(ColumnType::Bytes, doc, column_name, value, Some(codec));
    }

    fn record_str_or_bytes(
        &mut self,
        column_type: ColumnType,
        doc: RowId,
        column_name: &str,
        value: &[u8],
        codec_opt: Option<StrColumnCodec>,
    ) {
        let (hash_map, arena, dictionaries) = (
            if column_type == ColumnType::Str {
                &mut self.str_field_hash_map
            } else {
                &mut self.bytes_field_hash_map
            },
            &mut self.arena,
            &mut self.dictionaries,
        );
        hash_map.mutate_or_create(
            column_name.as_bytes(),
            |column_opt: Option<StrOrBytesColumnWriter>| {
                let mut column =
                    get_or_create_str_or_bytes_column_writer(column_opt, codec_opt, dictionaries);
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
        // Columns must be serialized in the order of their keys. The key code of a `Str` or
        // `Bytes` column depends on its codec.
        columns.sort_unstable_by_key(|(column_name, column_type, addr)| {
            (*column_name, self.column_key_code(*column_type, *addr))
        });
        let (arena, buffers, dictionaries) = (&self.arena, &mut self.buffers, &self.dictionaries);
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
                    let cardinality = str_or_bytes_column_writer.get_cardinality(num_docs);
                    let mut column_serializer = serializer.start_serialize_column_with_codec(
                        column_name,
                        column_type,
                        str_or_bytes_column_writer.codec(),
                    );
                    match str_or_bytes_column_writer {
                        StrOrBytesColumnWriter::Dictionary(dictionary_column_writer) => {
                            let dictionary_builder =
                                &dictionaries[dictionary_column_writer.dictionary_id as usize];
                            serialize_bytes_or_str_column(
                                cardinality,
                                num_docs,
                                dictionary_column_writer.sort_values_within_row,
                                dictionary_builder,
                                dictionary_column_writer.operation_iterator(
                                    arena,
                                    old_to_new_row_ids,
                                    &mut symbol_byte_buffer,
                                ),
                                buffers,
                                &self.arena,
                                &mut column_serializer,
                            )?;
                        }
                        StrOrBytesColumnWriter::Plain(plain_column_writer) => {
                            serialize_plain_bytes_or_str_column(
                                cardinality,
                                num_docs,
                                &plain_column_writer,
                                old_to_new_row_ids,
                                arena,
                                &mut symbol_byte_buffer,
                                buffers,
                                &mut column_serializer,
                            )?;
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

/// Returns the writer of an existing `Str` or `Bytes` column, or creates one.
///
/// A new column is created with `codec_opt`, or with the dictionary codec if it is `None`.
///
/// # Panics
///
/// Panics if the column exists with a codec different from `codec_opt`.
fn get_or_create_str_or_bytes_column_writer(
    column_opt: Option<StrOrBytesColumnWriter>,
    codec_opt: Option<StrColumnCodec>,
    dictionaries: &mut Vec<DictionaryBuilder>,
) -> StrOrBytesColumnWriter {
    let Some(column_writer) = column_opt else {
        return StrOrBytesColumnWriter::with_codec(codec_opt.unwrap_or_default(), dictionaries);
    };
    if let Some(codec) = codec_opt {
        assert_eq!(
            column_writer.codec(),
            codec,
            "a column cannot be recorded with two different codecs"
        );
    }
    column_writer
}

/// Serializes a plain-encoded column. See `PlainColumnSerializer` for the layout.
#[expect(clippy::too_many_arguments)]
fn serialize_plain_bytes_or_str_column(
    cardinality: Cardinality,
    num_docs: RowId,
    plain_column_writer: &PlainColumnWriter,
    old_to_new_row_ids: Option<&[RowId]>,
    arena: &MemoryArena,
    op_buffer: &mut Vec<u8>,
    buffers: &mut SpareBuffers,
    wrt: impl io::Write,
) -> io::Result<()> {
    let SpareBuffers {
        value_index_builders,
        plain_value_bytes,
        ..
    } = buffers;
    let ops = plain_column_writer
        .column_writer
        .operation_iterator::<ValueLen>(arena, None, op_buffer);
    let Some(old_to_new_row_ids) = old_to_new_row_ids else {
        // Values are read in insertion order, chunk by chunk, straight into the block buffer.
        let mut value_bytes_reader = plain_column_writer.value_bytes.iter_chunks(arena);
        return serialize_plain_operations(
            ops,
            |len, block_bytes| value_bytes_reader.read_exact_into(len as usize, block_bytes),
            cardinality,
            num_docs,
            value_index_builders,
            wrt,
        );
    };
    // The docs are remapped: the values are read in the new doc order.
    plain_value_bytes.clear();
    plain_column_writer
        .value_bytes
        .read_to_end(arena, plain_value_bytes);
    // (new doc, start of the value in `plain_value_bytes`, length) for each value, in insertion
    // order.
    let mut doc_values: Vec<(RowId, usize, u32)> = Vec::new();
    let mut new_doc: RowId = 0;
    let mut value_start: usize = 0;
    for op in ops {
        match op {
            ColumnOperation::NewDoc(old_doc) => {
                new_doc = old_to_new_row_ids[old_doc as usize];
            }
            ColumnOperation::Value(ValueLen(len)) => {
                doc_values.push((new_doc, value_start, len));
                value_start += len as usize;
            }
        }
    }
    // The sort is stable: the values of a doc keep their order.
    doc_values.sort_by_key(|(new_doc, _, _)| *new_doc);
    let mut remapped_ops: Vec<ColumnOperation<ValueLen>> = Vec::with_capacity(doc_values.len() * 2);
    let mut previous_new_doc: Option<RowId> = None;
    for &(new_doc, _, len) in &doc_values {
        if previous_new_doc != Some(new_doc) {
            remapped_ops.push(ColumnOperation::NewDoc(new_doc));
            previous_new_doc = Some(new_doc);
        }
        remapped_ops.push(ColumnOperation::Value(ValueLen(len)));
    }
    let mut doc_values_it = doc_values.iter();
    serialize_plain_operations(
        remapped_ops.into_iter(),
        |len, block_bytes| {
            let &(_, value_start, value_len) = doc_values_it
                .next()
                .expect("there is one doc value per value operation");
            debug_assert_eq!(value_len, len);
            block_bytes
                .extend_from_slice(&plain_value_bytes[value_start..value_start + len as usize]);
        },
        cardinality,
        num_docs,
        value_index_builders,
        wrt,
    )
}

/// Builds the column index from `ops`, and streams the values to a `PlainColumnSerializer`.
///
/// For each `Value(ValueLen(len))` operation, `append_value(len, block_bytes)` must append the
/// `len` bytes of the value to `block_bytes`.
fn serialize_plain_operations(
    ops: impl Iterator<Item = ColumnOperation<ValueLen>>,
    mut append_value: impl FnMut(u32, &mut Vec<u8>),
    cardinality: Cardinality,
    num_rows: RowId,
    value_index_builders: &mut PreallocatedIndexBuilders,
    wrt: impl io::Write,
) -> io::Result<()> {
    let mut plain_serializer = PlainColumnSerializer::new(wrt);
    let serializable_column_index = match cardinality {
        Cardinality::Full => {
            consume_plain_operations(
                ops,
                value_index_builders.borrow_required_index_builder(),
                &mut append_value,
                &mut plain_serializer,
            )?;
            SerializableColumnIndex::Full
        }
        Cardinality::Optional => {
            let optional_index_builder = value_index_builders.borrow_optional_index_builder();
            consume_plain_operations(
                ops,
                optional_index_builder,
                &mut append_value,
                &mut plain_serializer,
            )?;
            let optional_index = optional_index_builder.finish(num_rows);
            SerializableColumnIndex::Optional(SerializableOptionalIndex {
                non_null_row_ids: Box::new(optional_index),
                num_rows,
            })
        }
        Cardinality::Multivalued => {
            let multivalued_index_builder = value_index_builders.borrow_multivalued_index_builder();
            consume_plain_operations(
                ops,
                multivalued_index_builder,
                &mut append_value,
                &mut plain_serializer,
            )?;
            SerializableColumnIndex::Multivalued(multivalued_index_builder.finish(num_rows))
        }
    };
    plain_serializer.finish(serializable_column_index)
}

fn consume_plain_operations<TIndexBuilder: IndexBuilder, W: io::Write>(
    ops: impl Iterator<Item = ColumnOperation<ValueLen>>,
    index_builder: &mut TIndexBuilder,
    append_value: &mut impl FnMut(u32, &mut Vec<u8>),
    plain_serializer: &mut PlainColumnSerializer<W>,
) -> io::Result<()> {
    for op in ops {
        match op {
            ColumnOperation::NewDoc(doc) => {
                index_builder.record_row(doc);
            }
            ColumnOperation::Value(ValueLen(len)) => {
                index_builder.record_value();
                plain_serializer
                    .add_value_with(len as usize, |block_bytes| append_value(len, block_bytes))?;
            }
        }
    }
    Ok(())
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

#[cfg(test)]
mod tests {
    use stacker::MemoryArena;

    use crate::columnar::format_version::{VERSION_FOOTER_NUM_BYTES, parse_footer};
    use crate::columnar::writer::column_operation::ColumnOperation;
    use crate::dynamic_column::str_or_bytes_values_per_doc;
    use crate::{
        Cardinality, ColumnType, ColumnarReader, ColumnarWriter, DynamicColumn, NumericalValue,
        RowId, StrColumnCodec, Version,
    };

    fn columnar_version(columnar_bytes: &[u8]) -> Version {
        let footer: [u8; VERSION_FOOTER_NUM_BYTES] = columnar_bytes
            [columnar_bytes.len() - VERSION_FOOTER_NUM_BYTES..]
            .try_into()
            .unwrap();
        parse_footer(footer).unwrap()
    }

    /// Builds a columnar with a plain `Str` column named "text".
    fn build_plain_str_columnar(
        docs: &[Vec<String>],
        old_to_new_row_ids: Option<&[RowId]>,
    ) -> Vec<u8> {
        let mut columnar_writer = ColumnarWriter::default();
        for (doc, doc_values) in docs.iter().enumerate() {
            for value in doc_values {
                columnar_writer.record_str_with_codec(
                    doc as RowId,
                    "text",
                    value,
                    StrColumnCodec::Plain,
                );
            }
        }
        let mut buffer = Vec::new();
        columnar_writer
            .serialize(docs.len() as RowId, old_to_new_row_ids, &mut buffer)
            .unwrap();
        buffer
    }

    fn to_bytes_per_doc(docs: &[Vec<String>]) -> Vec<Vec<Vec<u8>>> {
        docs.iter()
            .map(|doc_values| {
                doc_values
                    .iter()
                    .map(|value| value.as_bytes().to_vec())
                    .collect()
            })
            .collect()
    }

    #[track_caller]
    fn test_plain_str_column_aux(docs: &[Vec<String>], expected_cardinality: Cardinality) {
        let columnar_bytes = build_plain_str_columnar(docs, None);
        assert_eq!(columnar_version(&columnar_bytes), Version::V3);
        let columnar = ColumnarReader::open(columnar_bytes).unwrap();
        let handles = columnar.read_columns("text").unwrap();
        assert_eq!(handles.len(), 1);
        assert_eq!(handles[0].column_type(), ColumnType::Str);
        assert_eq!(handles[0].str_codec(), Some(StrColumnCodec::Plain));
        assert!(handles[0].open_u64_lenient().is_err());
        let dynamic_column = handles[0].open().unwrap();
        assert!(matches!(dynamic_column, DynamicColumn::PlainStr(_)));
        assert_eq!(dynamic_column.get_cardinality(), expected_cardinality);
        assert_eq!(dynamic_column.str_codec(), Some(StrColumnCodec::Plain));
        assert_eq!(
            str_or_bytes_values_per_doc(&dynamic_column),
            to_bytes_per_doc(docs)
        );
    }

    fn doc(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| value.to_string()).collect()
    }

    #[test]
    fn test_plain_str_column_full() {
        let docs = vec![doc(&["hello"]), doc(&[""]), doc(&["happy tax payer"])];
        test_plain_str_column_aux(&docs, Cardinality::Full);
    }

    #[test]
    fn test_plain_str_column_optional() {
        let docs = vec![doc(&[]), doc(&["b"]), doc(&[]), doc(&["a"]), doc(&[])];
        test_plain_str_column_aux(&docs, Cardinality::Optional);
    }

    #[test]
    fn test_plain_str_column_multivalued() {
        let docs = vec![
            doc(&["b", "a"]),
            doc(&[]),
            doc(&["c"]),
            doc(&["a", "a", "a"]),
        ];
        test_plain_str_column_aux(&docs, Cardinality::Multivalued);
    }

    #[test]
    fn test_plain_str_column_many_blocks() {
        // Values span many arena chunks, and several blocks.
        let docs: Vec<Vec<String>> = (0..100_000)
            .map(|doc_id| {
                if doc_id % 3 == 0 {
                    doc(&[])
                } else {
                    vec![format!("/api/v1/user/{doc_id}/items?page={}", doc_id % 17); 2]
                }
            })
            .collect();
        test_plain_str_column_aux(&docs, Cardinality::Multivalued);
    }

    #[test]
    fn test_plain_str_column_with_doc_remap() {
        let docs = vec![
            doc(&["zero"]),
            doc(&["one", "uno"]),
            doc(&[]),
            doc(&["three"]),
        ];
        let old_to_new_row_ids: Vec<RowId> = vec![2, 0, 3, 1];
        let columnar_bytes = build_plain_str_columnar(&docs, Some(&old_to_new_row_ids));
        let columnar = ColumnarReader::open(columnar_bytes).unwrap();
        let dynamic_column = columnar.read_columns("text").unwrap()[0].open().unwrap();
        let expected = to_bytes_per_doc(&[
            doc(&["one", "uno"]),
            doc(&["three"]),
            doc(&["zero"]),
            doc(&[]),
        ]);
        assert_eq!(str_or_bytes_values_per_doc(&dynamic_column), expected);
    }

    #[test]
    fn test_plain_bytes_column() {
        let mut columnar_writer = ColumnarWriter::default();
        columnar_writer.record_bytes_with_codec(0, "bytes", &[0, 255], StrColumnCodec::Plain);
        columnar_writer.record_bytes_with_codec(2, "bytes", &[], StrColumnCodec::Plain);
        let mut buffer = Vec::new();
        columnar_writer.serialize(3, None, &mut buffer).unwrap();
        let columnar = ColumnarReader::open(buffer).unwrap();
        let handles = columnar.read_columns("bytes").unwrap();
        assert_eq!(handles[0].column_type(), ColumnType::Bytes);
        let dynamic_column = handles[0].open().unwrap();
        assert!(matches!(dynamic_column, DynamicColumn::PlainBytes(_)));
        assert_eq!(
            str_or_bytes_values_per_doc(&dynamic_column),
            vec![vec![vec![0, 255]], vec![], vec![vec![]]]
        );
    }

    #[test]
    fn test_record_str_uses_the_codec_of_the_column() {
        let mut columnar_writer = ColumnarWriter::default();
        columnar_writer.record_column_type_with_codec(
            "text",
            ColumnType::Str,
            StrColumnCodec::Plain,
        );
        columnar_writer.record_str(0, "text", "hello");
        let mut buffer = Vec::new();
        columnar_writer.serialize(1, None, &mut buffer).unwrap();
        let columnar = ColumnarReader::open(buffer).unwrap();
        let handles = columnar.read_columns("text").unwrap();
        assert_eq!(handles[0].str_codec(), Some(StrColumnCodec::Plain));
    }

    #[test]
    fn test_required_plain_column_without_values() {
        let mut columnar_writer = ColumnarWriter::default();
        columnar_writer.record_column_type_with_codec(
            "text",
            ColumnType::Str,
            StrColumnCodec::Plain,
        );
        let mut buffer = Vec::new();
        columnar_writer.serialize(2, None, &mut buffer).unwrap();
        let columnar = ColumnarReader::open(buffer).unwrap();
        let dynamic_column = columnar.read_columns("text").unwrap()[0].open().unwrap();
        assert_eq!(dynamic_column.num_values(), 0);
        assert_eq!(
            str_or_bytes_values_per_doc(&dynamic_column),
            vec![Vec::<Vec<u8>>::new(); 2]
        );
    }

    #[test]
    #[should_panic(expected = "two different codecs")]
    fn test_record_str_with_another_codec_panics() {
        let mut columnar_writer = ColumnarWriter::default();
        columnar_writer.record_str_with_codec(0, "text", "a", StrColumnCodec::Plain);
        columnar_writer.record_str_with_codec(1, "text", "b", StrColumnCodec::Dictionary);
    }

    #[test]
    fn test_columnar_without_plain_column_keeps_version_v2() {
        let mut columnar_writer = ColumnarWriter::default();
        columnar_writer.record_str(0, "text", "hello");
        let mut buffer = Vec::new();
        columnar_writer.serialize(1, None, &mut buffer).unwrap();
        assert_eq!(columnar_version(&buffer), Version::V2);
    }

    #[test]
    fn test_plain_column_and_other_types_with_the_same_name() {
        // The plain key code sorts after all of the other column types.
        let mut columnar_writer = ColumnarWriter::default();
        columnar_writer.record_str_with_codec(0, "field", "a", StrColumnCodec::Plain);
        columnar_writer.record_numerical(1, "field", 12u64);
        columnar_writer.record_bool(1, "field", true);
        columnar_writer.record_datetime(1, "field", common::DateTime::from_timestamp_secs(1));
        columnar_writer.record_str(1, "other", "b");
        let mut buffer = Vec::new();
        columnar_writer.serialize(2, None, &mut buffer).unwrap();
        let columnar = ColumnarReader::open(buffer).unwrap();
        let column_types: Vec<(ColumnType, Option<StrColumnCodec>)> = columnar
            .read_columns("field")
            .unwrap()
            .iter()
            .map(|handle| (handle.column_type(), handle.str_codec()))
            .collect();
        assert_eq!(
            column_types,
            vec![
                // Numerical columns are i64 when all of their values fit.
                (ColumnType::I64, None),
                (ColumnType::Bool, None),
                (ColumnType::DateTime, None),
                (ColumnType::Str, Some(StrColumnCodec::Plain)),
            ]
        );
        assert_eq!(columnar.list_columns().unwrap().len(), 5);
    }

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

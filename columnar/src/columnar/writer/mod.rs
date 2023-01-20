mod column_operation;
mod column_writers;
mod serializer;
mod value_index;

use std::io;
use std::net::Ipv6Addr;

use column_operation::ColumnOperation;
use common::CountingWriter;
use serializer::ColumnarSerializer;
use stacker::{Addr, ArenaHashMap, MemoryArena};

use crate::column_index::SerializableColumnIndex;
use crate::column_values::{
    ColumnValues, MonotonicallyMappableToU128, MonotonicallyMappableToU64, VecColumn,
};
use crate::columnar::column_type::ColumnType;
use crate::columnar::writer::column_writers::{
    ColumnWriter, NumericalColumnWriter, StrOrBytesColumnWriter,
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
    i64_values: Vec<i64>,
    u64_values: Vec<u64>,
    f64_values: Vec<f64>,
    bool_values: Vec<bool>,
    ip_addr_values: Vec<Ipv6Addr>,
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
/// columnar_writer.serialize(2u32, &mut wrt).unwrap();
/// ```
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
    buffers: SpareBuffers,
}

impl Default for ColumnarWriter {
    fn default() -> Self {
        ColumnarWriter {
            numerical_field_hash_map: ArenaHashMap::new(10_000),
            bool_field_hash_map: ArenaHashMap::new(10_000),
            ip_addr_field_hash_map: ArenaHashMap::new(10_000),
            bytes_field_hash_map: ArenaHashMap::new(10_000),
            str_field_hash_map: ArenaHashMap::new(10_000),
            datetime_field_hash_map: ArenaHashMap::new(10_000),
            dictionaries: Vec::new(),
            arena: MemoryArena::default(),
            buffers: SpareBuffers::default(),
        }
    }
}

#[inline]
fn mutate_or_create_column<V, TMutator>(
    arena_hash_map: &mut ArenaHashMap,
    column_name: &str,
    updater: TMutator,
) where
    V: Copy + 'static,
    TMutator: FnMut(Option<V>) -> V,
{
    assert!(
        !column_name.as_bytes().contains(&0u8),
        "key may not contain the 0 byte"
    );
    arena_hash_map.mutate_or_create(column_name.as_bytes(), updater);
}

impl ColumnarWriter {
    pub fn mem_usage(&self) -> usize {
        // TODO add dictionary builders.
        self.arena.mem_usage()
            + self.numerical_field_hash_map.mem_usage()
            + self.bool_field_hash_map.mem_usage()
            + self.bytes_field_hash_map.mem_usage()
            + self.str_field_hash_map.mem_usage()
            + self.ip_addr_field_hash_map.mem_usage()
            + self.datetime_field_hash_map.mem_usage()
    }

    pub fn record_column_type(&mut self, column_name: &str, column_type: ColumnType) {
        match column_type {
            ColumnType::Str | ColumnType::Bytes => {
                let (hash_map, dictionaries) = (
                    if column_type == ColumnType::Str {
                        &mut self.str_field_hash_map
                    } else {
                        &mut self.bytes_field_hash_map
                    },
                    &mut self.dictionaries,
                );
                mutate_or_create_column(
                    hash_map,
                    column_name,
                    |column_opt: Option<StrOrBytesColumnWriter>| {
                        if let Some(column_writer) = column_opt {
                            column_writer
                        } else {
                            let dictionary_id = dictionaries.len() as u32;
                            dictionaries.push(DictionaryBuilder::default());
                            StrOrBytesColumnWriter::with_dictionary_id(dictionary_id)
                        }
                    },
                );
            }
            ColumnType::Bool => {
                mutate_or_create_column(
                    &mut self.bool_field_hash_map,
                    column_name,
                    |column_opt: Option<ColumnWriter>| column_opt.unwrap_or_default(),
                );
            }
            ColumnType::DateTime => {
                mutate_or_create_column(
                    &mut self.datetime_field_hash_map,
                    column_name,
                    |column_opt: Option<ColumnWriter>| column_opt.unwrap_or_default(),
                );
            }
            ColumnType::I64 | ColumnType::F64 | ColumnType::U64 => {
                let numerical_type = column_type.numerical_type().unwrap();
                mutate_or_create_column(
                    &mut self.numerical_field_hash_map,
                    column_name,
                    |column_opt: Option<NumericalColumnWriter>| {
                        let mut column: NumericalColumnWriter = column_opt.unwrap_or_default();
                        column.force_numerical_type(numerical_type);
                        column
                    },
                );
            }
            ColumnType::IpAddr => mutate_or_create_column(
                &mut self.ip_addr_field_hash_map,
                column_name,
                |column_opt: Option<ColumnWriter>| column_opt.unwrap_or_default(),
            ),
        }
    }

    pub fn force_numerical_type(&mut self, column_name: &str, numerical_type: NumericalType) {
        mutate_or_create_column(
            &mut self.numerical_field_hash_map,
            column_name,
            |column_opt: Option<NumericalColumnWriter>| {
                let mut column: NumericalColumnWriter = column_opt.unwrap_or_default();
                column.force_numerical_type(numerical_type);
                column
            },
        );
    }

    pub fn record_numerical<T: Into<NumericalValue> + Copy>(
        &mut self,
        doc: RowId,
        column_name: &str,
        numerical_value: T,
    ) {
        let (hash_map, arena) = (&mut self.numerical_field_hash_map, &mut self.arena);
        mutate_or_create_column(
            hash_map,
            column_name,
            |column_opt: Option<NumericalColumnWriter>| {
                let mut column: NumericalColumnWriter = column_opt.unwrap_or_default();
                column.record_numerical_value(doc, numerical_value.into(), arena);
                column
            },
        );
    }

    pub fn record_ip_addr(&mut self, doc: RowId, column_name: &str, ip_addr: Ipv6Addr) {
        assert!(
            !column_name.as_bytes().contains(&0u8),
            "key may not contain the 0 byte"
        );
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
        mutate_or_create_column(hash_map, column_name, |column_opt: Option<ColumnWriter>| {
            let mut column: ColumnWriter = column_opt.unwrap_or_default();
            column.record(doc, val, arena);
            column
        });
    }

    pub fn record_datetime(&mut self, doc: RowId, column_name: &str, datetime: crate::DateTime) {
        let (hash_map, arena) = (&mut self.datetime_field_hash_map, &mut self.arena);
        mutate_or_create_column(hash_map, column_name, |column_opt: Option<ColumnWriter>| {
            let mut column: ColumnWriter = column_opt.unwrap_or_default();
            column.record(doc, NumericalValue::I64(datetime.timestamp_micros), arena);
            column
        });
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
        assert!(
            !column_name.as_bytes().contains(&0u8),
            "key may not contain the 0 byte"
        );
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
    pub fn serialize(&mut self, num_docs: RowId, wrt: &mut dyn io::Write) -> io::Result<()> {
        let mut serializer = ColumnarSerializer::new(wrt);
        let mut columns: Vec<(&[u8], ColumnType, Addr)> = self
            .numerical_field_hash_map
            .iter()
            .map(|(column_name, addr, _)| {
                let numerical_column_writer: NumericalColumnWriter =
                    self.numerical_field_hash_map.read(addr);
                let column_type = numerical_column_writer.numerical_type().into();
                (column_name, column_type, addr)
            })
            .collect();
        columns.extend(
            self.bytes_field_hash_map
                .iter()
                .map(|(term, addr, _)| (term, ColumnType::Bytes, addr)),
        );
        columns.extend(
            self.str_field_hash_map
                .iter()
                .map(|(column_name, addr, _)| (column_name, ColumnType::Str, addr)),
        );
        columns.extend(
            self.bool_field_hash_map
                .iter()
                .map(|(column_name, addr, _)| (column_name, ColumnType::Bool, addr)),
        );
        columns.extend(
            self.ip_addr_field_hash_map
                .iter()
                .map(|(column_name, addr, _)| (column_name, ColumnType::IpAddr, addr)),
        );
        columns.extend(
            self.datetime_field_hash_map
                .iter()
                .map(|(column_name, addr, _)| (column_name, ColumnType::DateTime, addr)),
        );
        columns.sort_unstable_by_key(|(column_name, col_type, _)| (*column_name, *col_type));

        let (arena, buffers, dictionaries) = (&self.arena, &mut self.buffers, &self.dictionaries);
        let mut symbol_byte_buffer: Vec<u8> = Vec::new();
        for (column_name, column_type, addr) in columns {
            match column_type {
                ColumnType::Bool | ColumnType::DateTime => {
                    let column_writer: ColumnWriter = if column_type == ColumnType::Bool {
                        self.bool_field_hash_map.read(addr)
                    } else {
                        self.datetime_field_hash_map.read(addr)
                    };
                    let cardinality = column_writer.get_cardinality(num_docs);
                    let mut column_serializer =
                        serializer.serialize_column(column_name, ColumnType::Bool);
                    serialize_bool_column(
                        cardinality,
                        num_docs,
                        column_writer.operation_iterator(arena, &mut symbol_byte_buffer),
                        buffers,
                        &mut column_serializer,
                    )?;
                }
                ColumnType::IpAddr => {
                    let column_writer: ColumnWriter = self.ip_addr_field_hash_map.read(addr);
                    let cardinality = column_writer.get_cardinality(num_docs);
                    let mut column_serializer =
                        serializer.serialize_column(column_name, ColumnType::IpAddr);
                    serialize_ip_addr_column(
                        cardinality,
                        num_docs,
                        column_writer.operation_iterator(arena, &mut symbol_byte_buffer),
                        buffers,
                        &mut column_serializer,
                    )?;
                }
                ColumnType::Bytes | ColumnType::Str => {
                    let str_or_bytes_column_writer: StrOrBytesColumnWriter =
                        if column_type == ColumnType::Bytes {
                            self.bytes_field_hash_map.read(addr)
                        } else {
                            self.str_field_hash_map.read(addr)
                        };
                    let dictionary_builder =
                        &dictionaries[str_or_bytes_column_writer.dictionary_id as usize];
                    let cardinality = str_or_bytes_column_writer
                        .column_writer
                        .get_cardinality(num_docs);
                    let mut column_serializer =
                        serializer.serialize_column(column_name, column_type);
                    serialize_bytes_or_str_column(
                        cardinality,
                        num_docs,
                        dictionary_builder,
                        str_or_bytes_column_writer
                            .operation_iterator(arena, &mut symbol_byte_buffer),
                        buffers,
                        &mut column_serializer,
                    )?;
                }
                ColumnType::I64 | ColumnType::F64 | ColumnType::U64 => {
                    let numerical_column_writer: NumericalColumnWriter =
                        self.numerical_field_hash_map.read(addr);
                    let numerical_type = column_type.numerical_type().unwrap();
                    let cardinality = numerical_column_writer.cardinality(num_docs);
                    let mut column_serializer =
                        serializer.serialize_column(column_name, ColumnType::from(numerical_type));
                    serialize_numerical_column(
                        cardinality,
                        num_docs,
                        numerical_type,
                        numerical_column_writer.operation_iterator(arena, &mut symbol_byte_buffer),
                        buffers,
                        &mut column_serializer,
                    )?;
                }
            };
        }
        serializer.finalize()?;
        Ok(())
    }
}

fn serialize_bytes_or_str_column(
    cardinality: Cardinality,
    num_docs: RowId,
    dictionary_builder: &DictionaryBuilder,
    operation_it: impl Iterator<Item = ColumnOperation<UnorderedId>>,
    buffers: &mut SpareBuffers,
    wrt: impl io::Write,
) -> io::Result<()> {
    let SpareBuffers {
        value_index_builders,
        u64_values,
        ..
    } = buffers;
    let mut counting_writer = CountingWriter::wrap(wrt);
    let term_id_mapping: TermIdMapping = dictionary_builder.serialize(&mut counting_writer)?;
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
        value_index_builders,
        u64_values,
        &mut wrt,
    )?;
    wrt.write_all(&dictionary_num_bytes.to_le_bytes()[..])?;
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
        i64_values,
        f64_values,
        ..
    } = buffers;
    match numerical_type {
        NumericalType::I64 => {
            send_to_serialize_column_mappable_to_u64(
                coerce_numerical_symbol::<i64>(op_iterator),
                cardinality,
                num_docs,
                value_index_builders,
                i64_values,
                wrt,
            )?;
        }
        NumericalType::U64 => {
            send_to_serialize_column_mappable_to_u64(
                coerce_numerical_symbol::<u64>(op_iterator),
                cardinality,
                num_docs,
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
                value_index_builders,
                f64_values,
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
        bool_values,
        ..
    } = buffers;
    send_to_serialize_column_mappable_to_u64(
        column_operations_it,
        cardinality,
        num_docs,
        value_index_builders,
        bool_values,
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
    T: Copy + std::fmt::Debug + Send + Sync + MonotonicallyMappableToU128 + PartialOrd,
>(
    op_iterator: impl Iterator<Item = ColumnOperation<T>>,
    cardinality: Cardinality,
    num_docs: RowId,
    value_index_builders: &mut PreallocatedIndexBuilders,
    values: &mut Vec<T>,
    mut wrt: impl io::Write,
) -> io::Result<()>
where
    for<'a> VecColumn<'a, T>: ColumnValues<T>,
{
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
            let optional_index = optional_index_builder.finish(num_docs);
            SerializableColumnIndex::Optional(Box::new(optional_index))
        }
        Cardinality::Multivalued => {
            let multivalued_index_builder = value_index_builders.borrow_multivalued_index_builder();
            consume_operation_iterator(op_iterator, multivalued_index_builder, values);
            let multivalued_index = multivalued_index_builder.finish(num_docs);
            SerializableColumnIndex::Multivalued(Box::new(multivalued_index))
        }
    };
    crate::column::serialize_column_mappable_to_u128(
        serializable_column_index,
        || values.iter().cloned(),
        values.len() as u32,
        &mut wrt,
    )?;
    Ok(())
}

fn send_to_serialize_column_mappable_to_u64<
    T: Copy + Default + std::fmt::Debug + Send + Sync + MonotonicallyMappableToU64 + PartialOrd,
>(
    op_iterator: impl Iterator<Item = ColumnOperation<T>>,
    cardinality: Cardinality,
    num_docs: RowId,
    value_index_builders: &mut PreallocatedIndexBuilders,
    values: &mut Vec<T>,
    mut wrt: impl io::Write,
) -> io::Result<()>
where
    for<'a> VecColumn<'a, T>: ColumnValues<T>,
{
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
            let optional_index = optional_index_builder.finish(num_docs);
            SerializableColumnIndex::Optional(Box::new(optional_index))
        }
        Cardinality::Multivalued => {
            let multivalued_index_builder = value_index_builders.borrow_multivalued_index_builder();
            consume_operation_iterator(op_iterator, multivalued_index_builder, values);
            let multivalued_index = multivalued_index_builder.finish(num_docs);
            SerializableColumnIndex::Multivalued(Box::new(multivalued_index))
        }
    };
    crate::column::serialize_column_mappable_to_u64(
        serializable_column_index,
        &VecColumn::from(&values[..]),
        &mut wrt,
    )?;
    Ok(())
}

fn coerce_numerical_symbol<T>(
    operation_iterator: impl Iterator<Item = ColumnOperation<NumericalValue>>,
) -> impl Iterator<Item = ColumnOperation<T>>
where T: Coerce {
    operation_iterator.map(|symbol| match symbol {
        ColumnOperation::NewDoc(doc) => ColumnOperation::NewDoc(doc),
        ColumnOperation::Value(numerical_value) => {
            ColumnOperation::Value(Coerce::coerce(numerical_value))
        }
    })
}

fn consume_operation_iterator<T: std::fmt::Debug, TIndexBuilder: IndexBuilder>(
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
            .operation_iterator(&mut arena, &mut buffer)
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
            .operation_iterator(&mut arena, &mut buffer)
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
            .operation_iterator(&mut arena, &mut buffer)
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
            .operation_iterator(&mut arena, &mut buffer)
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

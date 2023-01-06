mod column_operation;
mod column_writers;
mod serializer;
mod value_index;

use std::io::{self, Write};

use column_operation::ColumnOperation;
use fastfield_codecs::serialize::ValueIndexInfo;
use fastfield_codecs::{Column, MonotonicallyMappableToU64, VecColumn};
use serializer::ColumnarSerializer;
use stacker::{Addr, ArenaHashMap, MemoryArena};

use crate::column_type_header::{ColumnType, ColumnTypeAndCardinality, GeneralType};
use crate::dictionary::{DictionaryBuilder, IdMapping, UnorderedId};
use crate::value::{Coerce, NumericalType, NumericalValue};
use crate::writer::column_writers::{ColumnWriter, NumericalColumnWriter, StrColumnWriter};
use crate::writer::value_index::{IndexBuilder, SpareIndexBuilders};
use crate::{Cardinality, DocId};

/// Threshold above which a column data will be compressed
/// using ZSTD.
const COLUMN_COMPRESSION_THRESHOLD: usize = 100_000;

/// This is a set of buffers that are only here
/// to limit the amount of allocation.
#[derive(Default)]
struct SpareBuffers {
    value_index_builders: SpareIndexBuilders,
    i64_values: Vec<i64>,
    u64_values: Vec<u64>,
    f64_values: Vec<f64>,
    bool_values: Vec<bool>,
    column_buffer: Vec<u8>,
}

pub struct ColumnarWriter {
    numerical_field_hash_map: ArenaHashMap,
    bool_field_hash_map: ArenaHashMap,
    bytes_field_hash_map: ArenaHashMap,
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
            bytes_field_hash_map: ArenaHashMap::new(10_000),
            dictionaries: Vec::new(),
            arena: MemoryArena::default(),
            buffers: SpareBuffers::default(),
        }
    }
}

impl ColumnarWriter {
    pub fn record_numerical(
        &mut self,
        doc: DocId,
        column_name: &str,
        numerical_value: NumericalValue,
    ) {
        assert!(
            !column_name.as_bytes().contains(&0u8),
            "key may not contain the 0 byte"
        );
        let (hash_map, arena) = (&mut self.numerical_field_hash_map, &mut self.arena);
        hash_map.mutate_or_create(
            column_name.as_bytes(),
            |column_opt: Option<NumericalColumnWriter>| {
                let mut column: NumericalColumnWriter = column_opt.unwrap_or_default();
                column.record_numerical_value(doc, numerical_value, arena);
                column
            },
        );
    }

    pub fn record_bool(&mut self, doc: DocId, column_name: &str, val: bool) {
        assert!(
            !column_name.as_bytes().contains(&0u8),
            "key may not contain the 0 byte"
        );
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

    pub fn record_str(&mut self, doc: DocId, column_name: &str, value: &[u8]) {
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
            |column_opt: Option<StrColumnWriter>| {
                let mut column: StrColumnWriter = column_opt.unwrap_or_else(|| {
                    let dictionary_id = dictionaries.len() as u32;
                    dictionaries.push(DictionaryBuilder::default());
                    StrColumnWriter::with_dictionary_id(dictionary_id)
                });
                column.record_bytes(doc, value, dictionaries, arena);
                column
            },
        );
    }

    pub fn serialize(&mut self, num_docs: DocId, wrt: &mut dyn io::Write) -> io::Result<()> {
        let mut serializer = ColumnarSerializer::new(wrt);
        let mut field_columns: Vec<(&[u8], GeneralType, Addr)> = self
            .numerical_field_hash_map
            .iter()
            .map(|(term, addr, _)| (term, GeneralType::Numerical, addr))
            .collect();
        field_columns.extend(
            self.bytes_field_hash_map
                .iter()
                .map(|(term, addr, _)| (term, GeneralType::Str, addr)),
        );
        field_columns.extend(
            self.bool_field_hash_map
                .iter()
                .map(|(term, addr, _)| (term, GeneralType::Bool, addr)),
        );
        field_columns.sort_unstable_by_key(|(column_name, col_type, _)| (*column_name, *col_type));
        let (arena, buffers, dictionaries) = (&self.arena, &mut self.buffers, &self.dictionaries);
        let mut symbol_byte_buffer: Vec<u8> = Vec::new();
        for (column_name, bytes_or_numerical, addr) in field_columns {
            match bytes_or_numerical {
                GeneralType::Bool => {
                    let column_writer: ColumnWriter = self.bool_field_hash_map.read(addr);
                    let cardinality = column_writer.get_cardinality(num_docs);
                    let column_type_and_cardinality = ColumnTypeAndCardinality {
                        cardinality,
                        typ: ColumnType::Bool,
                    };
                    let column_serializer =
                        serializer.serialize_column(column_name, column_type_and_cardinality);
                    serialize_bool_column(
                        cardinality,
                        num_docs,
                        column_writer.operation_iterator(arena, &mut symbol_byte_buffer),
                        buffers,
                        column_serializer,
                    )?;
                }
                GeneralType::Str => {
                    let str_column_writer: StrColumnWriter = self.bytes_field_hash_map.read(addr);
                    let dictionary_builder =
                        &dictionaries[str_column_writer.dictionary_id as usize];
                    let cardinality = str_column_writer.column_writer.get_cardinality(num_docs);
                    let column_type_and_cardinality = ColumnTypeAndCardinality {
                        cardinality,
                        typ: ColumnType::Bytes,
                    };
                    let column_serializer =
                        serializer.serialize_column(column_name, column_type_and_cardinality);
                    serialize_bytes_column(
                        cardinality,
                        num_docs,
                        dictionary_builder,
                        str_column_writer.operation_iterator(arena, &mut symbol_byte_buffer),
                        buffers,
                        column_serializer,
                    )?;
                }
                GeneralType::Numerical => {
                    let numerical_column_writer: NumericalColumnWriter =
                        self.numerical_field_hash_map.read(addr);
                    let (numerical_type, cardinality) =
                        numerical_column_writer.column_type_and_cardinality(num_docs);
                    let column_type_and_cardinality = ColumnTypeAndCardinality {
                        cardinality,
                        typ: ColumnType::Numerical(numerical_type),
                    };
                    let column_serializer =
                        serializer.serialize_column(column_name, column_type_and_cardinality);
                    serialize_numerical_column(
                        cardinality,
                        num_docs,
                        numerical_type,
                        numerical_column_writer.operation_iterator(arena, &mut symbol_byte_buffer),
                        buffers,
                        column_serializer,
                    )?;
                }
            };
        }
        serializer.finalize()?;
        Ok(())
    }
}

fn compress_and_write_column<W: io::Write>(column_bytes: &[u8], wrt: &mut W) -> io::Result<()> {
    if column_bytes.len() >= COLUMN_COMPRESSION_THRESHOLD {
        wrt.write_all(&[1])?;
        let mut encoder = zstd::Encoder::new(wrt, 3)?;
        encoder.write_all(column_bytes)?;
        encoder.finish()?;
    } else {
        wrt.write_all(&[0])?;
        wrt.write_all(column_bytes)?;
    }
    Ok(())
}

fn serialize_bytes_column<W: io::Write>(
    cardinality: Cardinality,
    num_docs: DocId,
    dictionary_builder: &DictionaryBuilder,
    operation_it: impl Iterator<Item = ColumnOperation<UnorderedId>>,
    buffers: &mut SpareBuffers,
    mut wrt: W,
) -> io::Result<()> {
    let SpareBuffers {
        value_index_builders,
        u64_values,
        column_buffer,
        ..
    } = buffers;
    column_buffer.clear();
    let id_mapping: IdMapping = dictionary_builder.serialize(column_buffer)?;
    let dictionary_num_bytes: u32 = column_buffer.len() as u32;
    let operation_iterator = operation_it.map(|symbol: ColumnOperation<UnorderedId>| {
        // We map unordered ids to ordered ids.
        match symbol {
            ColumnOperation::Value(unordered_id) => {
                let ordered_id = id_mapping.to_ord(unordered_id);
                ColumnOperation::Value(ordered_id.0 as u64)
            }
            ColumnOperation::NewDoc(doc) => ColumnOperation::NewDoc(doc),
        }
    });
    serialize_column(
        operation_iterator,
        cardinality,
        num_docs,
        value_index_builders,
        u64_values,
        column_buffer,
    )?;
    column_buffer.write_all(&dictionary_num_bytes.to_le_bytes()[..])?;
    compress_and_write_column(column_buffer, &mut wrt)?;
    Ok(())
}

fn serialize_numerical_column<W: io::Write>(
    cardinality: Cardinality,
    num_docs: DocId,
    numerical_type: NumericalType,
    op_iterator: impl Iterator<Item = ColumnOperation<NumericalValue>>,
    buffers: &mut SpareBuffers,
    mut wrt: W,
) -> io::Result<()> {
    let SpareBuffers {
        value_index_builders,
        u64_values,
        i64_values,
        f64_values,
        column_buffer,
        ..
    } = buffers;
    column_buffer.clear();
    match numerical_type {
        NumericalType::I64 => {
            serialize_column(
                coerce_numerical_symbol::<i64>(op_iterator),
                cardinality,
                num_docs,
                value_index_builders,
                i64_values,
                column_buffer,
            )?;
        }
        NumericalType::U64 => {
            serialize_column(
                coerce_numerical_symbol::<u64>(op_iterator),
                cardinality,
                num_docs,
                value_index_builders,
                u64_values,
                column_buffer,
            )?;
        }
        NumericalType::F64 => {
            serialize_column(
                coerce_numerical_symbol::<f64>(op_iterator),
                cardinality,
                num_docs,
                value_index_builders,
                f64_values,
                column_buffer,
            )?;
        }
    };
    compress_and_write_column(column_buffer, &mut wrt)?;
    Ok(())
}

fn serialize_bool_column<W: io::Write>(
    cardinality: Cardinality,
    num_docs: DocId,
    column_operations_it: impl Iterator<Item = ColumnOperation<bool>>,
    buffers: &mut SpareBuffers,
    mut wrt: W,
) -> io::Result<()> {
    let SpareBuffers {
        value_index_builders,
        bool_values,
        column_buffer,
        ..
    } = buffers;
    column_buffer.clear();
    serialize_column(
        column_operations_it,
        cardinality,
        num_docs,
        value_index_builders,
        bool_values,
        column_buffer,
    )?;
    compress_and_write_column(column_buffer, &mut wrt)?;
    Ok(())
}

fn serialize_column<
    T: Copy + Default + std::fmt::Debug + Send + Sync + MonotonicallyMappableToU64 + PartialOrd,
>(
    op_iterator: impl Iterator<Item = ColumnOperation<T>>,
    cardinality: Cardinality,
    num_docs: DocId,
    value_index_builders: &mut SpareIndexBuilders,
    values: &mut Vec<T>,
    wrt: &mut Vec<u8>,
) -> io::Result<()>
where
    for<'a> VecColumn<'a, T>: Column<T>,
{
    values.clear();
    match cardinality {
        Cardinality::Required => {
            consume_operation_iterator(
                op_iterator,
                value_index_builders.borrow_required_index_builder(),
                values,
            );
            fastfield_codecs::serialize(
                VecColumn::from(&values[..]),
                wrt,
                &fastfield_codecs::ALL_CODEC_TYPES[..],
            )?;
        }
        Cardinality::Optional => {
            let optional_index_builder = value_index_builders.borrow_optional_index_builder();
            consume_operation_iterator(op_iterator, optional_index_builder, values);
            let optional_index = optional_index_builder.finish(num_docs);
            fastfield_codecs::serialize::serialize_new(
                ValueIndexInfo::SingleValue(Box::new(optional_index)),
                VecColumn::from(&values[..]),
                wrt,
                &fastfield_codecs::ALL_CODEC_TYPES[..],
            )?;
        }
        Cardinality::Multivalued => {
            let multivalued_index_builder = value_index_builders.borrow_multivalued_index_builder();
            consume_operation_iterator(op_iterator, multivalued_index_builder, values);
            let multivalued_index = multivalued_index_builder.finish(num_docs);
            fastfield_codecs::serialize::serialize_new(
                ValueIndexInfo::MultiValue(Box::new(multivalued_index)),
                VecColumn::from(&values[..]),
                wrt,
                &fastfield_codecs::ALL_CODEC_TYPES[..],
            )?;
        }
    }
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
                index_builder.record_doc(doc);
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
    use column_operation::ColumnOperation;
    use stacker::MemoryArena;

    use super::*;
    use crate::value::NumericalValue;
    use crate::Cardinality;

    #[test]
    fn test_column_writer_required_simple() {
        let mut arena = MemoryArena::default();
        let mut column_writer = super::ColumnWriter::default();
        column_writer.record(0u32, NumericalValue::from(14i64), &mut arena);
        column_writer.record(1u32, NumericalValue::from(15i64), &mut arena);
        column_writer.record(2u32, NumericalValue::from(-16i64), &mut arena);
        assert_eq!(column_writer.get_cardinality(3), Cardinality::Required);
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

mod column_operation;
mod value_index;

use std::io::{self, Write};

use column_operation::ColumnOperation;
use common::CountingWriter;
use fastfield_codecs::serialize::ValueIndexInfo;
use fastfield_codecs::{Column, MonotonicallyMappableToU64, VecColumn};
use ordered_float::NotNan;
use stacker::{Addr, ArenaHashMap, ExpUnrolledLinkedList, MemoryArena};

use crate::column_type_header::{ColumnType, ColumnTypeAndCardinality};
use crate::dictionary::{DictionaryBuilder, IdMapping, UnorderedId};
use crate::value::{Coerce, NumericalType, NumericalValue};
use crate::writer::column_operation::SymbolValue;
use crate::writer::value_index::{IndexBuilder, SpareIndexBuilders};
use crate::{Cardinality, ColumnarSerializer, DocId};

#[derive(Copy, Clone, Default)]
struct ColumnWriter {
    // Detected cardinality of the column so far.
    cardinality: Cardinality,
    // Last document inserted.
    // None if no doc has been added yet.
    last_doc_opt: Option<u32>,
    // Buffer containing the serialized values.
    values: ExpUnrolledLinkedList,
}

#[derive(Clone, Copy, Default)]
pub struct NumericalColumnWriter {
    compatible_numerical_types: CompatibleNumericalTypes,
    column_writer: ColumnWriter,
}

#[derive(Clone, Copy)]
struct CompatibleNumericalTypes {
    all_values_within_i64_range: bool,
    all_values_within_u64_range: bool,
}

impl Default for CompatibleNumericalTypes {
    fn default() -> CompatibleNumericalTypes {
        CompatibleNumericalTypes {
            all_values_within_i64_range: true,
            all_values_within_u64_range: true,
        }
    }
}

impl CompatibleNumericalTypes {
    pub fn accept_value(&mut self, numerical_value: NumericalValue) {
        match numerical_value {
            NumericalValue::I64(val_i64) => {
                let value_within_u64_range = val_i64 >= 0i64;
                self.all_values_within_u64_range &= value_within_u64_range;
            }
            NumericalValue::U64(val_u64) => {
                let value_within_i64_range = val_u64 < i64::MAX as u64;
                self.all_values_within_i64_range &= value_within_i64_range;
            }
            NumericalValue::F64(_) => {
                self.all_values_within_i64_range = false;
                self.all_values_within_u64_range = false;
            }
        }
    }

    pub fn to_numerical_type(self) -> NumericalType {
        if self.all_values_within_i64_range {
            NumericalType::I64
        } else if self.all_values_within_u64_range {
            NumericalType::U64
        } else {
            NumericalType::F64
        }
    }
}

impl NumericalColumnWriter {
    pub fn record_numerical_value(
        &mut self,
        doc: DocId,
        value: NumericalValue,
        arena: &mut MemoryArena,
    ) {
        self.compatible_numerical_types.accept_value(value);
        self.column_writer.record(doc, value, arena);
    }
}

impl ColumnWriter {
    fn symbol_iterator<'a, V: SymbolValue>(
        &self,
        arena: &MemoryArena,
        buffer: &'a mut Vec<u8>,
    ) -> impl Iterator<Item = ColumnOperation<V>> + 'a {
        buffer.clear();
        self.values.read_to_end(arena, buffer);
        let mut cursor: &[u8] = &buffer[..];
        std::iter::from_fn(move || {
            if cursor.is_empty() {
                return None;
            }
            let symbol = ColumnOperation::deserialize(&mut cursor)
                .expect("Failed to deserialize symbol from in-memory. This should never happen.");
            Some(symbol)
        })
    }

    fn delta_with_last_doc(&self, doc: DocId) -> u32 {
        self.last_doc_opt
            .map(|last_doc| doc - last_doc)
            .unwrap_or(doc + 1u32)
    }

    /// Records a change of the document being recorded.
    ///
    /// This function will also update the cardinality of the column
    /// if necessary.
    fn record(&mut self, doc: DocId, value: NumericalValue, arena: &mut MemoryArena) {
        // Difference between `doc` and the last doc.
        match self.delta_with_last_doc(doc) {
            0 => {
                // This is the last encounterred document.
                self.cardinality = Cardinality::Multivalued;
            }
            1 => {
                self.last_doc_opt = Some(doc);
                self.write_symbol::<NumericalValue>(ColumnOperation::NewDoc(doc), arena);
            }
            _ => {
                self.cardinality = self.cardinality.max(Cardinality::Optional);
                self.last_doc_opt = Some(doc);
                self.write_symbol::<NumericalValue>(ColumnOperation::NewDoc(doc), arena);
            }
        }
        self.write_symbol(ColumnOperation::Value(value), arena);
    }

    // Get the cardinality.
    // The overall number of docs in the column is necessary to
    // deal with the case where the all docs contain 1 value, except some documents
    // at the end of the column.
    fn get_cardinality(&self, num_docs: DocId) -> Cardinality {
        if self.delta_with_last_doc(num_docs) > 1 {
            self.cardinality.max(Cardinality::Optional)
        } else {
            self.cardinality
        }
    }

    fn write_symbol<V: SymbolValue>(
        &mut self,
        symbol: ColumnOperation<V>,
        arena: &mut MemoryArena,
    ) {
        self.values
            .writer(arena)
            .extend_from_slice(symbol.serialize().as_slice());
    }
}

#[derive(Copy, Clone, Default)]
pub struct BytesColumnWriter {
    dictionary_id: u32,
    column_writer: ColumnWriter,
}

impl BytesColumnWriter {
    pub fn with_dictionary_id(dictionary_id: u32) -> BytesColumnWriter {
        BytesColumnWriter {
            dictionary_id,
            column_writer: Default::default(),
        }
    }

    pub fn record_bytes(
        &mut self,
        doc: DocId,
        bytes: &[u8],
        dictionaries: &mut [DictionaryBuilder],
        arena: &mut MemoryArena,
    ) {
        let unordered_id = dictionaries[self.dictionary_id as usize].get_or_allocate_id(bytes);
        let numerical_value = NumericalValue::U64(unordered_id.0 as u64);
        self.column_writer.record(doc, numerical_value, arena);
    }
}

pub struct ColumnarWriter {
    numerical_field_hash_map: ArenaHashMap,
    bytes_field_hash_map: ArenaHashMap,
    arena: MemoryArena,
    // Dictionaries used to store dictionary-encoded values.
    dictionaries: Vec<DictionaryBuilder>,
    buffers: SpareBuffers,
}

#[derive(Default)]
struct SpareBuffers {
    byte_buffer: Vec<u8>,
    value_index_builders: SpareIndexBuilders,
    i64_values: Vec<i64>,
    u64_values: Vec<u64>,
    f64_values: Vec<ordered_float::NotNan<f64>>,
}

impl Default for ColumnarWriter {
    fn default() -> Self {
        ColumnarWriter {
            numerical_field_hash_map: ArenaHashMap::new(10_000),
            bytes_field_hash_map: ArenaHashMap::new(10_000),
            dictionaries: Vec::new(),
            arena: MemoryArena::default(),
            buffers: SpareBuffers::default(),
        }
    }
}

#[derive(Copy, Clone, Ord, PartialOrd, Eq, PartialEq, Debug)]
enum BytesOrNumerical {
    Bytes,
    Numerical,
}

impl ColumnarWriter {
    pub fn record_numerical(&mut self, doc: DocId, key: &[u8], numerical_value: NumericalValue) {
        let (hash_map, arena) = (&mut self.numerical_field_hash_map, &mut self.arena);
        hash_map.mutate_or_create(key, |column_opt: Option<NumericalColumnWriter>| {
            let mut column: NumericalColumnWriter = column_opt.unwrap_or_default();
            column.record_numerical_value(doc, numerical_value, arena);
            column
        });
    }

    pub fn record_bytes(&mut self, doc: DocId, key: &[u8], value: &[u8]) {
        let (hash_map, arena, dictionaries) = (
            &mut self.bytes_field_hash_map,
            &mut self.arena,
            &mut self.dictionaries,
        );
        hash_map.mutate_or_create(key, |column_opt: Option<BytesColumnWriter>| {
            let mut column: BytesColumnWriter = column_opt.unwrap_or_else(|| {
                let dictionary_id = dictionaries.len() as u32;
                dictionaries.push(DictionaryBuilder::default());
                BytesColumnWriter::with_dictionary_id(dictionary_id)
            });
            column.record_bytes(doc, value, dictionaries, arena);
            column
        });
    }

    pub fn serialize<W: io::Write>(
        &mut self,
        num_docs: DocId,
        mut serializer: ColumnarSerializer<W>,
    ) -> io::Result<()> {
        let mut field_columns: Vec<(&[u8], BytesOrNumerical, Addr)> = self
            .numerical_field_hash_map
            .iter()
            .map(|(term, addr, _)| (term, BytesOrNumerical::Numerical, addr))
            .collect();
        field_columns.extend(
            self.bytes_field_hash_map
                .iter()
                .map(|(term, addr, _)| (term, BytesOrNumerical::Bytes, addr)),
        );
        let mut key_buffer = Vec::new();
        field_columns.sort_unstable_by_key(|(key, col_type, _)| (*key, *col_type));
        let (arena, buffers, dictionaries) = (&self.arena, &mut self.buffers, &self.dictionaries);
        for (key, bytes_or_numerical, addr) in field_columns {
            let wrt = serializer.wrt();
            let start_offset = wrt.written_bytes();
            let column_type_and_cardinality: ColumnTypeAndCardinality =
                match bytes_or_numerical {
                BytesOrNumerical::Bytes => {
                    let BytesColumnWriter { dictionary_id, column_writer } =
                        self.bytes_field_hash_map.read(addr);
                    let dictionary_builder =
                        &dictionaries[dictionary_id as usize];
                    serialize_bytes_column(
                        &column_writer,
                        num_docs,
                        dictionary_builder,
                        arena,
                        buffers,
                        wrt,
                    )?;
                    ColumnTypeAndCardinality {
                        cardinality: column_writer.get_cardinality(num_docs),
                        typ: ColumnType::Bytes,
                    }
                }
                BytesOrNumerical::Numerical => {
                    let NumericalColumnWriter { compatible_numerical_types, column_writer  } =
                        self.numerical_field_hash_map.read(addr);
                    let cardinality = column_writer.get_cardinality(num_docs);
                    let numerical_type = compatible_numerical_types.to_numerical_type();
                    serialize_numerical_column(
                        cardinality,
                        numerical_type,
                        &column_writer,
                        num_docs,
                        arena,
                        buffers,
                        wrt,
                    )?;
                    ColumnTypeAndCardinality {
                        cardinality,
                        typ: ColumnType::Numerical(numerical_type),
                    }
                }
            };
            let end_offset = wrt.written_bytes();
            let key_with_type = prepare_key(key, column_type_and_cardinality, &mut key_buffer);
            serializer.record_column_offsets(key_with_type, start_offset..end_offset)?;
        }
        serializer.finalize()?;
        Ok(())
    }
}

/// Returns a key consisting of the concatenation of the key and the column_type_and_cardinality
/// code.
fn prepare_key<'a>(
    key: &[u8],
    column_type_cardinality: ColumnTypeAndCardinality,
    buffer: &'a mut Vec<u8>,
) -> &'a [u8] {
    buffer.clear();
    buffer.extend_from_slice(key);
    buffer.push(0u8);
    buffer.push(column_type_cardinality.to_code());
    &buffer[..]
}

fn serialize_bytes_column<W: io::Write>(
    column_writer: &ColumnWriter,
    num_docs: DocId,
    dictionary_builder: &DictionaryBuilder,
    arena: &MemoryArena,
    buffers: &mut SpareBuffers,
    wrt: &mut CountingWriter<W>,
) -> io::Result<()> {
    let start_offset = wrt.written_bytes();
    let id_mapping: IdMapping = dictionary_builder.serialize(wrt)?;
    let dictionary_num_bytes: u32 = (wrt.written_bytes() - start_offset) as u32;
    let cardinality = column_writer.get_cardinality(num_docs);
    let SpareBuffers {
        byte_buffer,
        value_index_builders,
        u64_values,
        ..
    } = buffers;
    let symbol_iterator = column_writer
        .symbol_iterator(arena, byte_buffer)
        .map(|symbol: ColumnOperation<UnorderedId>| {
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
        symbol_iterator,
        cardinality,
        num_docs,
        value_index_builders,
        u64_values,
        wrt,
    )?;
    wrt.write_all(&dictionary_num_bytes.to_le_bytes()[..])?;
    Ok(())
}

fn serialize_numerical_column<W: io::Write>(
    cardinality: Cardinality,
    numerical_type: NumericalType,
    column_writer: &ColumnWriter,
    num_docs: DocId,
    arena: &MemoryArena,
    buffers: &mut SpareBuffers,
    wrt: &mut W,
) -> io::Result<()> {
    let SpareBuffers {
        byte_buffer,
        value_index_builders,
        u64_values,
        i64_values,
        f64_values,
    } = buffers;
    let symbol_iterator = column_writer.symbol_iterator(arena, byte_buffer);
    match numerical_type {
        NumericalType::I64 => {
            serialize_column(
                coerce_numerical_symbol::<i64>(symbol_iterator),
                cardinality,
                num_docs,
                value_index_builders,
                i64_values,
                wrt,
            )?;
        }
        NumericalType::U64 => {
            serialize_column(
                coerce_numerical_symbol::<u64>(symbol_iterator),
                cardinality,
                num_docs,
                value_index_builders,
                u64_values,
                wrt,
            )?;
        }
        NumericalType::F64 => {
            serialize_column(
                coerce_numerical_symbol::<NotNan<f64>>(symbol_iterator),
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

fn serialize_column<
    T: Copy + Ord + Default + Send + Sync + MonotonicallyMappableToU64,
    W: io::Write,
>(
    symbol_iterator: impl Iterator<Item = ColumnOperation<T>>,
    cardinality: Cardinality,
    num_docs: DocId,
    value_index_builders: &mut SpareIndexBuilders,
    values: &mut Vec<T>,
    wrt: &mut W,
) -> io::Result<()>
where
    for<'a> VecColumn<'a, T>: Column<T>,
{
    match cardinality {
        Cardinality::Required => {
            consume_symbol_iterator(
                symbol_iterator,
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
            consume_symbol_iterator(symbol_iterator, optional_index_builder, values);
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
            consume_symbol_iterator(symbol_iterator, multivalued_index_builder, values);
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
    symbol_iterator: impl Iterator<Item = ColumnOperation<NumericalValue>>,
) -> impl Iterator<Item = ColumnOperation<T>>
where T: Coerce {
    symbol_iterator.map(|symbol| match symbol {
        ColumnOperation::NewDoc(doc) => ColumnOperation::NewDoc(doc),
        ColumnOperation::Value(numerical_value) => {
            ColumnOperation::Value(Coerce::coerce(numerical_value))
        }
    })
}

fn consume_symbol_iterator<T, TIndexBuilder: IndexBuilder>(
    symbol_iterator: impl Iterator<Item = ColumnOperation<T>>,
    index_builder: &mut TIndexBuilder,
    values: &mut Vec<T>,
) {
    for symbol in symbol_iterator {
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

    use ordered_float::NotNan;
    use stacker::MemoryArena;

    use super::prepare_key;
    use crate::column_type_header::{ColumnType, ColumnTypeAndCardinality};
    use crate::value::{NumericalType, NumericalValue};
    use crate::writer::column_operation::ColumnOperation;
    use crate::writer::CompatibleNumericalTypes;
    use crate::Cardinality;

    #[test]
    fn test_prepare_key_bytes() {
        let mut buffer: Vec<u8> = b"somegarbage".to_vec();
        let column_type_and_cardinality = ColumnTypeAndCardinality {
            typ: ColumnType::Bytes,
            cardinality: Cardinality::Optional,
        };
        let prepared_key = prepare_key(b"root\0child", column_type_and_cardinality, &mut buffer);
        assert_eq!(prepared_key.len(), 12);
        assert_eq!(&prepared_key[..10], b"root\0child");
        assert_eq!(prepared_key[10], 0u8);
        assert_eq!(prepared_key[11], column_type_and_cardinality.to_code());
    }

    #[test]
    fn test_column_writer_required_simple() {
        let mut arena = MemoryArena::default();
        let mut column_writer = super::ColumnWriter::default();
        column_writer.record(0u32, 14i64.into(), &mut arena);
        column_writer.record(1u32, 15i64.into(), &mut arena);
        column_writer.record(2u32, (-16i64).into(), &mut arena);
        assert_eq!(column_writer.get_cardinality(3), Cardinality::Required);
        let mut buffer = Vec::new();
        let symbols: Vec<ColumnOperation<NumericalValue>> = column_writer
            .symbol_iterator(&mut arena, &mut buffer)
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
        column_writer.record(1u32, 15i64.into(), &mut arena);
        column_writer.record(2u32, (-16i64).into(), &mut arena);
        assert_eq!(column_writer.get_cardinality(3), Cardinality::Optional);
        let mut buffer = Vec::new();
        let symbols: Vec<ColumnOperation<NumericalValue>> = column_writer
            .symbol_iterator(&mut arena, &mut buffer)
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
        column_writer.record(0u32, 15i64.into(), &mut arena);
        assert_eq!(column_writer.get_cardinality(2), Cardinality::Optional);
        let mut buffer = Vec::new();
        let symbols: Vec<ColumnOperation<NumericalValue>> = column_writer
            .symbol_iterator(&mut arena, &mut buffer)
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
        column_writer.record(0u32, 16i64.into(), &mut arena);
        column_writer.record(0u32, 17i64.into(), &mut arena);
        assert_eq!(column_writer.get_cardinality(1), Cardinality::Multivalued);
        let mut buffer = Vec::new();
        let symbols: Vec<ColumnOperation<NumericalValue>> = column_writer
            .symbol_iterator(&mut arena, &mut buffer)
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

    #[track_caller]
    fn test_column_writer_coercion_iter_aux(
        values: impl Iterator<Item = NumericalValue>,
        expected_numerical_type: NumericalType,
    ) {
        let mut compatible_numerical_types = CompatibleNumericalTypes::default();
        for value in values {
            compatible_numerical_types.accept_value(value);
        }
        assert_eq!(
            compatible_numerical_types.to_numerical_type(),
            expected_numerical_type
        );
    }

    #[track_caller]
    fn test_column_writer_coercion_aux(
        values: &[NumericalValue],
        expected_numerical_type: NumericalType,
    ) {
        test_column_writer_coercion_iter_aux(values.iter().copied(), expected_numerical_type);
        test_column_writer_coercion_iter_aux(values.iter().rev().copied(), expected_numerical_type);
    }

    #[test]
    fn test_column_writer_coercion() {
        test_column_writer_coercion_aux(&[], NumericalType::I64);
        test_column_writer_coercion_aux(&[1i64.into()], NumericalType::I64);
        test_column_writer_coercion_aux(&[1u64.into()], NumericalType::I64);
        // We don't detect exact integer at the moment. We could!
        test_column_writer_coercion_aux(&[NotNan::new(1f64).unwrap().into()], NumericalType::F64);
        test_column_writer_coercion_aux(&[u64::MAX.into()], NumericalType::U64);
        test_column_writer_coercion_aux(&[(i64::MAX as u64).into()], NumericalType::U64);
        test_column_writer_coercion_aux(&[(1u64 << 63).into()], NumericalType::U64);
        test_column_writer_coercion_aux(&[1i64.into(), 1u64.into()], NumericalType::I64);
        test_column_writer_coercion_aux(&[u64::MAX.into(), (-1i64).into()], NumericalType::F64);
    }
}

//! Segment plugin writing the fast field columns as Vortex files.
//!
//! The plugin does not accumulate anything while indexing. Custom plugins are serialized (and
//! merged) after the built-in components, so the plugin reads the `.fast` columnar of the
//! segment back from disk and converts each supported column into a Vortex file.
//!
//! Hidden contract: this relies on the built-in fast field component being written before custom
//! plugins, both on `serialize` and on `merge` (see `SegmentPlugin` ordering documentation).

use std::any::Any;

use tantivy::columnar::{Cardinality, Column, ColumnType, ColumnarReader, DynamicColumnHandle};
use tantivy::index::SegmentComponent;
use tantivy::indexer::doc_id_mapping::DocIdMapping;
use tantivy::{
    u64_to_f64, u64_to_i64, PluginMergeContext, PluginWriter, PluginWriterContext, Segment,
    SegmentPlugin,
};
use vortex::array::arrays::{ListArray, PrimitiveArray, TemporalArray};
use vortex::array::validity::Validity;
use vortex::array::{ArrayRef, IntoArray};
use vortex::buffer::Buffer;
use vortex::extension::datetime::TimeUnit;
use vortex::file::WriteOptionsSessionExt;

use crate::format::{
    ColumnEntry, CompositeWriter, StoredCardinality, StoredType, VORTEX_EXTENSION,
};
use crate::{vortex_env, vortex_err_to_tantivy};

/// Separator used by tantivy in the columnar names of JSON fields.
const JSON_PATH_SEGMENT_SEP: char = '\u{1}';

/// Writes every non-JSON numeric, date and text fast field as a Vortex file.
#[derive(Default)]
pub struct VortexPlugin;

impl SegmentPlugin for VortexPlugin {
    fn extensions(&self) -> &[&str] {
        &[VORTEX_EXTENSION]
    }

    fn create_writer(
        &self,
        _ctx: &PluginWriterContext,
    ) -> tantivy::Result<Box<dyn PluginWriter>> {
        Ok(Box::new(VortexPluginWriter))
    }

    fn merge(&self, ctx: PluginMergeContext) -> tantivy::Result<()> {
        // The built-in fast field plugin has already written the merged columnar.
        write_vortex_component(ctx.target_segment)
    }
}

struct VortexPluginWriter;

impl PluginWriter for VortexPluginWriter {
    fn serialize(
        self: Box<Self>,
        segment: &Segment,
        // The `.fast` file we read is already in the final doc order.
        _doc_id_map: Option<&DocIdMapping>,
    ) -> tantivy::Result<()> {
        write_vortex_component(segment)
    }

    fn mem_usage(&self) -> usize {
        0
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

/// Returns the stored type for a columnar column type, or `None` if the column is not converted.
fn stored_type_for(column_type: ColumnType) -> Option<StoredType> {
    match column_type {
        ColumnType::U64 => Some(StoredType::U64),
        ColumnType::I64 => Some(StoredType::I64),
        ColumnType::F64 => Some(StoredType::F64),
        ColumnType::DateTime => Some(StoredType::DateTime),
        ColumnType::Str => Some(StoredType::Str),
        ColumnType::Bool | ColumnType::IpAddr | ColumnType::Bytes => None,
    }
}

/// Reads the `.fast` file of `segment` and writes the `.vortex` component.
fn write_vortex_component(segment: &Segment) -> tantivy::Result<()> {
    let fast_field_file = segment.open_read(SegmentComponent::FastFields)?;
    let columnar = ColumnarReader::open(fast_field_file)?;
    let mut composite_writer = CompositeWriter::default();
    for (column_name, handle) in columnar.list_columns()? {
        if column_name.contains(JSON_PATH_SEGMENT_SEP) {
            continue;
        }
        let Some(converted) = convert_column(&handle)? else {
            continue;
        };
        let vortex_file = write_vortex_file(converted.array)?;
        let entry = ColumnEntry {
            name: column_name,
            stored_type: converted.stored_type,
            cardinality: converted.cardinality,
            start: 0,
            end: 0,
            min_value: converted.min_value,
            max_value: converted.max_value,
            num_values: converted.num_values,
        };
        composite_writer.add(entry, &vortex_file);
    }
    let bytes = composite_writer.finish();
    let mut write = segment.open_write(SegmentComponent::Custom(VORTEX_EXTENSION.to_string()))?;
    std::io::Write::write_all(&mut write, &bytes)?;
    tantivy::directory::TerminatingWrite::terminate(write)?;
    Ok(())
}

/// A columnar column converted into a Vortex array.
pub struct ConvertedColumn {
    pub stored_type: StoredType,
    pub cardinality: StoredCardinality,
    pub array: ArrayRef,
    pub min_value: u64,
    pub max_value: u64,
    pub num_values: u64,
}

/// The values of a column, in their monotonic `u64` mapping, laid out by document.
pub struct DocValues {
    pub cardinality: StoredCardinality,
    /// - `Full` and `Optional`: one value per doc (0 for docs without a value).
    /// - `Multivalued`: the flattened values of all docs.
    pub values: Vec<u64>,
    /// `Optional` only: whether each doc has a value.
    pub present: Vec<bool>,
    /// `Multivalued` only: `num_docs + 1` offsets into `values`.
    pub offsets: Vec<u32>,
}

/// Reads all values of a column, by document.
pub fn read_doc_values(column: &Column<u64>) -> DocValues {
    let num_docs = column.num_docs();
    let cardinality = match column.get_cardinality() {
        Cardinality::Full => StoredCardinality::Full,
        Cardinality::Optional => StoredCardinality::Optional,
        Cardinality::Multivalued => StoredCardinality::Multivalued,
    };
    let mut doc_values = DocValues {
        cardinality,
        values: Vec::with_capacity(num_docs as usize),
        present: Vec::new(),
        offsets: Vec::new(),
    };
    match cardinality {
        StoredCardinality::Full => {
            for doc in 0..num_docs {
                doc_values.values.push(column.first(doc).unwrap_or(0u64));
            }
        }
        StoredCardinality::Optional => {
            doc_values.present.reserve(num_docs as usize);
            for doc in 0..num_docs {
                let value_opt = column.first(doc);
                doc_values.present.push(value_opt.is_some());
                doc_values.values.push(value_opt.unwrap_or(0u64));
            }
        }
        StoredCardinality::Multivalued => {
            doc_values.offsets.reserve(num_docs as usize + 1);
            doc_values.offsets.push(0u32);
            for doc in 0..num_docs {
                doc_values.values.extend(column.values_for_doc(doc));
                doc_values.offsets.push(doc_values.values.len() as u32);
            }
        }
    }
    doc_values
}

/// Converts a columnar column into a Vortex array, or returns `None` if its type is not
/// supported.
pub fn convert_column(handle: &DynamicColumnHandle) -> tantivy::Result<Option<ConvertedColumn>> {
    let Some(stored_type) = stored_type_for(handle.column_type()) else {
        return Ok(None);
    };
    // For text columns, this is the term ord column.
    let Some(column) = handle.open_u64_lenient()? else {
        return Ok(None);
    };
    let doc_values = read_doc_values(&column);
    Ok(Some(convert_doc_values(stored_type, &doc_values)))
}

/// Builds the Vortex array for `doc_values`.
pub fn convert_doc_values(stored_type: StoredType, doc_values: &DocValues) -> ConvertedColumn {
    let (min_value, max_value, num_values) = compute_bounds(doc_values);
    let validity = match doc_values.cardinality {
        StoredCardinality::Optional => Validity::from_iter(doc_values.present.iter().copied()),
        StoredCardinality::Full | StoredCardinality::Multivalued => Validity::NonNullable,
    };
    let values_validity = if doc_values.cardinality == StoredCardinality::Multivalued {
        Validity::NonNullable
    } else {
        validity
    };
    let values_array = build_primitive_array(stored_type, &doc_values.values, values_validity);
    let array = if doc_values.cardinality == StoredCardinality::Multivalued {
        let offsets = PrimitiveArray::new(
            Buffer::<u32>::from(doc_values.offsets.clone()),
            Validity::NonNullable,
        );
        ListArray::try_new(values_array, offsets.into_array(), Validity::NonNullable)
            .expect("valid list array")
            .into_array()
    } else {
        values_array
    };
    ConvertedColumn {
        stored_type,
        cardinality: doc_values.cardinality,
        array,
        min_value,
        max_value,
        num_values,
    }
}

/// Converts the monotonic `u64` mapped values back into their logical type.
fn build_primitive_array(stored_type: StoredType, values: &[u64], validity: Validity) -> ArrayRef {
    match stored_type {
        StoredType::U64 => {
            PrimitiveArray::new(Buffer::<u64>::from(values.to_vec()), validity).into_array()
        }
        StoredType::Str => {
            // Term ords always fit in a u32.
            let ords: Vec<u32> = values.iter().map(|&ord| ord as u32).collect();
            PrimitiveArray::new(Buffer::<u32>::from(ords), validity).into_array()
        }
        StoredType::I64 => {
            let values: Vec<i64> = values.iter().map(|&val| u64_to_i64(val)).collect();
            PrimitiveArray::new(Buffer::<i64>::from(values), validity).into_array()
        }
        StoredType::F64 => {
            let values: Vec<f64> = values.iter().map(|&val| u64_to_f64(val)).collect();
            PrimitiveArray::new(Buffer::<f64>::from(values), validity).into_array()
        }
        StoredType::DateTime => {
            // Tantivy maps dates using the i64 mapping of their timestamp in nanoseconds.
            let nanos: Vec<i64> = values.iter().map(|&val| u64_to_i64(val)).collect();
            let storage = PrimitiveArray::new(Buffer::<i64>::from(nanos), validity).into_array();
            TemporalArray::new_timestamp(storage, TimeUnit::Nanoseconds, None).into_array()
        }
    }
}

/// Returns `(min, max, num_values)` over the values that are actually present.
fn compute_bounds(doc_values: &DocValues) -> (u64, u64, u64) {
    let mut min_value = u64::MAX;
    let mut max_value = u64::MIN;
    let mut num_values = 0u64;
    for (i, &value) in doc_values.values.iter().enumerate() {
        if doc_values.cardinality == StoredCardinality::Optional && !doc_values.present[i] {
            continue;
        }
        min_value = min_value.min(value);
        max_value = max_value.max(value);
        num_values += 1;
    }
    if num_values == 0 {
        return (0, 0, 0);
    }
    (min_value, max_value, num_values)
}

/// Writes `array` as a standalone Vortex file, using the default write strategy
/// (BtrBlocks compression, 8192 rows zones with statistics).
pub fn write_vortex_file(array: ArrayRef) -> tantivy::Result<Vec<u8>> {
    let env = vortex_env();
    let mut bytes: Vec<u8> = Vec::new();
    env.session
        .write_options()
        .blocking(&env.runtime)
        .write(&mut bytes, array.to_array_iterator())
        .map_err(vortex_err_to_tantivy)?;
    Ok(bytes)
}

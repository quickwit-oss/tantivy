use std::sync::Arc;

use arrow::array::{
    ArrayRef, BinaryBuilder, BooleanBuilder, Float64Builder, Int64Builder, ListBuilder,
    StringBuilder, TimestampMicrosecondBuilder, UInt32Array, UInt64Builder,
};
use arrow::datatypes::{DataType, Field, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use datafusion::common::Result;
use datafusion::error::DataFusionError;
use tantivy::index::SegmentReader;

/// Reads fast fields from a single segment and produces an Arrow RecordBatch.
///
/// If `doc_ids` is `Some`, only those document IDs are read (already filtered by
/// a tantivy query). If `None`, all alive (non-deleted) documents are read.
/// When `limit` is `Some(n)`, at most `n` documents are returned.
/// Fields are read according to the projected Arrow schema.
pub fn read_segment_fast_fields_to_batch(
    segment_reader: &SegmentReader,
    projected_schema: &SchemaRef,
    doc_ids: Option<&[u32]>,
    limit: Option<usize>,
    segment_ord: u32,
) -> Result<RecordBatch> {
    let fast_fields = segment_reader.fast_fields();

    let docs: Vec<u32> = match doc_ids {
        Some(ids) => ids.to_vec(),
        None => {
            let max_doc = segment_reader.max_doc();
            let alive_bitset = segment_reader.alive_bitset();
            let iter = (0..max_doc).filter(|&doc_id| {
                alive_bitset
                    .map(|bs| bs.is_alive(doc_id))
                    .unwrap_or(true)
            });
            match limit {
                Some(lim) => iter.take(lim).collect(),
                None => iter.collect(),
            }
        }
    };

    let num_docs = docs.len();
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(projected_schema.fields().len());

    for field in projected_schema.fields() {
        let name = field.name();

        // Handle synthetic internal columns
        if name == "_doc_id" {
            let array: ArrayRef =
                Arc::new(UInt32Array::from(docs.iter().copied().collect::<Vec<_>>()));
            columns.push(array);
            continue;
        }
        if name == "_segment_ord" {
            let array: ArrayRef = Arc::new(UInt32Array::from(vec![segment_ord; num_docs]));
            columns.push(array);
            continue;
        }

        let array: ArrayRef = match field.data_type() {
            DataType::UInt64 => {
                let col = fast_fields
                    .u64(name)
                    .map_err(|e| DataFusionError::Internal(format!("fast field u64 '{name}': {e}")))?;
                let mut builder = UInt64Builder::with_capacity(num_docs);
                for &doc_id in &docs {
                    match col.first(doc_id) {
                        Some(v) => builder.append_value(v),
                        None => builder.append_null(),
                    }
                }
                Arc::new(builder.finish())
            }
            DataType::Int64 => {
                let col = fast_fields
                    .i64(name)
                    .map_err(|e| DataFusionError::Internal(format!("fast field i64 '{name}': {e}")))?;
                let mut builder = Int64Builder::with_capacity(num_docs);
                for &doc_id in &docs {
                    match col.first(doc_id) {
                        Some(v) => builder.append_value(v),
                        None => builder.append_null(),
                    }
                }
                Arc::new(builder.finish())
            }
            DataType::Float64 => {
                let col = fast_fields
                    .f64(name)
                    .map_err(|e| DataFusionError::Internal(format!("fast field f64 '{name}': {e}")))?;
                let mut builder = Float64Builder::with_capacity(num_docs);
                for &doc_id in &docs {
                    match col.first(doc_id) {
                        Some(v) => builder.append_value(v),
                        None => builder.append_null(),
                    }
                }
                Arc::new(builder.finish())
            }
            DataType::Boolean => {
                let col = fast_fields
                    .bool(name)
                    .map_err(|e| DataFusionError::Internal(format!("fast field bool '{name}': {e}")))?;
                let mut builder = BooleanBuilder::with_capacity(num_docs);
                for &doc_id in &docs {
                    match col.first(doc_id) {
                        Some(v) => builder.append_value(v),
                        None => builder.append_null(),
                    }
                }
                Arc::new(builder.finish())
            }
            DataType::Timestamp(TimeUnit::Microsecond, None) => {
                let col = fast_fields
                    .date(name)
                    .map_err(|e| DataFusionError::Internal(format!("fast field date '{name}': {e}")))?;
                let mut builder = TimestampMicrosecondBuilder::with_capacity(num_docs);
                for &doc_id in &docs {
                    match col.first(doc_id) {
                        Some(dt) => builder.append_value(dt.into_timestamp_micros()),
                        None => builder.append_null(),
                    }
                }
                Arc::new(builder.finish())
            }
            DataType::Utf8 => {
                // Could be a Str fast field or IpAddr formatted as string
                if let Ok(Some(str_col)) = fast_fields.str(name) {
                    let mut builder = StringBuilder::with_capacity(num_docs, num_docs * 32);
                    let mut buf = String::new();
                    for &doc_id in &docs {
                        let mut ord_iter = str_col.term_ords(doc_id);
                        if let Some(ord) = ord_iter.next() {
                            buf.clear();
                            str_col
                                .ord_to_str(ord, &mut buf)
                                .map_err(|e| DataFusionError::Internal(format!("ord_to_str '{name}': {e}")))?;
                            builder.append_value(&buf);
                        } else {
                            builder.append_null();
                        }
                    }
                    Arc::new(builder.finish())
                } else if let Ok(col) = fast_fields.ip_addr(name) {
                    // IpAddr stored as Ipv6Addr; prefer IPv4 representation when possible
                    let mut builder = StringBuilder::with_capacity(num_docs, num_docs * 40);
                    for &doc_id in &docs {
                        match col.first(doc_id) {
                            Some(ip) => {
                                if let Some(v4) = ip.to_ipv4_mapped() {
                                    builder.append_value(v4.to_string());
                                } else {
                                    builder.append_value(ip.to_string());
                                }
                            }
                            None => builder.append_null(),
                        }
                    }
                    Arc::new(builder.finish())
                } else {
                    return Err(DataFusionError::Internal(format!(
                        "No str or ip_addr fast field found for Utf8 field '{name}'"
                    )));
                }
            }
            DataType::Binary => {
                let bytes_col = fast_fields
                    .bytes(name)
                    .map_err(|e| DataFusionError::Internal(format!("fast field bytes '{name}': {e}")))?
                    .ok_or_else(|| {
                        DataFusionError::Internal(format!("bytes fast field '{name}' not found"))
                    })?;
                let mut builder = BinaryBuilder::with_capacity(num_docs, num_docs * 64);
                let mut buf = Vec::new();
                for &doc_id in &docs {
                    let mut ord_iter = bytes_col.term_ords(doc_id);
                    if let Some(ord) = ord_iter.next() {
                        buf.clear();
                        bytes_col
                            .ord_to_bytes(ord, &mut buf)
                            .map_err(|e| DataFusionError::Internal(format!("ord_to_bytes '{name}': {e}")))?;
                        builder.append_value(&buf);
                    } else {
                        builder.append_null();
                    }
                }
                Arc::new(builder.finish())
            }
            // --- List<T> branches for multi-valued fields ---
            DataType::List(inner) => {
                build_list_array(inner, name, fast_fields, &docs, num_docs)?
            }
            other => {
                return Err(DataFusionError::Internal(format!(
                    "Unsupported Arrow data type for fast field '{name}': {other:?}"
                )));
            }
        };
        columns.push(array);
    }

    RecordBatch::try_new(projected_schema.clone(), columns)
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
}

/// Build a `ListArray` for a multi-valued fast field, dispatching on the inner type.
fn build_list_array(
    inner_field: &Arc<Field>,
    name: &str,
    fast_fields: &tantivy::fastfield::FastFieldReaders,
    docs: &[u32],
    _num_docs: usize,
) -> Result<ArrayRef> {
    match inner_field.data_type() {
        DataType::UInt64 => {
            let col = fast_fields
                .u64(name)
                .map_err(|e| DataFusionError::Internal(format!("fast field u64 '{name}': {e}")))?;
            let mut builder = ListBuilder::new(UInt64Builder::new());
            for &doc_id in docs {
                for val in col.values_for_doc(doc_id) {
                    builder.values().append_value(val);
                }
                builder.append(true);
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Int64 => {
            let col = fast_fields
                .i64(name)
                .map_err(|e| DataFusionError::Internal(format!("fast field i64 '{name}': {e}")))?;
            let mut builder = ListBuilder::new(Int64Builder::new());
            for &doc_id in docs {
                for val in col.values_for_doc(doc_id) {
                    builder.values().append_value(val);
                }
                builder.append(true);
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Float64 => {
            let col = fast_fields
                .f64(name)
                .map_err(|e| DataFusionError::Internal(format!("fast field f64 '{name}': {e}")))?;
            let mut builder = ListBuilder::new(Float64Builder::new());
            for &doc_id in docs {
                for val in col.values_for_doc(doc_id) {
                    builder.values().append_value(val);
                }
                builder.append(true);
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Boolean => {
            let col = fast_fields
                .bool(name)
                .map_err(|e| DataFusionError::Internal(format!("fast field bool '{name}': {e}")))?;
            let mut builder = ListBuilder::new(BooleanBuilder::new());
            for &doc_id in docs {
                for val in col.values_for_doc(doc_id) {
                    builder.values().append_value(val);
                }
                builder.append(true);
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            let col = fast_fields
                .date(name)
                .map_err(|e| DataFusionError::Internal(format!("fast field date '{name}': {e}")))?;
            let mut builder = ListBuilder::new(TimestampMicrosecondBuilder::new());
            for &doc_id in docs {
                for val in col.values_for_doc(doc_id) {
                    builder.values().append_value(val.into_timestamp_micros());
                }
                builder.append(true);
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Utf8 => {
            // Could be a Str fast field or IpAddr formatted as string
            if let Ok(Some(str_col)) = fast_fields.str(name) {
                let mut builder = ListBuilder::new(StringBuilder::new());
                let mut buf = String::new();
                for &doc_id in docs {
                    for ord in str_col.term_ords(doc_id) {
                        buf.clear();
                        str_col.ord_to_str(ord, &mut buf).map_err(|e| {
                            DataFusionError::Internal(format!("ord_to_str '{name}': {e}"))
                        })?;
                        builder.values().append_value(&buf);
                    }
                    builder.append(true);
                }
                Ok(Arc::new(builder.finish()))
            } else if let Ok(col) = fast_fields.ip_addr(name) {
                let mut builder = ListBuilder::new(StringBuilder::new());
                for &doc_id in docs {
                    for val in col.values_for_doc(doc_id) {
                        if let Some(v4) = val.to_ipv4_mapped() {
                            builder.values().append_value(v4.to_string());
                        } else {
                            builder.values().append_value(val.to_string());
                        }
                    }
                    builder.append(true);
                }
                Ok(Arc::new(builder.finish()))
            } else {
                Err(DataFusionError::Internal(format!(
                    "No str or ip_addr fast field found for List<Utf8> field '{name}'"
                )))
            }
        }
        DataType::Binary => {
            let bytes_col = fast_fields
                .bytes(name)
                .map_err(|e| DataFusionError::Internal(format!("fast field bytes '{name}': {e}")))?
                .ok_or_else(|| {
                    DataFusionError::Internal(format!("bytes fast field '{name}' not found"))
                })?;
            let mut builder = ListBuilder::new(BinaryBuilder::new());
            let mut buf = Vec::new();
            for &doc_id in docs {
                for ord in bytes_col.term_ords(doc_id) {
                    buf.clear();
                    bytes_col.ord_to_bytes(ord, &mut buf).map_err(|e| {
                        DataFusionError::Internal(format!("ord_to_bytes '{name}': {e}"))
                    })?;
                    builder.values().append_value(&buf);
                }
                builder.append(true);
            }
            Ok(Arc::new(builder.finish()))
        }
        other => Err(DataFusionError::Internal(format!(
            "Unsupported inner type for List fast field '{name}': {other:?}"
        ))),
    }
}

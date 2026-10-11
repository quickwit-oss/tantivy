//! On-disk layout of the `.vortex` segment component.
//!
//! The component is a concatenation of standalone Vortex files, one per fast field column,
//! followed by a JSON footer describing them:
//!
//! ```text
//! [vortex file 0][padding][vortex file 1][padding]...[footer json][footer len: u64 LE]
//! ```
//!
//! Each Vortex file starts on a 64 bytes boundary.

use std::io;

use serde::{Deserialize, Serialize};
use tantivy::columnar::ColumnType;

/// Extension (and component name) of the vortex segment file.
pub const VORTEX_EXTENSION: &str = "vortex";

const FILE_ALIGNMENT: usize = 64;

/// Logical type of a stored column.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
pub enum StoredType {
    U64,
    I64,
    F64,
    /// Stored as a Vortex timestamp (nanoseconds) extension array.
    DateTime,
    /// Stored as the term ords (u32) of the tantivy fast field dictionary.
    Str,
}

impl StoredType {
    pub fn column_type(self) -> ColumnType {
        match self {
            StoredType::U64 => ColumnType::U64,
            StoredType::I64 => ColumnType::I64,
            StoredType::F64 => ColumnType::F64,
            StoredType::DateTime => ColumnType::DateTime,
            StoredType::Str => ColumnType::Str,
        }
    }
}

/// How documents map to values.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
pub enum StoredCardinality {
    /// One row per doc, non-nullable.
    Full,
    /// One row per doc, nullable.
    Optional,
    /// One row per doc, each row is a (possibly empty) list.
    Multivalued,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ColumnEntry {
    /// Columnar column name (the field name for non-JSON fields).
    pub name: String,
    pub stored_type: StoredType,
    pub cardinality: StoredCardinality,
    /// Byte range of the Vortex file within the component.
    pub start: u64,
    pub end: u64,
    /// Bounds of the values, in their monotonic `u64` mapping. Only meaningful if
    /// `num_values > 0`.
    pub min_value: u64,
    pub max_value: u64,
    pub num_values: u64,
}

#[derive(Default)]
pub struct CompositeWriter {
    buffer: Vec<u8>,
    entries: Vec<ColumnEntry>,
}

impl CompositeWriter {
    /// Appends a Vortex file. `entry.start` and `entry.end` are overwritten.
    pub fn add(&mut self, mut entry: ColumnEntry, vortex_file: &[u8]) {
        let padding = self.buffer.len().next_multiple_of(FILE_ALIGNMENT) - self.buffer.len();
        self.buffer.resize(self.buffer.len() + padding, 0u8);
        entry.start = self.buffer.len() as u64;
        self.buffer.extend_from_slice(vortex_file);
        entry.end = self.buffer.len() as u64;
        self.entries.push(entry);
    }

    pub fn finish(mut self) -> Vec<u8> {
        let footer = serde_json::to_vec(&self.entries).expect("footer serialization cannot fail");
        self.buffer.extend_from_slice(&footer);
        self.buffer
            .extend_from_slice(&(footer.len() as u64).to_le_bytes());
        self.buffer
    }
}

/// Reads the footer of a `.vortex` component.
pub fn read_footer(bytes: &[u8]) -> io::Result<Vec<ColumnEntry>> {
    let invalid = |msg: &str| io::Error::new(io::ErrorKind::InvalidData, msg.to_string());
    if bytes.len() < 8 {
        return Err(invalid("vortex component too short"));
    }
    let (body, footer_len_bytes) = bytes.split_at(bytes.len() - 8);
    let footer_len = u64::from_le_bytes(footer_len_bytes.try_into().unwrap()) as usize;
    if footer_len > body.len() {
        return Err(invalid("invalid vortex footer length"));
    }
    let footer = &body[body.len() - footer_len..];
    serde_json::from_slice(footer).map_err(|err| invalid(&err.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_composite_roundtrip() {
        let entry = |name: &str| ColumnEntry {
            name: name.to_string(),
            stored_type: StoredType::U64,
            cardinality: StoredCardinality::Full,
            start: 0,
            end: 0,
            min_value: 1,
            max_value: 2,
            num_values: 3,
        };
        let mut writer = CompositeWriter::default();
        writer.add(entry("a"), b"abc");
        writer.add(entry("b"), b"defg");
        let bytes = writer.finish();
        let entries = read_footer(&bytes).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(&bytes[entries[0].start as usize..entries[0].end as usize], b"abc");
        assert_eq!(entries[1].start % 64, 0);
        assert_eq!(&bytes[entries[1].start as usize..entries[1].end as usize], b"defg");
    }
}

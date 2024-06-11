use std::io;
use std::io::Write;

use common::{CountingWriter, OwnedBytes};

use crate::column_index::multivalued_index::serialize_multivalued_index;
use crate::column_index::optional_index::serialize_optional_index;
use crate::column_index::ColumnIndex;
use crate::column_values::ColumnStats;
use crate::iterable::Iterable;
use crate::{Cardinality, RowId};

pub enum SerializableColumnIndex<'a> {
    Full,
    Optional {
        non_null_row_ids: Box<dyn Iterable<RowId> + 'a>,
        num_rows: RowId,
    },
    Multivalued {
        /// Iterator emitting the indices for the index
        indices: Box<dyn Iterable<RowId> + 'a>,
        /// In the merge case we can precompute the column stats
        stats: Option<ColumnStats>,
    },
}

impl<'a> SerializableColumnIndex<'a> {
    pub fn get_cardinality(&self) -> Cardinality {
        match self {
            SerializableColumnIndex::Full => Cardinality::Full,
            SerializableColumnIndex::Optional { .. } => Cardinality::Optional,
            SerializableColumnIndex::Multivalued { .. } => Cardinality::Multivalued,
        }
    }
}

/// Serialize a column index.
pub fn serialize_column_index(
    column_index: SerializableColumnIndex,
    output: &mut impl Write,
) -> io::Result<u32> {
    let mut output = CountingWriter::wrap(output);
    let cardinality = column_index.get_cardinality().to_code();
    output.write_all(&[cardinality])?;
    match column_index {
        SerializableColumnIndex::Full => {}
        SerializableColumnIndex::Optional {
            non_null_row_ids,
            num_rows,
        } => serialize_optional_index(non_null_row_ids.as_ref(), num_rows, &mut output)?,
        SerializableColumnIndex::Multivalued {
            indices: multivalued_index,
            stats,
        } => serialize_multivalued_index(&*multivalued_index, stats, &mut output)?,
    }
    let column_index_num_bytes = output.written_bytes() as u32;
    Ok(column_index_num_bytes)
}

/// Open a serialized column index.
pub fn open_column_index(mut bytes: OwnedBytes) -> io::Result<ColumnIndex> {
    if bytes.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "Failed to deserialize column index. Empty buffer.",
        ));
    }
    let cardinality_code = bytes[0];
    let cardinality = Cardinality::try_from_code(cardinality_code)?;
    bytes.advance(1);
    match cardinality {
        Cardinality::Full => Ok(ColumnIndex::Full),
        Cardinality::Optional => {
            let optional_index = super::optional_index::open_optional_index(bytes)?;
            Ok(ColumnIndex::Optional(optional_index))
        }
        Cardinality::Multivalued => {
            let multivalue_index = super::multivalued_index::open_multivalued_index(bytes)?;
            Ok(ColumnIndex::Multivalued(multivalue_index))
        }
    }
}

// TODO unit tests

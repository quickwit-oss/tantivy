use std::io;
use std::io::Write;

use common::{CountingWriter, OwnedBytes};

use crate::column_index::multivalued_index::{serialize_multivalued_index, self};
use crate::column_index::optional_index::serialize_optional_index;
use crate::column_index::ColumnIndex;
use crate::iterable::Iterable;
use crate::{Cardinality, RowId};

pub enum SerializableColumnIndex<'a> {
    Full,
    Optional {
        non_null_row_ids: Box<dyn Fn() -> Box<dyn Iterator<Item=RowId> + 'a> + 'a>,
        num_rows: RowId,
    },
    // TODO remove the Arc<dyn> apart from serialization this is not
    // dynamic at all.
    Multivalued(&'a dyn Fn() -> Box<dyn Iterator<Item=RowId> + 'a>),
}

impl<'a> SerializableColumnIndex<'a> {
    pub fn get_cardinality(&self) -> Cardinality {
        match self {
            SerializableColumnIndex::Full => Cardinality::Full,
            SerializableColumnIndex::Optional { .. } => Cardinality::Optional,
            SerializableColumnIndex::Multivalued(_) => Cardinality::Multivalued,
        }
    }
}

pub fn serialize_column_index<'a>(
    column_index: SerializableColumnIndex<'a>,
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
        SerializableColumnIndex::Multivalued(multivalued_index) => {
            let multivalued_index_ref: &'a dyn Fn() -> Box<dyn Iterator<Item=RowId> + 'a> = multivalued_index.as_ref();
            serialize_multivalued_index(multivalued_index_ref, &mut output)?
        }
    }
    let column_index_num_bytes = output.written_bytes() as u32;
    Ok(column_index_num_bytes)
}

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

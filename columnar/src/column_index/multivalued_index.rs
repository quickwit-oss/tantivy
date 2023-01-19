use std::io;
use std::io::Write;
use std::sync::Arc;

use common::OwnedBytes;

use crate::column_values::{ColumnValues, FastFieldCodecType};
use crate::RowId;

#[derive(Clone)]
pub struct MultivaluedIndex(Arc<dyn ColumnValues<RowId>>);

pub fn serialize_multivalued_index(
    multivalued_index: &dyn ColumnValues<RowId>,
    output: &mut impl Write,
) -> io::Result<()> {
    crate::column_values::serialize_column_values(
        &*multivalued_index,
        &[FastFieldCodecType::Bitpacked, FastFieldCodecType::Linear],
        output,
    )?;
    Ok(())
}

pub fn open_multivalued_index(bytes: OwnedBytes) -> io::Result<Arc<dyn ColumnValues<RowId>>> {
    let start_index_column: Arc<dyn ColumnValues<RowId>> =
        crate::column_values::open_u64_mapped(bytes)?;
    Ok(start_index_column)
}

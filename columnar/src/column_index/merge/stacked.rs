use std::iter;

use crate::column_index::{SerializableColumnIndex, Set};
use crate::iterable::Iterable;
use crate::{Cardinality, ColumnIndex, RowId, StackMergeOrder};

/// Simple case:
/// The new mapping just consists in stacking the different column indexes.
///
/// There are no sort nor deletes involved.
pub fn merge_column_index_stacked<'a>(
    columns: &'a [ColumnIndex],
    cardinality_after_merge: Cardinality,
    stack_merge_order: &'a StackMergeOrder,
) -> SerializableColumnIndex<'a> {
    match cardinality_after_merge {
        Cardinality::Full => SerializableColumnIndex::Full,
        Cardinality::Optional => SerializableColumnIndex::Optional {
            non_null_row_ids: Box::new(StackedOptionalIndex {
                columns,
                stack_merge_order,
            }),
            num_rows: stack_merge_order.num_rows(),
        },
        Cardinality::Multivalued => {
            let stacked_multivalued_index = StackedMultivaluedIndex {
                columns,
                stack_merge_order,
            };
            SerializableColumnIndex::Multivalued(Box::new(stacked_multivalued_index))
        }
    }
}

struct StackedOptionalIndex<'a> {
    columns: &'a [ColumnIndex],
    stack_merge_order: &'a StackMergeOrder,
}

impl<'a> Iterable<RowId> for StackedOptionalIndex<'a> {
    fn boxed_iter(&self) -> Box<dyn Iterator<Item = RowId> + 'a> {
        Box::new(
            self.columns
                .iter()
                .enumerate()
                .flat_map(|(columnar_id, column_index_opt)| {
                    let columnar_row_range = self.stack_merge_order.columnar_range(columnar_id);
                    let rows_it: Box<dyn Iterator<Item = RowId>> = match column_index_opt {
                        ColumnIndex::Full => Box::new(columnar_row_range),
                        ColumnIndex::Optional(optional_index) => Box::new(
                            optional_index
                                .iter_rows()
                                .map(move |row_id: RowId| columnar_row_range.start + row_id),
                        ),
                        ColumnIndex::Multivalued(_) => {
                            panic!("No multivalued index is allowed when stacking column index");
                        }
                        ColumnIndex::Empty { .. } => Box::new(std::iter::empty()),
                    };
                    rows_it
                }),
        )
    }
}

#[derive(Clone, Copy)]
struct StackedMultivaluedIndex<'a> {
    columns: &'a [ColumnIndex],
    stack_merge_order: &'a StackMergeOrder,
}

fn convert_column_opt_to_multivalued_index<'a>(
    column_index_opt: &'a ColumnIndex,
    num_rows: RowId,
) -> Box<dyn Iterator<Item = RowId> + 'a> {
    match column_index_opt {
        ColumnIndex::Empty { .. } => Box::new(iter::repeat(0u32).take(num_rows as usize + 1)),
        ColumnIndex::Full => Box::new(0..num_rows + 1),
        ColumnIndex::Optional(optional_index) => {
            Box::new(
                (0..num_rows)
                    // TODO optimize
                    .map(|row_id| optional_index.rank(row_id))
                    .chain(std::iter::once(optional_index.num_non_nulls())),
            )
        }
        ColumnIndex::Multivalued(multivalued_index) => multivalued_index.start_index_column.iter(),
    }
}

impl<'a> Iterable<RowId> for StackedMultivaluedIndex<'a> {
    fn boxed_iter(&self) -> Box<dyn Iterator<Item = RowId> + '_> {
        let multivalued_indexes =
            self.columns
                .iter()
                .enumerate()
                .map(|(columnar_id, column_opt)| {
                    let num_rows =
                        self.stack_merge_order.columnar_range(columnar_id).len() as RowId;
                    convert_column_opt_to_multivalued_index(column_opt, num_rows)
                });
        stack_multivalued_indexes(multivalued_indexes)
    }
}

// Refactor me
fn stack_multivalued_indexes<'a>(
    mut multivalued_indexes: impl Iterator<Item = Box<dyn Iterator<Item = RowId> + 'a>> + 'a,
) -> Box<dyn Iterator<Item = RowId> + 'a> {
    let mut offset = 0;
    let mut last_row_id = 0;
    let mut current_it = multivalued_indexes.next();
    Box::new(std::iter::from_fn(move || loop {
        let Some(multivalued_index) = current_it.as_mut() else {
            return None;
        };
        if let Some(row_id) = multivalued_index.next() {
            last_row_id = offset + row_id;
            return Some(last_row_id);
        }
        offset = last_row_id;
        loop {
            current_it = multivalued_indexes.next();
            if current_it.as_mut()?.next().is_some() {
                break;
            }
        }
    }))
}

#[cfg(test)]
mod tests {
    use crate::RowId;

    fn it<'a>(row_ids: &'a [RowId]) -> Box<dyn Iterator<Item = RowId> + 'a> {
        Box::new(row_ids.iter().copied())
    }

    #[test]
    fn test_stack() {
        let columns = [
            it(&[0u32, 0u32]),
            it(&[0u32, 1u32, 1u32, 4u32]),
            it(&[0u32, 3u32, 5u32]),
            it(&[0u32, 4u32]),
        ]
        .into_iter();
        let start_offsets: Vec<RowId> = super::stack_multivalued_indexes(columns).collect();
        assert_eq!(start_offsets, &[0, 0, 1, 1, 4, 7, 9, 13]);
    }
}

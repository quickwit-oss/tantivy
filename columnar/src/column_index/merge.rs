use std::iter;

use crate::column_index::{
    multivalued_index, serialize_column_index, SerializableColumnIndex, Set,
};
use crate::iterable::Iterable;
use crate::{Cardinality, ColumnIndex, MergeRowOrder, RowId, StackMergeOrder};

fn detect_cardinality(columns: &[Option<ColumnIndex>]) -> Cardinality {
    columns
        .iter()
        .flatten()
        .map(ColumnIndex::get_cardinality)
        .max()
        .unwrap_or(Cardinality::Full)
}

pub fn stack_column_index<'a>(
    columns: &'a [Option<ColumnIndex>],
    merge_row_order: &'a MergeRowOrder,
) -> SerializableColumnIndex<'a> {
    let MergeRowOrder::Stack(stack_merge_order) = merge_row_order else {
        panic!("only supporting stacking at the moment.");
    };
    let cardinality = detect_cardinality(columns);
    match cardinality {
        Cardinality::Full => SerializableColumnIndex::Full,
        Cardinality::Optional =>  {
            let stacked_optional_index: StackedOptionalIndex<'a> = StackedOptionalIndex {
                columns,
                stack_merge_order,
            };
            SerializableColumnIndex::Optional {
                non_null_row_ids: Box::new(move || Box::new(stacked_optional_index.iter())),
                num_rows: stack_merge_order.num_rows(),
            }
        },
        Cardinality::Multivalued => {
            let stacked_multivalued_index = StackedMultivaluedIndex {
                columns,
                stack_merge_order,
            };
            SerializableColumnIndex::Multivalued(Box::new(move || stacked_multivalued_index.boxed_iter()))
        }
    }
}

struct StackedOptionalIndex<'a> {
    columns: &'a [Option<ColumnIndex>],
    stack_merge_order: &'a StackMergeOrder,
}

impl<'a> StackedOptionalIndex<'a> {
    fn iter(&self) -> impl Iterator<Item=RowId> + 'a {
        Box::new(
            self.columns
                .iter()
                .enumerate()
                .flat_map(|(columnar_id, column_index_opt)| {
                    let columnar_row_range = self.stack_merge_order.columnar_range(columnar_id);
                    let rows_it: Box<dyn Iterator<Item = RowId>> = match column_index_opt {
                        Some(ColumnIndex::Full) => Box::new(columnar_row_range),
                        Some(ColumnIndex::Optional(optional_index)) => Box::new(
                            optional_index
                                .iter_rows()
                                .map(move |row_id: RowId| row_id + columnar_row_range.start),
                        ),
                        Some(ColumnIndex::Multivalued(_)) => {
                            panic!("No multivalued index is allowed when stacking column index");
                        }
                        None => Box::new(std::iter::empty()),
                    };
                    rows_it
                }),
        )
    }
}

#[derive(Clone, Copy)]
struct StackedMultivaluedIndex<'a> {
    columns: &'a [Option<ColumnIndex>],
    stack_merge_order: &'a StackMergeOrder,
}

fn convert_column_opt_to_multivalued_index<'a>(
    column_index_opt: Option<&'a ColumnIndex>,
    num_rows: RowId,
) -> Box<dyn Iterator<Item = RowId> + 'a> {
    match column_index_opt {
        None => Box::new(iter::repeat(0u32).take(num_rows as usize + 1)),
        Some(ColumnIndex::Full) => Box::new(0..num_rows + 1),
        Some(ColumnIndex::Optional(optional_index)) => {
            Box::new(
                (0..num_rows)
                    // TODO optimize
                    .map(|row_id| optional_index.rank(row_id))
                    .chain(std::iter::once(optional_index.num_non_nulls())),
            )
        }
        Some(ColumnIndex::Multivalued(multivalued_index)) => {
            multivalued_index.start_index_column.iter()
        }
    }
}

impl<'a> StackedMultivaluedIndex<'a> {
    fn boxed_iter(&self) -> Box<dyn Iterator<Item = RowId> + 'a> {
        let multivalued_indexes =
            self.columns
                .iter()
                .map(Option::as_ref)
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

fn stack_multivalued_index<'a>(
    columns: &'a [Option<ColumnIndex>],
    stack_merge_order: &StackMergeOrder,
) -> Box<dyn Iterable<RowId> + 'a> {
    todo!()
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

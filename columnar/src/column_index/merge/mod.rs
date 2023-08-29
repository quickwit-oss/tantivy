mod shuffled;
mod stacked;

use shuffled::merge_column_index_shuffled;
use stacked::merge_column_index_stacked;

use crate::column_index::SerializableColumnIndex;
use crate::{Cardinality, ColumnIndex, MergeRowOrder};

// For simplification, we never have cardinality go down due to deletes.
fn detect_cardinality(columns: &[Option<ColumnIndex>]) -> Cardinality {
    columns
        .iter()
        .flatten()
        .map(ColumnIndex::get_cardinality)
        .max()
        .unwrap_or(Cardinality::Full)
}

pub fn merge_column_index<'a>(
    columns: &'a [Option<ColumnIndex>],
    merge_row_order: &'a MergeRowOrder,
) -> SerializableColumnIndex<'a> {
    // For simplification, we do not try to detect whether the cardinality could be
    // downgraded thanks to deletes.
    let cardinality_after_merge = detect_cardinality(columns);
    match merge_row_order {
        MergeRowOrder::Stack(stack_merge_order) => {
            merge_column_index_stacked(columns, cardinality_after_merge, stack_merge_order)
        }
        MergeRowOrder::Shuffled(complex_merge_order) => {
            merge_column_index_shuffled(columns, cardinality_after_merge, complex_merge_order)
        }
    }
}

// TODO actually, the shuffled code path is a bit too general.
// In practise, we do not really shuffle everything.
// The merge order restricted to a specific column keeps the original row order.
//
// This may offer some optimization that we have not explored yet.

#[cfg(test)]
mod tests {
    use crate::column_index::merge::detect_cardinality;
    use crate::column_index::multivalued_index::MultiValueIndex;
    use crate::column_index::{merge_column_index, OptionalIndex, SerializableColumnIndex};
    use crate::{Cardinality, ColumnIndex, MergeRowOrder, RowAddr, RowId, ShuffleMergeOrder};

    #[test]
    fn test_detect_cardinality() {
        assert_eq!(detect_cardinality(&[]), Cardinality::Full);
        let optional_index: ColumnIndex = OptionalIndex::for_test(1, &[]).into();
        let multivalued_index: ColumnIndex = MultiValueIndex::for_test(&[0, 1]).into();
        assert_eq!(
            detect_cardinality(&[Some(optional_index.clone()), None]),
            Cardinality::Optional
        );
        assert_eq!(
            detect_cardinality(&[Some(optional_index.clone()), Some(ColumnIndex::Full)]),
            Cardinality::Optional
        );
        assert_eq!(
            detect_cardinality(&[Some(multivalued_index.clone()), None]),
            Cardinality::Multivalued
        );
        assert_eq!(
            detect_cardinality(&[
                Some(multivalued_index.clone()),
                Some(optional_index.clone())
            ]),
            Cardinality::Multivalued
        );
        assert_eq!(
            detect_cardinality(&[Some(optional_index), Some(multivalued_index)]),
            Cardinality::Multivalued
        );
    }

    #[test]
    fn test_merge_index_multivalued_sorted() {
        let column_indexes: Vec<Option<ColumnIndex>> =
            vec![Some(MultiValueIndex::for_test(&[0, 2, 5]).into())];
        let merge_row_order: MergeRowOrder = ShuffleMergeOrder::for_test(
            &[2],
            vec![
                RowAddr {
                    segment_ord: 0u32,
                    row_id: 1u32,
                },
                RowAddr {
                    segment_ord: 0u32,
                    row_id: 0u32,
                },
            ],
        )
        .into();
        let merged_column_index = merge_column_index(&column_indexes[..], &merge_row_order);
        let SerializableColumnIndex::Multivalued(start_index_iterable) = merged_column_index else {
            panic!("Excpected a multivalued index")
        };
        let start_indexes: Vec<RowId> = start_index_iterable.boxed_iter().collect();
        assert_eq!(&start_indexes, &[0, 3, 5]);
    }

    #[test]
    fn test_merge_index_multivalued_sorted_several_segment() {
        let column_indexes: Vec<Option<ColumnIndex>> = vec![
            Some(MultiValueIndex::for_test(&[0, 2, 5]).into()),
            None,
            Some(MultiValueIndex::for_test(&[0, 1, 4]).into()),
        ];
        let merge_row_order: MergeRowOrder = ShuffleMergeOrder::for_test(
            &[2, 0, 2],
            vec![
                RowAddr {
                    segment_ord: 2u32,
                    row_id: 1u32,
                },
                RowAddr {
                    segment_ord: 0u32,
                    row_id: 0u32,
                },
                RowAddr {
                    segment_ord: 2u32,
                    row_id: 0u32,
                },
            ],
        )
        .into();
        let merged_column_index = merge_column_index(&column_indexes[..], &merge_row_order);
        let SerializableColumnIndex::Multivalued(start_index_iterable) = merged_column_index else {
            panic!("Excpected a multivalued index")
        };
        let start_indexes: Vec<RowId> = start_index_iterable.boxed_iter().collect();
        assert_eq!(&start_indexes, &[0, 3, 5, 6]);
    }
}

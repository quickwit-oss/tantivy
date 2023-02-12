mod merge;
mod multivalued_index;
mod optional_index;
mod serialize;

use std::ops::Range;

pub use merge::merge_column_index;
pub use optional_index::{OptionalIndex, Set};
pub use serialize::{open_column_index, serialize_column_index, SerializableColumnIndex};

pub use crate::column_index::multivalued_index::{MultiValueIndex, MultiValueIndexCursor};
use crate::column_index::optional_index::OptionalIndexSelectCursor;
use crate::{Cardinality, RowId};

pub struct ColumnIndexSelectCursor {
    last_rank: Option<RowId>,
    cardinality_specific_impl: CardinalitySpecificSelectCursor,
}

impl From<CardinalitySpecificSelectCursor> for ColumnIndexSelectCursor {
    fn from(cardinality_specific_impl: CardinalitySpecificSelectCursor) -> Self {
        ColumnIndexSelectCursor {
            last_rank: None,
            cardinality_specific_impl,
        }
    }
}

enum CardinalitySpecificSelectCursor {
    Full,
    Optional(OptionalIndexSelectCursor),
    Multivalued(MultiValueIndexCursor),
}

/// This cursor object point is to compute batches of `select` operations.
///
/// Regardless of cardinality, a column index can always be seen as a mapping
/// from row_id -> start_value_row_id. By definition, it is increasing.
/// If `left <= right, column_index[left] <= column_index[right]`.
///
/// The select operation then identifies, given a value row id, which row it
/// belong to: it is the inverse mapping.
///
/// As a more formal definition, `select(rank)` is defined as the only `i` such that
/// mapping[i] <= rank and mapping[i+1] < rank.
/// Another way to define it is to say that it is the last i such that
/// mapping[i] <= rank.
/// Finally it can be defined as the number of `row_id` such that
/// mapping[i] <= rank.
///
/// `select_batch_in_place` is a complex function that copmutes
/// select operation in batches and in place.
///
/// For optimization reasons, it only supports supplying ever striclty increasing
/// values of `rank_ids`, even cross calls.
///
/// It is also required from the caller, to only supply rank_ids lower than max(mapping).
/// Within those condition, the returned `row_ids` are guaranteed to be unique.
///
/// # Panics
///
/// Panics if the supplied rank_ids are not increasing from one call to another.
/// We only check that the `rank_ids` Vec is increasing in debug mode for
/// performance reason.
impl ColumnIndexSelectCursor {
    /// Returns a list of
    pub fn select_batch_in_place(&mut self, rank_ids: &mut Vec<RowId>) {
        // `rank_ids` has to be sorted.
        debug_assert!(rank_ids.windows(2).all(|window| window[0] < window[1]));
        // Two consecutive calls must pass strictly increasing `rank_ids`.
        let (Some(first_rank), Some(new_last_rank)) = (rank_ids.first().copied(), rank_ids.last().copied()) else {
            // rank_ids is empty, there is nothing to do.
            return;
        };
        if let Some(last_rank) = self.last_rank {
            assert!(last_rank < first_rank);
        }
        self.last_rank = Some(new_last_rank);
        match &mut self.cardinality_specific_impl {
            CardinalitySpecificSelectCursor::Full => {
                // No need to do anything:
                // `value_idx` and `row_idx` are the same.
            }
            CardinalitySpecificSelectCursor::Optional(optional_index) => {
                optional_index.select_batch_in_place(&mut rank_ids[..]);
            }
            CardinalitySpecificSelectCursor::Multivalued(multivalued_index) => {
                // TODO important: avoid using 0u32, and restart from the beginning all of the time.
                multivalued_index.select_batch_in_place(rank_ids)
            }
        }
    }
}

#[derive(Clone)]
pub enum ColumnIndex {
    Full,
    Optional(OptionalIndex),
    /// In addition, at index num_rows, an extra value is added
    /// containing the overal number of values.
    Multivalued(MultiValueIndex),
}

impl From<OptionalIndex> for ColumnIndex {
    fn from(optional_index: OptionalIndex) -> ColumnIndex {
        ColumnIndex::Optional(optional_index)
    }
}

impl From<MultiValueIndex> for ColumnIndex {
    fn from(multi_value_index: MultiValueIndex) -> ColumnIndex {
        ColumnIndex::Multivalued(multi_value_index)
    }
}

impl ColumnIndex {
    pub fn get_cardinality(&self) -> Cardinality {
        match self {
            ColumnIndex::Full => Cardinality::Full,
            ColumnIndex::Optional(_) => Cardinality::Optional,
            ColumnIndex::Multivalued(_) => Cardinality::Multivalued,
        }
    }

    /// Returns true if and only if there are at least one value associated to the row.
    pub fn has_value(&self, row_id: RowId) -> bool {
        match self {
            ColumnIndex::Full => true,
            ColumnIndex::Optional(optional_index) => optional_index.contains(row_id),
            ColumnIndex::Multivalued(multivalued_index) => {
                multivalued_index.range(row_id).len() > 0
            }
        }
    }

    pub fn value_row_ids(&self, row_id: RowId) -> Range<RowId> {
        match self {
            ColumnIndex::Full => row_id..row_id + 1,
            ColumnIndex::Optional(optional_index) => {
                if let Some(val) = optional_index.rank_if_exists(row_id) {
                    val..val + 1
                } else {
                    0..0
                }
            }
            ColumnIndex::Multivalued(multivalued_index) => multivalued_index.range(row_id),
        }
    }

    pub fn select_cursor(&self) -> ColumnIndexSelectCursor {
        match self {
            ColumnIndex::Full => CardinalitySpecificSelectCursor::Full.into(),
            ColumnIndex::Optional(optional_index) => {
                CardinalitySpecificSelectCursor::Optional(optional_index.select_cursor()).into()
            }
            ColumnIndex::Multivalued(multivalued_index) => {
                CardinalitySpecificSelectCursor::Multivalued(multivalued_index.select_cursor())
                    .into()
            }
        }
    }
}

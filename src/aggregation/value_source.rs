use columnar::{Cardinality, Column, ColumnType, RowId};

use super::block_accessor::BlockValueSource;
use crate::DocId;

/// Whether the encoded values produced by a source preserve their semantic ordering.
///
/// Physical columns use Tantivy's monotonic encoding (or dictionary ordinals) and therefore
/// preserve semantic ordering. Calculated sources may use private ordinals whose ordering has no
/// semantic meaning.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[allow(dead_code)]
pub(crate) enum OrdinalOrdering {
    Semantic,
    Unordered,
}

/// Immutable properties collectors may use to select safe fast paths.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ValueSourceCapabilities {
    output_type: ColumnType,
    cardinality: Cardinality,
    encoded_bounds: Option<(u64, u64)>,
    ordinal_ordering: OrdinalOrdering,
    physical: bool,
}

#[allow(dead_code)]
impl ValueSourceCapabilities {
    pub(crate) fn output_type(self) -> ColumnType {
        self.output_type
    }

    pub(crate) fn cardinality(self) -> Cardinality {
        self.cardinality
    }

    pub(crate) fn encoded_bounds(self) -> Option<(u64, u64)> {
        self.encoded_bounds
    }

    pub(crate) fn ordinal_ordering(self) -> OrdinalOrdering {
        self.ordinal_ordering
    }

    pub(crate) fn is_physical(self) -> bool {
        self.physical
    }
}

/// Immutable and cheaply clonable recipe for a per-collector value source.
#[derive(Clone, Debug)]
pub(crate) enum SegmentValueSourcePlan {
    Physical(PhysicalValueSourcePlan),
}

#[derive(Clone, Debug)]
pub(crate) struct PhysicalValueSourcePlan {
    column: Column<u64>,
    capabilities: ValueSourceCapabilities,
}

impl SegmentValueSourcePlan {
    pub(crate) fn physical(column: Column<u64>, output_type: ColumnType) -> Self {
        let encoded_bounds =
            (column.values.num_vals() != 0).then(|| (column.min_value(), column.max_value()));
        let capabilities = ValueSourceCapabilities {
            output_type,
            cardinality: column.get_cardinality(),
            encoded_bounds,
            ordinal_ordering: OrdinalOrdering::Semantic,
            physical: true,
        };
        Self::Physical(PhysicalValueSourcePlan {
            column,
            capabilities,
        })
    }

    pub(crate) fn capabilities(&self) -> ValueSourceCapabilities {
        match self {
            Self::Physical(plan) => plan.capabilities,
        }
    }

    /// Returns the physical column when this plan can participate in a physical-only fast path.
    pub(crate) fn physical_column(&self) -> Option<&Column<u64>> {
        match self {
            Self::Physical(plan) => Some(&plan.column),
        }
    }

    /// Creates runtime state owned exclusively by one segment collector.
    pub(crate) fn instantiate(&self) -> SegmentValueSource {
        match self {
            Self::Physical(plan) => SegmentValueSource::Physical(PhysicalValueSource {
                column: plan.column.clone(),
                capabilities: plan.capabilities,
            }),
        }
    }
}

/// Stateful value source owned by one segment collector.
///
/// This type intentionally does not implement `Clone`: future calculated variants carry mutable
/// execution contexts and scratch buffers which must never be shared between collectors.
#[derive(Debug)]
pub(crate) enum SegmentValueSource {
    Physical(PhysicalValueSource),
}

#[derive(Debug)]
pub(crate) struct PhysicalValueSource {
    column: Column<u64>,
    capabilities: ValueSourceCapabilities,
}

#[allow(dead_code)]
impl SegmentValueSource {
    pub(crate) fn capabilities(&self) -> ValueSourceCapabilities {
        match self {
            Self::Physical(source) => source.capabilities,
        }
    }

    /// Returns the physical column when this runtime can participate in a physical-only fast path.
    pub(crate) fn physical_column(&self) -> Option<&Column<u64>> {
        match self {
            Self::Physical(source) => Some(&source.column),
        }
    }
}

impl BlockValueSource for SegmentValueSource {
    fn load_block(
        &mut self,
        docs: &[DocId],
        values: &mut Vec<u64>,
        docids: &mut Vec<DocId>,
        row_ids: &mut Vec<RowId>,
    ) -> Cardinality {
        match self {
            Self::Physical(source) => source.column.load_block(docs, values, docids, row_ids),
        }
    }
}

#[cfg(test)]
mod tests {
    use columnar::column_index::{ColumnIndex, OptionalIndex};
    use columnar::column_values::{
        serialize_and_load_u64_based_column_values, ALL_U64_CODEC_TYPES,
    };

    use super::*;
    use crate::aggregation::ColumnBlockAccessor;

    fn optional_column() -> Column<u64> {
        let values = serialize_and_load_u64_based_column_values::<u64>(
            &&[10u64, 30][..],
            &ALL_U64_CODEC_TYPES,
        );
        Column {
            index: ColumnIndex::Optional(OptionalIndex::for_test(4, &[1, 3])),
            values,
        }
    }

    #[test]
    fn physical_plan_reports_capabilities_and_handle() {
        let plan = SegmentValueSourcePlan::physical(optional_column(), ColumnType::U64);
        let capabilities = plan.capabilities();
        assert_eq!(capabilities.output_type(), ColumnType::U64);
        assert_eq!(capabilities.cardinality(), Cardinality::Optional);
        assert_eq!(capabilities.encoded_bounds(), Some((10, 30)));
        assert_eq!(capabilities.ordinal_ordering(), OrdinalOrdering::Semantic);
        assert!(capabilities.is_physical());
        assert!(plan.physical_column().is_some());
    }

    #[test]
    fn empty_physical_plan_has_no_encoded_bounds() {
        let plan = SegmentValueSourcePlan::physical(Column::build_empty_column(3), ColumnType::F64);
        assert_eq!(plan.capabilities().encoded_bounds(), None);
    }

    #[test]
    fn cloned_plans_instantiate_independent_runtimes() {
        let plan = SegmentValueSourcePlan::physical(optional_column(), ColumnType::U64);
        let mut left = plan.instantiate();
        let mut right = plan.clone().instantiate();
        let mut left_block = ColumnBlockAccessor::default();
        let mut right_block = ColumnBlockAccessor::default();

        left_block.fetch_block(&[1], &mut left);
        right_block.fetch_block(&[3], &mut right);

        assert_eq!(left_block.values(), &[10]);
        assert_eq!(right_block.values(), &[30]);
        assert_eq!(left.capabilities(), right.capabilities());
        assert!(left.physical_column().is_some());
    }
}

//! Internal source plumbing. The public typed producer API is deliberately deferred.

use std::sync::Arc;

use columnar::{Column, ColumnType};
use rustc_hash::FxHashMap;

use super::accessor_helpers::get_ff_reader;
use super::agg_req::{AggregationVariants, Aggregations};
use crate::{DocId, SegmentReader, TantivyError};

/// Definitions are shareable; each referenced name binds exactly once per segment.
/// The declared type is fixed across segments, including all-missing segments.
pub(crate) trait VirtualColumn: Send + Sync {
    fn column_type(&self) -> ColumnType;
    fn for_segment(&self, reader: &SegmentReader)
        -> crate::Result<Box<dyn VirtualColumnEvaluator>>;
}

/// Thread-confined mutable runtime. Evaluation is infallible: failed generation leaves `None`.
///
/// Output has exactly one slot per input document and is reset to `None` before every call.
/// Values use the definition's fixed monotonic numeric encoding. A slot is committed only after
/// generating that document's value successfully; batch failure leaves every slot empty.
/// This scalar internal contract does not expose physical row IDs or promise a public raw sink.
/// Calls may be empty, single-document, repeated or globally out of order. The framework sorts
/// and deduplicates each evaluator input block when needed, then restores the caller's original
/// document order and contribution multiplicity. Results must be deterministic for a document
/// in the request snapshot; mutation is for scratch/state reuse, not evaluation-order-dependent
/// results. Panics are not caught.
pub(crate) trait VirtualColumnEvaluator {
    fn evaluate(&mut self, docs: &[DocId], output: &mut [Option<u64>]);
}

#[derive(Clone, Default)]
pub(crate) struct VirtualColumns {
    definitions: FxHashMap<String, (ColumnType, Arc<dyn VirtualColumn>)>,
}

impl VirtualColumns {
    // M1 has internal synthetic producers only; M2 will expose a typed registration API.
    #[allow(dead_code)]
    pub(crate) fn register(
        &mut self,
        name: String,
        definition: Arc<dyn VirtualColumn>,
    ) -> crate::Result<()> {
        if self.definitions.contains_key(&name) {
            return Err(TantivyError::InvalidArgument(format!(
                "Duplicate virtual column `{name}`"
            )));
        }
        let column_type = definition.column_type();
        if !matches!(
            column_type,
            ColumnType::F64 | ColumnType::I64 | ColumnType::U64
        ) {
            return Err(TantivyError::InvalidArgument(format!(
                "Unsupported type {column_type:?} for virtual column `{name}`"
            )));
        }
        self.definitions.insert(name, (column_type, definition));
        Ok(())
    }

    pub(crate) fn validate_request(&self, aggs: &Aggregations) -> crate::Result<()> {
        if self.definitions.is_empty() {
            return Ok(());
        }
        self.validate_tree(aggs, "")
    }

    fn validate_tree(&self, aggs: &Aggregations, parent: &str) -> crate::Result<()> {
        for (name, agg) in aggs {
            let path = if parent.is_empty() {
                name.clone()
            } else {
                format!("{parent}.{name}")
            };
            for field in agg.agg.get_fast_field_names() {
                for virtual_name in self.definitions.keys() {
                    let references_virtual = if let AggregationVariants::TopHits(req) = &agg.agg {
                        // Only value fields support patterns; sort fields are exact names.
                        field == virtual_name
                            || (req.value_field_names().contains(&field)
                                && super::metric::top_hits_field_matches(field, virtual_name)?)
                    } else {
                        field == virtual_name
                    };
                    if !references_virtual {
                        continue;
                    }
                    use AggregationVariants::*;
                    let unsupported = match &agg.agg {
                        MultiTerms(_) | TopHits(_) | Composite(_) | DateHistogram(_)
                        | Filter(_) => true,
                        Terms(req) => req.missing.is_some() || req.min_doc_count == Some(0),
                        Cardinality(req) => matches!(req.missing, Some(super::Key::Str(_))),
                        _ => false,
                    };
                    if unsupported {
                        return Err(TantivyError::InvalidArgument(format!(
                            "Aggregation `{path}` does not support virtual column \
                             `{virtual_name}` with this request"
                        )));
                    }
                }
            }
            self.validate_tree(&agg.sub_aggregation, &path)?;
        }
        Ok(())
    }
}

/// IDs belong to the owning segment runtime table, never to a cloned evaluator or another segment.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct VirtualColumnId(usize);

#[derive(Clone, Debug)]
pub(crate) enum AggregationValueSource {
    Physical(Column<u64>),
    Virtual(VirtualColumnId),
}

impl AggregationValueSource {
    pub(crate) fn physical(&self) -> Option<&Column<u64>> {
        match self {
            Self::Physical(column) => Some(column),
            Self::Virtual(_) => None,
        }
    }

    /// Only physical sources have trusted global bounds. Never infer them from a computed block.
    pub(crate) fn bounds(&self) -> Option<(u64, u64)> {
        self.physical()
            .map(|column| (column.min_value(), column.max_value()))
    }
}

#[derive(Default)]
pub(crate) struct SegmentValueSources {
    definitions: VirtualColumns,
    bound: FxHashMap<String, (VirtualColumnId, ColumnType)>,
    runtimes: Vec<Box<dyn VirtualColumnEvaluator>>,
    /// Shared scalar output scratch. Keeping virtual-only scratch here preserves the physical
    /// block accessor's compact layout; consumers copy results before another source is loaded.
    output: Vec<Option<u64>>,
}

impl SegmentValueSources {
    pub(crate) fn new(definitions: VirtualColumns, reader: &SegmentReader) -> crate::Result<Self> {
        // Check schema/resolver names, not only populated columns. This also detects empty JSON
        // paths.
        for name in definitions.definitions.keys() {
            if reader.schema().get_field(name).is_ok()
                || reader.fast_fields().resolve_field(name)?.is_some()
            {
                return Err(TantivyError::InvalidArgument(format!(
                    "Virtual column `{name}` collides with a physical field or JSON path"
                )));
            }
        }
        Ok(Self {
            definitions,
            ..Self::default()
        })
    }

    pub(crate) fn resolve_virtual(
        &mut self,
        reader: &SegmentReader,
        field: &str,
    ) -> crate::Result<Option<(AggregationValueSource, ColumnType)>> {
        if let Some(&(id, column_type)) = self.bound.get(field) {
            return Ok(Some((AggregationValueSource::Virtual(id), column_type)));
        }
        let Some((column_type, definition)) = self.definitions.definitions.get(field) else {
            return Ok(None);
        };
        let column_type = *column_type;
        if definition.column_type() != column_type {
            return Err(TantivyError::InvalidArgument(format!(
                "Virtual type changed for `{field}`"
            )));
        }
        let runtime = definition.for_segment(reader)?;
        let id = VirtualColumnId(self.runtimes.len());
        self.runtimes.push(runtime);
        self.bound.insert(field.to_owned(), (id, column_type));
        Ok(Some((AggregationValueSource::Virtual(id), column_type)))
    }

    pub(crate) fn resolve(
        &mut self,
        reader: &SegmentReader,
        field: &str,
        allowed_types: Option<&[ColumnType]>,
    ) -> crate::Result<(AggregationValueSource, ColumnType)> {
        if let Some((source, column_type)) = self.resolve_virtual(reader, field)? {
            if allowed_types.is_some_and(|allowed| !allowed.contains(&column_type)) {
                return Err(TantivyError::InvalidArgument(format!(
                    "Unsupported virtual type for `{field}`"
                )));
            }
            return Ok((source, column_type));
        }
        let (column, column_type) = get_ff_reader(reader, field, allowed_types)?;
        Ok((AggregationValueSource::Physical(column), column_type))
    }

    pub(crate) fn runtime_and_output(
        &mut self,
        id: VirtualColumnId,
    ) -> (&mut dyn VirtualColumnEvaluator, &mut Vec<Option<u64>>) {
        (&mut *self.runtimes[id.0], &mut self.output)
    }
}

use std::fmt::Debug;

use columnar::ColumnType;
use rustc_hash::FxHashMap;

use super::OrderTarget;
use crate::aggregation::agg_data::{
    build_segment_agg_collectors, AggRefNode, AggregationsSegmentCtx,
};
use crate::aggregation::agg_limits::MemoryConsumption;
use crate::aggregation::bucket::get_agg_name_and_property;
use crate::aggregation::intermediate_agg_result::{
    IntermediateAggregationResult, IntermediateAggregationResults,
};
use crate::aggregation::segment_agg_result::SegmentAggregationCollector;
use crate::TantivyError;

#[derive(Clone, Debug, Default)]
/// Container to store term_ids/or u64 values and their buckets.
struct TermBuckets {
    pub(crate) entries: FxHashMap<u64, u32>,
    pub(crate) sub_aggs: FxHashMap<u64, Box<dyn SegmentAggregationCollector>>,
}

impl TermBuckets {
    fn get_memory_consumption(&self) -> usize {
        let sub_aggs_mem = self.sub_aggs.memory_consumption();
        let buckets_mem = self.entries.memory_consumption();
        sub_aggs_mem + buckets_mem
    }

    fn force_flush(&mut self, agg_data: &mut AggregationsSegmentCtx) -> crate::Result<()> {
        for sub_aggregations in &mut self.sub_aggs.values_mut() {
            sub_aggregations.as_mut().flush(agg_data)?;
        }
        Ok(())
    }
}

/// The collector puts values from the fast field into the correct buckets and does a conversion to
/// the correct datatype.
#[derive(Clone, Debug)]
pub struct SegmentTermCollector {
    /// The buckets containing the aggregation data.
    term_buckets: TermBuckets,
    accessor_idx: usize,
}

impl SegmentAggregationCollector for SegmentTermCollector {
    fn add_intermediate_aggregation_result(
        self: Box<Self>,
        agg_data: &AggregationsSegmentCtx,
        results: &mut IntermediateAggregationResults,
    ) -> crate::Result<()> {
        let name = agg_data.get_term_req_data(self.accessor_idx).name.clone();

        let entries: Vec<(u64, u32)> = self.term_buckets.entries.into_iter().collect();
        let bucket = super::into_intermediate_bucket_result(
            self.accessor_idx,
            entries,
            self.term_buckets.sub_aggs,
            agg_data,
        )?;
        results.push(name, IntermediateAggregationResult::Bucket(bucket))?;

        Ok(())
    }

    #[inline]
    fn collect(
        &mut self,
        doc: crate::DocId,
        agg_data: &mut AggregationsSegmentCtx,
    ) -> crate::Result<()> {
        self.collect_block(&[doc], agg_data)
    }

    #[inline]
    fn collect_block(
        &mut self,
        docs: &[crate::DocId],
        agg_data: &mut AggregationsSegmentCtx,
    ) -> crate::Result<()> {
        let mut req_data = agg_data.take_term_req_data(self.accessor_idx);

        let mem_pre = self.get_memory_consumption();

        if let Some(missing) = req_data.missing_value_for_accessor {
            req_data.column_block_accessor.fetch_block_with_missing(
                docs,
                &req_data.accessor,
                missing,
            );
        } else {
            req_data
                .column_block_accessor
                .fetch_block(docs, &req_data.accessor);
        }

        for term_id in req_data.column_block_accessor.iter_vals() {
            if let Some(allowed_bs) = req_data.allowed_term_ids.as_ref() {
                if !allowed_bs.contains(term_id as u32) {
                    continue;
                }
            }
            let entry = self.term_buckets.entries.entry(term_id).or_default();
            *entry += 1;
        }
        // has subagg
        if let Some(blueprint) = req_data.sub_aggregation_blueprint.as_ref() {
            for (doc, term_id) in req_data
                .column_block_accessor
                .iter_docid_vals(docs, &req_data.accessor)
            {
                if let Some(allowed_bs) = req_data.allowed_term_ids.as_ref() {
                    if !allowed_bs.contains(term_id as u32) {
                        continue;
                    }
                }
                let sub_aggregations = self
                    .term_buckets
                    .sub_aggs
                    .entry(term_id)
                    .or_insert_with(|| blueprint.clone());
                sub_aggregations.collect(doc, agg_data)?;
            }
        }

        let mem_delta = self.get_memory_consumption() - mem_pre;
        if mem_delta > 0 {
            agg_data
                .context
                .limits
                .add_memory_consumed(mem_delta as u64)?;
        }
        agg_data.put_back_term_req_data(self.accessor_idx, req_data);

        Ok(())
    }

    fn flush(&mut self, agg_data: &mut AggregationsSegmentCtx) -> crate::Result<()> {
        self.term_buckets.force_flush(agg_data)?;
        Ok(())
    }
}

impl SegmentTermCollector {
    pub fn from_req_and_validate(
        req_data: &mut AggregationsSegmentCtx,
        node: &AggRefNode,
    ) -> crate::Result<Self> {
        let terms_req_data = req_data.get_term_req_data(node.idx_in_req_data);
        let column_type = terms_req_data.column_type;
        let accessor_idx = node.idx_in_req_data;
        if column_type == ColumnType::Bytes {
            return Err(TantivyError::InvalidArgument(format!(
                "terms aggregation is not supported for column type {column_type:?}"
            )));
        }
        let term_buckets = TermBuckets::default();

        // Validate sub aggregation exists
        if let OrderTarget::SubAggregation(sub_agg_name) = &terms_req_data.req.order.target {
            let (agg_name, _agg_property) = get_agg_name_and_property(sub_agg_name);

            node.get_sub_agg(agg_name, &req_data.per_request)
                .ok_or_else(|| {
                    TantivyError::InvalidArgument(format!(
                        "could not find aggregation with name {agg_name} in metric \
                         sub_aggregations"
                    ))
                })?;
        }

        let has_sub_aggregations = !node.children.is_empty();
        let blueprint = if has_sub_aggregations {
            let sub_aggregation = build_segment_agg_collectors(req_data, &node.children)?;
            Some(sub_aggregation)
        } else {
            None
        };
        let terms_req_data = req_data.get_term_req_data_mut(node.idx_in_req_data);
        terms_req_data.sub_aggregation_blueprint = blueprint;

        Ok(SegmentTermCollector {
            term_buckets,
            accessor_idx,
        })
    }

    fn get_memory_consumption(&self) -> usize {
        let self_mem = std::mem::size_of::<Self>();
        let term_buckets_mem = self.term_buckets.get_memory_consumption();
        self_mem + term_buckets_mem
    }
}

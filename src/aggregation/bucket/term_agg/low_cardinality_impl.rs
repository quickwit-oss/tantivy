use std::vec;

use rustc_hash::FxHashMap;

use crate::aggregation::agg_data::{
    build_segment_agg_collectors, AggRefNode, AggregationsSegmentCtx,
};
use crate::aggregation::bucket::{get_agg_name_and_property, OrderTarget};
use crate::aggregation::intermediate_agg_result::{
    IntermediateAggregationResult, IntermediateAggregationResults,
};
use crate::aggregation::segment_agg_result::SegmentAggregationCollector;
use crate::{DocId, TantivyError};

const MAX_BATCH_SIZE: usize = 1_024;

#[derive(Debug, Clone)]
struct LowCardTermBuckets {
    entries: Box<[u32]>,
    sub_aggs: Vec<Box<dyn SegmentAggregationCollector>>,
    doc_buffers: Box<[Vec<DocId>]>,
}

impl LowCardTermBuckets {
    pub fn with_num_buckets(
        num_buckets: usize,
        sub_aggs_blueprint_opt: Option<&Box<dyn SegmentAggregationCollector>>,
    ) -> Self {
        let sub_aggs = sub_aggs_blueprint_opt
            .as_ref()
            .map(|blueprint| {
                std::iter::repeat_with(|| blueprint.clone_box())
                    .take(num_buckets)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        Self {
            entries: vec![0; num_buckets].into_boxed_slice(),
            sub_aggs,
            doc_buffers: std::iter::repeat_with(|| Vec::with_capacity(MAX_BATCH_SIZE))
                .take(num_buckets)
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        }
    }

    fn get_memory_consumption(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.entries.len() * std::mem::size_of::<u32>()
            + self.doc_buffers.len()
                * (std::mem::size_of::<Vec<DocId>>()
                    + std::mem::size_of::<DocId>() * MAX_BATCH_SIZE)
    }
}

#[derive(Debug, Clone)]
pub struct LowCardSegmentTermCollector {
    term_buckets: LowCardTermBuckets,
    accessor_idx: usize,
}

impl LowCardSegmentTermCollector {
    pub fn from_req_and_validate(
        req_data: &mut AggregationsSegmentCtx,
        node: &AggRefNode,
    ) -> crate::Result<Self> {
        let terms_req_data = req_data.get_term_req_data(node.idx_in_req_data);
        let accessor_idx = node.idx_in_req_data;
        let cardinality = terms_req_data
            .accessor
            .max_value()
            .max(terms_req_data.missing_value_for_accessor.unwrap_or(0))
            + 1;
        assert!(cardinality <= super::LOW_CARDINALITY_THRESHOLD);

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

        let term_buckets =
            LowCardTermBuckets::with_num_buckets(cardinality as usize, blueprint.as_ref());

        terms_req_data.sub_aggregation_blueprint = blueprint;

        Ok(LowCardSegmentTermCollector {
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

impl SegmentAggregationCollector for LowCardSegmentTermCollector {
    fn add_intermediate_aggregation_result(
        self: Box<Self>,
        agg_data: &AggregationsSegmentCtx,
        results: &mut IntermediateAggregationResults,
    ) -> crate::Result<()> {
        let name = agg_data.get_term_req_data(self.accessor_idx).name.clone();
        let sub_aggs: FxHashMap<u64, Box<dyn SegmentAggregationCollector>> = self
            .term_buckets
            .sub_aggs
            .into_iter()
            .enumerate()
            .filter(|(bucket_id, _sub_agg)| self.term_buckets.entries[*bucket_id] > 0)
            .map(|(bucket_id, sub_agg)| (bucket_id as u64, sub_agg))
            .collect();
        let entries: Vec<(u64, u32)> = self
            .term_buckets
            .entries
            .iter()
            .enumerate()
            .filter(|(_, count)| **count > 0)
            .map(|(bucket_id, count)| (bucket_id as u64, *count))
            .collect();

        let bucket =
            super::into_intermediate_bucket_result(self.accessor_idx, entries, sub_aggs, agg_data)?;
        results.push(name, IntermediateAggregationResult::Bucket(bucket))?;
        Ok(())
    }

    fn collect_block(
        &mut self,
        docs: &[crate::DocId],
        agg_data: &mut AggregationsSegmentCtx,
    ) -> crate::Result<()> {
        if docs.len() > MAX_BATCH_SIZE {
            for batch in docs.chunks(MAX_BATCH_SIZE) {
                self.collect_block(batch, agg_data)?;
            }
        }

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

        // has subagg
        if req_data.sub_aggregation_blueprint.is_some() {
            for (doc, term_id) in req_data
                .column_block_accessor
                .iter_docid_vals(docs, &req_data.accessor)
            {
                if let Some(allowed_bs) = req_data.allowed_term_ids.as_ref() {
                    if !allowed_bs.contains(term_id as u32) {
                        continue;
                    }
                }
                self.term_buckets.doc_buffers[term_id as usize].push(doc);
            }
            for (bucket_id, docs) in self.term_buckets.doc_buffers.iter_mut().enumerate() {
                self.term_buckets.entries[bucket_id] += docs.len() as u32;
                self.term_buckets.sub_aggs[bucket_id].collect_block(&docs[..], agg_data)?;
                docs.clear();
            }
        } else {
            for term_id in req_data.column_block_accessor.iter_vals() {
                if let Some(allowed_bs) = req_data.allowed_term_ids.as_ref() {
                    if !allowed_bs.contains(term_id as u32) {
                        continue;
                    }
                }
                self.term_buckets.entries[term_id as usize] += 1;
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

    fn collect(
        &mut self,
        doc: crate::DocId,
        agg_data: &mut AggregationsSegmentCtx,
    ) -> crate::Result<()> {
        self.collect_block(&[doc], agg_data)
    }

    fn flush(&mut self, agg_data: &mut AggregationsSegmentCtx) -> crate::Result<()> {
        for sub_aggregations in &mut self.term_buckets.sub_aggs.iter_mut() {
            sub_aggregations.as_mut().flush(agg_data)?;
        }
        Ok(())
    }
}

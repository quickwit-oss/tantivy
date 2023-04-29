use super::agg_req_with_accessor::AggregationsWithAccessor;
use super::intermediate_agg_result::IntermediateAggregationResults;
use super::segment_agg_result::SegmentAggregationCollector;
use crate::DocId;

pub(crate) const DOC_BLOCK_SIZE: usize = 64;
pub(crate) type DocBlock = [DocId; DOC_BLOCK_SIZE];

/// BufAggregationCollector buffers documents before calling collect_block().
#[derive(Clone)]
pub(crate) struct BufAggregationCollector {
    pub(crate) collector: Box<dyn SegmentAggregationCollector>,
    staged_docs: DocBlock,
    num_staged_docs: usize,
}

impl std::fmt::Debug for BufAggregationCollector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SegmentAggregationResultsCollector")
            .field("staged_docs", &&self.staged_docs[..self.num_staged_docs])
            .field("num_staged_docs", &self.num_staged_docs)
            .finish()
    }
}

impl BufAggregationCollector {
    pub fn new(collector: Box<dyn SegmentAggregationCollector>) -> Self {
        Self {
            collector,
            num_staged_docs: 0,
            staged_docs: [0; DOC_BLOCK_SIZE],
        }
    }
}

impl SegmentAggregationCollector for BufAggregationCollector {
    #[inline]
    fn add_intermediate_aggregation_result(
        self: Box<Self>,
        agg_with_accessor: &AggregationsWithAccessor,
        results: &mut IntermediateAggregationResults,
    ) -> crate::Result<()> {
        Box::new(self.collector).add_intermediate_aggregation_result(agg_with_accessor, results)
    }

    #[inline]
    fn collect(
        &mut self,
        doc: crate::DocId,
        agg_with_accessor: &mut AggregationsWithAccessor,
    ) -> crate::Result<()> {
        self.staged_docs[self.num_staged_docs] = doc;
        self.num_staged_docs += 1;
        if self.num_staged_docs == self.staged_docs.len() {
            self.collector
                .collect_block(&self.staged_docs[..self.num_staged_docs], agg_with_accessor)?;
            self.num_staged_docs = 0;
        }
        Ok(())
    }

    #[inline]
    fn collect_block(
        &mut self,
        docs: &[crate::DocId],
        agg_with_accessor: &mut AggregationsWithAccessor,
    ) -> crate::Result<()> {
        self.collector.collect_block(docs, agg_with_accessor)?;

        Ok(())
    }

    #[inline]
    fn flush(&mut self, agg_with_accessor: &mut AggregationsWithAccessor) -> crate::Result<()> {
        self.collector
            .collect_block(&self.staged_docs[..self.num_staged_docs], agg_with_accessor)?;
        self.num_staged_docs = 0;

        self.collector.flush(agg_with_accessor)?;

        Ok(())
    }
}

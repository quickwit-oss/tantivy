use super::agg_req_with_accessor::AggregationsWithAccessor;
use super::intermediate_agg_result::IntermediateAggregationResults;
use super::segment_agg_result::SegmentAggregationCollector;
use crate::DocId;

pub(crate) const DOC_BLOCK_SIZE: usize = 64;
pub(crate) type DocBlock = [DocId; DOC_BLOCK_SIZE];

/// BufAggregationCollector buffers documents before calling collect_block().
#[derive(Clone)]
pub(crate) struct BufAggregationCollector<T> {
    pub(crate) collector: T,
    staged_docs: DocBlock,
    num_staged_docs: usize,
}

impl<T> std::fmt::Debug for BufAggregationCollector<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SegmentAggregationResultsCollector")
            .field("staged_docs", &&self.staged_docs[..self.num_staged_docs])
            .field("num_staged_docs", &self.num_staged_docs)
            .finish()
    }
}

impl<T: SegmentAggregationCollector> BufAggregationCollector<T> {
    pub fn new(collector: T) -> Self {
        Self {
            collector,
            num_staged_docs: 0,
            staged_docs: [0; DOC_BLOCK_SIZE],
        }
    }
}

impl<T: SegmentAggregationCollector + Clone + 'static> SegmentAggregationCollector
    for BufAggregationCollector<T>
{
    fn into_intermediate_aggregations_result(
        self: Box<Self>,
        agg_with_accessor: &AggregationsWithAccessor,
    ) -> crate::Result<IntermediateAggregationResults> {
        Box::new(self.collector).into_intermediate_aggregations_result(agg_with_accessor)
    }

    fn collect(
        &mut self,
        doc: crate::DocId,
        agg_with_accessor: &AggregationsWithAccessor,
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

    fn collect_block(
        &mut self,
        docs: &[crate::DocId],
        agg_with_accessor: &AggregationsWithAccessor,
    ) -> crate::Result<()> {
        for doc in docs {
            self.collect(*doc, agg_with_accessor)?;
        }
        Ok(())
    }

    fn flush(&mut self, agg_with_accessor: &AggregationsWithAccessor) -> crate::Result<()> {
        self.collector
            .collect_block(&self.staged_docs[..self.num_staged_docs], agg_with_accessor)?;
        self.num_staged_docs = 0;

        self.collector.flush(agg_with_accessor)?;

        Ok(())
    }
}

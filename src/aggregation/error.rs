use super::bucket::DateHistogramParseError;

/// Error that may occur when opening a directory
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum AggregationError {
    /// Date histogram parse error
    #[error("Date histogram parse error: {0:?}")]
    DateHistogramParseError(#[from] DateHistogramParseError),
    /// Memory limit exceeded
    #[error(
        "Aborting aggregation because memory limit was exceeded. Limit: {limit:?}, Current: \
         {current:?}"
    )]
    MemoryExceeded {
        /// Memory consumption limit
        limit: u64,
        /// Current memory consumption
        current: u64,
    },
    /// Bucket limit exceeded
    #[error(
        "Aborting aggregation because bucket limit was exceeded. Limit: {limit:?}, Current: \
         {current:?}"
    )]
    BucketLimitExceeded {
        /// Bucket limit
        limit: u32,
        /// Current num buckets
        current: u32,
    },
}

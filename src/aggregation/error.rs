use super::bucket::DateHistogramParseError;

/// Error that may occur when opening a directory
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum AggregationError {
    /// Failed to open the directory.
    #[error("Date histogram parse error: {0:?}")]
    DateHistogramParseError(#[from] DateHistogramParseError),
}

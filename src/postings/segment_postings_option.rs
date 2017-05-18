

/// Object describing the amount of information required when reading a postings.
///
/// Since decoding information is not free, this makes it possible to
/// avoid this extra cost when the information is not required.
/// For instance, positions are useful when running phrase queries
/// but useless in other queries.
#[derive(Clone, Copy, Debug, PartialEq, PartialOrd, Ord, Eq)]
pub enum SegmentPostingsOption {
    /// Only the doc ids are decoded
    NoFreq,
    /// DocIds and term frequencies are decoded
    Freq,
    /// DocIds, term frequencies and positions will be decoded.
    FreqAndPositions,
}

#[cfg(test)]
mod tests {

    use super::SegmentPostingsOption;

    #[test]
    fn test_cmp_segment_postings_option() {
        assert!(SegmentPostingsOption::FreqAndPositions > SegmentPostingsOption::Freq);
        assert!(SegmentPostingsOption::Freq > SegmentPostingsOption::NoFreq);
    }
}

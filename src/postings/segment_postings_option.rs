

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

impl SegmentPostingsOption {

    /// Returns true iff this option includes encoding
    /// term frequencies.
    pub fn has_freq(&self) -> bool {
        match *self {
            SegmentPostingsOption::NoFreq => false,
            _ => true,
        }
    }

    /// Returns true iff this option include encoding
    ///  term positions.
    pub fn has_positions(&self) -> bool {
        match *self {
            SegmentPostingsOption::FreqAndPositions => true,
            _ => false,
        }
    }
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

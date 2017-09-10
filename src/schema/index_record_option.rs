
/// Describing the amount of information indexed.
///
/// Since decoding information is not free, this makes it possible to
/// avoid this extra cost when the information is not required.
/// For instance, positions are useful when running phrase queries
/// but useless in other queries.
#[derive(Clone, Copy, Debug, PartialEq, PartialOrd, Ord, Eq, Hash, Serialize, Deserialize)]
pub enum IndexRecordOption {
    #[serde(rename = "basic")]
    Basic,
    #[serde(rename = "freq")]
    WithFreqs,
    #[serde(rename = "position")]
    WithFreqsAndPositions,
}

impl IndexRecordOption {
    /// Returns true iff the term frequency will be encoded.
    pub fn is_termfreq_enabled(&self) -> bool {
        match *self {
            IndexRecordOption::WithFreqsAndPositions |
            IndexRecordOption::WithFreqs => true,
            _ => false,
        }
    }

    /// Returns true iff the term positions within the document are stored as well.
    pub fn is_position_enabled(&self) -> bool {
        match *self {
            IndexRecordOption::WithFreqsAndPositions => true,
            _ => false,
        }
    }

    /// Returns true iff this option includes encoding
    /// term frequencies.
    pub fn has_freq(&self) -> bool {
        match *self {
            IndexRecordOption::Basic => false,
            IndexRecordOption::WithFreqs |
            IndexRecordOption::WithFreqsAndPositions => true,
        }
    }

    /// Returns true iff this option include encoding
    ///  term positions.
    pub fn has_positions(&self) -> bool {
        match *self {
            IndexRecordOption::Basic |
            IndexRecordOption::WithFreqs => false,
            IndexRecordOption::WithFreqsAndPositions => true,
        }
    }
}
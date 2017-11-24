
/// `IndexRecordOption` describes an amount of information associated
/// for a given field.
///
/// It is used in the schema to configure how much data should be
/// indexed for a given field.
///
/// It is also used to describe the amount of information that
/// you want to be decoded as you go through a posting list.
///
/// For instance, positions are useful when running phrase queries
/// but useless for most queries.
///
#[derive(Clone, Copy, Debug, PartialEq, PartialOrd, Ord, Eq, Hash, Serialize, Deserialize)]
pub enum IndexRecordOption {
    /// records only the `DocId`s
    #[serde(rename = "basic")]
    Basic,
    /// records the document ids as well as the term frequency.
    #[serde(rename = "freq")]
    WithFreqs,
    /// records the document id, the term frequency and the positions of
    /// the occurences in the document.
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
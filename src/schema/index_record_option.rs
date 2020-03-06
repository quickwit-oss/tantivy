use serde::{Deserialize, Serialize};

/// `IndexRecordOption` describes an amount information associated
/// to a given indexed field.
///
/// It is both used to:
///
///  * describe in the schema the amount of information
/// that should be retained during indexing (See
/// [`TextFieldIndexing.html.set_index_option`](
///     ../schema/struct.TextFieldIndexing.html#method.set_index_option))
///  * to request for a given
/// amount of information to be decoded as one goes through a posting list.
/// (See [`InvertedIndexReader.read_postings`](
///     ../struct.InvertedIndexReader.html#method.read_postings))
///
#[derive(Clone, Copy, Debug, PartialEq, PartialOrd, Ord, Eq, Hash, Serialize, Deserialize)]
pub enum IndexRecordOption {
    /// records only the `DocId`s
    #[serde(rename = "basic")]
    Basic,
    /// records the document ids as well as the term frequency.
    /// The term frequency can help giving better scoring of the documents.
    #[serde(rename = "freq")]
    WithFreqs,
    /// records the document id, the term frequency and the positions of
    /// the occurences in the document.
    /// Positions are required to run [PhraseQueries](../query/struct.PhraseQuery.html).
    #[serde(rename = "position")]
    WithFreqsAndPositions,
}

impl IndexRecordOption {
    /// Returns true iff this option includes encoding
    /// term frequencies.
    pub fn has_freq(self) -> bool {
        match self {
            IndexRecordOption::Basic => false,
            IndexRecordOption::WithFreqs | IndexRecordOption::WithFreqsAndPositions => true,
        }
    }

    /// Returns true iff this option include encoding
    ///  term positions.
    pub fn has_positions(self) -> bool {
        match self {
            IndexRecordOption::Basic | IndexRecordOption::WithFreqs => false,
            IndexRecordOption::WithFreqsAndPositions => true,
        }
    }
}

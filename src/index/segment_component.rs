use std::fmt::{Display, Formatter};
use std::slice;

/// Enum describing each component of a tantivy segment.
///
/// Each component is stored in its own file,
/// using the pattern `segment_uuid`.`component_extension`,
/// except the delete component that takes an `segment_uuid`.`delete_opstamp`.`component_extension`
#[derive(Debug, Copy, Clone, Hash, Ord, PartialOrd, Eq, PartialEq)]
pub enum SegmentComponent {
    /// Postings (or inverted list). Sorted lists of document ids, associated with terms
    Postings,
    /// Positions of terms in each document.
    Positions,
    /// Column-oriented random-access storage of fields.
    FastFields,
    /// Stores the sum  of the length (in terms) of each field for each document.
    /// Field norms are stored as a special u64 fast field.
    FieldNorms,
    /// Dictionary associating `Term`s to `TermInfo`s which is
    /// simply an address into the `postings` file and the `positions` file.
    Terms,
    /// Row-oriented, compressed storage of the documents.
    /// Accessing a document from the store is relatively slow, as it
    /// requires to decompress the entire block it belongs to.
    Store,
    /// Temporary storage of the documents, before streamed to `Store`.
    TempStore,
    /// Bitset describing which document of the segment is alive.
    /// (It was representing deleted docs but changed to represent alive docs from v0.17)
    Delete,
}

impl TryFrom<&str> for SegmentComponent {
    type Error = String;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "idx" => Ok(SegmentComponent::Postings),
            "pos" => Ok(SegmentComponent::Positions),
            "term" => Ok(SegmentComponent::Terms),
            "store" => Ok(SegmentComponent::Store),
            "temp" => Ok(SegmentComponent::TempStore),
            "fast" => Ok(SegmentComponent::FastFields),
            "fieldnorm" => Ok(SegmentComponent::FieldNorms),
            "del" => Ok(SegmentComponent::Delete),
            other => Err(other.to_string()),
        }
    }
}

impl Display for SegmentComponent {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            SegmentComponent::Postings => write!(f, "idx"),
            SegmentComponent::Positions => write!(f, "pos"),
            SegmentComponent::FastFields => write!(f, "fast"),
            SegmentComponent::FieldNorms => write!(f, "fieldnorm"),
            SegmentComponent::Terms => write!(f, "term"),
            SegmentComponent::Store => write!(f, "store"),
            SegmentComponent::TempStore => write!(f, "temp"),
            SegmentComponent::Delete => write!(f, "del"),
        }
    }
}

impl SegmentComponent {
    /// Iterates through the components.
    pub fn iterator() -> slice::Iter<'static, SegmentComponent> {
        static SEGMENT_COMPONENTS: [SegmentComponent; 8] = [
            SegmentComponent::Postings,
            SegmentComponent::Positions,
            SegmentComponent::FastFields,
            SegmentComponent::FieldNorms,
            SegmentComponent::Terms,
            SegmentComponent::Store,
            SegmentComponent::TempStore,
            SegmentComponent::Delete,
        ];
        SEGMENT_COMPONENTS.iter()
    }
}

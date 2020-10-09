use std::slice;

/// Enum describing each component of a tantivy segment.
/// Each component is stored in its own file,
/// using the pattern `segment_uuid`.`component_extension`,
/// except the delete component that takes an `segment_uuid`.`delete_opstamp`.`component_extension`
#[derive(Copy, Clone)]
pub enum SegmentComponent {
    /// Postings (or inverted list). Sorted lists of document ids, associated to terms
    POSTINGS,
    /// Positions of terms in each document.
    POSITIONS,
    /// Index to seek within the position file
    POSITIONSSKIP,
    /// Column-oriented random-access storage of fields.
    FASTFIELDS,
    /// Stores the sum  of the length (in terms) of each field for each document.
    /// Field norms are stored as a special u64 fast field.
    FIELDNORMS,
    /// Dictionary associating `Term`s to `TermInfo`s which is
    /// simply an address into the `postings` file and the `positions` file.
    TERMS,
    /// Row-oriented, compressed storage of the documents.
    /// Accessing a document from the store is relatively slow, as it
    /// requires to decompress the entire block it belongs to.
    STORE,
    /// Bitset describing which document of the segment is deleted.
    DELETE,
}

impl SegmentComponent {
    /// Iterates through the components.
    pub fn iterator() -> slice::Iter<'static, SegmentComponent> {
        static SEGMENT_COMPONENTS: [SegmentComponent; 8] = [
            SegmentComponent::POSTINGS,
            SegmentComponent::POSITIONS,
            SegmentComponent::POSITIONSSKIP,
            SegmentComponent::FASTFIELDS,
            SegmentComponent::FIELDNORMS,
            SegmentComponent::TERMS,
            SegmentComponent::STORE,
            SegmentComponent::DELETE,
        ];
        SEGMENT_COMPONENTS.iter()
    }
}

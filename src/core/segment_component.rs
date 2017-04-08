#[derive(Copy, Clone)]
pub enum SegmentComponent {
    POSTINGS,
    POSITIONS,
    FASTFIELDS,
    FIELDNORMS,
    TERMS,
    STORE,
    DELETE
}

impl SegmentComponent {
    
    pub fn iterator() -> impl Iterator<Item=&'static SegmentComponent> {
        static SEGMENT_COMPONENTS: [SegmentComponent;  7] = [
            SegmentComponent::POSTINGS,
            SegmentComponent::POSITIONS,
            SegmentComponent::FASTFIELDS,
            SegmentComponent::FIELDNORMS,
            SegmentComponent::TERMS,
            SegmentComponent::STORE,
            SegmentComponent::DELETE
        ];
        SEGMENT_COMPONENTS.into_iter()
    }
    
}
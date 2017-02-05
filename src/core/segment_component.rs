#[derive(Copy, Clone)]
pub enum SegmentComponent {
    INFO,
    POSTINGS,
    POSITIONS,
    FASTFIELDS,
    FIELDNORMS,
    TERMS,
    STORE,
    DELETE
}

impl SegmentComponent {
    
    pub fn path_suffix(&self, opstamp: u64)-> String {
        match *self {
            SegmentComponent::POSITIONS => ".pos".to_string(),
            SegmentComponent::INFO => ".info".to_string(),
            SegmentComponent::POSTINGS => ".idx".to_string(),
            SegmentComponent::TERMS => ".term".to_string(),
            SegmentComponent::STORE => ".store".to_string(),
            SegmentComponent::FASTFIELDS => ".fast".to_string(),
            SegmentComponent::FIELDNORMS => ".fieldnorm".to_string(),
            SegmentComponent::DELETE => {format!(".{}.del", opstamp)},
        }
    }
}


    
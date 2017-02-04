#[derive(Copy, Clone)]
pub enum SegmentComponent {
    INFO,
    POSTINGS,
    POSITIONS,
    FASTFIELDS,
    FIELDNORMS,
    TERMS,
    STORE,
    DELETE(u64), //< The argument here is an opstamp.
                 // All of the deletes with an opstamp smaller or equal
                 // to this opstamp have been taken in account.
}

impl SegmentComponent {
    
    pub fn path_suffix(&self)-> String {
        match *self {
            SegmentComponent::POSITIONS => ".pos".to_string(),
            SegmentComponent::INFO => ".info".to_string(),
            SegmentComponent::POSTINGS => ".idx".to_string(),
            SegmentComponent::TERMS => ".term".to_string(),
            SegmentComponent::STORE => ".store".to_string(),
            SegmentComponent::FASTFIELDS => ".fast".to_string(),
            SegmentComponent::FIELDNORMS => ".fieldnorm".to_string(),
            SegmentComponent::DELETE(opstamp) => {
                format!(".{}.del", opstamp)
            }
        }
    }
}


    
use std::vec::IntoIter;

#[derive(Copy, Clone)]
pub enum SegmentComponent {
    INFO,
    POSTINGS,
    POSITIONS,
    FASTFIELDS,
    FIELDNORMS,
    TERMS,
    STORE,
}

impl SegmentComponent {
    pub fn values() -> IntoIter<SegmentComponent> {
        vec!(
            SegmentComponent::INFO,
            SegmentComponent::POSTINGS,
            SegmentComponent::POSITIONS,
            SegmentComponent::FASTFIELDS,
            SegmentComponent::FIELDNORMS,
            SegmentComponent::TERMS,
            SegmentComponent::STORE,
        ).into_iter()
    }
    
    pub fn path_suffix(&self)-> &'static str {
        match *self {
            SegmentComponent::POSITIONS => ".pos",
            SegmentComponent::INFO => ".info",
            SegmentComponent::POSTINGS => ".idx",
            SegmentComponent::TERMS => ".term",
            SegmentComponent::STORE => ".store",
            SegmentComponent::FASTFIELDS => ".fast",
            SegmentComponent::FIELDNORMS => ".fieldnorm",
        }
    }
}


    
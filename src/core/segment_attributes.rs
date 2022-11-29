pub trait SegmentAttributesMerger: Send + Sync {
    fn merge_json(&self, segment_attributes_json: Vec<&serde_json::Value>) -> serde_json::Value;
    fn default(&self) -> serde_json::Value;
    fn box_clone(&self) -> Box<dyn SegmentAttributesMerger>;
}

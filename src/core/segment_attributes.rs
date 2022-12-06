/// Allows to implement custom behaviour while merging `SegmentAttributes` of multiple segments.
///
/// Segment attributes are represented by `serde_json::Value` and may be set on commit or merge
pub trait SegmentAttributesMerger: Send + Sync {
    /// Implements merging of multiple segment attributes, used in merge
    fn merge_json(&self, segment_attributes_json: Vec<&serde_json::Value>) -> serde_json::Value;
    /// Default value of segment attributes that set on `Segment` creation
    fn default(&self) -> serde_json::Value;
}

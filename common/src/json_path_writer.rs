use crate::replace_in_place;

/// Separates the different segments of a json path.
pub const JSON_PATH_SEGMENT_SEP: u8 = 1u8;
pub const JSON_PATH_SEGMENT_SEP_STR: &str =
    unsafe { std::str::from_utf8_unchecked(&[JSON_PATH_SEGMENT_SEP]) };

/// Create a new JsonPathWriter, that creates flattened json paths for tantivy.
#[derive(Clone, Debug, Default)]
pub struct JsonPathWriter {
    path: String,
    indices: Vec<usize>,
    expand_dots: bool,
}

impl JsonPathWriter {
    pub fn new() -> Self {
        JsonPathWriter {
            path: String::new(),
            indices: Vec::new(),
            expand_dots: false,
        }
    }

    /// When expand_dots is enabled, json object like
    /// `{"k8s.node.id": 5}` is processed as if it was
    /// `{"k8s": {"node": {"id": 5}}}`.
    /// This option has the merit of allowing users to
    /// write queries  like `k8s.node.id:5`.
    /// On the other, enabling that feature can lead to
    /// ambiguity.
    pub fn set_expand_dots(&mut self, expand_dots: bool) {
        self.expand_dots = expand_dots;
    }

    /// Push a new segment to the path.
    pub fn push(&mut self, segment: &str) {
        let len_path = self.path.len();
        self.indices.push(len_path);
        if !self.path.is_empty() {
            self.path.push_str(JSON_PATH_SEGMENT_SEP_STR);
        }
        self.path.push_str(segment);
        if self.expand_dots {
            // This might include the separation byte, which is ok because it is not a dot.
            let appended_segment = &mut self.path[len_path..];
            // The unsafe below is safe as long as b'.' and JSON_PATH_SEGMENT_SEP are
            // valid single byte ut8 strings.
            // By utf-8 design, they cannot be part of another codepoint.
            replace_in_place(b'.', JSON_PATH_SEGMENT_SEP, unsafe {
                appended_segment.as_bytes_mut()
            });
        }
    }

    /// Remove the last segment. Does nothing if the path is empty.
    pub fn pop(&mut self) {
        if let Some(last_idx) = self.indices.pop() {
            self.path.truncate(last_idx);
        }
    }

    /// Clear the path.
    pub fn clear(&mut self) {
        self.path.clear();
        self.indices.clear();
    }

    /// Get the current path.
    pub fn as_str(&self) -> &str {
        &self.path
    }
}

impl From<JsonPathWriter> for String {
    fn from(value: JsonPathWriter) -> Self {
        value.path
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_path_writer_test() {
        let mut writer = JsonPathWriter::new();

        writer.push("root");
        assert_eq!(writer.as_str(), "root");

        writer.push("child");
        assert_eq!(writer.as_str(), "root\u{1}child");

        writer.pop();
        assert_eq!(writer.as_str(), "root");

        writer.push("k8s.node.id");
        assert_eq!(writer.as_str(), "root\u{1}k8s.node.id");

        writer.set_expand_dots(true);
        writer.pop();
        writer.push("k8s.node.id");
        assert_eq!(writer.as_str(), "root\u{1}k8s\u{1}node\u{1}id");
    }
}

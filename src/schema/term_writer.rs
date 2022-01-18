use crate::fastfield::FastValue;
use crate::schema::{Field, Type};
use crate::Term;

// Copyright (C) 2021 Quickwit, Inc.
//
// Quickwit is offered under the AGPL v3.0 and as commercial software.
// For commercial licensing, contact us at hello@quickwit.io.
//
// AGPL:
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.
//

use super::term::JSON_MARKER_BIT;
pub struct JsonTermWriter {
    buffer: Vec<u8>,
    path_stack: Vec<usize>,
}

impl JsonTermWriter {
    pub fn new() -> Self {
        let mut buffer = Vec::with_capacity(100);
        buffer.resize(5, 0);
        let mut path_stack = Vec::with_capacity(10);
        path_stack.push(5);
        Self { buffer, path_stack }
    }

    pub fn with_field(field: Field) -> Self {
        let mut json_term_writer: JsonTermWriter = Self::new();
        json_term_writer.set_field(field);
        json_term_writer
    }

    fn trim_to_end_of_path(&mut self) {
        let end_of_path = *self.path_stack.last().unwrap();
        self.buffer.resize(end_of_path, 0u8);
        self.buffer[end_of_path - 1] = super::term::JSON_PATH_SEGMENT_SEP;
    }

    fn close_path_and_set_type(&mut self, typ: Type) {
        self.trim_to_end_of_path();
        self.buffer[4] = JSON_MARKER_BIT | typ.to_code();
        let end_of_path = self.buffer.len();
        self.buffer[end_of_path - 1] = super::term::JSON_END_OF_PATH;
    }

    pub fn push_path_segment(&mut self, segment: &str) {
        // the path stack should never be empty.
        self.trim_to_end_of_path();
        self.buffer.extend_from_slice(segment.as_bytes());
        self.buffer.push(super::term::JSON_PATH_SEGMENT_SEP);
        self.path_stack.push(self.buffer.len());
    }

    pub fn pop_path_segment(&mut self) {
        self.path_stack.pop();
        assert!(self.path_stack.len() > 0);
        self.trim_to_end_of_path();
    }

    pub fn set_text(&mut self, text: &str) {
        self.close_path_and_set_type(Type::Str);
        self.buffer.extend_from_slice(text.as_bytes());
    }

    pub fn set_i64(&mut self, val: i64) {
        self.close_path_and_set_type(Type::I64);
        self.buffer
            .extend_from_slice(val.to_u64().to_be_bytes().as_slice());
    }

    pub fn set_field(&mut self, field: Field) {
        self.buffer[0..4].copy_from_slice(field.field_id().to_be_bytes().as_ref());
    }

    pub fn term(&self) -> Term<&[u8]> {
        Term::wrap(&self.buffer[..])
    }
}

#[cfg(test)]
mod tests {
    use crate::schema::Field;

    use super::JsonTermWriter;

    #[test]
    fn test_json_writer() {
        let field = Field::from_field_id(1);
        let mut json_writer = JsonTermWriter::with_field(field);
        json_writer.push_path_segment("attributes");
        json_writer.push_path_segment("color");
        json_writer.set_text("red");
        assert_eq!(
            format!("{:?}", json_writer.term()),
            "Term(type=Str, field=1, path=attributes.color, \"red\")"
        );
        json_writer.set_text("blue");
        assert_eq!(
            format!("{:?}", json_writer.term()),
            "Term(type=Str, field=1, path=attributes.color, \"blue\")"
        );
        json_writer.pop_path_segment();
        json_writer.push_path_segment("dimensions");
        json_writer.push_path_segment("width");
        json_writer.set_i64(400);
        assert_eq!(
            format!("{:?}", json_writer.term()),
            "Term(type=I64, field=1, path=attributes.dimensions.width, 400)"
        );
        json_writer.pop_path_segment();
        json_writer.push_path_segment("height");
        json_writer.set_i64(300);
        assert_eq!(
            format!("{:?}", json_writer.term()),
            "Term(type=I64, field=1, path=attributes.dimensions.height, 300)"
        );
    }
}

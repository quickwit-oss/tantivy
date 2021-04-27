use std::io;

use crate::fastfield::serializer::FastFieldSerializer;
use crate::schema::{Document, Field, Value};
use crate::DocId;

/// Writer for byte array (as in, any number of bytes per document) fast fields
///
/// This `BytesFastFieldWriter` is only useful for advanced user.
/// The normal way to get your associated bytes in your index
/// is to
/// - declare your field with fast set to `Cardinality::SingleValue`
/// in your schema
/// - add your document simply by calling `.add_document(...)` with associating bytes to the field.
///
/// The `BytesFastFieldWriter` can be acquired from the
/// fast field writer by calling
/// [`.get_bytes_writer(...)`](./struct.FastFieldsWriter.html#method.get_bytes_writer).
///
/// Once acquired, writing is done by calling `.add_document_val(&[u8])`
/// once per document, even if there are no bytes associated to it.
pub struct BytesFastFieldWriter {
    field: Field,
    vals: Vec<u8>,
    doc_index: Vec<u64>,
}

impl BytesFastFieldWriter {
    /// Creates a new `BytesFastFieldWriter`
    pub fn new(field: Field) -> Self {
        BytesFastFieldWriter {
            field,
            vals: Vec::new(),
            doc_index: Vec::new(),
        }
    }

    /// Access the field associated to the `BytesFastFieldWriter`
    pub fn field(&self) -> Field {
        self.field
    }

    /// Finalize the current document.
    pub(crate) fn next_doc(&mut self) {
        self.doc_index.push(self.vals.len() as u64);
    }

    /// Shift to the next document and add all of the
    /// matching field values present in the document.
    pub fn add_document(&mut self, doc: &Document) {
        self.next_doc();
        for field_value in doc.get_all(self.field) {
            if let Value::Bytes(ref bytes) = field_value {
                self.vals.extend_from_slice(bytes);
                return;
            }
        }
    }

    /// Register the bytes associated to a document.
    ///
    /// The method returns the `DocId` of the document that was
    /// just written.
    pub fn add_document_val(&mut self, val: &[u8]) -> DocId {
        let doc = self.doc_index.len() as DocId;
        self.next_doc();
        self.vals.extend_from_slice(val);
        doc
    }

    /// Serializes the fast field values by pushing them to the `FastFieldSerializer`.
    pub fn serialize(&self, serializer: &mut FastFieldSerializer) -> io::Result<()> {
        // writing the offset index
        let mut doc_index_serializer =
            serializer.new_u64_fast_field_with_idx(self.field, 0, self.vals.len() as u64, 0)?;
        for &offset in &self.doc_index {
            doc_index_serializer.add_val(offset)?;
        }
        doc_index_serializer.add_val(self.vals.len() as u64)?;
        doc_index_serializer.close_field()?;
        // writing the values themselves
        serializer
            .new_bytes_fast_field_with_idx(self.field, 1)
            .write_all(&self.vals)?;
        Ok(())
    }
}

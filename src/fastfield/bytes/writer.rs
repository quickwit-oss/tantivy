use std::io;

use crate::schema::{Document, Field, Value};
use crate::DocId;
use crate::{fastfield::serializer::FastFieldSerializer, indexer::index_sorter::DocidMapping};

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

    /// The memory used (inclusive childs)
    pub fn mem_usage(&self) -> usize {
        self.vals.capacity() + self.doc_index.capacity() * std::mem::size_of::<u64>()
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

    /// Returns an iterator over values per docid in ascending docid order.
    ///
    /// Normally the order is simply iterating self.docid_index.
    /// With docid_map it accounts for the new mapping, returning values in the order of the
    /// new docids.
    fn get_ordered_values<'a: 'b, 'b>(
        &'a self,
        docid_map: Option<&'b DocidMapping>,
    ) -> impl Iterator<Item = &'b [u8]> {
        let docid_iter = if let Some(docid_map) = docid_map {
            Box::new(docid_map.iter().cloned()) as Box<dyn Iterator<Item = u32>>
        } else {
            Box::new(self.doc_index.iter().enumerate().map(|el| el.0 as u32))
                as Box<dyn Iterator<Item = u32>>
        };
        docid_iter.map(move |docid| self.get_values_for_docid(docid))
    }

    /// returns all values for a docids
    fn get_values_for_docid(&self, docid: u32) -> &[u8] {
        let start_pos = self.doc_index[docid as usize] as usize;
        let end_pos = if docid as usize + 1 == self.doc_index.len() {
            // special case, last docid has no offset information
            self.vals.len()
        } else {
            self.doc_index[docid as usize + 1] as usize
        };
        &self.vals[start_pos..end_pos]
    }

    /// Serializes the fast field values by pushing them to the `FastFieldSerializer`.
    pub fn serialize(
        &self,
        serializer: &mut FastFieldSerializer,
        docid_map: Option<&DocidMapping>,
    ) -> io::Result<()> {
        // writing the offset index
        let mut doc_index_serializer =
            serializer.new_u64_fast_field_with_idx(self.field, 0, self.vals.len() as u64, 0)?;
        let mut offset = 0;
        for vals in self.get_ordered_values(docid_map) {
            doc_index_serializer.add_val(offset)?;
            offset += vals.len() as u64;
        }
        doc_index_serializer.add_val(self.vals.len() as u64)?;
        doc_index_serializer.close_field()?;
        // writing the values themselves
        let mut value_serializer = serializer.new_bytes_fast_field_with_idx(self.field, 1);
        // the else could be removed, but this is faster (difference not benchmarked)
        if let Some(docid_map) = docid_map {
            for vals in self.get_ordered_values(Some(docid_map)) {
                // sort values in case of remapped docids?
                value_serializer.write_all(vals)?;
            }
        } else {
            value_serializer.write_all(&self.vals)?;
        }
        Ok(())
    }
}

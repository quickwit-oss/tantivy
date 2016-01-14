use core::global::DocId;
use core::schema::Field;

// Trait sufficient to serialize a segment.
pub trait SerializableSegment<'a> {
    type TFieldCur: FieldCursor<'a>;
    fn field_cursor(&'a self) -> Self::TFieldCur;
}

pub trait DocCursor: Iterator<Item=DocId> {
    fn doc(&self) -> DocId;
}

pub trait TermCursor<'a>: Iterator<Item=&'a String> {
    type TDocCur: DocCursor;
    fn get_term(&self) -> &'a String;
    fn doc_cursor(&self) -> Self::TDocCur;
}

pub trait FieldCursor<'a>: Iterator<Item=&'a Field> {
    type TTermCur: TermCursor<'a>;
    fn get_field(&self) -> Option<&'a Field>;
    fn term_cursor(&'a self) -> Self::TTermCur;
}

use core::global::*;
use core::schema::*;

// Trait sufficient to serialize a segment.
pub trait SerializableSegment<'a> {
    type TermCur: TermCursor; // TODO rename TermCursorImpl
    fn term_cursor(&'a self) -> Self::TermCur;
}

pub trait DocCursor: Iterator<Item=DocId> {
    fn doc(&self) -> DocId;
    fn len(&self) -> DocId;
}

// TODO make iteration over Fields somehow sorted

pub trait TermCursor  {
    type DocCur: DocCursor;
    fn next(&mut self,) -> Option<(Term, Self::DocCur)>;
}

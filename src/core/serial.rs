use core::global::*;
use core::schema::*;

// Trait sufficient to serialize a segment.
pub trait SerializableSegment<'a> {
    type TermCur: TermCursor<'a>; // TODO rename TermCursorImpl
    fn term_cursor(&'a self) -> Self::TermCur;
}

pub trait DocCursor: Iterator<Item=DocId> {
    fn doc(&self) -> DocId;
    fn len(&self) -> usize;
}

// TODO make iteration over Fields somehow sorted

pub trait TermCursor<'a>  {
    type DocCur: DocCursor;
    fn next(&mut self,) -> Option<(Term<'a>, Self::DocCur)>;
}

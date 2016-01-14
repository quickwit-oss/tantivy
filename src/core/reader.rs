use core::directory::Directory;
use core::global::DocId;
use core::schema::Field;



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

pub trait IndexFlushable<'a> {
    type TFieldCur: FieldCursor<'a>;
    fn field_cursor(&'a self) -> Self::TFieldCur;
}

pub struct SegmentIndexReader {
    directory: Directory,
}

//
// impl SegmentIndexReader {
//
//     pub fn open(directory: &Directory) -> IndexReader {
// 		IndexReader {
// 			directory: (*directory).clone(),
//         }
//     }
//
// }

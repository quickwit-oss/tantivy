use core::directory::Directory;
use core::global::DocId;
use core::schema::Field;


//
// pub trait DocCursor {
//     fn get(&self) -> DocId;
//     fn next(&self) -> bool;
// }

// pub trait TermCursor {
//     // fn doc_cursor<'a>(&'a self) -> Box<'a, DocEnum>;
//     fn get(&self) -> &str;
//     fn next(&self) -> bool;
// }


// term is not empty
// field


pub trait DocCursor<'a>: Iterator<Item=DocId> {
    fn doc(&self) -> DocId;
}

pub trait TermCursor<'a>: Iterator<Item=&'a String> {
    fn get_term(&self) -> &'a String;
    fn doc_cursor<'b>(&'b self) -> Box<DocCursor<Item=DocId> + 'b>;
}

pub trait FieldCursor<'a>: Iterator<Item=&'a Field> {
    fn get_field(&self) -> Option<&'a Field>;
    fn term_cursor<'b>(&'b self) -> Box<TermCursor<Item=&'b String> + 'b>;
}

pub trait IndexFlushable {
    fn field_cursor<'a>(&'a self) -> Box<FieldCursor<Item=&'a Field> + 'a>;
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

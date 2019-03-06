use schema::IntOptions;
use schema::TextOptions;
use std::ops::BitOr;

/// A stored fields of a document can be retrieved given its `DocId`.
/// Stored field are stored together and LZ4 compressed.
/// Reading the stored fields of a document is relatively slow.
/// (100 microsecs)
#[derive(Clone)]
pub struct STORED_FLAG;
impl SchemaFlag for STORED_FLAG {}
pub const STORED: SchemaFlagList<STORED_FLAG, ()> = SchemaFlagList {
    head: STORED_FLAG,
    tail: (),
};

#[derive(Clone)]
pub struct INDEXED_FLAG;
impl SchemaFlag for INDEXED_FLAG {}
pub const INDEXED: SchemaFlagList<INDEXED_FLAG, ()> = SchemaFlagList {
    head: INDEXED_FLAG,
    tail: (),
};

#[derive(Clone)]
pub struct FAST_FLAG;
impl SchemaFlag for FAST_FLAG {}
pub const FAST: SchemaFlagList<FAST_FLAG, ()> = SchemaFlagList {
    head: FAST_FLAG,
    tail: (),
};

pub trait SchemaFlag: Clone {}

impl<Left: SchemaFlag, Right: SchemaFlag> BitOr<Right> for SchemaFlagList<Left, ()> {
    type Output = SchemaFlagList<Left, Right>;

    fn bitor(self, rhs: Right) -> Self::Output {
        SchemaFlagList {
            head: self.head.clone(),
            tail: rhs.clone(),
        }
    }
}

impl<Head, OldHead, OldTail> BitOr<SchemaFlagList<Head, ()>> for SchemaFlagList<OldHead, OldTail>
    where Head: Clone, OldHead: Clone, OldTail: Clone {
    type Output = SchemaFlagList<Head, SchemaFlagList<OldHead, OldTail>>;

    fn bitor(self, head: SchemaFlagList<Head, ()>) -> Self::Output {
        SchemaFlagList {
            head: head.head,
            tail: self.clone(),
        }
    }
}

impl<T: Clone + Into<IntOptions>> BitOr<IntOptions> for SchemaFlagList<T, ()>  {
    type Output = IntOptions;

    fn bitor(self, rhs: IntOptions) -> Self::Output {
        self.head.into() | rhs
    }
}

impl<T: Clone + Into<TextOptions>> BitOr<TextOptions> for SchemaFlagList<T, ()>  {
    type Output = TextOptions;

    fn bitor(self, rhs: TextOptions) -> Self::Output {
        self.head.into() | rhs
    }
}

#[derive(Clone)]
pub struct SchemaFlagList<Head: Clone, Tail: Clone> {
    pub head: Head,
    pub tail: Tail,
}

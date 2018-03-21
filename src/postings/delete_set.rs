use fastfield::DeleteBitSet;
use DocId;

pub trait DeleteSet: 'static + From<Option<DeleteBitSet>> {
    fn is_deleted(&self, doc: DocId) -> bool;
    fn empty() -> Self;
}


#[derive(Default)]
pub struct NoDelete;
impl DeleteSet for NoDelete {
    #[inline(always)]
    fn is_deleted(&self, _doc: DocId) -> bool {
        false
    }
    fn empty() -> Self {
        NoDelete
    }
}

impl From<Option<DeleteBitSet>> for NoDelete {
    fn from(delete_bitset_opt: Option<DeleteBitSet>) -> Self {
        assert!(delete_bitset_opt.is_none(), "NoDelete should not be used if there are some deleted documents.");
        NoDelete
    }
}
use crate::{Column, DocId, RowId};

#[derive(Debug, Default, Clone)]
pub struct ColumnBlockAccessor<T> {
    val_cache: Vec<T>,
    docid_cache: Vec<DocId>,
    row_id_cache: Vec<RowId>,
}

impl<T: PartialOrd + Copy + std::fmt::Debug + Send + Sync + 'static + Default>
    ColumnBlockAccessor<T>
{
    #[inline]
    pub fn fetch_block(&mut self, docs: &[u32], accessor: &Column<T>) {
        self.docid_cache.clear();
        self.row_id_cache.clear();
        accessor.row_ids_for_docs(docs, &mut self.docid_cache, &mut self.row_id_cache);
        self.val_cache.resize(self.row_id_cache.len(), T::default());
        accessor
            .values
            .get_vals(&self.row_id_cache, &mut self.val_cache);
    }

    #[inline]
    pub fn iter_vals(&self) -> impl Iterator<Item = T> + '_ {
        self.val_cache.iter().cloned()
    }

    #[inline]
    pub fn iter_docid_vals(&self) -> impl Iterator<Item = (DocId, T)> + '_ {
        self.docid_cache
            .iter()
            .cloned()
            .zip(self.val_cache.iter().cloned())
    }
}

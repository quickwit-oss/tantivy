use crate::{Column, DocId, RowId};

#[derive(Debug, Default, Clone)]
pub struct ColumnBlockAccessor<T> {
    val_cache: Vec<T>,
    docid_cache: Vec<DocId>,
    missing_docids_cache: Vec<DocId>,
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
    pub fn fetch_block_with_missing(&mut self, docs: &[u32], accessor: &Column<T>, missing: T) {
        self.fetch_block(docs, accessor);
        // We can compare docid_cache with docs to find missing docs
        if docs.len() != self.docid_cache.len() {
            self.missing_docids_cache.clear();
            find_missing_docs(docs, &self.docid_cache, |doc| {
                self.missing_docids_cache.push(doc);
                self.val_cache.push(missing);
            });
            self.docid_cache
                .extend_from_slice(&self.missing_docids_cache);
        }
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

fn find_missing_docs<F>(docs: &[u32], hits: &[u32], mut callback: F)
where F: FnMut(u32) {
    let mut docs_iter = docs.iter();
    let mut hits_iter = hits.iter();

    let mut doc = docs_iter.next();
    let mut hit = hits_iter.next();

    while let (Some(&current_doc), Some(&current_hit)) = (doc, hit) {
        if current_doc < current_hit {
            callback(current_doc);
            doc = docs_iter.next();
        } else if current_doc == current_hit {
            doc = docs_iter.next();
            hit = hits_iter.next();
        } else {
            hit = hits_iter.next();
        }
    }

    while let Some(&current_doc) = doc {
        callback(current_doc);
        doc = docs_iter.next();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_find_missing_docs() {
        let docs: Vec<u32> = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        let hits: Vec<u32> = vec![2, 4, 6, 8, 10];

        let mut missing_docs: Vec<u32> = Vec::new();

        find_missing_docs(&docs, &hits, |missing_doc| {
            missing_docs.push(missing_doc);
        });

        assert_eq!(missing_docs, vec![1, 3, 5, 7, 9]);
    }

    #[test]
    fn test_find_missing_docs_empty() {
        let docs: Vec<u32> = Vec::new();
        let hits: Vec<u32> = vec![2, 4, 6, 8, 10];

        let mut missing_docs: Vec<u32> = Vec::new();

        find_missing_docs(&docs, &hits, |missing_doc| {
            missing_docs.push(missing_doc);
        });

        assert_eq!(missing_docs, vec![]);
    }

    #[test]
    fn test_find_missing_docs_all_missing() {
        let docs: Vec<u32> = vec![1, 2, 3, 4, 5];
        let hits: Vec<u32> = Vec::new();

        let mut missing_docs: Vec<u32> = Vec::new();

        find_missing_docs(&docs, &hits, |missing_doc| {
            missing_docs.push(missing_doc);
        });

        assert_eq!(missing_docs, vec![1, 2, 3, 4, 5]);
    }
}

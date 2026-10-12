use std::io;
use std::sync::Arc;

use columnar::{Cardinality, ColumnType, RowId, StrColumn};
use rustc_hash::FxHashMap;

use super::{ValueSource, ValueSourceDictionary};
use crate::DocId;

/// IDs belong to this query source, not to any OnPair symbol dictionary.
/// They stay stable across groups and are deliberately not lexicographically ordered.
#[derive(Debug)]
pub(crate) struct CompactStrValueSource {
    column: StrColumn,
    terms: Vec<Arc<[u8]>>,
    ids: FxHashMap<Arc<[u8]>, u64>,
    buffer: Vec<u8>,
    term_bytes: usize,
}

impl CompactStrValueSource {
    pub(crate) fn new(column: StrColumn) -> Self {
        Self {
            column,
            terms: Vec::new(),
            ids: FxHashMap::default(),
            buffer: Vec::new(),
            term_bytes: 0,
        }
    }
}

impl ValueSource for CompactStrValueSource {
    fn column_type(&self) -> ColumnType {
        ColumnType::Str
    }

    fn load_block(
        &mut self,
        docs: &[DocId],
        values: &mut Vec<u64>,
        docids: &mut Vec<DocId>,
        row_ids: &mut Vec<RowId>,
    ) -> Cardinality {
        self.try_load_block(docs, values, docids, row_ids)
            .expect("compact storage read failed")
    }

    fn try_load_block(
        &mut self,
        docs: &[DocId],
        values: &mut Vec<u64>,
        docids: &mut Vec<DocId>,
        _row_ids: &mut Vec<RowId>,
    ) -> io::Result<Cardinality> {
        values.clear();
        docids.clear();
        for &doc in docs {
            for value in self.column.value_range(doc)? {
                self.column.read_value(value, &mut self.buffer)?;
                let id = if let Some(&id) = self.ids.get(self.buffer.as_slice()) {
                    id
                } else {
                    let id = self.terms.len() as u64;
                    let term: Arc<[u8]> = Arc::from(self.buffer.as_slice());
                    self.term_bytes += term.len() + 2 * std::mem::size_of::<usize>();
                    self.terms.push(term.clone());
                    self.ids.insert(term, id);
                    id
                };
                values.push(id);
                docids.push(doc);
            }
        }
        Ok(self.column.index().get_cardinality())
    }

    fn as_compact_str_column(&self) -> Option<&StrColumn> {
        Some(&self.column)
    }
    fn intern_string(&mut self, value: &str) -> Option<u64> {
        if let Some(&id) = self.ids.get(value.as_bytes()) {
            return Some(id);
        }
        let id = self.terms.len() as u64;
        let term: Arc<[u8]> = Arc::from(value.as_bytes());
        self.term_bytes += term.len() + 2 * std::mem::size_of::<usize>();
        self.terms.push(term.clone());
        self.ids.insert(term, id);
        Some(id)
    }
    fn term_dictionary(&self) -> Option<&dyn ValueSourceDictionary> {
        Some(self)
    }
    fn memory_usage(&self) -> usize {
        self.term_bytes
            + self.buffer.capacity()
            + self.terms.capacity() * std::mem::size_of::<Arc<[u8]>>()
            + self.ids.capacity() * (std::mem::size_of::<(Arc<[u8]>, u64)>() + 1)
    }
}

impl ValueSourceDictionary for CompactStrValueSource {
    fn ords_sorted_with_terms(&self) -> bool {
        false
    }
    fn num_terms(&self) -> u64 {
        self.terms.len() as u64
    }
    fn sorted_ords_to_term_cb(
        &self,
        ids: &[u64],
        callback: &mut dyn FnMut(&[u8]),
    ) -> io::Result<bool> {
        for &id in ids {
            let Some(term) = self.terms.get(id as usize) else {
                return Ok(false);
            };
            callback(term);
        }
        Ok(true)
    }
}

/// Value count only needs the document index. Synthetic zeros preserve multiplicity.
#[derive(Debug)]
pub(crate) struct CompactCountValueSource(pub(crate) StrColumn);

impl ValueSource for CompactCountValueSource {
    fn column_type(&self) -> ColumnType {
        ColumnType::U64
    }
    fn load_block(
        &mut self,
        docs: &[DocId],
        values: &mut Vec<u64>,
        docids: &mut Vec<DocId>,
        row_ids: &mut Vec<RowId>,
    ) -> Cardinality {
        self.try_load_block(docs, values, docids, row_ids)
            .expect("compact count index read failed")
    }
    fn try_load_block(
        &mut self,
        docs: &[DocId],
        values: &mut Vec<u64>,
        docids: &mut Vec<DocId>,
        _row_ids: &mut Vec<RowId>,
    ) -> io::Result<Cardinality> {
        values.clear();
        docids.clear();
        for &doc in docs {
            let count = self.0.value_range(doc)?.len();
            values.resize(values.len() + count, 0);
            docids.resize(docids.len() + count, doc);
        }
        Ok(self.0.index().get_cardinality())
    }
}

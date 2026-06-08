use std::io;
use std::io::Write;
use std::ops::Range;
use std::sync::Arc;

use common::{CountingWriter, OwnedBytes};

use super::optional_index::{open_optional_index, serialize_optional_index};
use super::{OptionalIndex, SerializableOptionalIndex, Set};
use crate::column_values::{
    CodecType, ColumnValues, load_u64_based_column_values, serialize_u64_based_column_values,
};
use crate::iterable::Iterable;
use crate::{DocId, RowId, Version};

/// Advances `start` forward to the smallest index `d` in `[start, max_idx]` with
/// `col.get_val(d + 1) > pos`, using exponential search into binary search.
///
/// Precondition: `start <= max_idx` and `col.get_val(start + 1) <= pos`.
fn exponential_search_first_end_gt(
    col: &dyn ColumnValues<RowId>,
    start: DocId,
    pos: RowId,
    max_idx: DocId,
) -> DocId {
    debug_assert!(start <= max_idx);
    debug_assert!(cumulative_end_for_doc_idx(col, start) <= pos);
    let mut lo = start;
    let mut step = 1u32;
    loop {
        let next = lo.saturating_add(step).min(max_idx);
        if cumulative_end_for_doc_idx(col, next) > pos {
            if next == lo + 1 {
                return next;
            }
            return binary_search_first_end_gt(col, lo, next, pos);
        }
        if next == max_idx {
            return max_idx;
        }
        lo = next;
        step = step.saturating_mul(2);
    }
}

fn cumulative_end_for_doc_idx(col: &dyn ColumnValues<RowId>, doc_idx: DocId) -> RowId {
    col.get_val(doc_idx + 1)
}

/// Smallest `idx` in `[lo + 1, hi]` with `col.get_val(idx + 1) > pos`.
///
/// Monotone **binary search** for the **first index where the predicate holds**.
///
/// Precondition: `col.get_val(lo + 1) <= pos` and `col.get_val(hi + 1) > pos`.
fn binary_search_first_end_gt(
    col: &dyn ColumnValues<RowId>,
    lo: DocId,
    hi: DocId,
    pos: RowId,
) -> DocId {
    debug_assert!(lo < hi);
    debug_assert!(cumulative_end_for_doc_idx(col, lo) <= pos);
    debug_assert!(cumulative_end_for_doc_idx(col, hi) > pos);
    let mut left = lo + 1;
    let mut right = hi;
    while left < right {
        let mid = left + (right - left) / 2;
        if cumulative_end_for_doc_idx(col, mid) > pos {
            right = mid;
        } else {
            left = mid + 1;
        }
    }
    left
}

pub struct SerializableMultivalueIndex<'a> {
    pub doc_ids_with_values: SerializableOptionalIndex<'a>,
    pub start_offsets: Box<dyn Iterable<u32> + 'a>,
}

pub fn serialize_multivalued_index(
    multivalued_index: &SerializableMultivalueIndex,
    output: &mut impl Write,
) -> io::Result<()> {
    let SerializableMultivalueIndex {
        doc_ids_with_values,
        start_offsets,
    } = multivalued_index;
    let mut count_writer = CountingWriter::wrap(output);
    let SerializableOptionalIndex {
        non_null_row_ids,
        num_rows,
    } = doc_ids_with_values;
    serialize_optional_index(&**non_null_row_ids, *num_rows, &mut count_writer)?;
    let optional_len = count_writer.written_bytes() as u32;
    let output = count_writer.finish();
    serialize_u64_based_column_values(
        &**start_offsets,
        &[CodecType::Bitpacked, CodecType::Linear],
        output,
    )?;
    output.write_all(&optional_len.to_le_bytes())?;
    Ok(())
}

pub fn open_multivalued_index(
    bytes: OwnedBytes,
    format_version: Version,
) -> io::Result<MultiValueIndex> {
    match format_version {
        Version::V1 => {
            let start_index_column: Arc<dyn ColumnValues<RowId>> =
                load_u64_based_column_values(bytes)?;
            Ok(MultiValueIndex::MultiValueIndexV1(MultiValueIndexV1 {
                start_index_column,
            }))
        }
        Version::V2 => {
            let (body_bytes, optional_index_len) = bytes.rsplit(4);
            let optional_index_len =
                u32::from_le_bytes(optional_index_len.as_slice().try_into().unwrap());
            let (optional_index_bytes, start_index_bytes) =
                body_bytes.split(optional_index_len as usize);
            let optional_index = open_optional_index(optional_index_bytes)?;
            let start_index_column: Arc<dyn ColumnValues<RowId>> =
                load_u64_based_column_values(start_index_bytes)?;
            Ok(MultiValueIndex::MultiValueIndexV2(MultiValueIndexV2 {
                optional_index,
                start_index_column,
            }))
        }
    }
}

#[derive(Clone)]
/// Index to resolve value range for given doc_id.
/// Starts at 0.
pub enum MultiValueIndex {
    MultiValueIndexV1(MultiValueIndexV1),
    MultiValueIndexV2(MultiValueIndexV2),
}

#[derive(Clone)]
/// Index to resolve value range for given doc_id.
/// Starts at 0.
pub struct MultiValueIndexV1 {
    pub start_index_column: Arc<dyn crate::ColumnValues<RowId>>,
}

impl MultiValueIndexV1 {
    /// Returns `[start, end)`, such that the values associated with
    /// the given document are `start..end`.
    #[inline]
    pub(crate) fn range(&self, doc_id: DocId) -> Range<RowId> {
        if doc_id >= self.num_docs() {
            return 0..0;
        }
        let start = self.start_index_column.get_val(doc_id);
        let end = self.start_index_column.get_val(doc_id + 1);
        start..end
    }

    /// Returns the number of documents in the index.
    #[inline]
    pub fn num_docs(&self) -> u32 {
        self.start_index_column.num_vals() - 1
    }

    /// Converts a list of ranks (row ids of values) in a 1:n index to the corresponding list of
    /// document ids. `ranks` is updated in place.
    ///
    /// Correctness: positions need to be sorted; they must be monotonically increasing row ids.
    ///
    /// Requires `start_index_column.num_vals >= 2` (`num_docs >= 1` here, since `num_docs =
    /// num_vals - 1`). The previous linear scan had the same requirement because it also called
    /// `get_val(d + 1)`.
    pub(crate) fn select_batch_in_place(&self, docid_start: DocId, ranks: &mut Vec<u32>) {
        if ranks.is_empty() {
            return;
        }
        let col = self.start_index_column.as_ref();
        debug_assert!(
            col.num_vals() >= 2,
            "multivalue cumulative column must have num_vals >= 2 (num_docs >= 1)"
        );
        let mut cur_doc = docid_start;
        let mut last_doc = None;

        assert!(col.get_val(docid_start) <= ranks[0]);

        let mut write_doc_pos = 0;
        for i in 0..ranks.len() {
            let pos = ranks[i];
            if cumulative_end_for_doc_idx(col, cur_doc) <= pos {
                cur_doc = exponential_search_first_end_gt(
                    col,
                    cur_doc,
                    pos,
                    self.num_docs().saturating_sub(1),
                );
            }
            ranks[write_doc_pos] = cur_doc;
            write_doc_pos += if last_doc == Some(cur_doc) { 0 } else { 1 };
            last_doc = Some(cur_doc);
        }
        ranks.truncate(write_doc_pos);
    }
}

#[derive(Clone)]
/// Index to resolve value range for given doc_id.
/// Starts at 0.
pub struct MultiValueIndexV2 {
    pub optional_index: OptionalIndex,
    pub start_index_column: Arc<dyn crate::ColumnValues<RowId>>,
}

impl std::fmt::Debug for MultiValueIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let index = match self {
            MultiValueIndex::MultiValueIndexV1(idx) => &idx.start_index_column,
            MultiValueIndex::MultiValueIndexV2(idx) => &idx.start_index_column,
        };
        f.debug_struct("MultiValuedIndex")
            .field("num_rows", &index.num_vals())
            .finish_non_exhaustive()
    }
}

impl MultiValueIndex {
    pub fn for_test(start_offsets: &[RowId]) -> MultiValueIndex {
        assert!(!start_offsets.is_empty());
        assert_eq!(start_offsets[0], 0);
        let mut doc_with_values = Vec::new();
        let mut compact_start_offsets: Vec<u32> = vec![0];
        for doc in 0..start_offsets.len() - 1 {
            if start_offsets[doc] < start_offsets[doc + 1] {
                doc_with_values.push(doc as RowId);
                compact_start_offsets.push(start_offsets[doc + 1]);
            }
        }
        let serializable_multivalued_index = SerializableMultivalueIndex {
            doc_ids_with_values: SerializableOptionalIndex {
                non_null_row_ids: Box::new(&doc_with_values[..]),
                num_rows: start_offsets.len() as u32 - 1,
            },
            start_offsets: Box::new(&compact_start_offsets[..]),
        };
        let mut buffer = Vec::new();
        serialize_multivalued_index(&serializable_multivalued_index, &mut buffer).unwrap();
        let bytes = OwnedBytes::new(buffer);
        open_multivalued_index(bytes, Version::V2).unwrap()
    }

    pub fn get_start_index_column(&self) -> &Arc<dyn crate::ColumnValues<RowId>> {
        match self {
            MultiValueIndex::MultiValueIndexV1(idx) => &idx.start_index_column,
            MultiValueIndex::MultiValueIndexV2(idx) => &idx.start_index_column,
        }
    }

    /// Returns `[start, end)` values range, such that the values associated with
    /// the given document are `start..end`.
    #[inline]
    pub(crate) fn range(&self, doc_id: DocId) -> Range<RowId> {
        match self {
            MultiValueIndex::MultiValueIndexV1(idx) => idx.range(doc_id),
            MultiValueIndex::MultiValueIndexV2(idx) => idx.range(doc_id),
        }
    }

    /// Returns the number of documents in the index.
    #[inline]
    pub fn num_docs(&self) -> u32 {
        match self {
            MultiValueIndex::MultiValueIndexV1(idx) => idx.start_index_column.num_vals() - 1,
            MultiValueIndex::MultiValueIndexV2(idx) => idx.optional_index.num_docs(),
        }
    }

    /// Returns an iterator over document ids that have at least one value.
    pub fn iter_non_null_docs(&self) -> Box<dyn Iterator<Item = DocId> + '_> {
        match self {
            MultiValueIndex::MultiValueIndexV1(idx) => {
                let mut doc: DocId = 0u32;
                let num_docs = idx.num_docs();
                Box::new(std::iter::from_fn(move || {
                    // This is not the most efficient way to do this, but it's legacy code.
                    while doc < num_docs {
                        let cur = doc;
                        doc += 1;
                        let start = idx.start_index_column.get_val(cur);
                        let end = idx.start_index_column.get_val(cur + 1);
                        if end > start {
                            return Some(cur);
                        }
                    }
                    None
                }))
            }
            MultiValueIndex::MultiValueIndexV2(idx) => {
                Box::new(idx.optional_index.iter_non_null_docs())
            }
        }
    }

    /// Converts a list of ranks (row ids of values) in a 1:n index to the corresponding list of
    /// docids. Positions are converted inplace to docids.
    ///
    /// Correctness: positions needs to be sorted. idx_reader needs to contain monotonically
    /// increasing positions.
    pub(crate) fn select_batch_in_place(&self, docid_start: DocId, ranks: &mut Vec<u32>) {
        match self {
            MultiValueIndex::MultiValueIndexV1(idx) => {
                idx.select_batch_in_place(docid_start, ranks)
            }
            MultiValueIndex::MultiValueIndexV2(idx) => {
                idx.select_batch_in_place(docid_start, ranks)
            }
        }
    }
}
impl MultiValueIndexV2 {
    /// Returns `[start, end)`, such that the values associated with
    /// the given document are `start..end`.
    #[inline]
    pub(crate) fn range(&self, doc_id: DocId) -> Range<RowId> {
        let Some(rank) = self.optional_index.rank_if_exists(doc_id) else {
            return 0..0;
        };
        let start = self.start_index_column.get_val(rank);
        let end = self.start_index_column.get_val(rank + 1);
        start..end
    }

    /// Returns the number of documents in the index.
    #[inline]
    pub fn num_docs(&self) -> u32 {
        self.optional_index.num_docs()
    }

    /// Converts a list of ranks (row ids of values) in a 1:n index to the corresponding list of
    /// document ids. `ranks` is updated in place.
    ///
    /// Correctness: positions need to be sorted; they must be monotonically increasing row ids.
    ///
    /// The compact cumulative column must have `num_vals >= 2` to read `get_val(rank + 1)` (same as
    /// the old linear scan).
    pub(crate) fn select_batch_in_place(&self, docid_start: DocId, ranks: &mut Vec<u32>) {
        if ranks.is_empty() {
            return;
        }
        let col = self.start_index_column.as_ref();
        debug_assert!(
            col.num_vals() >= 2,
            "multivalue cumulative column must have num_vals >= 2"
        );
        let max_rank = col.num_vals() - 2;
        let mut cur_pos_in_idx = self.optional_index.rank(docid_start);
        let mut last_doc = None;

        assert!(cur_pos_in_idx <= ranks[0]);

        let mut write_doc_pos = 0;
        for i in 0..ranks.len() {
            let pos = ranks[i];
            if cumulative_end_for_doc_idx(col, cur_pos_in_idx) <= pos {
                cur_pos_in_idx =
                    exponential_search_first_end_gt(col, cur_pos_in_idx, pos, max_rank);
            }
            ranks[write_doc_pos] = cur_pos_in_idx;
            write_doc_pos += if last_doc == Some(cur_pos_in_idx) {
                0
            } else {
                1
            };
            last_doc = Some(cur_pos_in_idx);
        }
        ranks.truncate(write_doc_pos);

        for rank in ranks.iter_mut() {
            *rank = self.optional_index.select(*rank);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Range;

    use common::OwnedBytes;
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};

    use super::{MultiValueIndex, MultiValueIndexV1, MultiValueIndexV2};
    use crate::column_index::Set;
    use crate::column_values::{
        CodecType, load_u64_based_column_values, serialize_u64_based_column_values,
    };
    use crate::{ColumnarReader, DocId, DynamicColumn, RowId};

    fn index_to_pos_helper(
        index: &MultiValueIndex,
        doc_id_range: Range<u32>,
        positions: &[u32],
    ) -> Vec<u32> {
        let mut positions = positions.to_vec();
        index.select_batch_in_place(doc_id_range.start, &mut positions);
        positions
    }

    fn multivalue_v1_for_test(offsets: &[RowId]) -> MultiValueIndexV1 {
        let mut buf = Vec::new();
        let vals: Vec<RowId> = offsets.to_vec();
        serialize_u64_based_column_values(
            &&vals[..],
            &[CodecType::Bitpacked, CodecType::Linear],
            &mut buf,
        )
        .unwrap();
        let col = load_u64_based_column_values(OwnedBytes::new(buf)).unwrap();
        MultiValueIndexV1 {
            start_index_column: col,
        }
    }

    /// Reference: previous one-doc-at-a-time forward scan.
    fn select_batch_v1_linear_scan(
        idx: &MultiValueIndexV1,
        docid_start: DocId,
        ranks: &mut Vec<u32>,
    ) {
        if ranks.is_empty() {
            return;
        }
        let col = idx.start_index_column.as_ref();
        let mut cur_doc = docid_start;
        let mut last_doc = None;
        assert!(col.get_val(docid_start) <= ranks[0]);
        let mut write_doc_pos = 0;
        for i in 0..ranks.len() {
            let pos = ranks[i];
            loop {
                let end = col.get_val(cur_doc + 1);
                if end > pos {
                    ranks[write_doc_pos] = cur_doc;
                    write_doc_pos += if last_doc == Some(cur_doc) { 0 } else { 1 };
                    last_doc = Some(cur_doc);
                    break;
                }
                cur_doc += 1;
            }
        }
        ranks.truncate(write_doc_pos);
    }

    fn select_batch_v2_linear_scan(
        idx: &MultiValueIndexV2,
        docid_start: DocId,
        ranks: &mut Vec<u32>,
    ) {
        if ranks.is_empty() {
            return;
        }
        let col = idx.start_index_column.as_ref();
        let mut cur_pos_in_idx = idx.optional_index.rank(docid_start);
        let mut last_doc = None;
        assert!(cur_pos_in_idx <= ranks[0]);
        let mut write_doc_pos = 0;
        for i in 0..ranks.len() {
            let pos = ranks[i];
            loop {
                let end = col.get_val(cur_pos_in_idx + 1);
                if end > pos {
                    ranks[write_doc_pos] = cur_pos_in_idx;
                    write_doc_pos += if last_doc == Some(cur_pos_in_idx) {
                        0
                    } else {
                        1
                    };
                    last_doc = Some(cur_pos_in_idx);
                    break;
                }
                cur_pos_in_idx += 1;
            }
        }
        ranks.truncate(write_doc_pos);
        for rank in ranks.iter_mut() {
            *rank = idx.optional_index.select(*rank);
        }
    }

    #[test]
    fn test_positions_to_docid() {
        let index = MultiValueIndex::for_test(&[0, 10, 12, 15, 22, 23]);
        assert_eq!(index.num_docs(), 5);
        let positions = &[10u32, 11, 15, 20, 21, 22];
        assert_eq!(index_to_pos_helper(&index, 0..5, positions), vec![1, 3, 4]);
        assert_eq!(index_to_pos_helper(&index, 1..5, positions), vec![1, 3, 4]);

        assert_eq!(index_to_pos_helper(&index, 0..5, &[9]), vec![0]);
        assert_eq!(index_to_pos_helper(&index, 1..5, &[10]), vec![1]);
        assert_eq!(index_to_pos_helper(&index, 1..5, &[11]), vec![1]);
        assert_eq!(index_to_pos_helper(&index, 2..5, &[12]), vec![2]);
        assert_eq!(index_to_pos_helper(&index, 2..5, &[12, 14]), vec![2]);
        assert_eq!(index_to_pos_helper(&index, 2..5, &[12, 14, 15]), vec![2, 3]);
    }

    #[test]
    fn test_range_to_rowids() {
        use crate::ColumnarWriter;

        let mut columnar_writer = ColumnarWriter::default();

        // This column gets coerced to u64
        columnar_writer.record_numerical(1, "full", u64::MAX);
        columnar_writer.record_numerical(1, "full", u64::MAX);

        columnar_writer.record_numerical(5, "full", u64::MAX);
        columnar_writer.record_numerical(5, "full", u64::MAX);

        let mut wrt: Vec<u8> = Vec::new();
        columnar_writer.serialize(7, &mut wrt).unwrap();

        let reader = ColumnarReader::open(wrt).unwrap();
        // Open the column as u64
        let column = reader.read_columns("full").unwrap()[0]
            .open()
            .unwrap()
            .coerce_numerical(crate::NumericalType::U64)
            .unwrap();
        let DynamicColumn::U64(column) = column else {
            panic!();
        };

        let row_id_range = column.index.docid_range_to_rowids(1..2);
        assert_eq!(row_id_range, 0..2);

        let row_id_range = column.index.docid_range_to_rowids(0..2);
        assert_eq!(row_id_range, 0..2);

        let row_id_range = column.index.docid_range_to_rowids(0..4);
        assert_eq!(row_id_range, 0..2);

        let row_id_range = column.index.docid_range_to_rowids(3..4);
        assert_eq!(row_id_range, 2..2);

        let row_id_range = column.index.docid_range_to_rowids(1..6);
        assert_eq!(row_id_range, 0..4);

        let row_id_range = column.index.docid_range_to_rowids(3..6);
        assert_eq!(row_id_range, 2..4);

        let row_id_range = column.index.docid_range_to_rowids(0..6);
        assert_eq!(row_id_range, 0..4);

        let row_id_range = column.index.docid_range_to_rowids(0..6);
        assert_eq!(row_id_range, 0..4);

        let check = |range, expected| {
            let full_range = 0..=u64::MAX;
            let mut docids = Vec::new();
            column.get_docids_for_value_range(full_range, range, &mut docids);
            assert_eq!(docids, expected);
        };

        // check(0..1, vec![]);
        // check(0..2, vec![1]);
        check(1..2, vec![1]);
    }

    #[test]
    fn select_batch_matches_linear_scan_v1() {
        let offsets = &[0u32, 0, 100, 150, 150, 200];
        let idx = multivalue_v1_for_test(offsets);
        let num_docs = offsets.len() as u32 - 1;
        let rank_lists: &[&[u32]] = &[
            &[0u32],
            &[50, 99],
            &[100, 101, 199],
            &[0, 50, 100, 150, 199],
        ];
        for doc_start in 0..num_docs {
            for ranks in rank_lists {
                if idx.start_index_column.get_val(doc_start) > ranks[0] {
                    continue;
                }
                let mut fast = ranks.to_vec();
                let mut slow = ranks.to_vec();
                idx.select_batch_in_place(doc_start, &mut fast);
                select_batch_v1_linear_scan(&idx, doc_start, &mut slow);
                assert_eq!(fast, slow, "doc_start={doc_start} ranks={ranks:?}");
            }
        }
    }

    #[test]
    fn select_batch_matches_linear_scan_v2_random() {
        let mut rng = StdRng::from_seed([7u8; 32]);
        for num_docs in [20u32, 50, 200] {
            let mut offs = vec![0u32];
            let mut total = 0u32;
            for _ in 0..num_docs {
                total += rng.random_range(0u32..4u32);
                offs.push(total);
            }
            let idx = MultiValueIndex::for_test(&offs);
            let MultiValueIndex::MultiValueIndexV2(ref v2) = idx else {
                panic!("for_test should produce V2");
            };
            for _ in 0..200 {
                let doc_start = rng.random_range(0..num_docs);
                let Some(&max_row) = offs.last() else {
                    continue;
                };
                if max_row == 0 {
                    continue;
                }
                let n_ranks = rng.random_range(1..30usize);
                let mut ranks: Vec<u32> =
                    (0..n_ranks).map(|_| rng.random_range(0..max_row)).collect();
                ranks.sort_unstable();
                ranks.dedup();
                if ranks.is_empty() {
                    continue;
                }
                let rank_start = v2.optional_index.rank(doc_start);
                if rank_start > ranks[0] {
                    continue;
                }
                let mut fast = ranks.clone();
                let mut slow = ranks.clone();
                idx.select_batch_in_place(doc_start, &mut fast);
                select_batch_v2_linear_scan(v2, doc_start, &mut slow);
                assert_eq!(
                    fast, slow,
                    "doc_start={doc_start} ranks={ranks:?} num_docs={num_docs}",
                );
            }
        }
    }
}

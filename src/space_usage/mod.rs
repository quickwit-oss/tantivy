/*!
Representations for the space usage of various parts of a Tantivy index.

This can be used programmatically, and will also be exposed in a human readable fashion in
tantivy-cli.

One important caveat for all of this functionality is that none of it currently takes storage-level
details into consideration. For example, if your file system block size is 4096 bytes, we can
under-count actual resultant space usage by up to 4095 bytes per file.
*/

use schema::Field;
use std::collections::HashMap;
use SegmentComponent;

/// Indicates space usage in bytes
pub type ByteCount = usize;

/// Enum containing any of the possible space usage results for segment components.
pub enum ComponentSpaceUsage {
    /// Data is stored per field in a uniform way
    PerField(PerFieldSpaceUsage),
    /// Data is stored in separate pieces in the store
    Store(StoreSpaceUsage),
    /// Some sort of raw byte count
    Basic(ByteCount),
}

/// Represents combined space usage of an entire searcher and its component segments.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SearcherSpaceUsage {
    segments: Vec<SegmentSpaceUsage>,
    total: ByteCount,
}

impl SearcherSpaceUsage {
    pub(crate) fn new() -> SearcherSpaceUsage {
        SearcherSpaceUsage {
            segments: Vec::new(),
            total: 0,
        }
    }

    /// Add a segment, to `self`.
    /// Performs no deduplication or other intelligence.
    pub(crate) fn add_segment(&mut self, segment: SegmentSpaceUsage) {
        self.total += segment.total();
        self.segments.push(segment);
    }

    /// Per segment space usage
    pub fn segments(&self) -> &[SegmentSpaceUsage] {
        &self.segments[..]
    }

    /// Returns total byte usage of this searcher, including all large subcomponents.
    /// Does not account for smaller things like `meta.json`.
    pub fn total(&self) -> ByteCount {
        self.total
    }
}

/// Represents combined space usage for all of the large components comprising a segment.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SegmentSpaceUsage {
    num_docs: u32,

    termdict: PerFieldSpaceUsage,
    postings: PerFieldSpaceUsage,
    positions: PerFieldSpaceUsage,
    positions_idx: PerFieldSpaceUsage,
    fast_fields: PerFieldSpaceUsage,
    fieldnorms: PerFieldSpaceUsage,

    store: StoreSpaceUsage,

    deletes: ByteCount,

    total: ByteCount,
}

impl SegmentSpaceUsage {
    pub(crate) fn new(
        num_docs: u32,
        termdict: PerFieldSpaceUsage,
        postings: PerFieldSpaceUsage,
        positions: PerFieldSpaceUsage,
        positions_idx: PerFieldSpaceUsage,
        fast_fields: PerFieldSpaceUsage,
        fieldnorms: PerFieldSpaceUsage,
        store: StoreSpaceUsage,
        deletes: ByteCount,
    ) -> SegmentSpaceUsage {
        let total = termdict.total()
            + postings.total()
            + positions.total()
            + fast_fields.total()
            + fieldnorms.total()
            + store.total()
            + deletes;
        SegmentSpaceUsage {
            num_docs,
            termdict,
            postings,
            positions,
            positions_idx,
            fast_fields,
            fieldnorms,
            store,
            deletes,
            total,
        }
    }

    /// Space usage for the given component
    ///
    /// Clones the underlying data.
    /// Use the components directly if this is somehow in performance critical code.
    pub fn component(&self, component: SegmentComponent) -> ComponentSpaceUsage {
        use SegmentComponent::*;
        use self::ComponentSpaceUsage::*;
        match component {
            POSTINGS => PerField(self.postings().clone()),
            POSITIONS => PerField(self.positions().clone()),
            POSITIONSSKIP => PerField(self.positions_skip_idx().clone()),
            FASTFIELDS => PerField(self.fast_fields().clone()),
            FIELDNORMS => PerField(self.fieldnorms().clone()),
            TERMS => PerField(self.termdict().clone()),
            STORE => Store(self.store().clone()),
            DELETE => Basic(self.deletes()),
        }
    }

    /// Num docs in segment
    pub fn num_docs(&self) -> u32 {
        self.num_docs
    }

    /// Space usage for term dictionary
    pub fn termdict(&self) -> &PerFieldSpaceUsage {
        &self.termdict
    }

    /// Space usage for postings list
    pub fn postings(&self) -> &PerFieldSpaceUsage {
        &self.postings
    }

    /// Space usage for positions
    pub fn positions(&self) -> &PerFieldSpaceUsage {
        &self.positions
    }

    /// Space usage for positions skip idx
    pub fn positions_skip_idx(&self) -> &PerFieldSpaceUsage {
        &self.positions_idx
    }

    /// Space usage for fast fields
    pub fn fast_fields(&self) -> &PerFieldSpaceUsage {
        &self.fast_fields
    }

    /// Space usage for field norms
    pub fn fieldnorms(&self) -> &PerFieldSpaceUsage {
        &self.fieldnorms
    }

    /// Space usage for stored documents
    pub fn store(&self) -> &StoreSpaceUsage {
        &self.store
    }

    /// Space usage for document deletions
    pub fn deletes(&self) -> ByteCount {
        self.deletes
    }

    /// Total space usage in bytes for this segment.
    pub fn total(&self) -> ByteCount {
        self.total
    }
}

/// Represents space usage for the Store for this segment.
///
/// This is composed of two parts.
/// `data` represents the compressed data itself.
/// `offsets` represents a lookup to find the start of a block
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StoreSpaceUsage {
    data: ByteCount,
    offsets: ByteCount,
}

impl StoreSpaceUsage {
    pub(crate) fn new(data: ByteCount, offsets: ByteCount) -> StoreSpaceUsage {
        StoreSpaceUsage { data, offsets }
    }

    /// Space usage for the data part of the store
    pub fn data_usage(&self) -> ByteCount {
        self.data
    }

    /// Space usage for the offsets part of the store (doc ID -> offset)
    pub fn offsets_usage(&self) -> ByteCount {
        self.offsets
    }

    /// Total space usage in bytes for this Store
    pub fn total(&self) -> ByteCount {
        self.data + self.offsets
    }
}

/// Represents space usage for all of the (field, index) pairs that appear in a CompositeFile.
///
/// A field can appear with a single index (typically 0) or with multiple indexes.
/// Multiple indexes are used to handle variable length things, where
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PerFieldSpaceUsage {
    fields: HashMap<Field, FieldUsage>,
    total: ByteCount
}

impl PerFieldSpaceUsage {
    pub(crate) fn new(fields: HashMap<Field, FieldUsage>) -> PerFieldSpaceUsage {
        let total = fields.values().map(|x| x.total()).sum();
        PerFieldSpaceUsage { fields, total }
    }

    /// Per field space usage
    pub fn fields(&self) -> impl Iterator<Item = (&Field, &FieldUsage)> {
        self.fields.iter()
    }

    /// Bytes used by the represented file
    pub fn total(&self) -> ByteCount {
        self.total
    }
}

/// Represents space usage of a given field, breaking it down into the (field, index) pairs that
/// comprise it.
///
/// See documentation for PerFieldSpaceUsage for slightly more information.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FieldUsage {
    field: Field,
    num_bytes: ByteCount,
    /// A field can be composed of more than one piece.
    /// These pieces are indexed by arbitrary numbers starting at zero.
    /// `self.num_bytes` includes all of `self.sub_num_bytes`.
    sub_num_bytes: Vec<Option<ByteCount>>,
}

impl FieldUsage {
    pub(crate) fn empty(field: Field) -> FieldUsage {
        FieldUsage {
            field,
            num_bytes: 0,
            sub_num_bytes: Vec::new(),
        }
    }

    pub(crate) fn add_field_idx(&mut self, idx: usize, size: ByteCount) {
        if self.sub_num_bytes.len() < idx + 1{
            self.sub_num_bytes.resize(idx + 1, None);
        }
        assert!(self.sub_num_bytes[idx].is_none());
        self.sub_num_bytes[idx] = Some(size);
        self.num_bytes += size
    }

    /// Field
    pub fn field(&self) -> Field {
        self.field
    }

    /// Space usage for each index
    pub fn sub_num_bytes(&self) -> &[Option<ByteCount>] {
        &self.sub_num_bytes[..]
    }

    /// Total bytes used for this field in this context
    pub fn total(&self) -> ByteCount {
        self.num_bytes
    }
}
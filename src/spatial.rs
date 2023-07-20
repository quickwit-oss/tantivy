//! TODO
//!
//! ```
//! # fn main() -> tantivy::Result<()> {
//! use std::sync::{Arc, Weak};
//!
//! use rstar::{primitives::GeomWithData, RTree, AABB};
//! use tantivy::{
//!     collector::DocSetCollector,
//!     doc,
//!     schema::{Schema, STORED},
//!     spatial::{SpatialIndex, SpatialQuery},
//!     DocAddress, Index, Result, Warmer,
//! };
//!
//! let mut schema = Schema::builder();
//! let x = schema.add_f64_field("x", STORED);
//! let y = schema.add_f64_field("y", STORED);
//! let schema = schema.build();
//!
//! let index = Index::create_in_ram(schema);
//!
//! let mut writer = index.writer_with_num_threads(1, 10_000_000)?;
//! writer.add_document(doc!(x => 0.5, y => 0.5))?;
//! writer.add_document(doc!(x => 1.5, y => 0.5))?;
//! writer.add_document(doc!(x => 0.5, y => 1.5))?;
//! writer.add_document(doc!(x => 0.25, y => 0.75))?;
//! writer.add_document(doc!(x => 0.75, y => 0.25))?;
//! writer.commit()?;
//!
//! let spatial_index = Arc::new(SpatialIndex::new(move |reader| {
//!     let store_reader = reader.get_store_reader(0)?;
//!
//!     Ok(RTree::bulk_load(
//!         reader
//!             .doc_ids_alive()
//!             .map(|doc_id| {
//!                 let doc = store_reader.get(doc_id)?;
//!                 let x = doc.get_first(x).unwrap().as_f64().unwrap();
//!                 let y = doc.get_first(y).unwrap().as_f64().unwrap();
//!
//!                 Ok(GeomWithData::new([x, y], doc_id))
//!             })
//!             .collect::<Result<_>>()?,
//!     ))
//! }));
//!
//! let warmers = vec![Arc::downgrade(&spatial_index) as Weak<dyn Warmer>];
//! let reader = index.reader_builder().warmers(warmers).try_into()?;
//!
//! let spatial_query =
//!     SpatialQuery::locate_in_envelope(&spatial_index, AABB::from_corners([0., 0.], [1., 1.]));
//!
//! let searcher = reader.searcher();
//! let results = searcher.search(&spatial_query, &DocSetCollector)?;
//!
//! assert_eq!(
//!     results,
//!     [
//!         DocAddress {
//!             segment_ord: 0,
//!             doc_id: 0,
//!         },
//!         DocAddress {
//!             segment_ord: 0,
//!             doc_id: 3,
//!         },
//!         DocAddress {
//!             segment_ord: 0,
//!             doc_id: 4,
//!         },
//!     ]
//!     .into(),
//! );
//! # Ok(()) }
//! ```
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::Arc;

use arc_swap::ArcSwap;
use common::BitSet;
use rstar::primitives::GeomWithData;
use rstar::{Envelope, Point, PointDistance, RTree, RTreeObject};

use crate::query::{BitSetDocSet, ConstScorer, EnableScoring, Explanation, Query, Scorer, Weight};
use crate::{
    DocId, Opstamp, Result, Score, Searcher, SearcherGeneration, SegmentId, SegmentReader,
    TantivyError, Warmer,
};

type SegmentKey = (SegmentId, Option<Opstamp>);

/// TODO
pub type SegmentTree<T> = RTree<GeomWithData<T, DocId>>;

type Trees<T> = HashMap<SegmentKey, Arc<SegmentTree<T>>>;

type Inner<T> = dyn Fn(&SegmentTree<T>, &mut BitSet) + Send + Sync;

/// TODO
pub struct SpatialIndex<T: RTreeObject> {
    trees: ArcSwap<Trees<T>>,
    builder: Box<dyn Fn(&SegmentReader) -> Result<SegmentTree<T>> + Send + Sync>,
}

impl<T: RTreeObject> SpatialIndex<T> {
    /// TODO
    pub fn new<B>(builder: B) -> Self
    where B: Fn(&SegmentReader) -> Result<SegmentTree<T>> + Send + Sync + 'static {
        Self {
            trees: Default::default(),
            builder: Box::new(builder),
        }
    }
}

impl<T: RTreeObject> Warmer for SpatialIndex<T>
where SegmentTree<T>: Send + Sync
{
    fn warm(&self, searcher: &Searcher) -> Result<()> {
        let mut trees = self.trees.load_full();

        for reader in searcher.segment_readers() {
            let key = (reader.segment_id(), reader.delete_opstamp());

            if trees.contains_key(&key) {
                continue;
            }

            let tree = (self.builder)(reader)?;

            Arc::make_mut(&mut trees).insert(key, Arc::new(tree));
        }

        self.trees.store(trees);

        Ok(())
    }

    fn garbage_collect(&self, live_generations: &[&SearcherGeneration]) {
        let live_keys = live_generations
            .iter()
            .flat_map(|gen| gen.segments())
            .map(|(&segment_id, &opstamp)| (segment_id, opstamp))
            .collect::<HashSet<_>>();

        let mut trees = self.trees.load_full();

        Arc::make_mut(&mut trees).retain(|key, _tree| live_keys.contains(key));

        self.trees.store(trees);
    }
}

/// TODO
pub struct SpatialQuery<T: RTreeObject> {
    trees: Arc<Trees<T>>,
    inner: Arc<Inner<T>>,
}

impl<T: RTreeObject> Clone for SpatialQuery<T> {
    fn clone(&self) -> Self {
        Self {
            trees: Arc::clone(&self.trees),
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<T: RTreeObject> fmt::Debug for SpatialQuery<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SpatialQuery").finish_non_exhaustive()
    }
}

impl<T: RTreeObject> SpatialQuery<T> {
    fn new<I>(index: &SpatialIndex<T>, inner: I) -> Self
    where I: Fn(&SegmentTree<T>, &mut BitSet) + Send + Sync + 'static {
        Self {
            trees: index.trees.load_full(),
            inner: Arc::new(inner),
        }
    }

    /// TODO
    pub fn locate_all_at_point(
        index: &SpatialIndex<T>,
        point: <T::Envelope as Envelope>::Point,
    ) -> Self
    where
        T: PointDistance,
        <T::Envelope as Envelope>::Point: Send + Sync + 'static,
    {
        Self::new(index, move |tree, bitset| {
            tree.locate_all_at_point(&point)
                .for_each(|node| bitset.insert(node.data))
        })
    }

    /// TODO
    pub fn locate_in_envelope(index: &SpatialIndex<T>, envelope: T::Envelope) -> Self
    where T::Envelope: Send + Sync + 'static {
        Self::new(index, move |tree, bitset| {
            tree.locate_in_envelope(&envelope)
                .for_each(|node| bitset.insert(node.data))
        })
    }

    /// TODO
    pub fn locate_in_envelope_intersecting(index: &SpatialIndex<T>, envelope: T::Envelope) -> Self
    where T::Envelope: Send + Sync + 'static {
        Self::new(index, move |tree, bitset| {
            tree.locate_in_envelope_intersecting(&envelope)
                .for_each(|node| bitset.insert(node.data))
        })
    }

    /// TODO
    pub fn locate_within_distance(
        index: &SpatialIndex<T>,
        query_point: <T::Envelope as Envelope>::Point,
        max_squared_radius: <<T::Envelope as Envelope>::Point as Point>::Scalar,
    ) -> Self
    where
        T: PointDistance,
        <T::Envelope as Envelope>::Point: Clone + Send + Sync + 'static,
        <<T::Envelope as Envelope>::Point as Point>::Scalar: Send + Sync + 'static,
    {
        Self::new(index, move |tree, bitset| {
            tree.locate_within_distance(query_point.clone(), max_squared_radius)
                .for_each(|node| bitset.insert(node.data))
        })
    }
}

impl<T: RTreeObject + 'static> Query for SpatialQuery<T>
where SegmentTree<T>: Send + Sync
{
    fn weight(&self, _: EnableScoring<'_>) -> Result<Box<dyn Weight>> {
        Ok(Box::new(self.clone()))
    }
}

impl<T: RTreeObject + 'static> Weight for SpatialQuery<T>
where SegmentTree<T>: Send + Sync
{
    fn scorer(&self, reader: &SegmentReader, boost: Score) -> Result<Box<dyn Scorer>> {
        let key = (reader.segment_id(), reader.delete_opstamp());

        let tree = &self.trees[&key];

        let mut bitset = BitSet::with_max_value(reader.max_doc());

        (self.inner)(tree, &mut bitset);

        Ok(Box::new(ConstScorer::new(
            BitSetDocSet::from(bitset),
            boost,
        )))
    }

    fn explain(&self, reader: &SegmentReader, doc: DocId) -> Result<Explanation> {
        let mut scorer = self.scorer(reader, 1.0)?;
        if scorer.seek(doc) == doc {
            Ok(Explanation::new("SpatialQuery", 1.0))
        } else {
            Err(TantivyError::InvalidArgument(format!(
                "Document #({doc}) does not match"
            )))
        }
    }
}

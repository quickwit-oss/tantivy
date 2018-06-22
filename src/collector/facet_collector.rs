use collector::Collector;
use docset::SkipResult;
use fastfield::FacetReader;
use schema::Facet;
use schema::Field;
use std::cell::UnsafeCell;
use std::collections::btree_map;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::BinaryHeap;
use std::collections::Bound;
use std::iter::Peekable;
use std::mem;
use std::{u64, usize};
use termdict::TermMerger;

use std::cmp::Ordering;
use DocId;
use Result;
use Score;
use SegmentLocalId;
use SegmentReader;

struct Hit<'a> {
    count: u64,
    facet: &'a Facet,
}

impl<'a> Eq for Hit<'a> {}

impl<'a> PartialEq<Hit<'a>> for Hit<'a> {
    fn eq(&self, other: &Hit) -> bool {
        self.count == other.count
    }
}

impl<'a> PartialOrd<Hit<'a>> for Hit<'a> {
    fn partial_cmp(&self, other: &Hit) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<'a> Ord for Hit<'a> {
    fn cmp(&self, other: &Self) -> Ordering {
        other.count.cmp(&self.count)
    }
}

struct SegmentFacetCounter {
    pub facet_reader: FacetReader,
    pub facet_ords: Vec<u64>,
    pub facet_counts: Vec<u64>,
}

fn facet_depth(facet_bytes: &[u8]) -> usize {
    if facet_bytes.is_empty() {
        0
    } else {
        facet_bytes.iter().cloned().filter(|b| *b == 0u8).count() + 1
    }
}

/// Collector for faceting
///
/// The collector collects all facets. You need to configure it
/// beforehand with the facet you want to extract.
///
/// This is done by calling `.add_facet(...)` with the root of the
/// facet you want to extract as argument.
///
/// Facet counts will only be computed for the facet that are direct children
/// of such a root facet.
///
/// For instance, if your index represents books, your hierarchy of facets
/// may contain `category`, `language`.
///
/// The category facet may include `subcategories`. For instance, a book
/// could belong to `/category/fiction/fantasy`.
///
/// If you request the facet counts for `/category`, the result will be
/// the breakdown of counts for the direct children of `/category`
/// (e.g. `/category/fiction`, `/category/biography`, `/category/personal_development`).
///
/// Once collection is finished, you can harvest its results in the form
/// of a `FacetCounts` object, and extract your face                t counts from it.
///
/// This implementation assumes you are working with a number of facets that
/// is much hundreds of time lower than your number of documents.
///
///
/// ```rust
/// #[macro_use]
/// extern crate tantivy;
/// use tantivy::schema::{Facet, SchemaBuilder, TEXT};
/// use tantivy::{Index, Result};
/// use tantivy::collector::FacetCollector;
/// use tantivy::query::AllQuery;
///
/// # fn main() { example().unwrap(); }
/// fn example() -> Result<()> {
///     let mut schema_builder = SchemaBuilder::new();
///
///     // Facet have their own specific type.
///     // It is not a bad practise to put all of your
///     // facet information in the same field.
///     let facet = schema_builder.add_facet_field("facet");
///     let title = schema_builder.add_text_field("title", TEXT);
///     let schema = schema_builder.build();
///     let index = Index::create_in_ram(schema);
///     {
///         let mut index_writer = index.writer(3_000_000)?;
///         // a document can be associated to any number of facets
///         index_writer.add_document(doc!(
///             title => "The Name of the Wind",
///             facet => Facet::from("/lang/en"),
///             facet => Facet::from("/category/fiction/fantasy")
///         ));
///         index_writer.add_document(doc!(
///             title => "Dune",
///             facet => Facet::from("/lang/en"),
///             facet => Facet::from("/category/fiction/sci-fi")
///         ));
///         index_writer.add_document(doc!(
///             title => "La Vénus d'Ille",
///             facet => Facet::from("/lang/fr"),
///             facet => Facet::from("/category/fiction/fantasy"),
///             facet => Facet::from("/category/fiction/horror")
///         ));
///         index_writer.add_document(doc!(
///             title => "The Diary of a Young Girl",
///             facet => Facet::from("/lang/en"),
///             facet => Facet::from("/category/biography")
///         ));
///         index_writer.commit().unwrap();
///     }
///
///     index.load_searchers()?;
///     let searcher = index.searcher();
///
///     {
///			let mut facet_collector = FacetCollector::for_field(facet);
///         facet_collector.add_facet("/lang");
///         facet_collector.add_facet("/category");
///         searcher.search(&AllQuery, &mut facet_collector).unwrap();
///
///         // this object contains count aggregate for all of the facets.
///         let counts = facet_collector.harvest();
///
///         // This lists all of the facet counts
///         let facets: Vec<(&Facet, u64)> = counts
///             .get("/category")
///             .collect();
///         assert_eq!(facets, vec![
///             (&Facet::from("/category/biography"), 1),
///             (&Facet::from("/category/fiction"), 3)
///         ]);
///     }
///
///     {
///			let mut facet_collector = FacetCollector::for_field(facet);
///         facet_collector.add_facet("/category/fiction");
///         searcher.search(&AllQuery, &mut facet_collector).unwrap();
///
///         // this object contains count aggregate for all of the facets.
///         let counts = facet_collector.harvest();
///
///         // This lists all of the facet counts
///         let facets: Vec<(&Facet, u64)> = counts
///             .get("/category/fiction")
///             .collect();
///         assert_eq!(facets, vec![
///             (&Facet::from("/category/fiction/fantasy"), 2),
///             (&Facet::from("/category/fiction/horror"), 1),
///             (&Facet::from("/category/fiction/sci-fi"), 1)
///         ]);
///     }
///
///    {
///			let mut facet_collector = FacetCollector::for_field(facet);
///         facet_collector.add_facet("/category/fiction");
///         searcher.search(&AllQuery, &mut facet_collector).unwrap();
///
///         // this object contains count aggregate for all of the facets.
///         let counts = facet_collector.harvest();
///
///         // This lists all of the facet counts
///         let facets: Vec<(&Facet, u64)> = counts.top_k("/category/fiction", 1);
///         assert_eq!(facets, vec![
///             (&Facet::from("/category/fiction/fantasy"), 2)
///         ]);
///     }
///
///     Ok(())
/// }
/// ```
pub struct FacetCollector {
    facet_ords: Vec<u64>,
    field: Field,
    ff_reader: Option<UnsafeCell<FacetReader>>,
    segment_counters: Vec<SegmentFacetCounter>,

    // facet_ord -> collapse facet_id
    current_segment_collapse_mapping: Vec<usize>,
    // collapse facet_id -> count
    current_segment_counts: Vec<u64>,
    // collapse facet_id -> facet_ord
    current_collapse_facet_ords: Vec<u64>,

    facets: BTreeSet<Facet>,
}

fn skip<'a, I: Iterator<Item = &'a Facet>>(
    target: &[u8],
    collapse_it: &mut Peekable<I>,
) -> SkipResult {
    loop {
        match collapse_it.peek() {
            Some(facet_bytes) => match facet_bytes.encoded_bytes().cmp(target) {
                Ordering::Less => {}
                Ordering::Greater => {
                    return SkipResult::OverStep;
                }
                Ordering::Equal => {
                    return SkipResult::Reached;
                }
            },
            None => {
                return SkipResult::End;
            }
        }
        collapse_it.next();
    }
}

impl FacetCollector {
    /// Create a facet collector to collect the facets
    /// from a specific facet `Field`.
    ///
    /// This function does not check whether the field
    /// is of the proper type.
    pub fn for_field(field: Field) -> FacetCollector {
        FacetCollector {
            facet_ords: Vec::with_capacity(255),
            segment_counters: Vec::new(),
            field,
            ff_reader: None,
            facets: BTreeSet::new(),

            current_segment_collapse_mapping: Vec::new(),
            current_collapse_facet_ords: Vec::new(),
            current_segment_counts: Vec::new(),
        }
    }

    /// Adds a facet that we want to record counts
    ///
    /// Adding facet `Facet::from("/country")` for instance,
    /// will record the counts of all of the direct children of the facet country
    /// (e.g. `/country/FR`, `/country/UK`).
    ///
    /// Adding two facets within which one is the prefix of the other is forbidden.
    /// If you need the correct number of unique documents for two such facets,
    /// just add them in separate `FacetCollector`.
    pub fn add_facet<T>(&mut self, facet_from: T)
    where
        Facet: From<T>,
    {
        let facet = Facet::from(facet_from);
        for old_facet in &self.facets {
            assert!(
                !old_facet.is_prefix_of(&facet),
                "Tried to add a facet which is a descendant of an already added facet."
            );
            assert!(
                !facet.is_prefix_of(old_facet),
                "Tried to add a facet which is an ancestor of an already added facet."
            );
        }
        self.facets.insert(facet);
    }

    fn set_collapse_mapping(&mut self, facet_reader: &FacetReader) {
        self.current_segment_collapse_mapping.clear();
        self.current_collapse_facet_ords.clear();
        self.current_segment_counts.clear();
        let mut collapse_facet_it = self.facets.iter().peekable();
        self.current_collapse_facet_ords.push(0);
        let mut facet_streamer = facet_reader.facet_dict().range().into_stream();
        if !facet_streamer.advance() {
            return;
        }
        'outer: loop {
            // at the begining of this loop, facet_streamer
            // is positionned on a term that has not been processed yet.
            let skip_result = skip(facet_streamer.key(), &mut collapse_facet_it);
            match skip_result {
                SkipResult::Reached => {
                    // we reach a facet we decided to collapse.
                    let collapse_depth = facet_depth(facet_streamer.key());
                    let mut collapsed_id = 0;
                    self.current_segment_collapse_mapping.push(0);
                    while facet_streamer.advance() {
                        let depth = facet_depth(facet_streamer.key());
                        if depth <= collapse_depth {
                            continue 'outer;
                        }
                        if depth == collapse_depth + 1 {
                            collapsed_id = self.current_collapse_facet_ords.len();
                            self.current_collapse_facet_ords
                                .push(facet_streamer.term_ord());
                            self.current_segment_collapse_mapping.push(collapsed_id);
                        } else {
                            self.current_segment_collapse_mapping.push(collapsed_id);
                        }
                    }
                    break;
                }
                SkipResult::End | SkipResult::OverStep => {
                    self.current_segment_collapse_mapping.push(0);
                    if !facet_streamer.advance() {
                        break;
                    }
                }
            }
        }
    }

    fn finalize_segment(&mut self) {
        if self.ff_reader.is_some() {
            self.segment_counters.push(SegmentFacetCounter {
                facet_reader: self.ff_reader.take().unwrap().into_inner(),
                facet_ords: mem::replace(&mut self.current_collapse_facet_ords, Vec::new()),
                facet_counts: mem::replace(&mut self.current_segment_counts, Vec::new()),
            });
        }
    }

    /// Returns the results of the collection.
    ///
    /// This method does not just return the counters,
    /// it also translates the facet ordinals of the last segment.
    pub fn harvest(mut self) -> FacetCounts {
        self.finalize_segment();

        let collapsed_facet_ords: Vec<&[u64]> = self
            .segment_counters
            .iter()
            .map(|segment_counter| &segment_counter.facet_ords[..])
            .collect();
        let collapsed_facet_counts: Vec<&[u64]> = self
            .segment_counters
            .iter()
            .map(|segment_counter| &segment_counter.facet_counts[..])
            .collect();

        let facet_streams = self
            .segment_counters
            .iter()
            .map(|seg_counts| seg_counts.facet_reader.facet_dict().range().into_stream())
            .collect::<Vec<_>>();

        let mut facet_merger = TermMerger::new(facet_streams);
        let mut facet_counts = BTreeMap::new();

        while facet_merger.advance() {
            let count = facet_merger
                .current_kvs()
                .iter()
                .map(|it| {
                    let seg_ord = it.segment_ord;
                    let term_ord = it.streamer.term_ord();
                    collapsed_facet_ords[seg_ord]
                        .binary_search(&term_ord)
                        .map(|collapsed_term_id| {
                            if collapsed_term_id == 0 {
                                0
                            } else {
                                collapsed_facet_counts[seg_ord][collapsed_term_id]
                            }
                        })
                        .unwrap_or(0)
                })
                .sum();
            if count > 0u64 {
                let bytes: Vec<u8> = facet_merger.key().to_owned();
                // may create an corrupted facet if the term dicitonary is corrupted
                let facet = unsafe { Facet::from_encoded(bytes) };
                facet_counts.insert(facet, count);
            }
        }
        FacetCounts { facet_counts }
    }
}

impl Collector for FacetCollector {
    fn set_segment(&mut self, _: SegmentLocalId, reader: &SegmentReader) -> Result<()> {
        self.finalize_segment();
        let facet_reader = reader.facet_reader(self.field)?;
        self.set_collapse_mapping(&facet_reader);
        self.current_segment_counts
            .resize(self.current_collapse_facet_ords.len(), 0);
        self.ff_reader = Some(UnsafeCell::new(facet_reader));
        Ok(())
    }

    fn collect(&mut self, doc: DocId, _: Score) {
        let facet_reader: &mut FacetReader = unsafe {
            &mut *self
                .ff_reader
                .as_ref()
                .expect("collect() was called before set_segment. This should never happen.")
                .get()
        };
        facet_reader.facet_ords(doc, &mut self.facet_ords);
        let mut previous_collapsed_ord: usize = usize::MAX;
        for &facet_ord in &self.facet_ords {
            let collapsed_ord = self.current_segment_collapse_mapping[facet_ord as usize];
            self.current_segment_counts[collapsed_ord] += if collapsed_ord == previous_collapsed_ord
            {
                0
            } else {
                1
            };
            previous_collapsed_ord = collapsed_ord;
        }
    }

    fn requires_scoring(&self) -> bool {
        false
    }
}

/// Intermediary result of the `FacetCollector` that stores
/// the facet counts for all the segments.
pub struct FacetCounts {
    facet_counts: BTreeMap<Facet, u64>,
}

pub struct FacetChildIterator<'a> {
    underlying: btree_map::Range<'a, Facet, u64>,
}

impl<'a> Iterator for FacetChildIterator<'a> {
    type Item = (&'a Facet, u64);

    fn next(&mut self) -> Option<Self::Item> {
        self.underlying.next().map(|(facet, count)| (facet, *count))
    }
}

impl FacetCounts {
    pub fn get<T>(&self, facet_from: T) -> FacetChildIterator
    where
        Facet: From<T>,
    {
        let facet = Facet::from(facet_from);
        let left_bound = Bound::Excluded(facet.clone());
        let right_bound = if facet.is_root() {
            Bound::Unbounded
        } else {
            let mut facet_after_bytes: Vec<u8> = facet.encoded_bytes().to_owned();
            facet_after_bytes.push(1u8);
            let facet_after = unsafe { Facet::from_encoded(facet_after_bytes) }; // ok logic
            Bound::Excluded(facet_after)
        };
        let underlying: btree_map::Range<_, _> = self.facet_counts.range((left_bound, right_bound));
        FacetChildIterator { underlying }
    }

    pub fn top_k<T>(&self, facet: T, k: usize) -> Vec<(&Facet, u64)>
    where
        Facet: From<T>,
    {
        let mut heap = BinaryHeap::with_capacity(k);
        let mut it = self.get(facet);

        for (facet, count) in (&mut it).take(k) {
            heap.push(Hit { count, facet });
        }

        let mut lowest_count: u64 = heap.peek().map(|hit| hit.count).unwrap_or(u64::MIN);
        for (facet, count) in it {
            if count > lowest_count {
                lowest_count = count;
                if let Some(mut head) = heap.peek_mut() {
                    *head = Hit { count, facet };
                }
            }
        }
        heap.into_sorted_vec()
            .into_iter()
            .map(|hit| (hit.facet, hit.count))
            .collect::<Vec<_>>()
    }
}

#[cfg(test)]
mod tests {
    use super::{FacetCollector, FacetCounts};
    use core::Index;
    use query::AllQuery;
    use rand::{thread_rng, Rng};
    use schema::Field;
    use schema::{Document, Facet, SchemaBuilder};
    use std::iter;

    #[test]
    fn test_facet_collector_drilldown() {
        let mut schema_builder = SchemaBuilder::new();
        let facet_field = schema_builder.add_facet_field("facet");
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);

        let mut index_writer = index.writer_with_num_threads(1, 3_000_000).unwrap();
        let num_facets: usize = 3 * 4 * 5;
        let facets: Vec<Facet> = (0..num_facets)
            .map(|mut n| {
                let top = n % 3;
                n /= 3;
                let mid = n % 4;
                n /= 4;
                let leaf = n % 5;
                Facet::from(&format!("/top{}/mid{}/leaf{}", top, mid, leaf))
            })
            .collect();
        for i in 0..num_facets * 10 {
            let mut doc = Document::new();
            doc.add_facet(facet_field, facets[i % num_facets].clone());
            index_writer.add_document(doc);
        }
        index_writer.commit().unwrap();
        index.load_searchers().unwrap();
        let searcher = index.searcher();

        let mut facet_collector = FacetCollector::for_field(facet_field);
        facet_collector.add_facet(Facet::from("/top1"));
        searcher.search(&AllQuery, &mut facet_collector).unwrap();

        let counts: FacetCounts = facet_collector.harvest();
        {
            let facets: Vec<(String, u64)> = counts
                .get("/top1")
                .map(|(facet, count)| (facet.to_string(), count))
                .collect();
            assert_eq!(
                facets,
                [
                    ("/top1/mid0", 50),
                    ("/top1/mid1", 50),
                    ("/top1/mid2", 50),
                    ("/top1/mid3", 50),
                ].iter()
                    .map(|&(facet_str, count)| (String::from(facet_str), count))
                    .collect::<Vec<_>>()
            );
        }
    }

    #[test]
    #[should_panic(
        expected = "Tried to add a facet which is a descendant of \
                    an already added facet."
    )]
    fn test_misused_facet_collector() {
        let mut facet_collector = FacetCollector::for_field(Field(0));
        facet_collector.add_facet(Facet::from("/country"));
        facet_collector.add_facet(Facet::from("/country/europe"));
    }

    #[test]
    fn test_non_used_facet_collector() {
        let mut facet_collector = FacetCollector::for_field(Field(0));
        facet_collector.add_facet(Facet::from("/country"));
        facet_collector.add_facet(Facet::from("/countryeurope"));
    }

    #[test]
    fn test_facet_collector_topk() {
        let mut schema_builder = SchemaBuilder::new();
        let facet_field = schema_builder.add_facet_field("facet");
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);

        let mut docs: Vec<Document> = vec![("a", 10), ("b", 100), ("c", 7), ("d", 12), ("e", 21)]
            .into_iter()
            .flat_map(|(c, count)| {
                let facet = Facet::from(&format!("/facet_{}", c));
                let doc = doc!(facet_field => facet);
                iter::repeat(doc).take(count)
            })
            .collect();
        thread_rng().shuffle(&mut docs[..]);

        let mut index_writer = index.writer_with_num_threads(1, 3_000_000).unwrap();
        for doc in docs {
            index_writer.add_document(doc);
        }
        index_writer.commit().unwrap();
        index.load_searchers().unwrap();

        let searcher = index.searcher();

        let mut facet_collector = FacetCollector::for_field(facet_field);
        facet_collector.add_facet("/");
        searcher.search(&AllQuery, &mut facet_collector).unwrap();

        let counts: FacetCounts = facet_collector.harvest();
        {
            let facets: Vec<(&Facet, u64)> = counts.top_k("/", 3);
            assert_eq!(
                facets,
                vec![
                    (&Facet::from("/facet_b"), 100),
                    (&Facet::from("/facet_e"), 21),
                    (&Facet::from("/facet_d"), 12),
                ]
            );
        }
    }

}

#[cfg(all(test, feature = "unstable"))]
mod bench {

    use collector::FacetCollector;
    use query::AllQuery;
    use rand::{thread_rng, Rng};
    use schema::Facet;
    use schema::SchemaBuilder;
    use test::Bencher;
    use Index;

    #[bench]
    fn bench_facet_collector(b: &mut Bencher) {
        let mut schema_builder = SchemaBuilder::new();
        let facet_field = schema_builder.add_facet_field("facet");
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);

        let mut docs = vec![];
        for val in 0..50 {
            let facet = Facet::from(&format!("/facet_{}", val));
            for _ in 0..val * val {
                docs.push(doc!(facet_field=>facet.clone()));
            }
        }
        // 40425 docs
        thread_rng().shuffle(&mut docs[..]);

        let mut index_writer = index.writer_with_num_threads(1, 3_000_000).unwrap();
        for doc in docs {
            index_writer.add_document(doc);
        }
        index_writer.commit().unwrap();
        index.load_searchers().unwrap();

        b.iter(|| {
            let searcher = index.searcher();
            let mut facet_collector = FacetCollector::for_field(facet_field);
            searcher.search(&AllQuery, &mut facet_collector).unwrap();
        });
    }
}

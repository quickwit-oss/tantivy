use crate::collector::Collector;
use crate::collector::SegmentCollector;
use crate::fastfield::FacetReader;
use crate::schema::Facet;
use crate::schema::Field;
use crate::DocId;
use crate::Score;
use crate::SegmentOrdinal;
use crate::SegmentReader;
use std::cmp::Ordering;
use std::collections::btree_map;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::BinaryHeap;
use std::iter::Peekable;
use std::ops::Bound;
use std::{u64, usize};

struct Hit<'a> {
    count: u64,
    facet: &'a Facet,
}

impl<'a> Eq for Hit<'a> {}

impl<'a> PartialEq<Hit<'a>> for Hit<'a> {
    fn eq(&self, other: &Hit<'_>) -> bool {
        self.count == other.count
    }
}

impl<'a> PartialOrd<Hit<'a>> for Hit<'a> {
    fn partial_cmp(&self, other: &Hit<'_>) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<'a> Ord for Hit<'a> {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .count
            .cmp(&self.count)
            .then(self.facet.cmp(other.facet))
    }
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
/// use tantivy::collector::FacetCollector;
/// use tantivy::query::AllQuery;
/// use tantivy::schema::{Facet, Schema, INDEXED, TEXT};
/// use tantivy::{doc, Index};
///
/// fn example() -> tantivy::Result<()> {
///     let mut schema_builder = Schema::builder();
///
///     // Facet have their own specific type.
///     // It is not a bad practise to put all of your
///     // facet information in the same field.
///     let facet = schema_builder.add_facet_field("facet", INDEXED);
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
///         index_writer.commit()?;
///     }
///     let reader = index.reader()?;
///     let searcher = reader.searcher();
///
///     {
///         let mut facet_collector = FacetCollector::for_field(facet);
///         facet_collector.add_facet("/lang");
///         facet_collector.add_facet("/category");
///         let facet_counts = searcher.search(&AllQuery, &facet_collector)?;
///
///         // This lists all of the facet counts
///         let facets: Vec<(&Facet, u64)> = facet_counts
///             .get("/category")
///             .collect();
///         assert_eq!(facets, vec![
///             (&Facet::from("/category/biography"), 1),
///             (&Facet::from("/category/fiction"), 3)
///         ]);
///     }
///
///     {
///         let mut facet_collector = FacetCollector::for_field(facet);
///         facet_collector.add_facet("/category/fiction");
///         let facet_counts = searcher.search(&AllQuery, &facet_collector)?;
///
///         // This lists all of the facet counts
///         let facets: Vec<(&Facet, u64)> = facet_counts
///             .get("/category/fiction")
///             .collect();
///         assert_eq!(facets, vec![
///             (&Facet::from("/category/fiction/fantasy"), 2),
///             (&Facet::from("/category/fiction/horror"), 1),
///             (&Facet::from("/category/fiction/sci-fi"), 1)
///         ]);
///     }
///
///     {
///         let mut facet_collector = FacetCollector::for_field(facet);
///         facet_collector.add_facet("/category/fiction");
///         let facet_counts = searcher.search(&AllQuery, &facet_collector)?;
///
///         // This lists all of the facet counts
///         let facets: Vec<(&Facet, u64)> = facet_counts.top_k("/category/fiction", 1);
///         assert_eq!(facets, vec![
///             (&Facet::from("/category/fiction/fantasy"), 2)
///         ]);
///     }
///
///     Ok(())
/// }
/// # assert!(example().is_ok());
/// ```
pub struct FacetCollector {
    field: Field,
    facets: BTreeSet<Facet>,
}

pub struct FacetSegmentCollector {
    reader: FacetReader,
    facet_ords_buf: Vec<u64>,
    // facet_ord -> collapse facet_id
    collapse_mapping: Vec<usize>,
    // collapse facet_id -> count
    counts: Vec<u64>,
    // collapse facet_id -> facet_ord
    collapse_facet_ords: Vec<u64>,
}

enum SkipResult {
    Found,
    NotFound,
}

fn skip<'a, I: Iterator<Item = &'a Facet>>(
    target: &[u8],
    collapse_it: &mut Peekable<I>,
) -> SkipResult {
    loop {
        match collapse_it.peek() {
            Some(facet_bytes) => match facet_bytes.encoded_str().as_bytes().cmp(target) {
                Ordering::Less => {}
                Ordering::Greater => {
                    return SkipResult::NotFound;
                }
                Ordering::Equal => {
                    return SkipResult::Found;
                }
            },
            None => {
                return SkipResult::NotFound;
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
            field,
            facets: BTreeSet::default(),
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
}

impl Collector for FacetCollector {
    type Fruit = FacetCounts;

    type Child = FacetSegmentCollector;

    fn for_segment(
        &self,
        _: SegmentOrdinal,
        reader: &SegmentReader,
    ) -> crate::Result<FacetSegmentCollector> {
        let facet_reader = reader.facet_reader(self.field)?;

        let mut collapse_mapping = Vec::new();
        let mut counts = Vec::new();
        let mut collapse_facet_ords = Vec::new();

        let mut collapse_facet_it = self.facets.iter().peekable();
        collapse_facet_ords.push(0);
        {
            let mut facet_streamer = facet_reader.facet_dict().range().into_stream()?;
            if facet_streamer.advance() {
                'outer: loop {
                    // at the begining of this loop, facet_streamer
                    // is positionned on a term that has not been processed yet.
                    let skip_result = skip(facet_streamer.key(), &mut collapse_facet_it);
                    match skip_result {
                        SkipResult::Found => {
                            // we reach a facet we decided to collapse.
                            let collapse_depth = facet_depth(facet_streamer.key());
                            let mut collapsed_id = 0;
                            collapse_mapping.push(0);
                            while facet_streamer.advance() {
                                let depth = facet_depth(facet_streamer.key());
                                if depth <= collapse_depth {
                                    continue 'outer;
                                }
                                if depth == collapse_depth + 1 {
                                    collapsed_id = collapse_facet_ords.len();
                                    collapse_facet_ords.push(facet_streamer.term_ord());
                                    collapse_mapping.push(collapsed_id);
                                } else {
                                    collapse_mapping.push(collapsed_id);
                                }
                            }
                            break;
                        }
                        SkipResult::NotFound => {
                            collapse_mapping.push(0);
                            if !facet_streamer.advance() {
                                break;
                            }
                        }
                    }
                }
            }
        }

        counts.resize(collapse_facet_ords.len(), 0);

        Ok(FacetSegmentCollector {
            reader: facet_reader,
            facet_ords_buf: Vec::with_capacity(255),
            collapse_mapping,
            counts,
            collapse_facet_ords,
        })
    }

    fn requires_scoring(&self) -> bool {
        false
    }

    fn merge_fruits(&self, segments_facet_counts: Vec<FacetCounts>) -> crate::Result<FacetCounts> {
        let mut facet_counts: BTreeMap<Facet, u64> = BTreeMap::new();
        for segment_facet_counts in segments_facet_counts {
            for (facet, count) in segment_facet_counts.facet_counts {
                *(facet_counts.entry(facet).or_insert(0)) += count;
            }
        }
        Ok(FacetCounts { facet_counts })
    }
}

impl SegmentCollector for FacetSegmentCollector {
    type Fruit = FacetCounts;

    fn collect(&mut self, doc: DocId, _: Score) {
        self.reader.facet_ords(doc, &mut self.facet_ords_buf);
        let mut previous_collapsed_ord: usize = usize::MAX;
        for &facet_ord in &self.facet_ords_buf {
            let collapsed_ord = self.collapse_mapping[facet_ord as usize];
            self.counts[collapsed_ord] += if collapsed_ord == previous_collapsed_ord {
                0
            } else {
                1
            };
            previous_collapsed_ord = collapsed_ord;
        }
    }

    /// Returns the results of the collection.
    ///
    /// This method does not just return the counters,
    /// it also translates the facet ordinals of the last segment.
    fn harvest(self) -> FacetCounts {
        let mut facet_counts = BTreeMap::new();
        let facet_dict = self.reader.facet_dict();
        for (collapsed_facet_ord, count) in self.counts.iter().cloned().enumerate() {
            if count == 0 {
                continue;
            }
            let mut facet = vec![];
            let facet_ord = self.collapse_facet_ords[collapsed_facet_ord];
            // TODO handle errors.
            if facet_dict.ord_to_term(facet_ord as u64, &mut facet).is_ok() {
                if let Ok(facet) = Facet::from_encoded(facet) {
                    facet_counts.insert(facet, count);
                }
            }
        }
        FacetCounts { facet_counts }
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
    /// Returns an iterator over all of the facet count pairs inside this result.
    /// See the documentation for `FacetCollector` for a usage example.
    pub fn get<T>(&self, facet_from: T) -> FacetChildIterator<'_>
    where
        Facet: From<T>,
    {
        let facet = Facet::from(facet_from);
        let left_bound = Bound::Excluded(facet.clone());
        let right_bound = if facet.is_root() {
            Bound::Unbounded
        } else {
            let mut facet_after_bytes: String = facet.encoded_str().to_owned();
            facet_after_bytes.push('\u{1}');
            let facet_after = Facet::from_encoded_string(facet_after_bytes);
            Bound::Excluded(facet_after)
        };
        let underlying: btree_map::Range<'_, _, _> =
            self.facet_counts.range((left_bound, right_bound));
        FacetChildIterator { underlying }
    }

    /// Returns a vector of top `k` facets with their counts, sorted highest-to-lowest by counts.
    /// See the documentation for `FacetCollector` for a usage example.
    pub fn top_k<T>(&self, facet: T, k: usize) -> Vec<(&Facet, u64)>
    where
        Facet: From<T>,
    {
        let mut heap = BinaryHeap::with_capacity(k);
        let mut it = self.get(facet);

        // push the first k elements to first bring the heap
        // to capacity
        for (facet, count) in (&mut it).take(k) {
            heap.push(Hit { count, facet });
        }

        let mut lowest_count: u64 = heap.peek().map(|hit| hit.count).unwrap_or(u64::MIN); //< the `unwrap_or` case may be triggered but the value
                                                                                          // is never used in that case.

        for (facet, count) in it {
            if count > lowest_count {
                if let Some(mut head) = heap.peek_mut() {
                    *head = Hit { count, facet };
                }
                // the heap gets reconstructed at this point
                if let Some(head) = heap.peek() {
                    lowest_count = head.count;
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
    use crate::collector::Count;
    use crate::core::Index;
    use crate::query::{AllQuery, QueryParser, TermQuery};
    use crate::schema::{Document, Facet, Field, IndexRecordOption, Schema, INDEXED};
    use crate::Term;
    use rand::distributions::Uniform;
    use rand::prelude::SliceRandom;
    use rand::{thread_rng, Rng};
    use std::iter;

    #[test]
    fn test_facet_collector_drilldown() {
        let mut schema_builder = Schema::builder();
        let facet_field = schema_builder.add_facet_field("facet", INDEXED);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);

        let mut index_writer = index.writer_for_tests().unwrap();
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
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        let mut facet_collector = FacetCollector::for_field(facet_field);
        facet_collector.add_facet(Facet::from("/top1"));
        let counts = searcher.search(&AllQuery, &facet_collector).unwrap();

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
                ]
                .iter()
                .map(|&(facet_str, count)| (String::from(facet_str), count))
                .collect::<Vec<_>>()
            );
        }
    }

    #[test]
    #[should_panic(expected = "Tried to add a facet which is a descendant of \
                               an already added facet.")]
    fn test_misused_facet_collector() {
        let mut facet_collector = FacetCollector::for_field(Field::from_field_id(0));
        facet_collector.add_facet(Facet::from("/country"));
        facet_collector.add_facet(Facet::from("/country/europe"));
    }

    #[test]
    fn test_doc_unsorted_multifacet() {
        let mut schema_builder = Schema::builder();
        let facet_field = schema_builder.add_facet_field("facets", INDEXED);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut index_writer = index.writer_for_tests().unwrap();
        index_writer.add_document(doc!(
            facet_field => Facet::from_text(&"/subjects/A/a"),
            facet_field => Facet::from_text(&"/subjects/B/a"),
            facet_field => Facet::from_text(&"/subjects/A/b"),
            facet_field => Facet::from_text(&"/subjects/B/b"),
        ));
        index_writer.commit().unwrap();
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        assert_eq!(searcher.num_docs(), 1);
        let mut facet_collector = FacetCollector::for_field(facet_field);
        facet_collector.add_facet("/subjects");
        let counts = searcher.search(&AllQuery, &facet_collector).unwrap();
        let facets: Vec<(&Facet, u64)> = counts.get("/subjects").collect();
        assert_eq!(facets[0].1, 1);
    }

    #[test]
    fn test_doc_search_by_facet() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let facet_field = schema_builder.add_facet_field("facet", INDEXED);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut index_writer = index.writer_for_tests()?;
        index_writer.add_document(doc!(
            facet_field => Facet::from_text(&"/A/A"),
        ));
        index_writer.add_document(doc!(
            facet_field => Facet::from_text(&"/A/B"),
        ));
        index_writer.add_document(doc!(
            facet_field => Facet::from_text(&"/A/C/A"),
        ));
        index_writer.add_document(doc!(
            facet_field => Facet::from_text(&"/D/C/A"),
        ));
        index_writer.commit()?;
        let reader = index.reader()?;
        let searcher = reader.searcher();
        assert_eq!(searcher.num_docs(), 4);

        let count_facet = |facet_str: &str| {
            let term = Term::from_facet(facet_field, &Facet::from_text(facet_str));
            searcher
                .search(&TermQuery::new(term, IndexRecordOption::Basic), &Count)
                .unwrap()
        };

        assert_eq!(count_facet("/"), 4);
        assert_eq!(count_facet("/A"), 3);
        assert_eq!(count_facet("/A/B"), 1);
        assert_eq!(count_facet("/A/C"), 1);
        assert_eq!(count_facet("/A/C/A"), 1);
        assert_eq!(count_facet("/C/A"), 0);

        let query_parser = QueryParser::for_index(&index, vec![]);
        {
            let query = query_parser.parse_query("facet:/A/B")?;
            assert_eq!(1, searcher.search(&query, &Count).unwrap());
        }
        {
            let query = query_parser.parse_query("facet:/A")?;
            assert_eq!(3, searcher.search(&query, &Count)?);
        }
        Ok(())
    }

    #[test]
    fn test_non_used_facet_collector() {
        let mut facet_collector = FacetCollector::for_field(Field::from_field_id(0));
        facet_collector.add_facet(Facet::from("/country"));
        facet_collector.add_facet(Facet::from("/countryeurope"));
    }

    #[test]
    fn test_facet_collector_topk() {
        let mut schema_builder = Schema::builder();
        let facet_field = schema_builder.add_facet_field("facet", INDEXED);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);

        let uniform = Uniform::new_inclusive(1, 100_000);
        let mut docs: Vec<Document> = vec![("a", 10), ("b", 100), ("c", 7), ("d", 12), ("e", 21)]
            .into_iter()
            .flat_map(|(c, count)| {
                let facet = Facet::from(&format!("/facet/{}", c));
                let doc = doc!(facet_field => facet);
                iter::repeat(doc).take(count)
            })
            .map(|mut doc| {
                doc.add_facet(
                    facet_field,
                    &format!("/facet/{}", thread_rng().sample(&uniform)),
                );
                doc
            })
            .collect();
        docs[..].shuffle(&mut thread_rng());

        let mut index_writer = index.writer_for_tests().unwrap();
        for doc in docs {
            index_writer.add_document(doc);
        }
        index_writer.commit().unwrap();
        let searcher = index.reader().unwrap().searcher();

        let mut facet_collector = FacetCollector::for_field(facet_field);
        facet_collector.add_facet("/facet");
        let counts: FacetCounts = searcher.search(&AllQuery, &facet_collector).unwrap();

        {
            let facets: Vec<(&Facet, u64)> = counts.top_k("/facet", 3);
            assert_eq!(
                facets,
                vec![
                    (&Facet::from("/facet/b"), 100),
                    (&Facet::from("/facet/e"), 21),
                    (&Facet::from("/facet/d"), 12),
                ]
            );
        }
    }

    #[test]
    fn test_facet_collector_topk_tie_break() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let facet_field = schema_builder.add_facet_field("facet", INDEXED);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);

        let docs: Vec<Document> = vec![("b", 2), ("a", 2), ("c", 4)]
            .into_iter()
            .flat_map(|(c, count)| {
                let facet = Facet::from(&format!("/facet/{}", c));
                let doc = doc!(facet_field => facet);
                iter::repeat(doc).take(count)
            })
            .collect();

        let mut index_writer = index.writer_for_tests()?;
        for doc in docs {
            index_writer.add_document(doc);
        }
        index_writer.commit()?;

        let searcher = index.reader()?.searcher();
        let mut facet_collector = FacetCollector::for_field(facet_field);
        facet_collector.add_facet("/facet");
        let counts: FacetCounts = searcher.search(&AllQuery, &facet_collector)?;

        let facets: Vec<(&Facet, u64)> = counts.top_k("/facet", 2);
        assert_eq!(
            facets,
            vec![(&Facet::from("/facet/c"), 4), (&Facet::from("/facet/a"), 2)]
        );
        Ok(())
    }
}

#[cfg(all(test, feature = "unstable"))]
mod bench {

    use crate::collector::FacetCollector;
    use crate::query::AllQuery;
    use crate::schema::{Facet, Schema, INDEXED};
    use crate::Index;
    use rand::seq::SliceRandom;
    use rand::thread_rng;
    use test::Bencher;

    #[bench]
    fn bench_facet_collector(b: &mut Bencher) {
        let mut schema_builder = Schema::builder();
        let facet_field = schema_builder.add_facet_field("facet", INDEXED);
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
        docs[..].shuffle(&mut thread_rng());

        let mut index_writer = index.writer_for_tests().unwrap();
        for doc in docs {
            index_writer.add_document(doc);
        }
        index_writer.commit().unwrap();
        let reader = index.reader().unwrap();
        b.iter(|| {
            let searcher = reader.searcher();
            let facet_collector = FacetCollector::for_field(facet_field);
            searcher.search(&AllQuery, &facet_collector).unwrap();
        });
    }
}

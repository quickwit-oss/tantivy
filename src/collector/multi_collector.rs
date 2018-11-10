use super::Collector;
use super::SegmentCollector;
use DocId;
use Score;
use Result;
use SegmentLocalId;
use SegmentReader;
use downcast::Downcast;
use collector::Fruit;
use std::marker::PhantomData;


pub struct MultiFruit {
    sub_fruits: Vec<Option<Box<Fruit>>>
}


pub struct CollectorWrapper<TCollector: Collector>(TCollector);

impl<TCollector: Collector> Collector for CollectorWrapper<TCollector> {
    type Fruit = Box<Fruit>;
    type Child = Box<BoxableSegmentCollector>;

    fn for_segment(&self, segment_local_id: u32, reader: &SegmentReader) -> Result<Box<BoxableSegmentCollector>> {
        let child = self.0.for_segment(segment_local_id, reader)?;
        Ok(Box::new(SegmentCollectorWrapper(child)))
    }

    fn requires_scoring(&self) -> bool {
        self.0.requires_scoring()
    }

    fn merge_fruits(&self, children: Vec<<Self as Collector>::Fruit>) -> <Self as Collector>::Fruit {
        let typed_fruit: Vec<TCollector::Fruit> = children.into_iter()
            .map(|untyped_fruit| {
                // TODO errors?
                *Downcast::<TCollector::Fruit>::downcast(untyped_fruit).unwrap()
            }).collect();
        let merged_fruit = self.0.merge_fruits(typed_fruit);
        Box::new(merged_fruit)
    }
}


impl SegmentCollector for Box<BoxableSegmentCollector> {

    type Fruit = Box<Fruit>;

    fn collect(&mut self, doc: u32, score: f32) {
        self.as_mut().collect(doc, score);
    }

    fn harvest(self) -> Box<Fruit> {
        BoxableSegmentCollector::harvest_from_box(self)
    }
}

pub trait BoxableSegmentCollector {
    fn collect(&mut self, doc: u32, score: f32);
    fn harvest_from_box(self: Box<Self>) -> Box<Fruit>;
}



pub struct SegmentCollectorWrapper<TSegmentCollector: SegmentCollector>(TSegmentCollector);


impl<TSegmentCollector: SegmentCollector> BoxableSegmentCollector for SegmentCollectorWrapper<TSegmentCollector> {

    fn collect(&mut self, doc: u32, score: f32) {
        self.0.collect(doc, score);
    }

    fn harvest_from_box(self: Box<Self>) -> Box<Fruit> {
        Box::new(self.0.harvest())
    }
}

pub struct FruitHandle<TFruit: Fruit> {
    pos: usize,
    _phantom: PhantomData<TFruit>
}

impl<TFruit: Fruit> FruitHandle<TFruit> {
    pub fn extract(self, fruits: &mut MultiFruit) -> TFruit {
        let boxed_fruit = fruits.sub_fruits[self.pos]
            .take()
            .expect("");
        *Downcast::<TFruit>::downcast(boxed_fruit)
            .expect("Failed")
    }
}

/// Multicollector makes it possible to collect on more than one collector.
/// It should only be used for use cases where the Collector types is unknown
/// at compile time.
/// If the type of the collectors is known, you should prefer to use `ChainedCollector`.
///
/// ```rust
/// #[macro_use]
/// extern crate tantivy;
/// use tantivy::schema::{SchemaBuilder, TEXT};
/// use tantivy::{Index, Result};
/// use tantivy::collector::{Count, TopDocs, MultiCollector};
/// use tantivy::query::QueryParser;
///
/// # fn main() { example().unwrap(); }
/// fn example() -> Result<()> {
///     let mut schema_builder = SchemaBuilder::new();
///     let title = schema_builder.add_text_field("title", TEXT);
///     let schema = schema_builder.build();
///     let index = Index::create_in_ram(schema);
///     {
///         let mut index_writer = index.writer(3_000_000)?;
///         index_writer.add_document(doc!(
///             title => "The Name of the Wind",
///         ));
///         index_writer.add_document(doc!(
///             title => "The Diary of Muadib",
///         ));
///         index_writer.add_document(doc!(
///             title => "A Dairy Cow",
///         ));
///         index_writer.add_document(doc!(
///             title => "The Diary of a Young Girl",
///         ));
///         index_writer.commit().unwrap();
///     }
///
///     index.load_searchers()?;
///     let searcher = index.searcher();
///
///     let mut collectors = MultiCollector::new();
///     let top_docs_handle = collectors.add_collector(TopDocs::with_limit(2));
///     let count_handle = collectors.add_collector(Count);
///     let query_parser = QueryParser::for_index(&index, vec![title]);
///     let query = query_parser.parse_query("diary")?;
///     let mut multi_fruit = searcher.search(&query, &collectors)?;
///
///     let count = count_handle.extract(&mut multi_fruit);
///     let top_docs = top_docs_handle.extract(&mut multi_fruit);
///
///     # assert_eq!(count, 2);
///     # assert_eq!(top_docs.len(), 2);
///
///     Ok(())
/// }
/// ```
pub struct MultiCollector<'a> {
    collector_wrappers: Vec<Box<Collector<Child=Box<BoxableSegmentCollector>,Fruit=Box<Fruit>> + 'a>>
}

impl<'a> MultiCollector<'a> {

    /// Create a new `MultiCollector`
    pub fn new() -> MultiCollector<'a> {
        MultiCollector {
            collector_wrappers: Vec::new()
        }
    }

    pub fn add_collector<'b: 'a, TCollector: Collector + 'b>(&mut self, collector: TCollector) -> FruitHandle<TCollector::Fruit> {
        let pos = self.collector_wrappers.len();
        self.collector_wrappers.push(Box::new(CollectorWrapper(collector)));
        FruitHandle {
            pos,
            _phantom: PhantomData
        }
    }
}

impl<'a> Collector for MultiCollector<'a> {
    type Fruit = MultiFruit;
    type Child = MultiCollectorChild;

    fn for_segment(&self, segment_local_id: SegmentLocalId, segment: &SegmentReader) -> Result<MultiCollectorChild> {
        let children = self.collector_wrappers
            .iter()
            .map(|collector_wrapper| {
                collector_wrapper.for_segment(segment_local_id, segment)
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(MultiCollectorChild {
            children
        })
    }

    fn requires_scoring(&self) -> bool {
        self.collector_wrappers
            .iter()
            .any(|c| c.requires_scoring())
    }

    fn merge_fruits(&self, segments_multifruits: Vec<MultiFruit>)
        -> MultiFruit {
        let mut segment_fruits_list: Vec<Vec<Box<Fruit>>> =
            (0..self.collector_wrappers.len())
                .map(|_| Vec::with_capacity(segments_multifruits.len()))
                .collect::<Vec<_>>();
        for segment_multifruit in segments_multifruits {
            for (idx, segment_fruit_opt) in segment_multifruit.sub_fruits.into_iter().enumerate() {
                if let Some(segment_fruit) = segment_fruit_opt {
                    segment_fruits_list[idx].push(segment_fruit);
                }
            }
        }
        let sub_fruits = self.collector_wrappers
            .iter()
            .zip(segment_fruits_list)
            .map(|(child_collector, segment_fruits)|
                Some(child_collector.merge_fruits(segment_fruits))
            )
            .collect();
        MultiFruit { sub_fruits }
    }

}


pub struct MultiCollectorChild {
    children: Vec<Box<BoxableSegmentCollector>>
}

impl SegmentCollector for MultiCollectorChild {
    type Fruit = MultiFruit;

    fn collect(&mut self, doc: DocId, score: Score) {
        for child in &mut self.children {
            child.collect(doc, score);
        }
    }

    fn harvest(self) -> MultiFruit {
        MultiFruit {
            sub_fruits: self.children
                .into_iter()
                .map(|child| Some(child.harvest()) )
                .collect()
        }
    }
}


#[cfg(test)]
mod tests {

    use super::*;
    use collector::{Collector, Count, TopDocs};
    use schema::{TEXT, SchemaBuilder};
    use query::TermQuery;
    use Index;
    use Term;
    use schema::IndexRecordOption;

    #[test]
    fn test_multi_collector() {
        let mut schema_builder = SchemaBuilder::new();
        let text = schema_builder.add_text_field("text", TEXT);
        let schema = schema_builder.build();

        let index = Index::create_in_ram(schema);
        {
            let mut index_writer = index.writer_with_num_threads(1, 3_000_000).unwrap();
            index_writer.add_document(doc!(text=>"abc"));
            index_writer.add_document(doc!(text=>"abc abc abc"));
            index_writer.add_document(doc!(text=>"abc abc"));
            index_writer.commit().unwrap();
            index_writer.add_document(doc!(text=>""));
            index_writer.add_document(doc!(text=>"abc abc abc abc"));
            index_writer.add_document(doc!(text=>"abc"));
            index_writer.commit().unwrap();
        }
        index.load_searchers().unwrap();
        let searcher = index.searcher();
        let term = Term::from_field_text(text, "abc");
        let query = TermQuery::new(term, IndexRecordOption::Basic);

        let mut collectors = MultiCollector::new();
        let topdocs_handler = collectors.add_collector(TopDocs::with_limit(2));
        let count_handler = collectors.add_collector(Count);
        let mut multifruits = collectors.search(&searcher, &query).unwrap();

        assert_eq!(count_handler.extract(&mut multifruits), 5);
        assert_eq!(topdocs_handler.extract(&mut multifruits).len(), 2);
    }
}


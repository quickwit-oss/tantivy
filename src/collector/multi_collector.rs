use super::Collector;
use super::SegmentCollector;
use DocId;
use Score;
use Result;
use SegmentLocalId;
use SegmentReader;
use downcast::Downcast;


pub struct CollectorWrapper<'a, TCollector: 'a + Collector>(&'a mut TCollector);

impl<'a, T: 'a + Collector> CollectorWrapper<'a, T> {
    pub fn new(collector: &'a mut T) -> CollectorWrapper<'a, T> {
        CollectorWrapper(collector)
    }
}

impl<'a, T: 'a + Collector> Collector for CollectorWrapper<'a, T> {
    type Child = T::Child;

    fn for_segment(&mut self, segment_local_id: u32, segment: &SegmentReader) -> Result<T::Child> {
        self.0.for_segment(segment_local_id, segment)
    }

    fn requires_scoring(&self) -> bool {
        self.0.requires_scoring()
    }

    fn merge_children(&mut self, children: Vec<T::Child>) {
        self.0.merge_children(children)
    }
}

trait UntypedCollector {
    fn for_segment(&mut self, segment_local_id: u32, segment: &SegmentReader) -> Result<Box<SegmentCollector>>;

    fn requires_scoring(&self) -> bool;

    fn merge_children_anys(&mut self, childrens: Vec<Box<SegmentCollector>>);
}


impl<'a, TCollector:'a + Collector> UntypedCollector for CollectorWrapper<'a, TCollector> {
    fn for_segment(&mut self, segment_local_id: u32, segment: &SegmentReader) -> Result<Box<SegmentCollector>> {
        let segment_collector = self.0.for_segment(segment_local_id, segment)?;
        Ok(Box::new(segment_collector))
    }

    fn requires_scoring(&self) -> bool {
        self.0.requires_scoring()
    }

    fn merge_children_anys(&mut self, childrens: Vec<Box<SegmentCollector>>) {
        let typed_children: Vec<TCollector::Child> = childrens.into_iter()
            .map(|untyped_child_collector| {
                *Downcast::<TCollector::Child>::downcast(untyped_child_collector).unwrap()
            }).collect();
        self.0.merge_children(typed_children);
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
/// use tantivy::collector::{CountCollector, TopScoreCollector, MultiCollector};
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
///     {
///         let mut top_collector = TopScoreCollector::with_limit(2);
///         let mut count_collector = CountCollector::default();
///         {
///             let mut collectors = MultiCollector::new();
///             collectors.add_collector(&mut top_collector);
///             collectors.add_collector(&mut count_collector);
///             let query_parser = QueryParser::for_index(&index, vec![title]);
///             let query = query_parser.parse_query("diary")?;
///             searcher.search(&*query, &mut collectors).unwrap();
///         }
///         assert_eq!(count_collector.count(), 2);
///         assert!(top_collector.at_capacity());
///     }
///
///     Ok(())
/// }
/// ```
pub struct MultiCollector<'a> {
    collector_wrappers: Vec<Box<UntypedCollector + 'a>>
}

impl<'a> MultiCollector<'a> {
    pub fn new() -> MultiCollector<'a> {
        MultiCollector {
            collector_wrappers: Vec::new()
        }
    }

    pub fn add_collector<TCollector: 'a + Collector>(&mut self, collector: &'a mut TCollector) {
        let collector_wrapper = CollectorWrapper(collector);
        self.collector_wrappers.push(Box::new(collector_wrapper));
    }
}

impl<'a> Collector for MultiCollector<'a> {

    type Child = MultiCollectorChild;

    fn for_segment(&mut self, segment_local_id: SegmentLocalId, segment: &SegmentReader) -> Result<MultiCollectorChild> {
        let children = self.collector_wrappers
            .iter_mut()
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

    fn merge_children(&mut self, children: Vec<MultiCollectorChild>) {
        let mut per_collector_children: Vec<Vec<Box<SegmentCollector>>> =
            (0..self.collector_wrappers.len())
                .map(|_| Vec::with_capacity(children.len()))
                .collect::<Vec<_>>();
        for child in children {
            for (idx, segment_collector) in child.children.into_iter().enumerate() {
                per_collector_children[idx].push(segment_collector);
            }
        }
        for (collector, children) in self.collector_wrappers.iter_mut().zip(per_collector_children) {
            collector.merge_children_anys(children);
        }
    }

}

trait UntypedSegmentCollector {
    fn collect();
}

pub struct MultiCollectorChild {
    children: Vec<Box<SegmentCollector>>
}

impl SegmentCollector for MultiCollectorChild {
    fn collect(&mut self, doc: DocId, score: Score) {
        for child in &mut self.children {
            child.collect(doc, score);
        }
    }
}


#[cfg(test)]
mod tests {

    use super::*;
    use collector::{Collector, CountCollector, TopCollector};
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
        let mut top_collector = TopCollector::with_limit(2);
        let mut count_collector = CountCollector::default();
        {
            let mut collectors = MultiCollector::new();
            collectors.add_collector(&mut top_collector);
            collectors.add_collector(&mut count_collector);
            collectors.search(&*searcher, &query).unwrap();
        }
        assert_eq!(count_collector.count(), 5);
    }
}

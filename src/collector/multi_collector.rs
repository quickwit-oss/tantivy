use super::Collector;
use DocId;
use Result;
use Score;
use SegmentLocalId;
use SegmentReader;

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
/// use tantivy::collector::{CountCollector, TopCollector, MultiCollector};
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
///         let mut top_collector = TopCollector::with_limit(2);
///         let mut count_collector = CountCollector::default();
///         {
///             let mut collectors =
///                 MultiCollector::from(vec![&mut top_collector, &mut count_collector]);
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
    collectors: Vec<&'a mut Collector>,
}

impl<'a> MultiCollector<'a> {
    /// Constructor
    pub fn from(collectors: Vec<&'a mut Collector>) -> MultiCollector {
        MultiCollector { collectors }
    }
}

impl<'a> Collector for MultiCollector<'a> {
    fn set_segment(
        &mut self,
        segment_local_id: SegmentLocalId,
        segment: &SegmentReader,
    ) -> Result<()> {
        for collector in &mut self.collectors {
            collector.set_segment(segment_local_id, segment)?;
        }
        Ok(())
    }

    fn collect(&mut self, doc: DocId, score: Score) {
        for collector in &mut self.collectors {
            collector.collect(doc, score);
        }
    }
    fn requires_scoring(&self) -> bool {
        self.collectors
            .iter()
            .any(|collector| collector.requires_scoring())
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use collector::{Collector, CountCollector, TopCollector};

    #[test]
    fn test_multi_collector() {
        let mut top_collector = TopCollector::with_limit(2);
        let mut count_collector = CountCollector::default();
        {
            let mut collectors =
                MultiCollector::from(vec![&mut top_collector, &mut count_collector]);
            collectors.collect(1, 0.2);
            collectors.collect(2, 0.1);
            collectors.collect(3, 0.5);
        }
        assert_eq!(count_collector.count(), 3);
        assert!(top_collector.at_capacity());
    }
}

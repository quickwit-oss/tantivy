use super::Collector;
use DocId;
use Result;
use Score;
use SegmentLocalId;
use SegmentReader;
use collector::SegmentCollector;

/// `CountCollector` collector only counts how many
/// documents match the query.
///
/// ```rust
/// #[macro_use]
/// extern crate tantivy;
/// use tantivy::schema::{SchemaBuilder, TEXT};
/// use tantivy::{Index, Result};
/// use tantivy::collector::Count;
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
///         let query_parser = QueryParser::for_index(&index, vec![title]);
///         let query = query_parser.parse_query("diary")?;
///         let count = searcher.search(&query, &Count).unwrap();
///
///         assert_eq!(count, 2);
///     }
///
///     Ok(())
/// }
/// ```
pub struct Count;


impl Collector for Count {

    type Fruit = usize;

    type Child = SegmentCountCollector;

    fn for_segment(&self, _: SegmentLocalId, _: &SegmentReader) -> Result<SegmentCountCollector> {
        Ok(SegmentCountCollector::default())
    }

    fn requires_scoring(&self) -> bool {
        false
    }

    fn merge_fruits(&self, segment_counts: Vec<usize>) -> usize {
        segment_counts.into_iter().sum()
    }
}


#[derive(Default)]
pub struct SegmentCountCollector {
    count: usize,
}


impl SegmentCollector for SegmentCountCollector {
    type Fruit = usize;

    fn collect(&mut self, _: DocId, _: Score) {
        self.count += 1;
    }

    fn harvest(self) -> usize {
        self.count
    }
}


#[cfg(test)]
mod tests {
    use super::{Count, SegmentCountCollector};
    use collector::SegmentCollector;
    use collector::Collector;

    #[test]
    fn test_count_collect_does_not_requires_scoring() {
        assert!(!Count.requires_scoring());
    }

    #[test]
    fn test_segment_count_collector() {
        {
            let count_collector = SegmentCountCollector::default();
            assert_eq!(count_collector.harvest(), 0);
        }
        {
            let mut count_collector = SegmentCountCollector::default();
            count_collector.collect(0u32, 1f32);
            assert_eq!(count_collector.harvest(), 1);
        }
        {
            let mut count_collector = SegmentCountCollector::default();
            count_collector.collect(0u32, 1f32);
            assert_eq!(count_collector.harvest(), 1);
        }
        {
            let mut count_collector = SegmentCountCollector::default();
            count_collector.collect(0u32, 1f32);
            count_collector.collect(1u32, 1f32);
            assert_eq!(count_collector.harvest(), 2);
        }

    }

}

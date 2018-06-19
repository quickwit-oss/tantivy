use super::Collector;
use DocId;
use Result;
use Score;
use SegmentLocalId;
use SegmentReader;

/// `CountCollector` collector only counts how many
/// documents match the query.
///
/// ```rust
/// #[macro_use]
/// extern crate tantivy;
/// use tantivy::schema::{SchemaBuilder, TEXT};
/// use tantivy::{Index, Result};
/// use tantivy::collector::CountCollector;
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
///	        let mut count_collector = CountCollector::default();
///         let query_parser = QueryParser::for_index(&index, vec![title]);
///         let query = query_parser.parse_query("diary")?;
///         searcher.search(&*query, &mut count_collector).unwrap();
///
///         assert_eq!(count_collector.count(), 2);
///     }
///
///     Ok(())
/// }
/// ```
#[derive(Default)]
pub struct CountCollector {
    count: usize,
}

impl CountCollector {
    /// Returns the count of documents that were
    /// collected.
    pub fn count(&self) -> usize {
        self.count
    }
}

impl Collector for CountCollector {
    fn set_segment(&mut self, _: SegmentLocalId, _: &SegmentReader) -> Result<()> {
        Ok(())
    }

    fn collect(&mut self, _: DocId, _: Score) {
        self.count += 1;
    }

    fn requires_scoring(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {

    use collector::{Collector, CountCollector};

    #[test]
    fn test_count_collector() {
        let mut count_collector = CountCollector::default();
        assert_eq!(count_collector.count(), 0);
        count_collector.collect(0u32, 1f32);
        assert_eq!(count_collector.count(), 1);
        assert_eq!(count_collector.count(), 1);
        count_collector.collect(1u32, 1f32);
        assert_eq!(count_collector.count(), 2);
        assert!(!count_collector.requires_scoring());
    }

}

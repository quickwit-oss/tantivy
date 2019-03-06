// # Searching a range on an indexed int field.
//
// Below is an example of creating an indexed integer field in your schema
// You can use RangeQuery to get a Count of all occurrences in a given range.

#[macro_use]
extern crate tantivy;
use tantivy::collector::Count;
use tantivy::query::RangeQuery;
use tantivy::schema::{Schema, INDEXED};
use tantivy::Index;
use tantivy::Result;

fn run() -> Result<()> {
    // For the sake of simplicity, this schema will only have 1 field
    let mut schema_builder = Schema::builder();

    // `INDEXED` is a short-hand to indicate that our field should be "searchable".
    let year_field = schema_builder.add_u64_field("year", INDEXED);
    let schema = schema_builder.build();
    let index = Index::create_in_ram(schema);
    {
        let mut index_writer = index.writer_with_num_threads(1, 6_000_000)?;
        for year in 1950u64..2019u64 {
            index_writer.add_document(doc!(year_field => year));
        }
        index_writer.commit()?;
        // The index will be a range of years
    }
    index.load_searchers()?;
    let searcher = index.searcher();
    // The end is excluded i.e. here we are searching up to 1969
    let docs_in_the_sixties = RangeQuery::new_u64(year_field, 1960..1970);
    // Uses a Count collector to sum the total number of docs in the range
    let num_60s_books = searcher.search(&docs_in_the_sixties, &Count)?;
    assert_eq!(num_60s_books, 10);
    Ok(())
}

fn main() {
    run().unwrap()
}

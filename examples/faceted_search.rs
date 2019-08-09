// # Basic Example
//
// This example covers the basic functionalities of
// tantivy.
//
// We will :
// - define our schema
// = create an index in a directory
// - index few documents in our index
// - search for the best document matchings "sea whale"
// - retrieve the best document original content.

// ---
// Importing tantivy...
#[macro_use]
extern crate tantivy;
use tantivy::collector::FacetCollector;
use tantivy::query::AllQuery;
use tantivy::schema::*;
use tantivy::Index;
use tempfile::TempDir;

fn main() -> tantivy::Result<()> {
    // Let's create a temporary directory for the
    // sake of this example
    let index_path = TempDir::new()?;
    let mut schema_builder = Schema::builder();

    schema_builder.add_text_field("name", TEXT | STORED);

    // this is our faceted field
    schema_builder.add_facet_field("tags");

    let schema = schema_builder.build();

    let index = Index::create_in_dir(&index_path, schema.clone())?;

    let mut index_writer = index.writer(50_000_000)?;

    let name = schema.get_field("name").unwrap();
    let tags = schema.get_field("tags").unwrap();

    // For convenience, tantivy also comes with a macro to
    // reduce the boilerplate above.
    index_writer.add_document(doc!(
        name => "the ditch",
        tags => Facet::from("/pools/north")
    ));

    index_writer.add_document(doc!(
        name => "little stacey",
        tags => Facet::from("/pools/south")
    ));

    index_writer.commit()?;

    let reader = index.reader()?;

    let searcher = reader.searcher();

    let mut facet_collector = FacetCollector::for_field(tags);
    facet_collector.add_facet("/pools");

    let facet_counts = searcher.search(&AllQuery, &facet_collector).unwrap();

    // This lists all of the facet counts
    let facets: Vec<(&Facet, u64)> = facet_counts.get("/pools").collect();
    assert_eq!(
        facets,
        vec![
            (&Facet::from("/pools/north"), 1),
            (&Facet::from("/pools/south"), 1),
        ]
    );

    Ok(())
}

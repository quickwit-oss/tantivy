use std::ops::Bound;

// # Searching a range on an indexed int field.
//
// Below is an example of creating an indexed integer field in your schema
// You can use RangeQuery to get a Count of all occurrences in a given range.
use tantivy::collector::{Count, TopDocs};
use tantivy::query::{BooleanQuery, InvertedIndexRangeQuery, RangeQuery};
use tantivy::schema::{Schema, FAST, INDEXED, STORED};
use tantivy::{doc, Document, Index, IndexWriter, Result, TantivyDocument, Term};

fn main() -> Result<()> {
    // For the sake of simplicity, this schema will only have 1 field
    let mut schema_builder = Schema::builder();

    // `INDEXED` is a short-hand to indicate that our field should be "searchable".
    let start_range = schema_builder.add_text_field("start_range", STORED | FAST);
    let end_range = schema_builder.add_text_field("end_range",  STORED | FAST);
    let schema = schema_builder.build();
    let index = Index::create_in_ram(schema.clone());
    let reader = index.reader()?;
    {
        let mut index_writer: IndexWriter = index.writer_with_num_threads(1, 20_000_000)?;
        // Add sample range data to the index
        let ranges = vec![("10A", "20A"), ("B30", "B40"), ("5C0", "5C6")];

        for (start, end) in ranges {
            let mut doc = TantivyDocument::default();
            doc.add_text(start_range, start);
            doc.add_text(end_range, end);
            index_writer.add_document(doc)?;
        }
        index_writer.commit()?;
    }
    reader.reload()?;
    let searcher = reader.searcher();
    // The end is excluded i.e. here we are searching up to 1969
    let one = RangeQuery::new(
        Bound::Unbounded,
        Bound::Included(Term::from_field_text(start_range, "5C1")),
    );
    let two = RangeQuery::new(
        Bound::Included(Term::from_field_text(end_range, "5C1")),
        Bound::Unbounded,
        
    );
    let query = BooleanQuery::intersection(vec![Box::new(one), Box::new(two)]);
    let top_docs = searcher.search(&query, &TopDocs::with_limit(10))?;

    for (_score, doc_address) in top_docs {
        let retrieved_doc: TantivyDocument = searcher.doc(doc_address)?;
        println!("{}", retrieved_doc.to_json(&schema));
    }

    Ok(())
}

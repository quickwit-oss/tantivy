use tantivy::collector::{OrderField, TopDocs};
use tantivy::query::QueryParser;
use tantivy::schema::{Schema, FAST, STORED, TEXT};
use tantivy::Index;
use tantivy::{doc, ReloadPolicy};
use tempfile::TempDir;

fn main() -> tantivy::Result<()> {
    let index_path = TempDir::new()?;

    let mut schema_builder = Schema::builder();

    schema_builder.add_text_field("title", TEXT | STORED);
    schema_builder.add_u64_field("star", FAST);
    schema_builder.add_u64_field("fork", FAST);
    let schema = schema_builder.build();

    let index = Index::create_in_dir(&index_path, schema.clone())?;

    let mut index_writer = index.writer_with_num_threads(1, 50_000_000)?;

    let title = schema.get_field("title").unwrap();
    let star = schema.get_field("star").unwrap();
    let fork = schema.get_field("fork").unwrap();

    index_writer.add_document(doc!(title => "a is good good", star => 10u64, fork => 11u64));
    index_writer.add_document(doc!(title => "b is good", star => 11u64, fork => 10u64));
    index_writer.add_document(doc!(title => "c is good", star => 9u64, fork => 12u64));
    index_writer.add_document(doc!(title => "d is good", star => 11u64, fork => 9u64));

    assert!(index_writer.commit().is_ok());

    let reader = index
        .reader_builder()
        .reload_policy(ReloadPolicy::OnCommit)
        .try_into()?;

    let searcher = reader.searcher();

    let query = QueryParser::for_index(&index, vec![title]).parse_query("good")?;
    let top_docs = TopDocs::with_limit(10).order_by(&vec![
        OrderField::with_score().desc(),
        OrderField::with_field(star).asc(),
        OrderField::with_field(fork).desc(),
    ]);
    let resulting_docs = searcher.search(&query, &top_docs)?;

    for (feature, doc_address) in resulting_docs {
        let retrieved_doc = searcher.doc(doc_address)?;
        println!("{:?}, {:?}", doc_address, feature);
        println!("{}\n", schema.to_json(&retrieved_doc));
    }

    Ok(())
}

// # Reload after commit
//
// `IndexWriter::commit()` only guarantees that your documents are durably
// persisted to the `Directory`. It does **not** guarantee that an already-open
// `IndexReader` can see them right away: with the default
// `ReloadPolicy::OnCommitWithDelay`, the reader reloads on a background thread
// that is not synchronized with `commit()`'s return, on any `Directory`
// implementation (including an in-memory `RamDirectory`). A search performed
// immediately after `commit()` can therefore still miss the documents you
// just added.
//
// If you need a search right after a commit to deterministically reflect that
// commit, build the reader with `ReloadPolicy::Manual` and call
// `reader.reload()` yourself, as shown below.

use tantivy::collector::TopDocs;
use tantivy::query::QueryParser;
use tantivy::schema::*;
use tantivy::{doc, Index, IndexWriter, ReloadPolicy};

fn main() -> tantivy::Result<()> {
    let mut schema_builder = Schema::builder();
    let title = schema_builder.add_text_field("title", TEXT | STORED);
    let schema = schema_builder.build();

    let index = Index::create_in_ram(schema);

    // `ReloadPolicy::Manual` puts us in control of exactly when the reader
    // sees new commits, instead of racing against the background reload.
    let reader = index
        .reader_builder()
        .reload_policy(ReloadPolicy::Manual)
        .try_into()?;

    let mut index_writer: IndexWriter = index.writer(15_000_000)?;
    index_writer.add_document(doc!(
        title => "The Old Man and the Sea"
    ))?;
    index_writer.commit()?;

    // Without this explicit reload, the searcher below could still be
    // looking at the pre-commit (empty) version of the index.
    reader.reload()?;

    let query_parser = QueryParser::for_index(&index, vec![title]);
    let query = query_parser.parse_query("sea")?;

    let searcher = reader.searcher();
    let top_docs = searcher.search(&query, &TopDocs::with_limit(10).order_by_score())?;

    assert!(!top_docs.is_empty());
    println!(
        "Found {} matching document(s) right after commit.",
        top_docs.len()
    );

    Ok(())
}

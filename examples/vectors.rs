// # Basic Example
//
// This example covers the basic functionalities of
// tantivy.
//
// We will :
// - define our schema
// - create an index in a directory
// - index a few documents into our index
// - search for the best document matching a basic query
// - retrieve the best document's original content.

use log::{info, trace};
// ---
// Importing tantivy...
use tantivy::collector::{TopDocs};
use tantivy::query::{QueryParser, TermQuery, VectorQuery};
use tantivy::schema::*;
use tantivy::{doc, Index, ReloadPolicy};
use tempfile::TempDir;

fn main() -> tantivy::Result<()> {
    env_logger::init();
    info!("Vectors example");

    let index_path = TempDir::new()?;


    let mut schema_builder = Schema::builder();

    let vector_options = VectorOptions{
        dimension: 3,
        indexed: true,
        stored: true
    };
   
    schema_builder.add_text_field("title", TEXT | STORED);
    schema_builder.add_text_field("body", TEXT);
    schema_builder.add_vector_field("vector", vector_options);



    let schema = schema_builder.build();

    
    let index = Index::create_in_dir(&index_path, schema.clone())?;
    let mut index_writer = index.writer(50_000_000)?;

    let title = schema.get_field("title").unwrap();
    let body = schema.get_field("body").unwrap();
    let vector = schema.get_field("vector").unwrap();

    let mut old_man_doc = Document::default();
    old_man_doc.add_text(title, "The Old Man and the Sea");
    old_man_doc.add_text(
        body,
        "He was an old man who fished alone in a skiff in the Gulf Stream and \
         he had gone eighty-four days now without taking a fish.",
    );
    old_man_doc.add_vector(vector, vec![0.3, 0.5, 0.4]);

    // ... and add it to the `IndexWriter`.
    index_writer.add_document(old_man_doc)?;

    // For convenience, tantivy also comes with a macro to
    // reduce the boilerplate above.
    index_writer.add_document(doc!(
    title => "Of Mice and Men",
    body => "A few miles south of Soledad, the Salinas River drops in close to the hillside \
            bank and runs deep and green. The water is warm too, for it has slipped twinkling \
            over the yellow sands in the sunlight before reaching the narrow pool. On one \
            side of the river the golden foothill slopes curve up to the strong and rocky \
            Gabilan Mountains, but on the valley side the water is lined with trees—willows \
            fresh and green with every spring, carrying in their lower leaf junctures the \
            debris of the winter’s flooding; and sycamores with mottled, white, recumbent \
            limbs and branches that arch over the pool",
    vector => vec![0.2, 0.5, 0.3]

    ))?;

    // Multivalued field just need to be repeated.
    index_writer.add_document(doc!(
    title => "Frankenstein",
    title => "The Modern Prometheus",
    body => "You will rejoice to hear that no disaster has accompanied the commencement of an \
             enterprise which you have regarded with such evil forebodings.  I arrived here \
             yesterday, and my first task is to assure my dear sister of my welfare and \
             increasing confidence in the success of my undertaking.",
    vector => vec![0.4, 0.5, 0.3]
    ))?;


    trace!("Commit!");
    index_writer.commit()?;

  
    let reader = index
        .reader_builder()
        .reload_policy(ReloadPolicy::OnCommit)
        .try_into()?;

    // We now need to acquire a searcher.
    //
    // A searcher points to a snapshotted, immutable version of the index.
    //
    // Some search experience might require more than
    // one query. Using the same searcher ensures that all of these queries will run on the
    // same version of the index.
    //
    // Acquiring a `searcher` is very cheap.
    //
    // You should acquire a searcher every time you start processing a request and
    // and release it right after your query is finished.
    let searcher = reader.searcher();

    // ### Query

    let v: Vec<f32> = vec![0.4, 0.5, 0.3];
    let term_query = VectorQuery::new(vector, v);
    let top_docs = searcher.search(&term_query, &TopDocs::with_limit(10))?;
    for (score, doc_address) in top_docs {
        println!("doc_address: {:?}, score: {:?}", doc_address, score);
    }


/* 
    let query_parser = QueryParser::for_index(&index, vec![title, body]);
    let query = query_parser.parse_query("sea whale")?;
    let top_docs = searcher.search(&query, &TopDocs::with_limit(10))?;
    for (_score, doc_address) in top_docs {
        let retrieved_doc = searcher.doc(doc_address)?;
        println!("{}", schema.to_json(&retrieved_doc));
    }
*/
    Ok(())
}

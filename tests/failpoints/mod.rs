use fail;
use std::path::Path;
use tantivy::directory::{Directory, ManagedDirectory, RAMDirectory, TerminatingWrite};
use tantivy::doc;
use tantivy::schema::{Schema, TEXT};
use tantivy::{Index, Term};

#[test]
fn test_failpoints_managed_directory_gc_if_delete_fails() {
    let _scenario = fail::FailScenario::setup();

    let test_path: &'static Path = Path::new("some_path_for_test");

    let ram_directory = RAMDirectory::create();
    let mut managed_directory = ManagedDirectory::wrap(ram_directory).unwrap();
    managed_directory
        .open_write(test_path)
        .unwrap()
        .terminate()
        .unwrap();
    assert!(managed_directory.exists(test_path).unwrap());
    // triggering gc and setting the delete operation to fail.
    //
    // We are checking that the gc operation is not removing the
    // file from managed.json to ensure that the file will be removed
    // in the next gc.
    //
    // The initial 1*off is there to allow for the removal of the
    // lock file.
    fail::cfg("RAMDirectory::delete", "1*off->1*return").unwrap();
    assert!(managed_directory.garbage_collect(Default::default).is_ok());
    assert!(managed_directory.exists(test_path).unwrap());

    // running the gc a second time should remove the file.
    assert!(managed_directory.garbage_collect(Default::default).is_ok());
    assert!(
        !managed_directory.exists(test_path).unwrap(),
        "The file should have been deleted"
    );
}

#[test]
fn test_write_commit_fails() -> tantivy::Result<()> {
    let _fail_scenario_guard = fail::FailScenario::setup();
    let mut schema_builder = Schema::builder();
    let text_field = schema_builder.add_text_field("text", TEXT);
    let index = Index::create_in_ram(schema_builder.build());

    let mut index_writer = index.writer_with_num_threads(1, 3_000_000)?;
    for _ in 0..100 {
        index_writer.add_document(doc!(text_field => "a"));
    }
    index_writer.commit()?;
    fail::cfg("RAMDirectory::atomic_write", "return(error_write_failed)").unwrap();
    for _ in 0..100 {
        index_writer.add_document(doc!(text_field => "b"));
    }
    assert!(index_writer.commit().is_err());

    let num_docs_containing = |s: &str| {
        let term_a = Term::from_field_text(text_field, s);
        index.reader()?.searcher().doc_freq(&term_a)
    };
    assert_eq!(num_docs_containing("a")?, 100);
    assert_eq!(num_docs_containing("b")?, 0);
    Ok(())
}

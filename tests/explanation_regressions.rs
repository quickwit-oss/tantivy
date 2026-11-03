use tantivy::collector::TopDocs;
use tantivy::query::{BooleanQuery, BoostQuery, Occur, Query, TermQuery};
use tantivy::schema::{IndexRecordOption, Schema, TEXT};
use tantivy::{doc, DocAddress, Index, Term};

fn term(field: tantivy::schema::Field, value: &str) -> Box<dyn Query> {
    Box::new(TermQuery::new(Term::from_field_text(field, value), IndexRecordOption::WithFreqs))
}

#[test]
fn boolean_explain_nonmatching_early_doc_returns_error() -> tantivy::Result<()> {
    let mut builder = Schema::builder();
    let field = builder.add_text_field("title", TEXT);
    let index = Index::create_in_ram(builder.build());
    let mut writer = index.writer_with_num_threads::<tantivy::TantivyDocument>(1, 50_000_000)?;
    writer.add_document(doc!(field => "amber"))?;
    writer.add_document(doc!(field => "quartz"))?;
    writer.commit()?;
    let reader = index.reader()?;
    let searcher = reader.searcher();
    for clauses in [vec![(Occur::Should, term(field, "absent"))], vec![(Occur::Must, term(field, "quartz"))], vec![(Occur::Should, term(field, "quartz")), (Occur::Should, term(field, "absent"))]] {
        let query = BooleanQuery::new(clauses);
        assert!(query.explain(&searcher, DocAddress::new(0, 0)).is_err());
    }
    Ok(())
}

fn boost_case(outer: f32, inner: f32, field_boost: f32) -> tantivy::Result<()> {
    let mut builder = Schema::builder();
    let field = builder.add_text_field("title", TEXT);
    let index = Index::create_in_ram(builder.build());
    let mut writer = index.writer_with_num_threads::<tantivy::TantivyDocument>(1, 50_000_000)?;
    for i in 0..10 {
        writer.add_document(doc!(field => if i == 0 { "amber" } else { "quartz" }))?;
    }
    writer.commit()?;
    let reader = index.reader()?;
    let searcher = reader.searcher();
    let query = BoostQuery::new(Box::new(BoostQuery::new(Box::new(BoostQuery::new(term(field, "amber"), field_boost)), inner)), outer);
    let hits = searcher.search(&query, &TopDocs::with_limit(10).order_by_score())?;
    assert_eq!(hits.len(), 1);
    assert!(hits[0].0.is_finite());
    let explanation = query.explain(&searcher, hits[0].1)?;
    assert_eq!(explanation.value(), hits[0].0);
    fn finite(node: &serde_json::Value) {
        assert!(node["value"].as_f64().is_some_and(f64::is_finite), "{node}");
        if let Some(children) = node["details"].as_array() {
            for child in children { finite(child); }
        }
    }
    finite(&serde_json::to_value(explanation).unwrap());
    Ok(())
}

#[test]
fn zero_boost_explanation_preserves_outer_to_inner_arithmetic() -> tantivy::Result<()> {
    boost_case(0.0, 2.0, 3e38)
}

#[test]
fn small_boost_explanation_preserves_outer_to_inner_arithmetic() -> tantivy::Result<()> {
    boost_case(1e-20, 1e20, 1e20)
}

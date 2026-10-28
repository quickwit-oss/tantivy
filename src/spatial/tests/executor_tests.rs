use super::*;
use crate::collector::Count;
use crate::merge_policy::NoMergePolicy;
use crate::query::TermQuery;
use crate::schema::{IndexRecordOption, Schema, SPHERE, STRING};
use crate::{Index, IndexWriter, TantivyDocument, Term};

#[test]
fn test_deleted_inner_geometry_does_not_match_join() -> crate::Result<()> {
    let mut builder = Schema::builder();
    let field = builder.add_spatial_field("geometry", SPHERE);
    let kind = builder.add_text_field("kind", STRING);
    let schema = builder.build();
    let index = Index::create_in_ram(schema.clone());
    let mut writer: IndexWriter = index.writer_with_num_threads(1, 50_000_000)?;
    writer.set_merge_policy(Box::new(NoMergePolicy));
    for json in [
        r#"{"kind":"parcel","geometry":{"type":"Polygon","coordinates":[[[-0.01,-0.01],[0.01,-0.01],[0.01,0.01],[-0.01,0.01],[-0.01,-0.01]]]}}"#,
        r#"{"kind":"fiber","geometry":{"type":"LineString","coordinates":[[-0.005,0],[0.005,0]]}}"#,
    ] {
        writer.add_document(TantivyDocument::parse_json(&schema, json)?)?;
    }
    writer.commit()?;
    let reader = index.reader()?;
    let term = |text| TermQuery::new(Term::from_field_text(kind, text), IndexRecordOption::Basic);
    let join = SpatialExecutor::new(PlanNode::Join {
        field,
        outer: Box::new(PlanNode::Query(Box::new(term("parcel")))),
        inner: Box::new(PlanNode::Query(Box::new(term("fiber")))),
        relation: SpatialRelation::Near(61.0 / 6_371_000.0),
    });
    assert_eq!(reader.searcher().search(&join, &Count)?, 1);

    writer.delete_term(Term::from_field_text(kind, "fiber"));
    writer.commit()?;
    reader.reload()?;
    let searcher = reader.searcher();
    assert_eq!(searcher.segment_readers().len(), 1);
    let segment = &searcher.segment_readers()[0];
    // Keep the deleted fiber in the segment so the join must filter it out.
    assert_eq!((segment.max_doc(), segment.num_deleted_docs()), (2, 1));
    assert_eq!(searcher.search(&term("fiber"), &Count)?, 0);
    assert_eq!(searcher.search(&join, &Count)?, 0);
    Ok(())
}

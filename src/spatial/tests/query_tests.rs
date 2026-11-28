use std::sync::Arc;

use crate::collector::Count;
use crate::merge_policy::NoMergePolicy;
use crate::query::{SpatialPredicate, SpatialQuery};
use crate::schema::{Schema, PLANE, SPHERE, STRING};
use crate::spatial::geometry::Geometry;
use crate::spatial::plane::Plane;
use crate::spatial::plugin::{SpatialFieldWriter, SpatialIndexFactory};
use crate::spatial::spatial_index_manager::{SpatialIndex, SurfaceIndex};
use crate::spatial::sphere::Sphere;
use crate::spatial::{Cons, NativeSpatial, Nil, S2Plugin};
use crate::{Index, IndexWriter, TantivyDocument, TantivyError, Term};

#[test]
fn test_plane_queries_follow_registered_surface() -> crate::Result<()> {
    let mut builder = Schema::builder();
    let field = builder.add_spatial_field("geometry", PLANE.set_spatial_index("cartesian"));
    let kind = builder.add_text_field("kind", STRING);
    let schema = builder.build();
    let index = Index::builder()
        .schema(schema.clone())
        .register_plugin(Arc::new(S2Plugin::<SpatialIndexes, NativeSpatial>::new()))
        .create_in_ram()?;
    index
        .spatial_indices()
        .register("cartesian", Box::new(SurfaceIndex::<Plane>::new()));
    let mut writer: IndexWriter = index.writer_with_num_threads(1, 50_000_000)?;
    writer.set_merge_policy(Box::new(NoMergePolicy));
    for json in [
        r#"{"kind":"area","geometry":{"type":"Polygon","coordinates":[[[0,0],[2,0],[2,2],[0,2],[0,0]]]}}"#,
        r#"{"kind":"interior","geometry":{"type":"LineString","coordinates":[[0.5,1],[1.5,1]]}}"#,
        r#"{"kind":"boundary","geometry":{"type":"LineString","coordinates":[[0.5,0],[1.5,0]]}}"#,
        r#"{"kind":"outside","geometry":{"type":"Point","coordinates":[5,5]}}"#,
    ] {
        writer.add_document(TantivyDocument::parse_json(&schema, json)?)?;
        writer.commit()?;
    }
    let reader = index.reader()?;
    let positive_distance = SpatialQuery::with_predicate(
        field,
        vec![[1.0, 1.0]],
        SpatialPredicate::DistanceWithin(1.0),
    );
    assert!(
        matches!(reader.searcher().search(&positive_distance, &Count),
        Err(TantivyError::InvalidArgument(message))
            if message == "positive planar distance is not supported")
    );
    for merge in [false, true] {
        if merge {
            writer.merge(&index.searchable_segment_ids()?).wait()?;
            reader.reload()?;
        }
        for (predicate, coordinates, count) in [
            (
                SpatialPredicate::Contains,
                vec![[0.5, 0.5], [1.5, 0.5], [1.0, 1.5]],
                1,
            ),
            (
                SpatialPredicate::Covers,
                vec![[0.5, 0.5], [1.5, 0.5], [1.0, 1.5]],
                1,
            ),
            (
                SpatialPredicate::CoveredBy,
                vec![[-1.0, -1.0], [3.0, -1.0], [3.0, 3.0], [-1.0, 3.0]],
                3,
            ),
            (
                SpatialPredicate::Intersects,
                vec![[-1.0, -1.0], [3.0, -1.0], [3.0, 3.0], [-1.0, 3.0]],
                3,
            ),
            (SpatialPredicate::DistanceWithin(0.0), vec![[0.5, 0.0]], 2),
        ] {
            let query = SpatialQuery::with_predicate(field, coordinates, predicate);
            assert_eq!(
                reader.searcher().search(&query, &Count)?,
                count,
                "{query:?}, merge {merge}"
            );
        }
    }
    writer.delete_term(Term::from_field_text(kind, "boundary"));
    writer.commit()?;
    reader.reload()?;
    let query = SpatialQuery::with_predicate(
        field,
        vec![[0.5, 0.0]],
        SpatialPredicate::DistanceWithin(0.0),
    );
    assert_eq!(reader.searcher().search(&query, &Count)?, 1);
    writer.delete_term(Term::from_field_text(kind, "area"));
    writer.commit()?;
    reader.reload()?;
    assert_eq!(reader.searcher().search(&query, &Count)?, 0);
    Ok(())
}

#[test]
fn test_sphere_covers_is_unsupported_before_search() -> crate::Result<()> {
    let implementation = SurfaceIndex::<Sphere>::new();
    let geometry = Geometry::<Plane>::Point([0.0, 0.0]);
    assert!(implementation.prepare_covers(&geometry).is_err());
    let mut builder = Schema::builder();
    let field = builder.add_spatial_field("geometry", SPHERE);
    let index = Index::builder()
        .schema(builder.build())
        .register_plugin(Arc::new(S2Plugin::<SpatialIndexes, NativeSpatial>::new()))
        .create_in_ram()?;
    let query = SpatialQuery::with_predicate(
        field,
        vec![[0.0, 0.0], [1.0, 0.0], [1.0, 1.0], [0.0, 1.0]],
        SpatialPredicate::Covers,
    );
    assert!(matches!(index.reader()?.searcher().search(&query, &Count),
        Err(TantivyError::InvalidArgument(message))
            if message == "Covers is not supported on this surface"));
    Ok(())
}

type SpatialIndexes = Cons<SurfaceIndex<Sphere>, Cons<Cartesian, Nil>>;

struct Cartesian;

impl SpatialIndexFactory<NativeSpatial> for Cartesian {
    const NAME: &'static str = "cartesian";

    fn create_index() -> Box<dyn SpatialIndex> {
        Box::new(SurfaceIndex::<Plane>::new())
    }

    fn create_field_writer() -> Box<dyn SpatialFieldWriter> {
        <SurfaceIndex<Plane> as SpatialIndexFactory<NativeSpatial>>::create_field_writer()
    }
}

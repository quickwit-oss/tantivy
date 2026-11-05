use crate::collector::DocSetCollector;
use crate::directory::{CompositeFile, RamDirectory};
use crate::fastfield::AliveBitSet;
use crate::index::SegmentComponent;
use crate::indexer::segment_updater::merge_filtered_segments;
use crate::indexer::{DocIdMapping, NoMergePolicy, SingleSegmentIndexWriter};
use crate::query::SpatialQuery;
use crate::schema::{
    Field, OwnedValue, Schema, SpatialOptions, Value, FAST, INDEXED, SPHERE, STORED,
};
use crate::spatial::cell_index_reader::CellIndexReader;
use crate::spatial::edge_reader::EdgeReader;
use crate::spatial::geometry::Geometry;
use crate::spatial::plane::Plane;
use crate::spatial::spatial_index_manager::SurfaceIndex;
use crate::spatial::sphere::Sphere;
use crate::spatial::surface::Surface;
use crate::{Index, IndexSettings, IndexSortByField, IndexWriter, Order, TantivyDocument, Term};

fn schema(options: SpatialOptions) -> (Schema, Field, Field) {
    let mut builder = Schema::builder();
    let id = builder.add_u64_field("id", FAST | INDEXED | STORED);
    let geometry = builder.add_spatial_field("geometry", STORED | options);
    (builder.build(), id, geometry)
}

fn ring(x: f64) -> Vec<[f64; 2]> {
    let corners = [[x, 2.0], [x + 0.01, 2.0], [x + 0.01, 2.01], [x, 2.01]];
    let mut points = Vec::new();
    for edge in 0..4 {
        let a = corners[edge];
        let b = corners[(edge + 1) % 4];
        for step in 0..8 {
            let t = step as f64 / 8.0;
            points.push([a[0] + t * (b[0] - a[0]), a[1] + t * (b[1] - a[1])]);
        }
    }
    points.push(points[0]);
    points
}

fn document(id: Field, geometry: Field, value: u64) -> TantivyDocument {
    let mut doc = TantivyDocument::default();
    doc.add_u64(id, value);
    let x = value as f64 / 10.0;
    let shape = Geometry::<Plane>::MultiPolygon(vec![vec![ring(x)], vec![ring(x + 0.05)]]);
    doc.add_field_value(geometry, &OwnedValue::Geometry(shape));
    doc.add_field_value(
        geometry,
        &OwnedValue::Geometry(Geometry::Polygon(vec![ring(x + 0.1)])),
    );
    doc
}

fn sorted_settings() -> IndexSettings {
    IndexSettings {
        sort_by_field: Some(IndexSortByField {
            field: "id".to_string(),
            order: Order::Asc,
        }),
        ..IndexSettings::default()
    }
}

fn hit_ids(index: &Index, query: &SpatialQuery, id: Field) -> crate::Result<Vec<u64>> {
    let searcher = index.reader()?.searcher();
    let mut values = Vec::new();
    for address in searcher.search(query, &DocSetCollector)? {
        let doc: TantivyDocument = searcher.doc(address)?;
        let value = doc.get_first(id).unwrap().as_u64().unwrap();
        assert_eq!(
            searcher
                .segment_reader(address.segment_ord)
                .fast_fields()
                .u64("id")?
                .first(address.doc_id),
            Some(value)
        );
        values.push(value);
    }
    values.sort_unstable();
    Ok(values)
}

fn assert_queries(
    index: &Index,
    id: Field,
    geometry: Field,
    expected: &[u64],
) -> crate::Result<()> {
    let broad = SpatialQuery::intersects_bounds(geometry, [[-30.0, -30.0], [30.0, 30.0]]);
    assert_eq!(hit_ids(index, &broad, id)?, expected);
    for &value in expected {
        let x = value as f64 / 10.0;
        // The query cuts through the first member of the stored multipolygon.
        let crossing =
            SpatialQuery::intersects_bounds(geometry, [[x + 0.004, 1.999], [x + 0.006, 2.011]]);
        assert_eq!(hit_ids(index, &crossing, id)?, [value]);
        let second_value =
            SpatialQuery::intersects_bounds(geometry, [[x + 0.104, 1.999], [x + 0.106, 2.011]]);
        assert_eq!(hit_ids(index, &second_value, id)?, [value]);
    }
    Ok(())
}

fn assert_sidecar(index: &Index, geometry: Field, populated: bool) -> crate::Result<()> {
    let searcher = index.reader()?.searcher();
    for reader in searcher.segment_readers() {
        let field = reader.spatial_fields().get_field(geometry)?.unwrap();
        assert_eq!(!field.doc_ids_bytes().is_empty(), populated);
        if populated {
            let implementation = index.spatial_indices().get("sphere").unwrap();
            let query = Geometry::Polygon(vec![vec![
                [-30.0, -30.0],
                [30.0, -30.0],
                [30.0, 30.0],
                [-30.0, 30.0],
                [-30.0, -30.0],
            ]]);
            let cells = CellIndexReader::open(field.cells_bytes());
            for [lon, lat] in [[-30.0, -30.0], [30.0, -30.0], [30.0, 30.0], [-30.0, 30.0]] {
                let cell = Sphere::cell_id_from_point(&Sphere::project(lon, lat));
                assert!(cells.find(cell).is_none());
            }
            let prepared = implementation.prepare_intersects(&query);
            // A fully covering query must not need the edge headers when doc IDs are present.
            let included = prepared.search_segment_bytes(
                field.cells_bytes(),
                &[],
                field.doc_ids_bytes(),
                reader.max_doc(),
            );
            assert_eq!(included.len(), reader.num_docs() as usize);
        }
    }
    Ok(())
}

#[test]
fn test_spatial_document_mappings_and_merge() -> crate::Result<()> {
    for manual in [false, true] {
        let (schema, id, geometry) = schema(SPHERE);
        let settings = if manual {
            IndexSettings {
                manual_doc_id_mapping: true,
                ..IndexSettings::default()
            }
        } else {
            sorted_settings()
        };
        let directory = RamDirectory::default();
        let mut writer: SingleSegmentIndexWriter = Index::builder()
            .schema(schema)
            .settings(settings)
            .single_segment_index_writer(directory.clone(), 50_000_000)?;
        for value in [30, 10, 20] {
            writer.add_document(document(id, geometry, value))?;
        }
        let index = if manual {
            writer.finalize_with_doc_id_mapping(&DocIdMapping::new_permutation(vec![1, 2, 0])?)?
        } else {
            writer.finalize()?
        };
        let searcher = index.reader()?.searcher();
        let store = searcher.segment_reader(0).get_store_reader(0)?;
        for (doc_id, expected) in [10, 20, 30].into_iter().enumerate() {
            let doc: TantivyDocument = store.get(doc_id as u32)?;
            assert_eq!(doc.get_first(id).unwrap().as_u64(), Some(expected));
            assert_eq!(doc.get_all(geometry).count(), 2);
            assert!(doc.get_first(geometry).unwrap().as_geometry().is_some());
        }
        let field = searcher
            .segment_reader(0)
            .spatial_fields()
            .get_field(geometry)?
            .unwrap();
        let edges = EdgeReader::<Sphere>::open(field.edges_bytes());
        assert_eq!(
            (0..edges.geometry_count())
                .map(|position| edges.doc_id_for(position))
                .collect::<Vec<_>>(),
            [2, 2, 2, 0, 0, 0, 1, 1, 1]
        );
        drop(searcher);
        assert_sidecar(&index, geometry, false)?;
        assert_queries(&index, id, geometry, &[10, 20, 30])?;
        let mut writer: IndexWriter = index.writer_with_num_threads(1, 50_000_000)?;
        writer.set_merge_policy(Box::new(NoMergePolicy));
        writer.delete_term(Term::from_field_u64(id, 20));
        writer.add_document(document(id, geometry, 40))?;
        writer.commit()?;
        let segments = index.searchable_segment_ids()?;
        writer.merge(&segments).wait()?;
        assert_sidecar(&index, geometry, true)?;
        assert_queries(&index, id, geometry, &[10, 30, 40])?;
        writer.garbage_collect_files().wait()?;
        assert_queries(&index, id, geometry, &[10, 30, 40])?;
        let reopened = Index::open(directory)?;
        assert_queries(&reopened, id, geometry, &[10, 30, 40])?;
    }
    Ok(())
}

fn named_index(
    schema: Schema,
    id: Field,
    geometry: Field,
    values: &[u64],
    with_geometry: bool,
) -> crate::Result<Index> {
    let index = Index::builder()
        .schema(schema)
        .settings(sorted_settings())
        .create_in_ram()?;
    index
        .spatial_indices()
        .register("named", Box::new(SurfaceIndex::<Sphere>::new()));
    let mut writer = SingleSegmentIndexWriter::new(index, 50_000_000)?;
    for &value in values {
        let doc = if with_geometry {
            document(id, geometry, value)
        } else {
            doc!(id => value)
        };
        writer.add_document(doc)?;
    }
    writer.finalize()
}

#[test]
fn test_spatial_standalone_merge_keeps_source_slots_and_manager() -> crate::Result<()> {
    let (schema, id, geometry) = schema(SPHERE.set_spatial_index("named"));
    let empty = named_index(schema.clone(), id, geometry, &[], false)?;
    empty
        .spatial_indices()
        .register("first-source-only", Box::new(SurfaceIndex::<Sphere>::new()));
    let last = named_index(schema.clone(), id, geometry, &[30, 10], true)?;
    let absent = named_index(schema.clone(), id, geometry, &[15], false)?;
    let filtered = named_index(schema.clone(), id, geometry, &[90], true)?;
    let middle = named_index(schema, id, geometry, &[20], true)?;
    let mut merged = None;
    for reverse in [false, true] {
        let mut sources = vec![
            (
                &last,
                Some(AliveBitSet::for_test_from_deleted_docs(&[0], 2)),
            ),
            (&absent, None),
            (
                &filtered,
                Some(AliveBitSet::for_test_from_deleted_docs(&[0], 1)),
            ),
            (&middle, None),
        ];
        if reverse {
            sources.reverse();
        }
        sources.insert(0, (&empty, None));
        let mut segments = Vec::new();
        let mut filters = Vec::new();
        for (source, filter) in sources {
            let source_segments = source.searchable_segments()?;
            assert_eq!(source_segments.len(), 1);
            segments.extend(source_segments);
            filters.push(filter);
        }
        let settings = if reverse {
            IndexSettings::default()
        } else {
            sorted_settings()
        };
        let index = merge_filtered_segments(&segments, settings, filters, RamDirectory::default())?;
        assert!(index.spatial_indices().get("first-source-only").is_some());
        assert_queries(&index, id, geometry, &[20, 30])?;
        let searcher = index.reader()?.searcher();
        assert_eq!(searcher.num_docs(), 3);
        let field = searcher
            .segment_reader(0)
            .spatial_fields()
            .get_field(geometry)?
            .unwrap();
        assert!(!field.doc_ids_bytes().is_empty());
        merged = Some(index);
    }
    let index = merged.unwrap();
    let segments = index.searchable_segments()?;
    let empty = merge_filtered_segments(
        &segments,
        sorted_settings(),
        vec![Some(AliveBitSet::for_test_from_deleted_docs(&[0, 1, 2], 3))],
        RamDirectory::default(),
    )?;
    assert_eq!(empty.reader()?.searcher().num_docs(), 0);
    assert_eq!(empty.reader()?.searcher().segment_reader(0).max_doc(), 0);
    for segment in empty.searchable_segments()? {
        for component in [
            SegmentComponent::SpatialCells,
            SegmentComponent::SpatialEdges,
            SegmentComponent::SpatialDocIds,
        ] {
            CompositeFile::open(&segment.open_read(component)?)?;
        }
    }
    assert_queries(&empty, id, geometry, &[])?;
    Ok(())
}

#[test]
fn test_spatial_files_are_optional_and_closed() -> crate::Result<()> {
    for spatial in [false, true] {
        let mut builder = Schema::builder();
        let id = builder.add_u64_field("id", STORED);
        if spatial {
            builder.add_spatial_field("geometry", SPHERE);
        }
        let index = Index::create_in_ram(builder.build());
        let mut writer: IndexWriter = index.writer_with_num_threads(1, 50_000_000)?;
        writer.add_document(doc!(id => 1u64))?;
        writer.commit()?;
        writer.garbage_collect_files().wait()?;
        let searcher = index.reader()?.searcher();
        searcher.space_usage()?;
        for segment in index.searchable_segments()? {
            for component in [
                SegmentComponent::SpatialCells,
                SegmentComponent::SpatialEdges,
                SegmentComponent::SpatialDocIds,
            ] {
                if spatial {
                    CompositeFile::open(&segment.open_read(component)?)?;
                } else {
                    assert!(segment.open_read(component).is_err());
                }
            }
        }
    }
    Ok(())
}

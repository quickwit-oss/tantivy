use std::collections::BTreeSet;
use std::io::{self, Write};
use std::path::Path;

use common::{BitSet, CountingWriter, OwnedBytes, ReadOnlyBitSet, TerminatingWrite};

use crate::collector::Count;
use crate::directory::{CompositeFile, CompositeWrite, Directory, FileSlice, RamDirectory};
use crate::index::SegmentComponent;
use crate::indexer::SingleSegmentIndexWriter;
use crate::query::{AllQuery, EnableScoring, Query, SpatialQuery};
use crate::schema::{Field, Schema, SPHERE, STORED};
use crate::spatial::cell_index_reader::CellIndexReader;
use crate::spatial::edge_reader::EdgeReader;
use crate::spatial::edge_writer::EDGE_FOOTER_BYTES;
use crate::spatial::executor::{PlanNode, SpatialExecutor, SpatialRelation};
use crate::spatial::geometry::Geometry;
use crate::spatial::plane::Plane;
use crate::spatial::plane_support_tests::assert_partition;
use crate::spatial::reader::SpatialReader;
use crate::spatial::shape_index::{write_cell_footer, ShapeIndex, CELL_FOOTER_LEN, CELL_REVISION};
use crate::spatial::spatial_index_manager::{MergeSource, SpatialIndex, SurfaceIndex};
use crate::spatial::sphere::Sphere;
use crate::spatial::stats::{sphere_stats_for_reader, sphere_stats_for_slices};
use crate::spatial::surface::Surface;
use crate::{Index, TantivyDocument, TERMINATED};

fn empty_cells() -> Vec<u8> {
    let mut out = CountingWriter::wrap(Vec::new());
    ShapeIndex::new().write(&mut out);
    out.finish()
}

fn invalid_payloads() -> Vec<Vec<u8>> {
    let mut wrong_revision = empty_cells();
    wrong_revision[CELL_FOOTER_LEN - 4..].copy_from_slice(&(CELL_REVISION + 1).to_le_bytes());
    let mut wrong_count = empty_cells();
    wrong_count[..4].copy_from_slice(&1u32.to_le_bytes());
    let mut overflow = Vec::new();
    write_cell_footer(&mut overflow, 1, u64::MAX).unwrap();
    let mut wrong_offset = empty_cells();
    wrong_offset[4..12].copy_from_slice(&1u64.to_le_bytes());
    vec![
        vec![0],
        vec![0; 7],
        vec![0; 12],
        vec![0; 19],
        vec![0; 20],
        wrong_revision,
        wrong_count,
        wrong_offset,
        overflow,
    ]
}

fn invalid_data<T>(result: io::Result<T>) {
    let error = result.err().expect("old or invalid cells must fail");
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert!(error.to_string().contains("rebuild the spatial index"));
}

fn old_plane_point_edges() -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.push(0x08);
    bytes.extend_from_slice(&16u32.to_le_bytes());
    bytes.extend_from_slice(&0u32.to_le_bytes());
    bytes.extend_from_slice(&0.0f64.to_le_bytes());
    bytes.extend_from_slice(&0.0f64.to_le_bytes());
    bytes.extend_from_slice(&0u64.to_le_bytes());
    bytes.extend_from_slice(&1u32.to_le_bytes());
    bytes.extend_from_slice(&16u32.to_le_bytes());
    bytes.extend_from_slice(&25u64.to_le_bytes());
    bytes
}

fn old_fixed_directory_point_edges() -> Vec<u8> {
    let mut bytes = vec![0; 16];
    bytes.extend_from_slice(&0u64.to_le_bytes());
    for field in [0u32, 1, 0, 1, 1, 0] {
        bytes.extend_from_slice(&field.to_le_bytes());
    }
    for field in [0u32, 1, 0] {
        bytes.extend_from_slice(&field.to_le_bytes());
    }
    bytes.extend_from_slice(&1u32.to_le_bytes());
    bytes.extend_from_slice(&32u32.to_le_bytes());
    bytes.extend_from_slice(&16u64.to_le_bytes());
    bytes.extend_from_slice(&1u32.to_le_bytes());
    bytes.extend_from_slice(&1u32.to_le_bytes());
    bytes.extend_from_slice(&48u64.to_le_bytes());
    bytes.extend_from_slice(b"TEDG");
    bytes.extend_from_slice(&1u32.to_le_bytes());
    bytes
}

#[test]
fn test_cell_footer_rejects_old_and_malformed_payloads() {
    assert_eq!(empty_cells().len(), 20);
    for bytes in invalid_payloads() {
        invalid_data(CellIndexReader::open(&bytes));
        invalid_data(CellIndexReader::open_with_doc_ids(&bytes, &[0; 4]));
    }
    for bytes in [Vec::new(), empty_cells()] {
        let reader = CellIndexReader::open_with_doc_ids(&bytes, &[]).unwrap();
        assert_eq!(reader.cell_count(), 0);
        assert_eq!(reader.iter().count(), 0);
        reader.visit_doc_ids(0, 0, |_| panic!("empty index has no doc IDs"));
    }
    let mut bytes = empty_cells();
    bytes[16..].copy_from_slice(&91u32.to_le_bytes());
    let error = CellIndexReader::open(&bytes).err().unwrap().to_string();
    assert!(error.contains("revision 91"));
    let old = CellIndexReader::open(&[0; 12]).err().unwrap().to_string();
    assert!(!old.contains("revision 0"));
}

#[test]
fn test_edge_footer_rejects_old_payload() {
    let old = old_plane_point_edges();
    assert_eq!(old.len(), 49);
    invalid_data(EdgeReader::<Plane>::open(&old));
    let old = old_fixed_directory_point_edges();
    assert_eq!(old.len(), 100);
    invalid_data(EdgeReader::<Plane>::open(&old));
    assert_eq!(EdgeReader::<Plane>::open(&[]).unwrap().geometry_count(), 0);
}

#[test]
fn test_edge_footer_checks_array_boundaries() {
    let bytes = serialize::<Plane>(&Geometry::Point([0.0, 0.0])).edges;
    let footer = bytes.len() - EDGE_FOOTER_BYTES;
    let mut wrong_header = bytes.clone();
    wrong_header[footer + 4..footer + 8].copy_from_slice(&19u32.to_le_bytes());
    let mut overflow = bytes.clone();
    overflow[footer + 8..footer + 16].copy_from_slice(&u64::MAX.to_le_bytes());
    let mut wrong_docs = bytes.clone();
    wrong_docs[footer + 16..footer + 24].copy_from_slice(&0u64.to_le_bytes());
    let mut wrong_directory = bytes.clone();
    wrong_directory[footer + 32..footer + 40].copy_from_slice(&0u64.to_le_bytes());
    for payload in [
        bytes[..bytes.len() - 1].to_vec(),
        wrong_header,
        overflow,
        wrong_docs,
        wrong_directory,
    ] {
        invalid_data(EdgeReader::<Plane>::open(&payload));
    }
}

#[test]
fn test_edge_document_ranges_reject_invalid_starts() {
    let point = Geometry::Point([0.0, 0.0]);
    let bytes = serialize_values::<Plane>(&[(0, &point), (1, &point), (2, &point)]).edges;
    let footer = bytes.len() - EDGE_FOOTER_BYTES;
    let starts = u64::from_le_bytes(bytes[footer + 32..footer + 40].try_into().unwrap()) as usize;

    for (slot, value) in [(0, 1u32), (3, 2)] {
        let mut invalid = bytes.clone();
        invalid[starts + slot * 4..starts + (slot + 1) * 4].copy_from_slice(&value.to_le_bytes());
        invalid_data(EdgeReader::<Plane>::open(&invalid));
    }
    for (slot, value, doc) in [(1, 4u32, 0), (1, 3, 1), (2, 0, 1)] {
        let mut invalid = bytes.clone();
        invalid[starts + slot * 4..starts + (slot + 1) * 4].copy_from_slice(&value.to_le_bytes());
        let reader = EdgeReader::<Plane>::open(&invalid).unwrap();
        invalid_data(reader.value_heads(doc));
        assert!(SurfaceIndex::<Plane>::new()
            .value_heads(&invalid, doc, &mut Vec::new())
            .is_err());
    }

    // The same directory span can describe three starts and no value heads.
    let mut bytes = serialize::<Plane>(&point).edges;
    let footer = bytes.len() - EDGE_FOOTER_BYTES;
    bytes[footer + 24..footer + 28].copy_from_slice(&2u32.to_le_bytes());
    bytes[footer + 28..footer + 32].copy_from_slice(&0u32.to_le_bytes());
    let reader = EdgeReader::<Plane>::open(&bytes).unwrap();
    invalid_data(reader.value_heads(0));
    invalid_data(reader.value_heads(1));
    assert_eq!(reader.value_heads(2).unwrap().count(), 0);
}

fn check_prepared_queries<S: Surface + Clone + Send + Sync + 'static>() {
    let implementation = SurfaceIndex::<S>::new();
    let geometry = Geometry::Polygon(vec![vec![
        [-1.0, -1.0],
        [1.0, -1.0],
        [1.0, 1.0],
        [-1.0, 1.0],
        [-1.0, -1.0],
    ]]);
    let queries = [
        implementation.prepare_intersects(&geometry),
        implementation.prepare_contains(&geometry),
        implementation.prepare_within(&geometry),
        implementation
            .prepare_distance(&geometry, if S::DIMENSIONS == 2 { 0.0 } else { 0.01 })
            .unwrap(),
    ];
    for query in queries {
        for max_doc in [0, 1] {
            for bytes in invalid_payloads() {
                invalid_data(query.search_segment_bytes(&bytes, &[], &[], max_doc));
            }
            for bytes in [Vec::new(), empty_cells()] {
                assert_eq!(
                    query
                        .search_segment_bytes(&bytes, &[], &[], max_doc)
                        .unwrap()
                        .len(),
                    0
                );
            }
        }
    }
}

#[test]
fn test_prepared_queries_check_cell_footer_on_both_surfaces() {
    check_prepared_queries::<Plane>();
    check_prepared_queries::<Sphere>();
}

#[test]
fn test_stats_checks_cell_footer() {
    for bytes in invalid_payloads() {
        invalid_data(sphere_stats_for_slices(&bytes, &[], &[]));
        let reader = SpatialReader::open(
            FileSlice::from(bytes),
            FileSlice::from(Vec::new()),
            OwnedBytes::empty(),
        )
        .unwrap();
        invalid_data(sphere_stats_for_reader(&reader));
    }
    assert_eq!(
        sphere_stats_for_slices(&empty_cells(), &[], &[])
            .unwrap()
            .total_cells,
        0
    );
}

struct Bytes {
    cells: Vec<u8>,
    edges: Vec<u8>,
    doc_ids: Vec<u8>,
}

fn serialize<S: Surface + Clone + Send + Sync + 'static>(geometry: &Geometry<Plane>) -> Bytes {
    serialize_values::<S>(&[(0, geometry)])
}

fn serialize_values<S: Surface + Clone + Send + Sync + 'static>(
    values: &[(u32, &Geometry<Plane>)],
) -> Bytes {
    let directory = RamDirectory::create();
    let mut cells = CountingWriter::wrap(directory.open_write(Path::new("cells")).unwrap());
    let mut edges = CountingWriter::wrap(directory.open_write(Path::new("edges")).unwrap());
    let mut writer = SurfaceIndex::<S>::new().create_field_writer();
    for &(doc, geometry) in values {
        writer.add_geometry(doc, geometry);
    }
    writer.serialize(&mut cells, &mut edges, None).unwrap();
    cells.terminate().unwrap();
    edges.terminate().unwrap();
    Bytes {
        cells: directory.atomic_read(Path::new("cells")).unwrap(),
        edges: directory.atomic_read(Path::new("edges")).unwrap(),
        doc_ids: Vec::new(),
    }
}

fn merge<S: Surface + Clone + Send + Sync + 'static>(
    inputs: &[&Bytes],
    alive: &[Option<ReadOnlyBitSet>],
    mapping: &[Vec<Option<u32>>],
) -> io::Result<Bytes> {
    let directory = RamDirectory::create();
    let mut cells = CountingWriter::wrap(directory.open_write(Path::new("cells")).unwrap());
    let mut edges = CountingWriter::wrap(directory.open_write(Path::new("edges")).unwrap());
    let mut docs = CountingWriter::wrap(directory.open_write(Path::new("docs")).unwrap());
    let sources: Vec<_> = inputs
        .iter()
        .enumerate()
        .map(|(i, input)| MergeSource {
            cells_bytes: &input.cells,
            edges_bytes: &input.edges,
            segment_name: i.to_string(),
        })
        .collect();
    let result = SurfaceIndex::<S>::new()
        .merge_field(&sources, alive, &mut cells, &mut edges, &mut docs, mapping);
    if result.is_err() {
        assert_eq!(
            (
                cells.written_bytes(),
                edges.written_bytes(),
                docs.written_bytes()
            ),
            (0, 0, 0)
        );
    }
    cells.terminate().unwrap();
    edges.terminate().unwrap();
    docs.terminate().unwrap();
    result?;
    Ok(Bytes {
        cells: directory.atomic_read(Path::new("cells")).unwrap(),
        edges: directory.atomic_read(Path::new("edges")).unwrap(),
        doc_ids: directory.atomic_read(Path::new("docs")).unwrap(),
    })
}

fn check_merge_rejection<S: Surface + Clone + Send + Sync + 'static>() {
    let left = serialize::<S>(&Geometry::Point([-90.0, -45.0]));
    let right = serialize::<S>(&Geometry::Point([90.0, 45.0]));
    let left_reader = CellIndexReader::open(&left.cells).unwrap();
    let right_reader = CellIndexReader::open(&right.cells).unwrap();
    assert!(left_reader.iter().all(|a| right_reader
        .iter()
        .all(|b| a.cell_id.range_max() < b.cell_id.range_min()
            || b.cell_id.range_max() < a.cell_id.range_min())));
    for reverse in [false, true] {
        let mut old = right.cells.clone();
        old.truncate(old.len() - 8);
        let old = Bytes {
            cells: old,
            edges: right.edges.clone(),
            doc_ids: Vec::new(),
        };
        let inputs = if reverse {
            vec![&old, &left]
        } else {
            vec![&left, &old]
        };
        invalid_data(merge::<S>(
            &inputs,
            &[None, None],
            &[vec![Some(0)], vec![Some(1)]],
        ));
    }
    for cells in invalid_payloads() {
        let old = Bytes {
            cells,
            edges: Vec::new(),
            doc_ids: Vec::new(),
        };
        invalid_data(merge::<S>(
            &[&left, &old],
            &[None, None],
            &[vec![Some(0)], vec![]],
        ));
    }
}

#[test]
fn test_merge_checks_every_source_before_pass_through() {
    check_merge_rejection::<Plane>();
    check_merge_rejection::<Sphere>();
}

fn doc_ids<S: Surface>(bytes: &Bytes) -> BTreeSet<u32> {
    let cells = CellIndexReader::open_with_doc_ids(&bytes.cells, &bytes.doc_ids).unwrap();
    let edges = EdgeReader::<S>::open(&bytes.edges).unwrap();
    let mut sidecar = BTreeSet::new();
    cells.visit_doc_ids(0, cells.cell_count(), |id| {
        sidecar.insert(id);
        true
    });
    let from_cells: BTreeSet<_> = cells
        .iter()
        .flat_map(|cell| cell.shapes)
        .map(|shape| edges.doc_id_for(shape.geometry_id.1))
        .collect();
    assert_eq!(sidecar, from_cells);
    sidecar
}

fn check_merged_plane_partition(bytes: &Bytes) {
    let reader = CellIndexReader::open(&bytes.cells).unwrap();
    let edges = EdgeReader::<Plane>::open(&bytes.edges).unwrap();
    let mut sets = Vec::new();
    let mut position = 0;
    while position < edges.geometry_count() {
        let (head, set) = edges.read_geometry_set(position);
        assert_eq!(head, position);
        position += set.members.len() as u32;
        sets.push(set);
    }
    let members: Vec<_> = sets
        .iter()
        .flat_map(|s| &s.members)
        .enumerate()
        .map(|(id, member)| ((0, id as u32), member))
        .collect();
    assert_partition(&reader.iter().collect::<Vec<_>>(), &members);
}

fn check_merge_round_trip<S: Surface + Clone + Send + Sync + 'static>() {
    let a = serialize::<S>(&Geometry::Point([0.0, 0.0]));
    let b = serialize::<S>(&Geometry::MultiPoint(vec![[0.0, 0.0], [90.0, 45.0]]));
    let merged = merge::<S>(&[&a, &b], &[None, None], &[vec![Some(5)], vec![Some(2)]]).unwrap();
    assert_eq!(doc_ids::<S>(&merged), BTreeSet::from([2, 5]));
    let edges = EdgeReader::<S>::open(&merged.edges).unwrap();
    for doc_id in [2, 5] {
        let heads = edges.value_heads(doc_id).unwrap().collect::<Vec<_>>();
        assert_eq!(heads.len(), 1);
        assert_eq!(edges.read_geometry_set(heads[0]).1.doc_id, doc_id);
    }
    if S::EXACT_SUPPORT {
        check_merged_plane_partition(&merged);
    }
    let c = serialize::<S>(&Geometry::Point([-45.0, -22.5]));
    let mut live = BitSet::with_max_value(6);
    live.insert(2);
    let again = merge::<S>(
        &[&merged, &c],
        &[Some(ReadOnlyBitSet::from(&live)), None],
        &[vec![None, None, Some(1), None, None, None], vec![Some(0)]],
    )
    .unwrap();
    assert_eq!(doc_ids::<S>(&again), BTreeSet::from([0, 1]));
    if S::EXACT_SUPPORT {
        check_merged_plane_partition(&again);
    }
    let dead = ReadOnlyBitSet::from(&BitSet::with_max_value(2));
    let empty = merge::<S>(&[&again], &[Some(dead)], &[vec![None, None]]).unwrap();
    assert_eq!(empty.cells, empty_cells());
    assert!(doc_ids::<S>(&empty).is_empty());
}

#[test]
fn test_merged_cells_edges_and_doc_ids_round_trip() {
    check_merge_round_trip::<Plane>();
    check_merge_round_trip::<Sphere>();
}

fn check_merge_value_directory<S: Surface + Clone + Send + Sync + 'static>() {
    let empty = Geometry::Polygon(Vec::new());
    let point = Geometry::Point([10.0, 10.0]);
    let mixed = Geometry::GeometryCollection(vec![empty.clone(), Geometry::Point([-40.0, 40.0])]);
    let a = serialize_values::<S>(&[(0, &point), (0, &empty), (1, &point)]);
    let b = serialize_values::<S>(&[(0, &mixed), (1, &point)]);
    assert_eq!(
        EdgeReader::<S>::open(&a.edges)
            .unwrap()
            .value_heads(0)
            .unwrap()
            .count(),
        2
    );
    let mut live = BitSet::with_max_value(2);
    live.insert(0);
    let merged = merge::<S>(
        &[&a, &b],
        &[
            Some(ReadOnlyBitSet::from(&live)),
            Some(ReadOnlyBitSet::from(&live)),
        ],
        &[vec![Some(4), None], vec![Some(2), None]],
    )
    .unwrap();
    assert_eq!(doc_ids::<S>(&merged), BTreeSet::from([2, 4]));
    let reader = EdgeReader::<S>::open(&merged.edges).unwrap();
    assert_eq!(reader.geometry_count(), 3);
    for (doc, count) in [(2, 2), (4, 1)] {
        let heads = reader.value_heads(doc).unwrap().collect::<Vec<_>>();
        assert_eq!(heads.len(), 1);
        let (head, set) = reader.read_geometry_set(heads[0]);
        assert_eq!(head, heads[0]);
        assert_eq!(set.doc_id, doc);
        assert_eq!(set.members.len(), count);
        if doc == 2 {
            assert!(set.members[0].vertices.is_empty());
        }
        for gid in head..head + count as u32 {
            let member = reader.locate_entry(gid);
            assert_eq!(
                (member.doc_id, member.head, member.member_count),
                (doc, head, count as u32)
            );
        }
    }
    for doc in [0, 1, 3, 5, u32::MAX] {
        assert_eq!(reader.value_heads(doc).unwrap().count(), 0);
    }
    let mut live = BitSet::with_max_value(5);
    live.insert(4);
    let again = merge::<S>(
        &[&merged],
        &[Some(ReadOnlyBitSet::from(&live))],
        &[vec![None, None, None, None, Some(0)]],
    )
    .unwrap();
    assert_eq!(doc_ids::<S>(&again), BTreeSet::from([0]));
    let reader = EdgeReader::<S>::open(&again.edges).unwrap();
    assert_eq!(reader.geometry_count(), 1);
    assert_eq!(reader.value_heads(0).unwrap().collect::<Vec<_>>(), vec![0]);
    assert_eq!(reader.locate_entry(0).vertex_count(), 1);
}

#[test]
fn test_merge_directory_preserves_empty_siblings_until_value_deletion() {
    check_merge_value_directory::<Plane>();
    check_merge_value_directory::<Sphere>();
}

fn index_with_payload(cells: &[u8], documents: u32) -> crate::Result<(Index, Field)> {
    let mut schema = Schema::builder();
    let field = schema.add_spatial_field("geometry", SPHERE);
    let id = schema.add_u64_field("id", STORED);
    let index = Index::create_in_ram(schema.build());
    let mut writer: SingleSegmentIndexWriter<TantivyDocument> =
        SingleSegmentIndexWriter::new(index, 50_000_000)?;
    for i in 0..documents {
        writer.add_document(doc!(id => u64::from(i)))?;
    }
    let index = writer.finalize()?;
    let segment = index.searchable_segments()?.pop().unwrap();
    for (component, bytes) in [
        (SegmentComponent::SpatialCells, cells),
        (SegmentComponent::SpatialEdges, &[][..]),
        (SegmentComponent::SpatialDocIds, &[][..]),
    ] {
        index
            .directory()
            .delete(&segment.relative_path(component.clone()))
            .unwrap();
        let mut output = CompositeWrite::wrap(segment.open_write(component)?);
        output.for_field(field).write_all(bytes)?;
        output.close()?;
    }
    Ok((index, field))
}

#[test]
fn test_zero_document_scorer_validates_present_cells() -> crate::Result<()> {
    for payload in [vec![0; 12], empty_cells()] {
        let valid = payload.len() == CELL_FOOTER_LEN;
        let (index, field) = index_with_payload(&payload, 0)?;
        let searcher = index.reader()?.searcher();
        let reader = searcher.segment_reader(0);
        assert_eq!(reader.max_doc(), 0);
        let query = SpatialQuery::intersects_bounds(field, [[-1.0, -1.0], [1.0, 1.0]]);
        let weight = query.weight(EnableScoring::disabled_from_searcher(&searcher))?;
        if valid {
            assert_eq!(weight.scorer(reader, 1.0)?.doc(), TERMINATED);
            assert_eq!(weight.count(reader)?, 0);
        } else {
            assert!(weight
                .scorer(reader, 1.0)
                .err()
                .unwrap()
                .to_string()
                .contains("rebuild"));
            assert!(weight
                .count(reader)
                .err()
                .unwrap()
                .to_string()
                .contains("rebuild"));
            assert!(weight
                .explain(reader, 0)
                .err()
                .unwrap()
                .to_string()
                .contains("rebuild"));
        }
    }
    Ok(())
}

#[test]
fn test_executor_rejects_old_cells() -> crate::Result<()> {
    let (index, field) = index_with_payload(&[0; 12], 1)?;
    let geometry = Geometry::<Plane>::Point([0.0, 0.0]);
    let all = || Box::new(PlanNode::Query(Box::new(AllQuery)));
    let searcher = index.reader()?.searcher();
    for node in [
        PlanNode::Intersects {
            field,
            geometry: geometry.clone(),
            filter: all(),
        },
        PlanNode::Within {
            field,
            geometry,
            radius: 0.01,
            filter: all(),
        },
        PlanNode::Join {
            field,
            outer: all(),
            inner: all(),
            relation: SpatialRelation::Intersects,
        },
    ] {
        let error = searcher
            .search(&SpatialExecutor::new(node), &Count)
            .unwrap_err();
        assert!(error.to_string().contains("rebuild"));
    }
    let segment = index.searchable_segments()?.pop().unwrap();
    let composite = CompositeFile::open(&segment.open_read(SegmentComponent::SpatialCells)?)?;
    assert_eq!(
        composite.open_read(field).unwrap().read_bytes()?.as_slice(),
        &[0; 12]
    );
    Ok(())
}

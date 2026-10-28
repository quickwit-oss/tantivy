use std::path::Path;

use common::{CountingWriter, TerminatingWrite};

use super::*;
use crate::directory::{Directory, RamDirectory};
use crate::spatial::covering::split_edges;
use crate::spatial::edge_reader::EdgeReader;
use crate::spatial::edge_writer::EdgeWriter;
use crate::spatial::geometry::Geometry;
use crate::spatial::geometry_set::{to_geometry_set, EdgeSet};
use crate::spatial::plane::Plane;
use crate::spatial::sphere::Sphere;

struct Fixture<S: Surface> {
    sets: Vec<GeometrySet<S>>,
    cells: Vec<u8>,
    edges: Vec<u8>,
    doc_ids: Vec<u8>,
}

impl<S: Surface> Fixture<S> {
    fn new(geometries: &[Geometry<S>]) -> Self {
        let sets: Vec<_> = geometries
            .iter()
            .enumerate()
            .map(|(doc, geometry)| to_geometry_set(geometry, doc as u32))
            .collect();
        let index = Clipper::new(ClipOptions::default()).build(&sets);
        let doc_ids: Vec<_> = sets
            .iter()
            .flat_map(|set| set.members.iter().map(|_| set.doc_id))
            .collect();
        let mut cells = CountingWriter::wrap(Vec::new());
        let mut docs = CountingWriter::wrap(Vec::new());
        index.write_with_doc_ids(&mut cells, Some(&mut docs), Some(&doc_ids));

        let directory = RamDirectory::create();
        let path = Path::new("edges");
        let mut write = CountingWriter::wrap(directory.open_write(path).unwrap());
        let mut edges = EdgeWriter::new(&mut write, 16);
        for set in &sets {
            edges.insert(set);
        }
        edges.finish();
        write.terminate().unwrap();
        Self {
            sets,
            cells: cells.finish(),
            edges: directory.atomic_read(path).unwrap(),
            doc_ids: docs.finish(),
        }
    }

    fn reader(&self) -> CellIndexReader<'_> {
        CellIndexReader::open_with_doc_ids(&self.cells, &self.doc_ids)
    }

    fn check(&self, query: &Intersects<S>, expected: &[u32]) {
        let max_doc = self.sets.len() as u32;
        let mut filter = BitSet::with_max_value(max_doc);
        for doc in 0..max_doc {
            filter.insert(doc);
        }
        for sidecar in [false, true] {
            let reader = if sidecar {
                self.reader()
            } else {
                CellIndexReader::open(&self.cells)
            };
            for terms_filter in [None, Some(&filter)] {
                let mut cache = EdgeCache::new(vec![EdgeReader::<S>::open(&self.edges)], 100_000);
                let hits = query.search(&reader, terms_filter, &mut cache, max_doc);
                let actual: Vec<_> = (0..max_doc).filter(|&doc| hits.contains(doc)).collect();
                assert_eq!(actual, expected, "sidecar {sidecar}");
            }
        }
        for &doc in expected {
            filter.remove(doc);
        }
        let mut cache = EdgeCache::new(vec![EdgeReader::<S>::open(&self.edges)], 100_000);
        assert_eq!(
            query
                .search(&self.reader(), Some(&filter), &mut cache, max_doc)
                .len(),
            0
        );
    }
}

fn rectangle<S: Surface>(x0: f64, y0: f64, x1: f64, y1: f64) -> Geometry<S> {
    Geometry::Polygon(vec![vec![
        S::face_uv_to_point(0, x0, y0),
        S::face_uv_to_point(0, x1, y0),
        S::face_uv_to_point(0, x1, y1),
        S::face_uv_to_point(0, x0, y1),
        S::face_uv_to_point(0, x0, y0),
    ]])
}

fn query<S: Surface>(geometries: Vec<Geometry<S>>) -> Intersects<S> {
    Intersects::new(
        to_geometry_set(&Geometry::GeometryCollection(geometries), 0),
        CovererOptions::default(),
    )
}

fn inside<S: Surface>(point: &S::Point, member: &EdgeSet<S>) -> bool {
    if !member.closed {
        return false;
    }
    let mut inside = member.contains_hilbert_start;
    let mut crosser = S::EdgeCrosser::new(&S::hilbert_start(), point);
    for ring in member.ring_offsets.windows(2) {
        for edge in member.vertices[ring[0]..ring[1]].windows(2) {
            inside ^= crosser.edge_or_vertex_crossing_two(&edge[0], &edge[1]);
        }
    }
    inside
}

fn check_later_member_crossing<S: Surface + Clone>() {
    let first = rectangle::<S>(-0.7, -0.7, -0.6, -0.6);
    let second = rectangle::<S>(0.2, 0.34, 0.4, 0.36);
    let fixture = Fixture::new(&[rectangle::<S>(0.29, 0.25, 0.31, 0.45)]);
    let candidate = &fixture.sets[0].members[0];
    let multipart = query(vec![first.clone(), second.clone()]);
    assert_eq!(multipart.query_index.cells.len(), 1);
    assert_eq!(fixture.reader().cell_count(), 1);
    assert!(
        multipart.query_index.cells[0].cell_id.level() < fixture.reader().cell_id_at(0).level()
    );
    for member in &multipart.query_edges.set.members {
        assert!(member.vertices.iter().all(|p| !inside::<S>(p, candidate)));
        assert!(candidate.vertices.iter().all(|p| !inside::<S>(p, member)));
    }
    let crossings: Vec<_> = multipart
        .query_edges
        .set
        .members
        .iter()
        .map(|member| {
            candidate
                .vertices
                .windows(2)
                .map(|edge| {
                    let mut crosser = S::EdgeCrosser::new(&edge[0], &edge[1]);
                    member
                        .vertices
                        .windows(2)
                        .filter(|q| crosser.crossing_sign_two(&q[0], &q[1]) > 0)
                        .count()
                })
                .sum::<usize>()
        })
        .collect();
    assert_eq!(crossings, vec![0, 4]);
    fixture.check(&query(vec![second.clone()]), &[0]);
    fixture.check(&query(vec![first.clone()]), &[]);
    fixture.check(&multipart, &[0]);
    fixture.check(&query(vec![second, first]), &[0]);
}

fn check_later_member_containment<S: Surface + Clone>() {
    let first = rectangle::<S>(0.1, 0.1, 0.2, 0.2);
    let second = rectangle::<S>(0.4, 0.4, 0.5, 0.5);
    let point = S::face_uv_to_point(0, 0.45, 0.45);
    let fixture = Fixture::new(&[
        Geometry::Point(point),
        rectangle::<S>(-0.7, -0.7, -0.6, -0.6),
    ]);
    let multipart = query(vec![first.clone(), second.clone()]);
    assert_eq!(multipart.query_index.cells.len(), 1);
    assert_eq!(fixture.reader().cell_count(), 1);
    let cell = &multipart.query_index.cells[0];
    assert!(!cell.shapes.iter().all(|s| s.edge_indices.is_empty()));
    assert!(fixture.reader().cell_id_at(0).level() <= cell.cell_id.level());
    assert!(!inside::<S>(&point, &multipart.query_edges.set.members[0]));
    assert!(inside::<S>(&point, &multipart.query_edges.set.members[1]));
    assert!(!fixture.sets[0].members[0].closed);
    assert_eq!(fixture.sets[0].members[0].vertices.len(), 1);
    fixture.check(&query(vec![second.clone()]), &[0]);
    fixture.check(&query(vec![first.clone()]), &[]);
    fixture.check(&multipart, &[0]);
    let reversed = query(vec![second, first]);
    let reversed_cell = &reversed.query_index.cells[0];
    let mut heap = BinaryHeap::new();
    for shape in &reversed_cell.shapes {
        heap.push((
            CoveringCell::<S> {
                pcell: S2PaddedCell::new(reversed_cell.cell_id, S::CELL_PADDING),
                query_edges: shape.edge_indices.clone(),
                contains_center: shape.contains_center,
                index_start: 0,
                index_end: 1,
                first_index_level: fixture.reader().cell_id_at(0).level(),
            },
            shape.geometry_id.1,
        ));
    }
    let (_, first_member) = heap.pop().unwrap();
    assert!(!inside::<S>(
        &point,
        &reversed.query_edges.set.members[first_member as usize]
    ));
    fixture.check(&reversed, &[0]);
}

#[test]
fn test_later_member_crossing_plane() {
    check_later_member_crossing::<Plane>();
}

#[test]
fn test_later_member_crossing_sphere() {
    check_later_member_crossing::<Sphere>();
}

#[test]
fn test_later_member_containment_plane() {
    check_later_member_containment::<Plane>();
}

#[test]
fn test_later_member_containment_sphere() {
    check_later_member_containment::<Sphere>();
}

#[test]
fn test_overlapping_members_have_separate_interiors() {
    let a = rectangle::<Plane>(-0.8, -0.2, 0.8, 0.8);
    let b = rectangle::<Plane>(-0.2, 0.2, 0.6, 0.9);
    let fixture = Fixture::new(&[rectangle::<Plane>(0.29, 0.39, 0.31, 0.41)]);
    let multipart = query(vec![a.clone(), b.clone()]);
    assert_eq!(multipart.query_index.cells.len(), 1);
    let cell = &multipart.query_index.cells[0];
    assert_eq!(cell.cell_id, S2CellId::from_face(0));
    assert_eq!(fixture.reader().cell_count(), 1);
    let candidate_cell = fixture.reader().cell_id_at(0);
    assert!(candidate_cell.level() > 3);
    assert!(cell.shapes[0].contains_center);
    assert!(!cell.shapes[1].contains_center);

    for shape in &cell.shapes {
        let get_edge = |idx| multipart.query_edges.get_edge(shape.geometry_id, idx);
        let mut pcell = S2PaddedCell::<Plane>::new(cell.cell_id, Plane::CELL_PADDING);
        let mut edges = shape.edge_indices.clone();
        for level in 1..=3 {
            let mut quadrants = split_edges(&pcell, &edges, &get_edge);
            let child = candidate_cell.parent(level);
            let (i, j) = pcell.get_child_ij(child.child_position() as i32);
            edges = std::mem::take(&mut quadrants[i][j]);
            pcell = S2PaddedCell::from_parent(&pcell, i, j);
        }
        assert!(edges.is_empty());
        assert!(inside::<Plane>(
            &pcell.get_center(),
            &multipart.query_edges.set.members[shape.geometry_id.1 as usize]
        ));
    }
    fixture.check(&query(vec![a.clone()]), &[0]);
    fixture.check(&query(vec![b.clone()]), &[0]);
    fixture.check(&multipart, &[0]);
    fixture.check(&query(vec![b, a]), &[0]);
}

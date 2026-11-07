use std::panic::{catch_unwind, AssertUnwindSafe};

use common::{BitSet, CountingWriter};

use super::*;
use crate::spatial::cell_index_reader::CellIndexReader;
use crate::spatial::clipper::Clipper;
use crate::spatial::edge_reader::EdgeReader;
use crate::spatial::edge_writer::{EdgeWriter, EDGE_FOOTER_BYTES};
use crate::spatial::geometry::to_geometry_set;
use crate::spatial::geometry::Geometry;
use crate::spatial::geometry_set::GeometrySet;
use crate::spatial::plane::Plane;
use crate::spatial::plane_support_tests::{assert_partition, interval_intersects, nominal_bound};
use crate::spatial::shape_index::ShapeIndex;
use crate::spatial::sphere::Sphere;

struct Fixture<S: Surface> {
    sets: Vec<GeometrySet<S>>,
    index: ShapeIndex,
    cells: Vec<u8>,
    edges: Vec<u8>,
}

#[test]
fn test_plane_source_coordinates_and_member_types_round_trip() {
    use crate::spatial::geometry_set::GeometryKind;
    let point = [(-73.0f64).next_up(), 40.0f64.next_down()];
    let geometry = Geometry::<Plane>::GeometryCollection(vec![
        Geometry::Point(point),
        Geometry::LineString(vec![point]),
        Geometry::LineString(Vec::new()),
        Geometry::Polygon(Vec::new()),
        Geometry::Polygon(vec![vec![point; 4]]),
    ])
    .project::<Plane>();
    let set = to_geometry_set(&geometry, 7);
    let fixture = Fixture::new(vec![set], ShapeIndex { cells: Vec::new() });
    let reader = EdgeReader::<Plane>::open(&fixture.edges).unwrap();
    for position in 0..5 {
        let (head, restored) = reader.read_geometry_set(position);
        assert_eq!(head, 0);
        assert_eq!(restored.doc_id, 7);
        assert_eq!(
            restored
                .members
                .iter()
                .map(|member| member.kind)
                .collect::<Vec<_>>(),
            vec![
                GeometryKind::Point,
                GeometryKind::LineString,
                GeometryKind::LineString,
                GeometryKind::Polygon,
                GeometryKind::Polygon,
            ]
        );
        assert_eq!(
            restored.members[0].vertices[0].map(f64::to_bits),
            point.map(f64::to_bits)
        );
        assert_eq!(
            restored.members[1].vertices[0].map(f64::to_bits),
            point.map(f64::to_bits)
        );
        assert!(restored.members[2].vertices.is_empty());
        assert!(restored.members[3].vertices.is_empty());
    }
}

#[test]
fn test_edge_directory_resolves_members_values_and_documents() {
    let sets = vec![
        to_geometry_set(&Geometry::<Plane>::Point([1.0, 2.0]), 3),
        to_geometry_set(
            &Geometry::<Plane>::MultiPoint(vec![[3.0, 4.0], [5.0, 6.0]]),
            1,
        ),
        to_geometry_set(
            &Geometry::<Plane>::LineString(vec![[7.0, 8.0], [9.0, 10.0]]),
            3,
        ),
        to_geometry_set(&Geometry::<Plane>::GeometryCollection(Vec::new()), 3),
    ];
    let mut fixture = Fixture::new(sets, ShapeIndex { cells: Vec::new() });
    let reader = EdgeReader::<Plane>::open(&fixture.edges).unwrap();

    assert_eq!(
        reader.value_heads(0).unwrap().collect::<Vec<_>>(),
        Vec::<u32>::new()
    );
    assert_eq!(reader.value_heads(1).unwrap().collect::<Vec<_>>(), vec![1]);
    assert_eq!(
        reader.value_heads(2).unwrap().collect::<Vec<_>>(),
        Vec::<u32>::new()
    );
    assert_eq!(
        reader.value_heads(3).unwrap().collect::<Vec<_>>(),
        vec![0, 3]
    );

    let first = reader.locate_entry(0);
    assert_eq!((first.head, first.member_count, first.doc_id), (0, 1, 3));
    let sibling = reader.locate_entry(2);
    assert_eq!(
        (sibling.head, sibling.member_count, sibling.doc_id),
        (1, 2, 1)
    );
    assert_eq!(sibling.vertex(0), [5.0, 6.0]);

    // Ownership lookup must not consult member offsets or payloads.
    let footer = fixture.edges.len() - EDGE_FOOTER_BYTES;
    let doc_ids_offset =
        u64::from_le_bytes(fixture.edges[footer + 16..footer + 24].try_into().unwrap()) as usize;
    fixture.edges[..doc_ids_offset].fill(0xff);
    let reader = EdgeReader::<Plane>::open(&fixture.edges).unwrap();
    assert_eq!(
        (0..4).map(|gid| reader.doc_id_for(gid)).collect::<Vec<_>>(),
        vec![3, 1, 1, 3]
    );
    assert_eq!(
        reader.value_heads(3).unwrap().collect::<Vec<_>>(),
        vec![0, 3]
    );
}

impl<S: Surface> Fixture<S> {
    fn new(sets: Vec<GeometrySet<S>>, index: ShapeIndex) -> Self {
        let mut cells = CountingWriter::wrap(Vec::new());
        index.write(&mut cells);
        let mut write = CountingWriter::wrap(Vec::new());
        let mut writer = EdgeWriter::new(&mut write);
        for set in &sets {
            writer.insert(set);
        }
        writer.finish();
        Self {
            sets,
            index,
            cells: cells.finish(),
            edges: write.finish(),
        }
    }

    fn source_cell(&self, index: usize, source: u16) -> ShapeCell {
        let mut cell = self.index.cells[index].clone();
        for shape in &mut cell.shapes {
            shape.geometry_id.0 = source;
        }
        cell
    }
}

fn path(positions: &[i32]) -> S2CellId {
    positions
        .iter()
        .fold(S2CellId::from_face(0), |id, &pos| id.child(pos))
}

fn cell(id: S2CellId, members: std::ops::Range<u32>) -> ShapeCell {
    ShapeCell {
        cell_id: id,
        shapes: members
            .map(|member| ClippedShape {
                geometry_id: (0, member),
                contains_center: false,
                edge_indices: vec![0],
            })
            .collect(),
    }
}

fn arrival_fixtures(finer_j: bool, diagonal: bool) -> Vec<Fixture<Plane>> {
    let (line, paths) = if diagonal {
        (
            vec![[-0.75, -0.75], [0.75, 0.75]],
            vec![
                vec![0],
                vec![1],
                vec![2, 0],
                vec![2, 1],
                vec![2, 2],
                vec![2, 3],
                vec![3],
            ],
        )
    } else {
        (
            vec![[-0.875, 0.1875], [0.875, 0.1875]],
            vec![
                vec![1],
                vec![2, 0, 0],
                vec![2, 0, 1],
                vec![2, 3, 2],
                vec![2, 3, 3],
            ],
        )
    };
    let g = Fixture::new(
        vec![to_geometry_set(
            &Geometry::LineString(
                line.into_iter()
                    .map(|p| Plane::face_uv_to_point(0, p[0], p[1]))
                    .collect(),
            ),
            0,
        )],
        ShapeIndex {
            cells: paths.iter().map(|p| cell(path(p), 0..1)).collect(),
        },
    );
    let points = (1..=6)
        .chain(9..=14)
        .map(|x| Plane::face_uv_to_point(0, x as f64 / 16.0, 0.375))
        .collect();
    let h = Fixture::new(
        vec![to_geometry_set(&Geometry::MultiPoint(points), 1)],
        ShapeIndex {
            cells: vec![cell(path(&[]), 0..12)],
        },
    );
    let j = Fixture::new(
        vec![to_geometry_set(
            &Geometry::Point(Plane::face_uv_to_point(0, 0.4375, 0.125)),
            2,
        )],
        ShapeIndex {
            cells: vec![cell(path(if finer_j { &[2, 0] } else { &[2] }), 0..1)],
        },
    );
    for fixture in [&g, &h, &j] {
        let members: Vec<_> = fixture
            .sets
            .iter()
            .flat_map(|s| &s.members)
            .enumerate()
            .map(|(gid, m)| ((0, gid as u32), m))
            .collect();
        assert_partition(&fixture.index.cells, &members);
    }
    vec![g, h, j]
}

fn sponge<S: Surface>(id: S2CellId) -> SpongeCell<S> {
    SpongeCell::new(id, S2PaddedCell::<S>::new(id, S::CELL_PADDING).bound())
}

fn absorb<S: Surface>(
    sponge: &mut SpongeCell<S>,
    cell: &ShapeCell,
    resolver: &EdgeResolver<'_, S>,
) {
    sponge.absorb_index_cell_anchors(cell, resolver);
    sponge.absorb_index_cell_edges(cell, resolver);
}

fn split<S: Surface>(sponge: SpongeCell<S>, resolver: &EdgeResolver<'_, S>) -> Vec<SpongeCell<S>> {
    coarse_split(sponge, resolver, &mut 0, &mut 0, &[])
        .into_iter()
        .map(|entry| match entry {
            HeapEntry::Sponge(s) => s,
            _ => unreachable!(),
        })
        .collect()
}

fn check_counts(sponge: &SpongeCell<Plane>) {
    assert_eq!(sponge.edge_count, sponge.edges.len());
    assert_eq!(
        sponge.short_edge_count,
        sponge
            .edges
            .iter()
            .filter(|edge| sponge.cell_id.level() < edge.max_level)
            .count()
    );
    for anchor in &sponge.anchors {
        if !anchor.contains_center {
            assert!(sponge
                .edges
                .iter()
                .any(|edge| edge.geometry_id == anchor.geometry_id
                    && interval_intersects(edge.v0, edge.v1, &nominal_bound(anchor.cell_id))));
        }
    }
}

#[test]
fn test_plane_repeated_split_before_late_arrival() {
    for reverse in [false, true] {
        for finer_j in [false, true] {
            let mut fixtures = arrival_fixtures(finer_j, false);
            if reverse {
                fixtures.swap(0, 1);
            }
            let (g, h) = if reverse { (1, 0) } else { (0, 1) };
            let readers: Vec<_> = fixtures
                .iter()
                .map(|f| CellIndexReader::open(&f.cells).unwrap())
                .collect();
            let resolver = EdgeResolver::<Plane>::new(
                fixtures
                    .iter()
                    .map(|f| EdgeReader::open(&f.edges).unwrap())
                    .collect(),
            );
            let alive = vec![None, None, None];
            let mut merge = Interleaver::new(
                readers.iter().map(|r| r.iter()).collect(),
                &resolver,
                &alive,
                vec![],
            );
            let first = merge.next().unwrap();
            assert_eq!(first.cell_id, path(&[1]));
            assert_eq!(merge.splits, 1);
            let pending = merge
                .heap
                .iter()
                .find_map(|entry| match entry {
                    HeapEntry::Sponge(s) if s.cell_id == path(&[2]) => Some(s),
                    _ => None,
                })
                .unwrap();
            check_counts(pending);
            assert_eq!(pending.edge_count, 12);
            assert_eq!(pending.short_edge_count, 12);
            assert_eq!(pending.anchors.len(), 12);
            assert!(pending
                .edges
                .iter()
                .all(|e| e.geometry_id.0 == h && e.max_level == 30));
            assert!(pending.anchors.iter().all(|a| a.geometry_id.0 != g));

            let second = merge.next().unwrap();
            assert_eq!(second.cell_id, path(&[2, 0]));
            assert_eq!(second.find_shape((g, 0)).unwrap().edge_indices, [0]);
            assert_eq!(second.shapes.len(), 8);
            assert_eq!(merge.splits, 2);
            let pending = merge
                .heap
                .iter()
                .find_map(|entry| match entry {
                    HeapEntry::Sponge(s) if s.cell_id == path(&[2, 3]) => Some(s),
                    _ => None,
                })
                .unwrap();
            check_counts(pending);
            assert_eq!(pending.edge_count, 6);
            assert!(pending.edges.iter().all(|e| e.geometry_id.0 == h));
            assert!(pending.anchors.iter().all(|a| a.geometry_id.0 != g));
            let mut cells = vec![first, second];
            cells.extend(merge.by_ref());
            assert_eq!(merge.splits, 2);
            let members: Vec<_> = fixtures
                .iter()
                .enumerate()
                .flat_map(|(source, fixture)| {
                    fixture
                        .sets
                        .iter()
                        .flat_map(|s| &s.members)
                        .enumerate()
                        .map(move |(gid, m)| ((source as u16, gid as u32), m))
                })
                .collect();
            assert_partition(&cells, &members);
        }
    }
}

#[test]
fn test_plane_equal_cell_absorption_orders() {
    let fixtures = arrival_fixtures(false, false);
    let resolver = EdgeResolver::<Plane>::new(
        fixtures
            .iter()
            .map(|f| EdgeReader::open(&f.edges).unwrap())
            .collect(),
    );
    for source_first in [false, true] {
        let mut root = sponge::<Plane>(path(&[]));
        absorb(&mut root, &fixtures[1].source_cell(0, 1), &resolver);
        absorb(&mut root, &fixtures[0].source_cell(0, 0), &resolver);
        let mut children = split(root, &resolver);
        let upper = children.remove(
            children
                .iter()
                .position(|s| s.cell_id == path(&[2]))
                .unwrap(),
        );
        let upper = if source_first {
            let mut incoming = sponge::<Plane>(path(&[2]));
            absorb(&mut incoming, &fixtures[2].source_cell(0, 2), &resolver);
            incoming.absorb_sponge_cell_edges(&upper);
            incoming.absorb_sponge_cell_anchors(&upper);
            incoming
        } else {
            let mut upper = upper;
            absorb(&mut upper, &fixtures[2].source_cell(0, 2), &resolver);
            upper
        };
        check_counts(&upper);
        assert_eq!(upper.edge_count, 13);
        let mut grandchildren = split(upper, &resolver);
        for (child_id, source_indices, count) in
            [(path(&[2, 0]), [1, 2], 8), (path(&[2, 3]), [3, 4], 7)]
        {
            let child = grandchildren
                .iter_mut()
                .find(|s| s.cell_id == child_id)
                .unwrap();
            for index in source_indices {
                absorb(child, &fixtures[0].source_cell(index, 0), &resolver);
            }
            check_counts(child);
            assert_eq!(child.edge_count, count);
            assert_eq!(
                child
                    .edges
                    .iter()
                    .filter(|e| e.geometry_id == (0, 0))
                    .count(),
                1
            );
            assert_eq!(
                child
                    .anchors
                    .iter()
                    .filter(|a| a.geometry_id == (0, 0))
                    .count(),
                2
            );
        }
        children.extend(grandchildren);
        let mut cells: Vec<_> = children
            .into_iter()
            .map(|s| flatten(HeapEntry::Sponge(s), &resolver).unwrap())
            .collect();
        cells.sort_unstable_by_key(|c| c.cell_id);
        let members: Vec<_> = fixtures
            .iter()
            .enumerate()
            .flat_map(|(source, fixture)| {
                fixture
                    .sets
                    .iter()
                    .flat_map(|s| &s.members)
                    .enumerate()
                    .map(move |(gid, m)| ((source as u16, gid as u32), m))
            })
            .collect();
        assert_partition(&cells, &members);
    }
}

#[test]
fn test_plane_containing_cell_absorbs_sponge() {
    let fixtures = arrival_fixtures(false, false);
    let resolver = EdgeResolver::<Plane>::new(
        fixtures
            .iter()
            .map(|f| EdgeReader::open(&f.edges).unwrap())
            .collect(),
    );
    for containing in [path(&[]), path(&[2])] {
        let mut child = sponge::<Plane>(path(&[2, 0]));
        for index in [1, 2] {
            absorb(&mut child, &fixtures[0].source_cell(index, 0), &resolver);
        }
        check_counts(&child);
        let mut source = fixtures[2].source_cell(0, 2);
        source.cell_id = containing;
        let mut parent = sponge::<Plane>(source.cell_id);
        absorb(&mut parent, &source, &resolver);
        parent.absorb_sponge_cell_edges(&child);
        parent.absorb_sponge_cell_anchors(&child);
        check_counts(&parent);
        let edge = parent
            .edges
            .iter()
            .find(|e| e.geometry_id == (0, 0))
            .unwrap();
        assert_eq!(
            edge.bound,
            Plane::edge_bound_uv(&edge.v0, &edge.v1, 0)
                .intersection(&cell_bound_uv::<Plane>(containing))
        );
        assert_eq!(
            parent
                .anchors
                .iter()
                .filter(|a| a.geometry_id == (0, 0))
                .count(),
            2
        );
        let output = flatten(HeapEntry::Sponge(parent), &resolver).unwrap();
        let mut cells = Vec::new();
        if !containing.is_face() {
            cells.push(fixtures[0].source_cell(0, 0));
        }
        cells.push(output);
        assert_partition(
            &cells,
            &[
                ((0, 0), &fixtures[0].sets[0].members[0]),
                ((2, 0), &fixtures[2].sets[0].members[0]),
            ],
        );
    }
}

#[test]
fn test_plane_corner_only_arrival() {
    for reverse in [false, true] {
        let mut fixtures = arrival_fixtures(false, true);
        if reverse {
            fixtures.swap(0, 1);
        }
        let g = if reverse { 1 } else { 0 };
        let readers: Vec<_> = fixtures
            .iter()
            .map(|f| CellIndexReader::open(&f.cells).unwrap())
            .collect();
        let resolver = EdgeResolver::<Plane>::new(
            fixtures
                .iter()
                .map(|f| EdgeReader::open(&f.edges).unwrap())
                .collect(),
        );
        let alive = vec![None, None, None];
        let mut merge = Interleaver::new(
            readers.iter().map(|r| r.iter()).collect(),
            &resolver,
            &alive,
            vec![],
        );
        let first = merge.next().unwrap();
        assert_eq!(first.cell_id, path(&[0]));
        assert_eq!(merge.splits, 1);
        let pending = merge
            .heap
            .iter()
            .find_map(|entry| match entry {
                HeapEntry::Sponge(s) if s.cell_id == path(&[2]) => Some(s),
                _ => None,
            })
            .unwrap();
        assert!(pending.edges.iter().all(|e| e.geometry_id.0 != g));
        let mut cells = vec![first];
        cells.extend(merge);
        let members: Vec<_> = fixtures
            .iter()
            .enumerate()
            .flat_map(|(source, fixture)| {
                fixture
                    .sets
                    .iter()
                    .flat_map(|s| &s.members)
                    .enumerate()
                    .map(move |(gid, m)| ((source as u16, gid as u32), m))
            })
            .collect();
        assert_partition(&cells, &members);
        for id in [path(&[1]), path(&[3]), path(&[2, 1])] {
            let cell = cells.iter().find(|c| c.cell_id == id).unwrap();
            assert_eq!(cell.find_shape((g, 0)).unwrap().edge_indices, [0]);
        }
    }
}

fn anchor<S: Surface>(gid: GeometryId, id: S2CellId, inside: bool) -> SpongeShape<S> {
    SpongeShape {
        geometry_id: gid,
        cell_id: id,
        center: S::cell_center(id),
        contains_center: inside,
        closed: true,
    }
}

fn point_edge(gid: GeometryId) -> SpongeEdge<Plane> {
    SpongeEdge {
        geometry_id: gid,
        edge_index: 0,
        max_level: 30,
        v0: [0.0, 0.0],
        v1: [0.0, 0.0],
        a_uv: [0.0, 0.0],
        b_uv: [0.0, 0.0],
        bound: R2Rect::from_point([0.0, 0.0]),
    }
}

#[test]
fn test_plane_rejects_disjoint_stored_occurrence() {
    let fixtures = arrival_fixtures(false, false);
    let resolver = EdgeResolver::<Plane>::new(
        fixtures
            .iter()
            .map(|f| EdgeReader::open(&f.edges).unwrap())
            .collect(),
    );
    let mut source = fixtures[0].source_cell(0, 0);
    source.cell_id = path(&[3]);
    let mut target = sponge::<Plane>(source.cell_id);
    assert!(catch_unwind(AssertUnwindSafe(|| absorb(&mut target, &source, &resolver))).is_err());

    let mut invalid = sponge::<Plane>(path(&[2, 2]));
    invalid.edges.push(point_edge((0, 0)));
    let mut target = sponge::<Plane>(invalid.cell_id);
    assert!(catch_unwind(AssertUnwindSafe(
        || target.absorb_sponge_cell_edges(&invalid)
    ))
    .is_err());
    assert!(catch_unwind(AssertUnwindSafe(|| split(invalid, &resolver))).is_err());
}

#[test]
fn test_false_fine_anchor_checked_in_both_orders() {
    for reverse in [false, true] {
        let mut s = sponge::<Plane>(path(&[]));
        s.anchors = vec![
            anchor((0, 0), path(&[2, 0]), true),
            anchor((0, 0), path(&[2, 1]), false),
        ];
        if reverse {
            s.anchors.reverse();
        }
        assert!(catch_unwind(AssertUnwindSafe(|| split(
            s,
            &EdgeResolver::new(Vec::new())
        )))
        .is_err());
    }
}

#[test]
fn test_split_rejects_mixed_or_duplicate_parent_anchors() {
    for second in [path(&[]), path(&[2])] {
        let mut s = sponge::<Plane>(path(&[]));
        s.anchors = vec![
            anchor((0, 0), path(&[]), true),
            anchor((0, 0), second, true),
        ];
        assert!(catch_unwind(AssertUnwindSafe(|| split(
            s,
            &EdgeResolver::new(Vec::new())
        )))
        .is_err());
    }
}

#[test]
fn test_flatten_rejects_unmatched_edge_groups() {
    for orphan in [0, 2, 4] {
        let mut s = sponge::<Plane>(path(&[]));
        s.anchors = vec![
            anchor((0, 1), path(&[]), true),
            anchor((0, 3), path(&[]), true),
        ];
        s.edges = vec![
            point_edge((0, 1)),
            point_edge((0, 3)),
            point_edge((0, orphan)),
        ];
        s.edges.sort_unstable_by_key(|e| e.geometry_id);
        assert!(catch_unwind(AssertUnwindSafe(|| flatten(
            HeapEntry::Sponge(s),
            &EdgeResolver::new(Vec::new())
        )))
        .is_err());
    }
}

#[test]
fn test_sphere_drops_unanchored_conservative_extra() {
    let p = |u, v| Sphere::face_uv_to_point(0, u, v);
    let geometries: [Geometry<Sphere>; 2] = [
        Geometry::LineString(vec![p(-1e-16, 0.25), p(-1e-16, 0.75)]),
        Geometry::Point(p(0.75, 0.75)),
    ];
    for reverse in [false, true] {
        let mut fixtures: Vec<_> = geometries
            .iter()
            .enumerate()
            .map(|(i, geometry)| {
                Fixture::new(
                    vec![to_geometry_set(geometry, i as u32)],
                    ShapeIndex {
                        cells: vec![cell(path(if i == 0 { &[1] } else { &[] }), 0..1)],
                    },
                )
            })
            .collect();
        if reverse {
            fixtures.swap(0, 1);
        }
        let (g, h) = if reverse { (1, 0) } else { (0, 1) };
        let resolver = EdgeResolver::<Sphere>::new(
            fixtures
                .iter()
                .map(|f| EdgeReader::open(&f.edges).unwrap())
                .collect(),
        );
        let mut root = sponge::<Sphere>(path(&[]));
        absorb(
            &mut root,
            &fixtures[g as usize].source_cell(0, g),
            &resolver,
        );
        absorb(
            &mut root,
            &fixtures[h as usize].source_cell(0, h),
            &resolver,
        );
        let edge = root.edges.iter().find(|e| e.geometry_id == (g, 0)).unwrap();
        let flat = FlatEdge {
            geometry_id: (g, 0),
            edge,
            a_uv: edge.a_uv,
            b_uv: edge.b_uv,
        };
        let clipped = MergeClippedEdge {
            edge_index: 0,
            bound: edge.bound,
        };
        let padded = S2PaddedCell::<Sphere>::new(path(&[]), Sphere::CELL_PADDING);
        let mut nominations: [[Vec<MergeClippedEdge>; 2]; 2] = Default::default();
        clip_to_children(&clipped, &flat, &padded.middle(), &mut nominations);
        let (i, j) = padded.get_child_ij(2);
        assert_eq!(nominations[i][j].len(), 1);
        let children = split(root, &resolver);
        let right = children.iter().find(|s| s.cell_id == path(&[2])).unwrap();
        assert_eq!(right.edges.len(), 1);
        assert_eq!(right.edge_count, 1);
        assert_eq!(right.edges[0].geometry_id, (h, 0));
        assert_eq!(right.anchors.len(), 1);
        for child in children {
            flatten(HeapEntry::Sponge(child), &resolver).unwrap();
        }
    }
}

#[test]
fn test_plane_repeated_merge_and_deletion() {
    for delete_points in [false, true] {
        let mut fixtures = arrival_fixtures(false, false);
        let j = fixtures.pop().unwrap();
        let readers: Vec<_> = fixtures
            .iter()
            .map(|f| CellIndexReader::open(&f.cells).unwrap())
            .collect();
        let resolver = EdgeResolver::<Plane>::new(
            fixtures
                .iter()
                .map(|f| EdgeReader::open(&f.edges).unwrap())
                .collect(),
        );
        let alive = vec![None, None];
        let mut cells: Vec<_> = Interleaver::new(
            readers.iter().map(|r| r.iter()).collect(),
            &resolver,
            &alive,
            vec![],
        )
        .collect();
        for cell in &mut cells {
            for shape in &mut cell.shapes {
                shape.geometry_id = (0, shape.geometry_id.1 + u32::from(shape.geometry_id.0 == 1));
            }
        }
        let sets: Vec<_> = fixtures.iter().flat_map(|f| f.sets.clone()).collect();
        // This control rewrites the cell output and rebuilds its edge file from the source sets.
        let merged = Fixture::new(sets, ShapeIndex { cells });
        let fixtures = [merged, j];
        let readers: Vec<_> = fixtures
            .iter()
            .map(|f| CellIndexReader::open(&f.cells).unwrap())
            .collect();
        let resolver = EdgeResolver::<Plane>::new(
            fixtures
                .iter()
                .map(|f| EdgeReader::open(&f.edges).unwrap())
                .collect(),
        );
        let mut live = BitSet::with_max_value(3);
        live.insert(0);
        let alive = vec![
            if delete_points {
                Some(ReadOnlyBitSet::from(&live))
            } else {
                None
            },
            None,
        ];
        let cells: Vec<_> = Interleaver::new(
            readers.iter().map(|r| r.iter()).collect(),
            &resolver,
            &alive,
            vec![],
        )
        .collect();
        let members: Vec<_> = fixtures
            .iter()
            .enumerate()
            .flat_map(|(source, fixture)| {
                fixture
                    .sets
                    .iter()
                    .flat_map(|s| s.members.iter().map(move |m| (s.doc_id, m)))
                    .enumerate()
                    .filter(move |(_, (doc, _))| !delete_points || *doc != 1)
                    .map(move |(gid, (_, m))| ((source as u16, gid as u32), m))
            })
            .collect();
        assert_partition(&cells, &members);
        for cell in &cells {
            for shape in &cell.shapes {
                let doc = resolver.doc_id_for(shape.geometry_id);
                assert!(doc < 3 && (!delete_points || doc != 1));
            }
        }
    }
}

fn rectangle(lo: [f64; 2], hi: [f64; 2], steps: usize) -> Vec<[f64; 2]> {
    let corners = [lo, [hi[0], lo[1]], hi, [lo[0], hi[1]]];
    let mut ring = Vec::new();
    for side in 0..4 {
        let (a, b) = (corners[side], corners[(side + 1) % 4]);
        for step in 0..steps {
            let t = step as f64 / steps as f64;
            ring.push([a[0] + t * (b[0] - a[0]), a[1] + t * (b[1] - a[1])]);
        }
    }
    ring.push(ring[0]);
    ring
}

#[test]
fn test_plane_merge_area_centers_and_holes() {
    for reverse in [false, true] {
        let a = to_geometry_set(
            &Geometry::<Plane>::Polygon(vec![rectangle([-0.75, -0.75], [0.75, 0.75], 16)]),
            0,
        );
        let b = to_geometry_set(
            &Geometry::<Plane>::GeometryCollection(vec![
                Geometry::Polygon(vec![
                    rectangle([-0.625, -0.625], [0.625, 0.625], 8),
                    rectangle([-0.25, -0.25], [0.25, 0.25], 4),
                ]),
                Geometry::LineString(vec![[-0.5, 0.0], [0.5, 0.0], [-0.5, 0.0]]),
            ]),
            1,
        );
        let coarse = Clipper::<Plane>::new(ClipOptions::default().with_max_edges_per_cell(1000))
            .build(std::slice::from_ref(&a));
        assert_eq!(coarse.cells.len(), 1);
        let fine = Clipper::<Plane>::new(
            ClipOptions::default()
                .with_max_edges_per_cell(1)
                .with_min_short_edge_fraction(0.0),
        )
        .build(std::slice::from_ref(&b));
        assert!(fine.cells.iter().any(|cell| cell.cell_id.level() >= 3));
        let mut fixtures = vec![Fixture::new(vec![a], coarse), Fixture::new(vec![b], fine)];
        if reverse {
            fixtures.reverse();
        }
        let readers: Vec<_> = fixtures
            .iter()
            .map(|f| CellIndexReader::open(&f.cells).unwrap())
            .collect();
        let resolver = EdgeResolver::<Plane>::new(
            fixtures
                .iter()
                .map(|f| EdgeReader::open(&f.edges).unwrap())
                .collect(),
        );
        let alive = vec![None, None];
        let cells: Vec<_> = Interleaver::new(
            readers.iter().map(|r| r.iter()).collect(),
            &resolver,
            &alive,
            vec![],
        )
        .collect();
        let members: Vec<_> = fixtures
            .iter()
            .enumerate()
            .flat_map(|(source, fixture)| {
                fixture
                    .sets
                    .iter()
                    .flat_map(|s| &s.members)
                    .enumerate()
                    .map(move |(gid, m)| ((source as u16, gid as u32), m))
            })
            .collect();
        assert_partition(&cells, &members);
        assert!(cells
            .iter()
            .flat_map(|c| &c.shapes)
            .any(|s| s.contains_center && s.edge_indices.is_empty()));
    }
}

#[test]
fn test_plane_fine_anchor_group_keeps_both_centers() {
    for reverse in [false, true] {
        let mut s = sponge::<Plane>(path(&[]));
        s.anchors = vec![
            anchor((0, 0), path(&[2, 0, 0]), false),
            anchor((0, 0), path(&[2, 0, 1]), true),
        ];
        if reverse {
            s.anchors.reverse();
        }
        for (edge_index, x) in [(0, 0.2), (1, 0.45)] {
            let (a, b) = ([x, -0.1], [x, 0.45]);
            s.edges.push(SpongeEdge {
                geometry_id: (0, 0),
                edge_index,
                max_level: get_edge_max_level::<Plane>(&a, &b),
                v0: Plane::face_uv_to_point(0, a[0], a[1]),
                v1: Plane::face_uv_to_point(0, b[0], b[1]),
                a_uv: a,
                b_uv: b,
                bound: R2Rect::from_point_pair(a, b),
            });
        }
        let children = split(s, &EdgeResolver::new(Vec::new()));
        let child = children.iter().find(|s| s.cell_id == path(&[2])).unwrap();
        check_counts(child);
        assert_eq!(child.edge_count, 2);
        assert_eq!(child.anchors.len(), 2);
        for (id, inside) in [(path(&[2, 0, 0]), false), (path(&[2, 0, 1]), true)] {
            let anchor = child.anchors.iter().find(|a| a.cell_id == id).unwrap();
            assert_eq!(anchor.center, Plane::cell_center(id));
            assert_eq!(anchor.contains_center, inside);
        }
    }
}

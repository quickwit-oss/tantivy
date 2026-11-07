use common::{CountingWriter, ReadOnlyBitSet};

use super::*;
use crate::spatial::edge_reader::EdgeReader;
use crate::spatial::edge_writer::EdgeWriter;
use crate::spatial::geometry::to_geometry_set;
use crate::spatial::geometry::Geometry;
use crate::spatial::merge_field::{merge_field, MergeSource};
use crate::spatial::plane::Plane;
use crate::spatial::shape_index::ShapeCell;

struct Fixture {
    cells: Vec<u8>,
    edges: Vec<u8>,
    max_doc: u32,
}

impl Fixture {
    fn new(values: &[(u32, Geometry<Plane>)], limit: usize) -> Self {
        let sets: Vec<_> = values
            .iter()
            .map(|(doc, geometry)| to_geometry_set(geometry, *doc))
            .collect();
        let index = Clipper::new(
            ClipOptions::default()
                .with_max_edges_per_cell(limit)
                .with_min_short_edge_fraction(0.0),
        )
        .build(&sets);
        let mut cells = CountingWriter::wrap(Vec::new());
        index.write(&mut cells);
        let mut write = CountingWriter::wrap(Vec::new());
        let mut writer = EdgeWriter::new(&mut write);
        for set in &sets {
            writer.insert(set);
        }
        writer.finish();
        Self {
            cells: cells.finish(),
            edges: write.finish(),
            max_doc: values.iter().map(|v| v.0 + 1).max().unwrap_or(0),
        }
    }

    fn hits(&self, query: &PlaneQuery, predicate: Predicate) -> Vec<u32> {
        let reader = CellIndexReader::open(&self.cells).unwrap();
        let resolver = EdgeResolver::<Plane>::new(vec![EdgeReader::open(&self.edges).unwrap()]);
        let hits = query.search(&reader, None, &resolver, self.max_doc, predicate, false);
        (0..self.max_doc)
            .filter(|&doc| hits.contains(doc))
            .collect()
    }

    fn merge(inputs: &[&Self], mapping: &[Vec<Option<u32>>]) -> Self {
        let mut cells = CountingWriter::wrap(Vec::new());
        let mut edges = CountingWriter::wrap(Vec::new());
        let mut docs = CountingWriter::wrap(Vec::new());
        let sources: Vec<_> = inputs
            .iter()
            .enumerate()
            .map(|(index, input)| MergeSource {
                cells_bytes: &input.cells,
                edges_bytes: &input.edges,
                segment_name: index.to_string(),
            })
            .collect();
        let alive: Vec<_> = mapping
            .iter()
            .map(|map| {
                let mut live = BitSet::with_max_value(map.len() as u32);
                for (doc, mapped) in map.iter().enumerate() {
                    if mapped.is_some() {
                        live.insert(doc as u32);
                    }
                }
                Some(ReadOnlyBitSet::from(&live))
            })
            .collect();
        merge_field::<Plane, _, _, _>(&sources, &alive, &mut cells, &mut edges, &mut docs, mapping)
            .unwrap();
        Self {
            cells: cells.finish(),
            edges: edges.finish(),
            max_doc: mapping
                .iter()
                .flatten()
                .filter_map(|doc| *doc)
                .max()
                .map_or(0, |doc| doc + 1),
        }
    }
}

fn rectangle(x0: f64, y0: f64, x1: f64, y1: f64) -> Vec<[f64; 2]> {
    vec![[x0, y0], [x1, y0], [x1, y1], [x0, y1], [x0, y0]]
}

fn subdivided_rectangle(x0: f64, y0: f64, x1: f64, y1: f64, steps: usize) -> Vec<[f64; 2]> {
    let mut ring = Vec::with_capacity(4 * steps + 1);
    for step in 0..steps {
        let t = step as f64 / steps as f64;
        ring.push([x0 + t * (x1 - x0), y0]);
    }
    for step in 0..steps {
        let t = step as f64 / steps as f64;
        ring.push([x1, y0 + t * (y1 - y0)]);
    }
    for step in 0..steps {
        let t = step as f64 / steps as f64;
        ring.push([x1 - t * (x1 - x0), y1]);
    }
    for step in 0..steps {
        let t = step as f64 / steps as f64;
        ring.push([x0, y1 - t * (y1 - y0)]);
    }
    ring.push(ring[0]);
    ring
}

fn has_interior_cell_containing(cells: &ShapeIndex, bound: &R2Rect) -> bool {
    cells.cells.iter().any(|cell| {
        cell.shapes
            .iter()
            .any(|shape| shape.contains_center && shape.edge_indices.is_empty())
            && source_bound(&cell_bound_uv::<Plane>(cell.cell_id)).contains(bound)
    })
}

fn query(geometry: &Geometry<Plane>) -> PlaneQuery {
    PlaneQuery::prepare(&to_geometry_set(geometry, 0)).unwrap()
}

#[test]
fn test_plane_analytic_line_with_boundary_endpoints() {
    let polygon = query(&Geometry::Polygon(vec![rectangle(0.0, 0.0, 1.0, 1.0)]));
    let line = Geometry::LineString(vec![[0.25, 1.0], [0.5, 0.5], [0.75, 1.0]]);
    for limit in [1, 100] {
        let fixture = Fixture::new(&[(0, line.clone())], limit);
        assert_eq!(fixture.hits(&polygon, Predicate::Intersects), vec![0]);
        assert_eq!(fixture.hits(&polygon, Predicate::CoveredBy), vec![0]);
        assert!(fixture.hits(&polygon, Predicate::Contains).is_empty());
    }
}

#[test]
fn test_plane_values_reduce_existentially_without_union() {
    let a = Geometry::Polygon(vec![rectangle(-1.0, -1.0, 0.0, 1.0)]);
    let b = Geometry::Polygon(vec![rectangle(0.0, -1.0, 1.0, 1.0)]);
    let line = query(&Geometry::LineString(vec![[-0.5, 0.0], [0.5, 0.0]]));
    for reverse in [false, true] {
        let mut values = vec![
            (0, a.clone()),
            (0, b.clone()),
            (1, Geometry::GeometryCollection(vec![a.clone(), b.clone()])),
            (2, Geometry::Polygon(Vec::new())),
            (2, a.clone()),
        ];
        if reverse {
            values.reverse();
        }
        let fixture = Fixture::new(&values, 1);
        assert_eq!(fixture.hits(&line, Predicate::Intersects), vec![0, 1, 2]);
        assert_eq!(fixture.hits(&line, Predicate::Covers), vec![1]);
        assert_eq!(fixture.hits(&line, Predicate::Contains), vec![1]);
    }
}

#[test]
fn test_plane_hole_roles_and_effective_empty_values() {
    let shell = rectangle(-2.0, -2.0, 2.0, 2.0);
    let mut doubled = shell.clone();
    doubled.extend_from_slice(&shell[1..]);
    let values = vec![
        (
            0,
            Geometry::Polygon(vec![shell.clone(), rectangle(3.0, 3.0, 4.0, 4.0)]),
        ),
        (
            1,
            Geometry::Polygon(vec![
                shell.clone(),
                rectangle(-1.0, -1.0, 0.5, 1.0),
                rectangle(-0.5, -1.0, 1.0, 1.0),
            ]),
        ),
        (2, Geometry::Polygon(vec![shell.clone(), shell])),
        (3, Geometry::Polygon(vec![doubled])),
    ];
    for limit in [1, 100] {
        let fixture = Fixture::new(&values, limit);
        for point in [[0.0, 0.0], [0.0, 0.5]] {
            let point = query(&Geometry::Point(point));
            assert_eq!(fixture.hits(&point, Predicate::Intersects), vec![0]);
            assert_eq!(fixture.hits(&point, Predicate::Contains), vec![0]);
        }
        assert!(fixture
            .hits(&query(&Geometry::Point([3.5, 3.5])), Predicate::Intersects)
            .is_empty());
    }
}

#[test]
fn test_plane_covered_side_partial_cancellation() {
    let effective = Geometry::Polygon(vec![
        rectangle(0.0, 0.0, 2.0, 1.0),
        rectangle(0.0, 0.0, 1.0, 1.0),
    ]);
    let rectangle = Geometry::Polygon(vec![rectangle(1.0, 0.0, 2.0, 1.0)]);
    for (left, right) in [
        (effective.clone(), rectangle.clone()),
        (rectangle, effective),
    ] {
        let fixture = Fixture::new(&[(0, left)], 1);
        let right = query(&right);
        for predicate in [
            Predicate::Intersects,
            Predicate::Covers,
            Predicate::Contains,
            Predicate::CoveredBy,
        ] {
            assert_eq!(fixture.hits(&right, predicate), vec![0]);
        }
    }
}

#[test]
fn test_plane_extra_discovery_does_not_add_hits() {
    let fixture = Fixture::new(
        &[
            (0, Geometry::Point([0.5, 0.5])),
            (1, Geometry::Point([100.0, 50.0])),
        ],
        1,
    );
    let mut polygon = query(&Geometry::Polygon(vec![rectangle(0.0, 0.0, 1.0, 1.0)]));
    assert_eq!(fixture.hits(&polygon, Predicate::Intersects), vec![0]);
    let mut extra = ShapeCell::new(crate::spatial::s2cell_id::S2CellId::from_face(0));
    let mut shape = crate::spatial::clipped_shape::ClippedShape::new((0, 0), false);
    shape.edge_indices.push(0);
    extra.add_shape(shape);
    polygon.index.cells.insert(0, extra);
    assert_eq!(fixture.hits(&polygon, Predicate::Intersects), vec![0]);
    assert_eq!(fixture.hits(&polygon, Predicate::CoveredBy), vec![0]);
}

fn transform(geometry: &Geometry<Plane>, variant: u8) -> Geometry<Plane> {
    let point = |p: [f64; 2]| if variant & 2 != 0 { [-p[0], p[1]] } else { p };
    let chain = |vertices: &Vec<[f64; 2]>| {
        let mut result = Vec::new();
        for pair in vertices.windows(2) {
            result.push(point(pair[0]));
            if variant & 4 != 0 {
                result.push(point([
                    (pair[0][0] + pair[1][0]) / 2.0,
                    (pair[0][1] + pair[1][1]) / 2.0,
                ]));
                result.push(point(pair[1]));
            }
        }
        if let Some(&last) = vertices.last() {
            result.push(point(last));
        }
        if variant & 1 != 0 {
            result.reverse();
        }
        result
    };
    match geometry {
        Geometry::Point(p) => Geometry::Point(point(*p)),
        Geometry::MultiPoint(points) => {
            Geometry::MultiPoint(points.iter().copied().map(point).collect())
        }
        Geometry::LineString(line) => Geometry::LineString(chain(line)),
        Geometry::MultiLineString(lines) => {
            Geometry::MultiLineString(lines.iter().map(chain).collect())
        }
        Geometry::Polygon(rings) => Geometry::Polygon(rings.iter().map(chain).collect()),
        Geometry::MultiPolygon(polygons) => Geometry::MultiPolygon(
            polygons
                .iter()
                .map(|rings| rings.iter().map(chain).collect())
                .collect(),
        ),
        Geometry::GeometryCollection(members) => {
            let mut members: Vec<_> = members
                .iter()
                .map(|member| transform(member, variant))
                .collect();
            if variant & 1 != 0 {
                members.reverse();
            }
            Geometry::GeometryCollection(members)
        }
    }
}

#[test]
fn test_plane_relation_truth_table_survives_equivalent_representations() {
    let area = Geometry::Polygon(vec![rectangle(0.0, 0.0, 2.0, 2.0)]);
    let line = vec![[0.0, 0.0], [1.0, 0.0]];
    // Columns are Intersects, Contains, Covers and CoveredBy, with stored value left.
    let cases = vec![
        (
            "crossing",
            area.clone(),
            Geometry::LineString(vec![[-1.0, 1.0], [3.0, 1.0]]),
            [true, false, false, false],
        ),
        (
            "boundary",
            area.clone(),
            Geometry::LineString(vec![[0.5, 0.0], [1.5, 0.0]]),
            [true, false, true, false],
        ),
        (
            "interior",
            area.clone(),
            Geometry::Point([1.0, 1.0]),
            [true, true, true, false],
        ),
        (
            "tangent",
            area.clone(),
            Geometry::Polygon(vec![rectangle(2.0, 2.0, 3.0, 3.0)]),
            [true, false, false, false],
        ),
        (
            "shared edge",
            area.clone(),
            Geometry::Polygon(vec![rectangle(2.0, 0.0, 3.0, 2.0)]),
            [true, false, false, false],
        ),
        (
            "disjoint",
            area.clone(),
            Geometry::Point([3.0, 3.0]),
            [false; 4],
        ),
        (
            "union seam",
            Geometry::MultiPolygon(vec![
                vec![rectangle(-1.0, -1.0, 0.0, 1.0)],
                vec![rectangle(0.0, -1.0, 1.0, 1.0)],
            ]),
            Geometry::LineString(vec![[0.0, -0.5], [0.0, 0.5]]),
            [true, true, true, false],
        ),
        (
            "diagonal",
            Geometry::MultiPolygon(vec![
                vec![rectangle(-1.0, -1.0, 0.0, 0.0)],
                vec![rectangle(0.0, 0.0, 1.0, 1.0)],
            ]),
            Geometry::Point([0.0, 0.0]),
            [true, false, true, false],
        ),
        (
            "hidden hole",
            Geometry::Polygon(vec![
                rectangle(-2.0, -2.0, 2.0, 2.0),
                rectangle(-0.5, -0.5, 0.5, 0.5),
            ]),
            Geometry::Polygon(vec![rectangle(-1.0, -1.0, 1.0, 1.0)]),
            [true, false, false, false],
        ),
        (
            "area precedence",
            Geometry::GeometryCollection(vec![
                area.clone(),
                Geometry::Point([0.0, 0.0]),
                Geometry::LineString(line.clone()),
            ]),
            Geometry::Point([0.0, 0.0]),
            [true, false, true, false],
        ),
        (
            "outside member",
            Geometry::GeometryCollection(vec![area.clone(), Geometry::Point([3.0, 3.0])]),
            area.clone(),
            [true, true, true, false],
        ),
        (
            "line endpoint",
            Geometry::LineString(line.clone()),
            Geometry::Point([0.0, 0.0]),
            [true, false, true, false],
        ),
        (
            "duplicate chains",
            Geometry::MultiLineString(vec![line.clone(), line]),
            Geometry::Point([0.0, 0.0]),
            [true, true, true, false],
        ),
        (
            "collapsed line",
            Geometry::LineString(vec![[0.0, -0.0]; 3]),
            Geometry::Point([-0.0, 0.0]),
            [true; 4],
        ),
        (
            "nonrepresentable events",
            Geometry::Polygon(vec![vec![[0.0, 0.0], [1.0, 1.0], [0.0, 1.0], [0.0, 0.0]]]),
            Geometry::Polygon(vec![vec![[0.0, 1.0], [1.0, -1.0], [1.0, 1.0], [0.0, 1.0]]]),
            [true, false, false, false],
        ),
        (
            "empty",
            Geometry::Polygon(Vec::new()),
            area.clone(),
            [false; 4],
        ),
        (
            "three incident edges at one third",
            Geometry::MultiLineString(vec![
                vec![[0.0, 0.0], [1.0, 1.0]],
                vec![[0.0, 1.0], [1.0, -1.0]],
            ]),
            Geometry::LineString(vec![[1.0, 0.0], [-1.0, 1.0]]),
            [true, false, false, false],
        ),
        (
            "point on open fragment",
            Geometry::LineString(vec![[0.0, 0.0], [2.0, 0.0]]),
            Geometry::GeometryCollection(vec![
                Geometry::Point([0.5, 0.0]),
                Geometry::LineString(Vec::new()),
            ]),
            [true, true, true, false],
        ),
        (
            "covered side proper self crossing",
            Geometry::Polygon(vec![vec![[0.0, 2.0], [2.0, 2.0], [1.0, 1.0], [0.0, 2.0]]]),
            Geometry::Polygon(vec![vec![
                [0.0, 0.0],
                [2.0, 2.0],
                [0.0, 2.0],
                [2.0, 0.0],
                [0.0, 0.0],
            ]]),
            [true, false, false, true],
        ),
        (
            "three bar pocket",
            Geometry::MultiPolygon(vec![
                vec![rectangle(0.0, 0.0, 4.0, 1.0)],
                vec![vec![
                    [-0.5, 0.0],
                    [0.5, 0.0],
                    [2.5, 4.0],
                    [1.5, 4.0],
                    [-0.5, 0.0],
                ]],
                vec![vec![
                    [3.5, 0.0],
                    [4.5, 0.0],
                    [2.5, 4.0],
                    [1.5, 4.0],
                    [3.5, 0.0],
                ]],
            ]),
            Geometry::Polygon(vec![vec![[0.5, 0.5], [3.5, 0.5], [2.0, 3.5], [0.5, 0.5]]]),
            [true, false, false, false],
        ),
    ];
    let predicates = [
        Predicate::Intersects,
        Predicate::Contains,
        Predicate::Covers,
        Predicate::CoveredBy,
    ];
    for (name, left, right, expected) in cases {
        for variant in 0..8 {
            let left = transform(&left, variant);
            let right = query(&transform(&right, variant));
            for limit in [1, 100] {
                let fixture = Fixture::new(&[(0, left.clone())], limit);
                for (predicate, expected) in predicates.into_iter().zip(expected) {
                    assert_eq!(
                        fixture.hits(&right, predicate),
                        if expected { vec![0] } else { vec![] },
                        "{name}, variant {variant}, limit {limit}, {predicate:?}"
                    );
                }
            }
        }
    }
}

#[test]
fn test_plane_whole_values_survive_merge_deletion_and_remapping() {
    let a = Geometry::Polygon(vec![rectangle(-1.0, -1.0, 0.0, 1.0)]);
    let b = Geometry::Polygon(vec![rectangle(0.0, -1.0, 1.0, 1.0)]);
    let point = [(-73.0f64).next_up(), 40.0f64.next_down()];
    let typed = Geometry::GeometryCollection(vec![
        Geometry::Point(point),
        Geometry::LineString(vec![point; 3]),
        Geometry::Polygon(vec![vec![point; 4]]),
        Geometry::LineString(Vec::new()),
    ]);
    let values = vec![
        (0, a.clone()),
        (0, b.clone()),
        (1, Geometry::GeometryCollection(vec![a.clone(), b.clone()])),
        (2, typed.clone()),
        (
            3,
            Geometry::Polygon(vec![
                rectangle(-2.0, -2.0, 2.0, 2.0),
                rectangle(-2.0, -2.0, 2.0, 2.0),
            ]),
        ),
        (3, Geometry::Polygon(vec![rectangle(-1.0, -1.0, 1.0, 1.0)])),
    ];
    let line = query(&Geometry::LineString(vec![[-0.5, 0.0], [0.5, 0.0]]));
    for reverse in [false, true] {
        let mut values = values.clone();
        if reverse {
            values.reverse();
        }
        let first = Fixture::new(&values, 1);
        let second = Fixture::new(&[(0, a.clone())], 100);
        let merged = Fixture::merge(
            &[&first, &second],
            &[vec![Some(3), Some(2), Some(1), Some(0)], vec![Some(4)]],
        );
        assert_eq!(merged.hits(&line, Predicate::Intersects), vec![0, 2, 3, 4]);
        assert_eq!(merged.hits(&line, Predicate::Covers), vec![0, 2]);
        assert_eq!(merged.hits(&line, Predicate::Contains), vec![0, 2]);
        assert_eq!(
            merged.hits(&query(&Geometry::Point(point)), Predicate::Contains),
            vec![1]
        );

        let reader = EdgeReader::<Plane>::open(&merged.edges).unwrap();
        let restored = (0..reader.geometry_count())
            .find_map(|position| {
                let (_, set) = reader.read_geometry_set(position);
                (set.doc_id == 1).then_some(set)
            })
            .unwrap();
        let expected = to_geometry_set(&typed, 1);
        for (actual, expected) in restored.members.iter().zip(&expected.members) {
            assert_eq!(actual.kind, expected.kind);
            assert_eq!(
                actual
                    .vertices
                    .iter()
                    .map(|point| point.map(f64::to_bits))
                    .collect::<Vec<_>>(),
                expected
                    .vertices
                    .iter()
                    .map(|point| point.map(f64::to_bits))
                    .collect::<Vec<_>>()
            );
        }
        assert_eq!(restored.members.len(), expected.members.len());

        let again = Fixture::merge(&[&merged], &[vec![None, Some(2), Some(0), Some(1), None]]);
        assert_eq!(again.hits(&line, Predicate::Intersects), vec![0, 1]);
        assert_eq!(again.hits(&line, Predicate::Covers), vec![0]);
        assert_eq!(again.hits(&line, Predicate::Contains), vec![0]);
        assert_eq!(
            again.hits(&query(&Geometry::Point(point)), Predicate::Contains),
            vec![2]
        );
    }
}

#[test]
fn test_plane_effective_empty_values_do_not_satisfy_interior_shortcuts() {
    let outer = Geometry::Polygon(vec![subdivided_rectangle(1.0, 1.0, 80.0, 80.0, 128)]);
    let inner_bound = R2Rect::from_point([12.0, 12.0]).union(&R2Rect::from_point([14.0, 14.0]));
    let outer_query = query(&outer);
    assert!(has_interior_cell_containing(
        outer_query.index(),
        &inner_bound
    ));

    let doubled = Geometry::Polygon(vec![vec![
        [12.0, 12.0],
        [14.0, 12.0],
        [14.0, 14.0],
        [12.0, 14.0],
        [12.0, 12.0],
        [14.0, 12.0],
        [14.0, 14.0],
        [12.0, 14.0],
        [12.0, 12.0],
    ]]);
    let fixture = Fixture::new(&[(0, doubled.clone())], 1);
    for predicate in [
        Predicate::Intersects,
        Predicate::CoveredBy,
        Predicate::Within,
    ] {
        assert!(fixture.hits(&outer_query, predicate).is_empty());
    }

    let with_point =
        Geometry::GeometryCollection(vec![doubled.clone(), Geometry::Point([13.0, 13.0])]);
    let fixture = Fixture::new(&[(0, with_point)], 1);
    for predicate in [
        Predicate::Intersects,
        Predicate::CoveredBy,
        Predicate::Within,
    ] {
        assert_eq!(fixture.hits(&outer_query, predicate), vec![0]);
    }

    let outer_fixture = Fixture::new(&[(0, outer)], 1);
    let reader = CellIndexReader::open(&outer_fixture.cells).unwrap();
    assert!((0..reader.cell_count()).any(|position| {
        let cell = reader.cell_at(position);
        cell.shapes
            .iter()
            .any(|shape| shape.contains_center && shape.edge_indices.is_empty())
            && source_bound(&cell_bound_uv::<Plane>(cell.cell_id)).contains(&inner_bound)
    }));
    let doubled_query = query(&doubled);
    for predicate in [
        Predicate::Intersects,
        Predicate::Contains,
        Predicate::Covers,
    ] {
        assert!(outer_fixture.hits(&doubled_query, predicate).is_empty());
    }
}

#[test]
fn test_plane_degenerate_material_and_extreme_coordinates() {
    let shell = Geometry::Polygon(vec![vec![
        [0.0, 0.0],
        [4.0, 0.0],
        [4.0, 4.0],
        [2.0, 4.0],
        [2.0, 6.0],
        [2.0, 4.0],
        [0.0, 4.0],
        [0.0, 0.0],
    ]]);
    let fixture = Fixture::new(&[(0, shell)], 1);
    assert!(fixture
        .hits(&query(&Geometry::Point([2.0, 5.0])), Predicate::Intersects)
        .is_empty());
    assert_eq!(
        fixture.hits(
            &query(&Geometry::LineString(vec![[2.0, 4.0], [2.0, 6.0]])),
            Predicate::Intersects,
        ),
        vec![0]
    );

    let subnormal = f64::from_bits(1);
    let polygon = Geometry::Polygon(vec![rectangle(0.0, 0.0, subnormal, subnormal)]);
    let fixture = Fixture::new(&[(0, polygon)], 1);
    let diagonal = query(&Geometry::LineString(vec![
        [0.0, 0.0],
        [subnormal, subnormal],
    ]));
    for predicate in [
        Predicate::Intersects,
        Predicate::Contains,
        Predicate::Covers,
    ] {
        assert_eq!(fixture.hits(&diagonal, predicate), vec![0]);
    }
}

#[test]
fn test_plane_within_distinguishes_boundary_from_interior() {
    let area = query(&Geometry::Polygon(vec![rectangle(0.0, 0.0, 2.0, 2.0)]));
    let fixture = Fixture::new(
        &[
            (0, Geometry::LineString(vec![[0.5, 0.0], [1.5, 0.0]])),
            (1, Geometry::LineString(vec![[0.5, 1.0], [1.5, 1.0]])),
        ],
        1,
    );
    assert_eq!(fixture.hits(&area, Predicate::CoveredBy), vec![0, 1]);
    assert_eq!(fixture.hits(&area, Predicate::Within), vec![1]);
}

#[test]
fn test_plane_holed_values_survive_merge_deletion_and_remapping() {
    let annulus = |center: f64| {
        Geometry::Polygon(vec![
            rectangle(center - 4.0, -4.0, center + 4.0, 4.0),
            rectangle(center - 2.0, -2.0, center + 2.0, 2.0),
        ])
    };
    let first = Fixture::new(&[(0, annulus(0.0)), (1, annulus(20.0))], 2);
    let second = Fixture::new(&[(0, annulus(40.0)), (1, annulus(60.0))], 2);
    let merged = Fixture::merge(
        &[&first, &second],
        &[vec![None, Some(1)], vec![Some(0), None]],
    );
    let fresh = Fixture::new(&[(0, annulus(40.0)), (1, annulus(20.0))], 2);
    for geometry in [
        Geometry::Point([20.0, 0.0]),
        Geometry::Point([20.0, 3.0]),
        Geometry::Point([40.0, 0.0]),
        Geometry::Point([43.0, 0.0]),
        Geometry::LineString(vec![[16.0, -5.0], [16.0, 5.0]]),
    ] {
        let prepared = query(&geometry);
        for predicate in [
            Predicate::Intersects,
            Predicate::Contains,
            Predicate::Covers,
            Predicate::CoveredBy,
            Predicate::Within,
        ] {
            assert_eq!(
                merged.hits(&prepared, predicate),
                fresh.hits(&prepared, predicate),
                "{geometry:?} {predicate:?}"
            );
        }
    }
}

//! Latitude-longitude rectangle query.
//!
//! Finds the geometries that a rectangle of latitude and longitude covers, or that it meets.
//! Two sides of such a rectangle are meridians, which are geodesics. The other two are parallels,
//! which are not: the polygon through the four corners has edges that bow toward the nearer
//! pole, by degrees where the rectangle spans a continent, and that take the short way around
//! where it is wider than a hemisphere. The rectangle is therefore tested as what it is.
//!
//! A covering of the rectangle finds the candidates, as `area_query` has it. A covering cell
//! within the rectangle decides what it holds without decoding it, for the geometries the
//! rectangle covers too. A geometry found only in such cells lies within the rectangle: it is
//! connected, so to leave the rectangle it would pass through a covering cell that the
//! rectangle's boundary crosses.
//!
//! The rectangle is closed, and a point within rounding of its boundary is inside it: the
//! vertices a segment stores are unit vectors, whose latitude and longitude are not exactly
//! those they were made from.
//!
//! This module does not port any single S2 class. It composes S2LatLngRect and the region
//! coverer with the serialized segment index.

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::f64::consts::{FRAC_PI_2, PI};

use common::BitSet;

use super::area_query::{edges, search_meeting, Area, Covering};
use super::cell_index_reader::CellIndexReader;
use super::crossings::robust_cross;
use super::edge_cache::EdgeCache;
use super::latlng_rect::S2LatLngRect;
use super::math::{cross, dot, normalize};
use super::r1interval::R1Interval;
use super::region::Region;
use super::s1interval::S1Interval;
use super::shape_index_region::{contains_point, SegmentIndex};
use super::sphere::Sphere;

/// How far outside the rectangle a unit vector may lie and still be inside it. A stored vertex
/// is within a few units of rounding of the coordinates it was made from, and a geometry in an
/// index cell may lie outside it by the cell's padding.
const SLACK: f64 = 1e-15;

/// How a geometry must relate to the rectangle to match.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RectRelation {
    /// The rectangle covers every part of every value of the document's field.
    CoveredBy,
    /// The rectangle and a value of the document's field have a point in common.
    Intersects,
}

/// The longitudes of a rectangle.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Span {
    /// Every longitude.
    Full,
    /// Half of them at most: the rectangle lies between its west and east meridians.
    Narrow,
    /// More than half: the rectangle is all but what lies between its east and west meridians.
    Wide,
}

/// A rectangle prepared for testing unit vectors against it, with no trigonometry for a point.
struct Bounds {
    rect: S2LatLngRect,
    /// The sines of the south and north latitudes: the heights the rectangle lies between.
    south: f64,
    north: f64,
    span: Span,
    /// The cosine and sine of the west, east and middle longitudes.
    west: [f64; 2],
    east: [f64; 2],
    middle: [f64; 2],
    /// A point outside the rectangle in each part of the sphere that it leaves out.
    outside: Vec<[f64; 3]>,
    /// A point of the rectangle.
    inside: [f64; 3],
}

impl Bounds {
    /// The rectangle from its west and south, and its east and north, in degrees. It crosses the
    /// antimeridian when its west is greater than its east.
    fn new([west, south]: [f64; 2], [east, north]: [f64; 2]) -> Self {
        let lng = if west == -180.0 && east == 180.0 {
            S1Interval::full()
        } else {
            // From 180 to -180 is the antimeridian, though S1Interval has it as empty.
            let east = if west == 180.0 && east == -180.0 {
                180.0
            } else {
                east
            };
            S1Interval::new(
                west.to_radians().clamp(-PI, PI),
                east.to_radians().clamp(-PI, PI),
            )
        };
        let west = lng.lo();
        let south = south.to_radians().clamp(-FRAC_PI_2, FRAC_PI_2);
        let north = north.to_radians().clamp(-FRAC_PI_2, FRAC_PI_2);
        let rect = S2LatLngRect::new(R1Interval::new(south, north), lng);
        let width = rect.lng().length();
        let span = if rect.lng().is_full() {
            Span::Full
        } else if width <= PI {
            Span::Narrow
        } else {
            Span::Wide
        };
        let direction = |lng: f64| [lng.cos(), lng.sin()];
        let east = lng.hi();
        let middle = west + width / 2.0;
        let point = |lng: f64, lat: f64| [lat.cos() * lng.cos(), lat.cos() * lng.sin(), lat.sin()];
        let mut outside = Vec::new();
        if span == Span::Full {
            if north.sin() < 1.0 {
                outside.push([0.0, 0.0, 1.0]);
            }
            if south.sin() > -1.0 {
                outside.push([0.0, 0.0, -1.0]);
            }
        } else {
            outside.push(point(rect.lng().complement_center(), 0.0));
        }
        Bounds {
            rect,
            // As a vertex's height is computed, so that a vertex at a bound is at its height.
            south: south.sin(),
            north: north.sin(),
            span,
            west: direction(west),
            east: direction(east),
            middle: direction(middle),
            outside,
            inside: point(middle, (south + north) / 2.0),
        }
    }

    /// Whether the rectangle contains the point. A pole has every longitude.
    fn contains(&self, p: &[f64; 3]) -> bool {
        if p[2] < self.south - SLACK || p[2] > self.north + SLACK {
            return false;
        }
        // The sines of the angles from the west meridian to the point and from the point to the
        // east meridian, scaled by the cosine of the point's latitude.
        let east_of_west = self.west[0] * p[1] - self.west[1] * p[0] >= -SLACK;
        let west_of_east = self.east[1] * p[0] - self.east[0] * p[1] >= -SLACK;
        match self.span {
            Span::Full => true,
            // A point opposite a rectangle with no width is east of its west and west of its
            // east too, but not on the rectangle's side of the sphere.
            Span::Narrow => {
                east_of_west
                    && west_of_east
                    && self.middle[0] * p[0] + self.middle[1] * p[1] >= -SLACK
            }
            Span::Wide => east_of_west || west_of_east,
        }
    }

    /// Whether the rectangle covers the geodesic edge between two points that it contains. The
    /// edge may rise above the rectangle's north or dip below its south between them, and in a
    /// rectangle wider than a hemisphere it may cut across the longitudes left out.
    fn covers_edge(&self, a: &[f64; 3], b: &[f64; 3]) -> bool {
        let normal = normalize(&robust_cross(a, b));
        let level = normal[0] * normal[0] + normal[1] * normal[1];
        // An edge along the equator neither rises nor dips.
        if level > 0.0 {
            // Where the edge's great circle is highest. It is lowest at the opposite point.
            let top = [-normal[0] * normal[2], -normal[1] * normal[2], level];
            let after_a = dot(&cross(a, &top), &normal);
            let before_b = dot(&cross(&top, b), &normal);
            let height = level.sqrt();
            if after_a > 0.0 && before_b > 0.0 && height > self.north + SLACK {
                return false;
            }
            if after_a < 0.0 && before_b < 0.0 && -height < self.south - SLACK {
                return false;
            }
        }
        // An edge along a meridian, or through a pole, has only the longitudes of its ends.
        if self.span == Span::Wide && normal[2] != 0.0 {
            let lng = |p: &[f64; 3]| p[1].atan2(p[0]);
            return self
                .rect
                .lng()
                .contains(&S1Interval::from_point_pair(lng(a), lng(b)));
        }
        true
    }
}

impl Area for Bounds {
    fn contains(&self, point: &[f64; 3]) -> bool {
        Bounds::contains(self, point)
    }

    /// An edge between two points outside the rectangle meets it where it crosses its boundary.
    fn meets_edge(&self, a: &[f64; 3], b: &[f64; 3]) -> bool {
        self.rect.boundary_intersects(a, b)
    }

    fn inside(&self) -> &[f64; 3] {
        &self.inside
    }
}

/// What is known of a member that is not the only point of its document.
struct Member {
    doc_id: u32,
    head: u32,
    sole: bool,
    /// Whether the rectangle covers the member. A member found only in covering cells within
    /// the rectangle is covered untested.
    covered: bool,
    /// Whether the member was found in a covering cell that the rectangle's boundary crosses,
    /// and so tested.
    tested: bool,
}

/// Prepared rectangle query, built once and applied per segment.
pub struct RectQuery {
    bounds: Bounds,
    covering: Covering,
    relation: RectRelation,
}

impl RectQuery {
    /// Build the query from the rectangle's west and south, and its east and north, in degrees
    /// within [-180, 180] and [-90, 90]. The south must not be above the north. A west greater
    /// than the east is a rectangle that crosses the antimeridian, and a west of -180 with an
    /// east of 180 is every longitude.
    pub fn new(west_south: [f64; 2], east_north: [f64; 2], relation: RectRelation) -> Self {
        let bounds = Bounds::new(west_south, east_north);
        let covering = Covering::new(&bounds.rect, |cell| bounds.rect.contains_cell(cell));
        RectQuery {
            bounds,
            covering,
            relation,
        }
    }

    /// Add the documents of one segment that match to `docs`.
    pub fn search_segment(
        &self,
        cell_reader: &CellIndexReader,
        edge_cache: &mut EdgeCache<'_, Sphere>,
        docs: &mut BitSet,
    ) {
        match self.relation {
            RectRelation::CoveredBy => self.search_covered(cell_reader, edge_cache, docs),
            RectRelation::Intersects => {
                search_meeting(&self.bounds, &self.covering, cell_reader, edge_cache, docs)
            }
        }
    }

    /// A document is covered when every member of its set is. A member the covering does not
    /// find lies outside the rectangle.
    fn search_covered(
        &self,
        cell_reader: &CellIndexReader,
        edge_cache: &mut EdgeCache<'_, Sphere>,
        docs: &mut BitSet,
    ) {
        let mut members: HashMap<u32, Member> = HashMap::new();
        self.covering.visit(cell_reader, |position, within| {
            let summary = edge_cache.summary((0, position));
            if let (true, Some(point)) = (summary.sole, &summary.point) {
                if within || self.bounds.contains(point) {
                    docs.insert(summary.doc_id);
                }
                return;
            }
            let member = match members.entry(position) {
                Entry::Occupied(entry) => entry.into_mut(),
                Entry::Vacant(entry) => entry.insert(Member {
                    doc_id: summary.doc_id,
                    head: summary.head,
                    sole: summary.sole,
                    covered: true,
                    tested: false,
                }),
            };
            if !within && !member.tested {
                member.tested = true;
                member.covered = match &summary.point {
                    Some(point) => self.bounds.contains(point),
                    None => self.member_covered(position, cell_reader, edge_cache),
                };
            }
        });

        // The members of each set with other members: those left to find, unless one is not
        // covered.
        let mut sets: HashMap<u32, (u32, Option<u32>)> = HashMap::new();
        for (position, member) in members {
            if member.sole {
                if member.covered {
                    docs.insert(member.doc_id);
                }
                continue;
            }
            let (_, left) = sets.entry(member.head).or_insert_with(|| {
                let count = edge_cache.get((0, position)).member_count();
                (member.doc_id, Some(count))
            });
            *left = match *left {
                Some(left) if member.covered => Some(left - 1),
                _ => None,
            };
        }
        for (doc_id, left) in sets.into_values() {
            if left == Some(0) {
                docs.insert(doc_id);
            }
        }
    }

    /// Whether the rectangle covers a line or polygon: its vertices and its edges between them,
    /// and for a polygon its interior, which then holds no point outside the rectangle.
    fn member_covered(
        &self,
        position: u32,
        cell_reader: &CellIndexReader,
        edge_cache: &mut EdgeCache<'_, Sphere>,
    ) -> bool {
        let entry = edge_cache.get((0, position));
        let member = entry.edge_set();
        if member.vertices.is_empty()
            || !member.vertices.iter().all(|v| self.bounds.contains(v))
            || !edges(member).all(|(a, b)| self.bounds.covers_edge(a, b))
        {
            return false;
        }
        if !member.closed {
            return true;
        }
        let mut segment = SegmentIndex {
            cell_reader,
            edge_cache,
        };
        !self
            .bounds
            .outside
            .iter()
            .any(|point| contains_point(&mut segment, (0, position), point))
    }
}

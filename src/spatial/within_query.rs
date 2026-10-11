//! Query for the geometries within a distance of a point.
//!
//! The closest edge query finds them too, nearest first, and keeps the distance of each. This
//! query only decides which they are, as a search for the area around the point: see
//! `area_query`. Interiors count: a polygon that contains the point is at distance zero.

use common::BitSet;

use super::area_query::{search_meeting, Area, Covering};
use super::cell_index_reader::CellIndexReader;
use super::edge_cache::EdgeCache;
use super::region::Region;
use super::s1chord_angle::S1ChordAngle;
use super::s2cap::S2Cap;
use super::s2edge_distances::is_distance_less;
use super::sphere::Sphere;

/// How much nearer than the distance, as a squared chord length, everything in a covering cell
/// must be for the cell to be taken whole. A geometry in an index cell may lie outside it by
/// the cell's padding.
const WITHIN_MARGIN: f64 = 1e-13;

/// What lies nearer to a point than a distance.
struct Around {
    center: [f64; 3],
    distance: S1ChordAngle,
}

impl Area for Around {
    fn contains(&self, point: &[f64; 3]) -> bool {
        is_distance_less(&self.center, point, point, self.distance)
    }

    fn meets_edge(&self, a: &[f64; 3], b: &[f64; 3]) -> bool {
        is_distance_less(&self.center, a, b, self.distance)
    }

    fn inside(&self) -> &[f64; 3] {
        &self.center
    }
}

/// Prepared query for the geometries nearer to a point than a distance, built once and applied
/// per segment.
pub struct WithinQuery {
    around: Around,
    /// None when nothing is nearer than the distance, which is then zero or less.
    covering: Option<Covering>,
}

impl WithinQuery {
    /// Build the query from a point, as a unit vector, and a distance.
    pub fn new(center: [f64; 3], distance: S1ChordAngle) -> Self {
        let covering = (distance > S1ChordAngle::zero()).then(|| {
            // Everything is nearer than an infinite distance, which a cap cannot have.
            if distance.is_infinity() {
                return Covering::new(&S2Cap::full(), |_| true);
            }
            let length2 = (distance.length2() - WITHIN_MARGIN).max(0.0);
            let nearer = S2Cap::new(center, S1ChordAngle::from_length2(length2));
            Covering::new(&S2Cap::new(center, distance), |cell| {
                nearer.contains_cell(cell)
            })
        });
        WithinQuery {
            around: Around { center, distance },
            covering,
        }
    }

    /// Add the documents of one segment that match to `docs`.
    pub fn search_segment(
        &self,
        cell_reader: &CellIndexReader,
        edge_cache: &mut EdgeCache<'_, Sphere>,
        docs: &mut BitSet,
    ) {
        if let Some(covering) = &self.covering {
            search_meeting(&self.around, covering, cell_reader, edge_cache, docs);
        }
    }
}

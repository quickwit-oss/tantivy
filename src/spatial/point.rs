
/// A point in the geographical coordinate system.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct GeoPoint {
    /// Longitude
    pub lon: f64,
    /// Latitude
    pub lat: f64,
}

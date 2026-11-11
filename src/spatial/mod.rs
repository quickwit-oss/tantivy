//! Spatial module.
//!
//! Geospatial indexing using S2 cell partitioning. Polygon edges are stored in a streaming
//! binary format backed by an S2 cell index, enabling polygon containment queries at search
//! engine scale.
//!
//! References:
//!
//! https://s2geometry.io/
//! https://s2geometry.io/about/overview
//! https://s2geometry.io/devguide/s2cell_hierarchy.html
//! https://proj.org/en/stable/operations/projections/s2.html
//! https://s2geometry.io/resources/earthcube.html

mod area_query;
pub mod cell_index;
pub mod cell_index_reader;
pub mod cell_union;
pub mod closest_edge_query;
pub mod containment;
pub mod contains_query;
pub mod crossings;
pub mod edge_cache;
pub mod edge_reader;
pub mod edge_writer;
pub mod exact;
pub mod executor;
pub mod geometry;
pub mod geometry_set;
pub mod interleave;
pub mod intersects_query;
pub mod latlng_rect;
pub mod math;
pub mod plane;
pub mod plugin;
mod polygon_union;
pub mod r1interval;
pub mod r2rect;
pub mod reader;
pub mod rect_query;
pub mod region;
pub mod region_coverer;
pub mod s1chord_angle;
pub mod s1interval;
pub mod s2cap;
pub mod s2cell;
pub mod s2cell_id;
pub mod s2coords;
pub mod s2edge_clipping;
pub mod s2edge_distances;
pub mod s2metrics;
pub mod s2padded_cell;
pub mod serializer;
pub mod shape_index_region;
pub mod spatial_index_manager;
pub mod sphere;
pub mod surface;
pub mod within_query;
pub mod writer;
pub mod xor;

pub use geometry::Geometry;
pub use geometry_set::RingOrientation;

/// Version of the spatial cell and edge index formats. It follows each file's footer, with a
/// magic number that names the file, so that an index written in another format fails to open
/// with an error that says so, rather than as corrupt.
pub(crate) const FORMAT_VERSION: u32 = 1;
/// Magic number of a cell index.
pub(crate) const CELL_INDEX_MAGIC: u32 = u32::from_le_bytes(*b"S2CI");
/// Magic number of an edge index.
pub(crate) const EDGE_INDEX_MAGIC: u32 = u32::from_le_bytes(*b"S2EI");
/// Bytes of the format version and magic number at the end of a file.
pub(crate) const FORMAT_MARKER_LEN: usize = 2 * std::mem::size_of::<u32>();

/// Write the format version and magic number that end a file.
pub(crate) fn write_format_marker<W: std::io::Write + ?Sized>(
    write: &mut W,
    magic: u32,
) -> std::io::Result<()> {
    write.write_all(&FORMAT_VERSION.to_le_bytes())?;
    write.write_all(&magic.to_le_bytes())
}

/// The file without its format marker, or an error naming what is wrong with the marker.
pub(crate) fn check_format_marker<'a>(
    data: &'a [u8],
    magic: u32,
    file: &str,
) -> crate::Result<&'a [u8]> {
    let wrong = |reason: String| {
        crate::error::DataCorruption::comment_only(format!(
            "The {}-byte spatial {file} {reason}; re-index the spatial field",
            data.len()
        ))
    };
    let Some((body, marker)) = data.split_last_chunk::<FORMAT_MARKER_LEN>() else {
        return Err(wrong("is too short to hold a format marker".to_string()).into());
    };
    let version = u32::from_le_bytes(marker[..4].try_into().unwrap());
    let found_magic = u32::from_le_bytes(marker[4..].try_into().unwrap());
    if found_magic != magic {
        return Err(wrong(
            "has no format marker: an older version of tantivy wrote it, or it is corrupt"
                .to_string(),
        )
        .into());
    }
    if version != FORMAT_VERSION {
        return Err(wrong(format!(
            "has format version {version}, and this version of tantivy reads version \
             {FORMAT_VERSION}"
        ))
        .into());
    }
    Ok(body)
}

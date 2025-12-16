//! Block kd-tree spatial indexing for triangulated polygons.
//!
//! Implements an immutable bulk-loaded spatial index using recursive median partitioning on
//! bounding box dimensions. Each leaf stores up to 512 triangles with delta-compressed coordinates
//! and doc IDs. The tree provides three query types (intersects, within, contains) that use exact
//! integer arithmetic for geometric predicates and accumulate results in bit sets for efficient
//! deduplication across leaves.
//!
//! The serialized format stores compressed leaf pages followed by the tree structure (leaf and
//! branch nodes), enabling zero-copy access through memory-mapped segments without upfront
//! decompression.
use std::io;
use std::io::Write;

use common::{BitSet, CountingWriter};

use crate::directory::WritePtr;
use crate::spatial::delta::{compress, decompress, Compressible};
use crate::spatial::triangle::Triangle;

#[derive(Clone, Copy)]
struct SpreadSurvey {
    min: i32,
    max: i32,
}

impl SpreadSurvey {
    fn survey(&mut self, value: i32) {
        self.min = self.min.min(value);
        self.max = self.max.max(value);
    }
    fn spread(&self) -> i32 {
        self.max - self.min
    }
}

impl Default for SpreadSurvey {
    fn default() -> Self {
        SpreadSurvey {
            min: i32::MAX,
            max: i32::MIN,
        }
    }
}

#[derive(Clone, Copy)]
struct BoundingBoxSurvey {
    bbox: [i32; 4],
}

impl BoundingBoxSurvey {
    fn survey(&mut self, triangle: &Triangle) {
        self.bbox[0] = triangle.words[0].min(self.bbox[0]);
        self.bbox[1] = triangle.words[1].min(self.bbox[1]);
        self.bbox[2] = triangle.words[2].max(self.bbox[2]);
        self.bbox[3] = triangle.words[3].max(self.bbox[3]);
    }
    fn bbox(&self) -> [i32; 4] {
        self.bbox
    }
}

impl Default for BoundingBoxSurvey {
    fn default() -> Self {
        BoundingBoxSurvey {
            bbox: [i32::MAX, i32::MAX, i32::MIN, i32::MIN],
        }
    }
}

enum BuildNode {
    Branch {
        bbox: [i32; 4],
        left: Box<BuildNode>,
        right: Box<BuildNode>,
    },
    Leaf {
        bbox: [i32; 4],
        pos: u64,
        len: u16,
    },
}

struct CompressibleTriangleI32<'a> {
    triangles: &'a [Triangle],
    dimension: usize,
}

impl<'a> CompressibleTriangleI32<'a> {
    fn new(triangles: &'a [Triangle], dimension: usize) -> Self {
        CompressibleTriangleI32 {
            triangles,
            dimension,
        }
    }
}

impl<'a> Compressible for CompressibleTriangleI32<'a> {
    type Value = i32;
    fn len(&self) -> usize {
        self.triangles.len()
    }
    fn get(&self, i: usize) -> i32 {
        self.triangles[i].words[self.dimension]
    }
}

struct CompressibleTriangleDocID<'a> {
    triangles: &'a [Triangle],
}

impl<'a> CompressibleTriangleDocID<'a> {
    fn new(triangles: &'a [Triangle]) -> Self {
        CompressibleTriangleDocID { triangles }
    }
}

impl<'a> Compressible for CompressibleTriangleDocID<'a> {
    type Value = u32;
    fn len(&self) -> usize {
        self.triangles.len()
    }
    fn get(&self, i: usize) -> u32 {
        self.triangles[i].doc_id
    }
}

// Leaf pages are first the count of triangles, followed by delta encoded doc_ids, followed by
// the delta encoded words in order. We will then have the length of the page. We build a tree
// after the pages with leaf nodes and branch nodes. Leaf nodes will contain the bounding box
// of the leaf followed position and length of the page. The leaf node is a level of direction
// to store the position and length of the page in a format that is easy to read directly from
// the mapping.

// We do not compress the tree nodes. We read them directly from the mapping.

//
fn write_leaf_pages(
    triangles: &mut [Triangle],
    write: &mut CountingWriter<WritePtr>,
) -> io::Result<BuildNode> {
    // If less than 512 triangles we are at a leaf, otherwise we still in the inner nodes.
    if triangles.len() <= 512 {
        let pos = write.written_bytes();
        let mut spreads = [SpreadSurvey::default(); 4];
        let mut bounding_box = BoundingBoxSurvey::default();
        for triangle in triangles.iter() {
            for i in 0..4 {
                spreads[i].survey(triangle.words[i]);
            }
            bounding_box.survey(triangle);
        }
        let mut max_spread = spreads[0].spread();
        let mut dimension = 0;
        for i in 1..4 {
            let current_spread = spreads[i].spread();
            if current_spread > max_spread {
                dimension = i;
                max_spread = current_spread;
            }
        }
        write.write_all(&(triangles.len() as u16).to_le_bytes())?;
        triangles.sort_by_key(|t| t.words[dimension]);
        compress(&CompressibleTriangleDocID::new(triangles), write)?;
        let compressible = [
            CompressibleTriangleI32::new(triangles, 0),
            CompressibleTriangleI32::new(triangles, 1),
            CompressibleTriangleI32::new(triangles, 2),
            CompressibleTriangleI32::new(triangles, 3),
            CompressibleTriangleI32::new(triangles, 4),
            CompressibleTriangleI32::new(triangles, 5),
            CompressibleTriangleI32::new(triangles, 6),
        ];
        for i in 0..7 {
            compress(&compressible[i], write)?;
        }
        let len = write.written_bytes() - pos;
        Ok(BuildNode::Leaf {
            bbox: bounding_box.bbox(),
            pos,
            len: len as u16,
        })
    } else {
        let mut spreads = [SpreadSurvey::default(); 4];
        let mut bounding_box = BoundingBoxSurvey::default();
        for triangle in triangles.iter() {
            for i in 0..4 {
                spreads[i].survey(triangle.words[i]);
            }
            bounding_box.survey(triangle);
        }
        let mut max_spread = spreads[0].spread();
        let mut dimension = 0;
        for i in 0..4 {
            let current_spread = spreads[i].spread();
            if current_spread > max_spread {
                dimension = i;
                max_spread = current_spread;
            }
        }
        // Partition the triangles.
        let mid = triangles.len() / 2;
        triangles.select_nth_unstable_by_key(mid, |t| t.words[dimension]);
        let partition = triangles[mid].words[dimension];
        let mut split_point = mid + 1;
        while split_point < triangles.len() && triangles[split_point].words[dimension] == partition
        {
            split_point += 1;
        }
        // If we reached the end of triangles then all of the triangles share the partition value
        // for the dimension. We handle this degeneracy by splitting at the midpoint so that we
        // won't have a leaf with zero triangles.
        if split_point == triangles.len() {
            split_point = mid; // Force split at midpoint index
        } else {
            // Our partition does not sort the triangles, it only partitions. We have scan our right
            // partition to find all the midpoint values and move them to the left partition.
            let mut reverse = triangles.len() - 1;
            loop {
                // Scan backwards looking for the partition value.
                while triangles[reverse].words[dimension] != partition {
                    reverse -= 1;
                }
                // If we have reached the split point then we are done.
                if reverse <= split_point {
                    break;
                }
                // Swap the midpoint value with our current split point.
                triangles.swap(split_point, reverse);
                // Move the split point up one.
                split_point += 1;
                // We know that what was at the split point was not the midpoint value.
                reverse -= 1;
            }
        }
        // Split into left and write partitions and create child nodes.
        let (left, right) = triangles.split_at_mut(split_point);
        let left_node = write_leaf_pages(left, write)?;
        let right_node = write_leaf_pages(right, write)?;
        // Return an inner node.
        Ok(BuildNode::Branch {
            bbox: bounding_box.bbox(),
            left: Box::new(left_node),
            right: Box::new(right_node),
        })
    }
}

fn write_leaf_nodes(node: &BuildNode, write: &mut CountingWriter<WritePtr>) -> io::Result<()> {
    match node {
        BuildNode::Branch {
            bbox: _,
            left,
            right,
        } => {
            write_leaf_nodes(right, write)?;
            write_leaf_nodes(left, write)?;
        }
        BuildNode::Leaf { bbox, pos, len } => {
            for &dimension in bbox.iter() {
                write.write_all(&dimension.to_le_bytes())?;
            }
            write.write_all(&pos.to_le_bytes())?;
            write.write_all(&len.to_le_bytes())?;
            write.write_all(&[0u8; 6])?;
        }
    }
    Ok(())
}

fn write_branch_nodes(
    node: &BuildNode,
    branch_offset: &mut i32,
    leaf_offset: &mut i32,
    write: &mut CountingWriter<WritePtr>,
) -> io::Result<i32> {
    match node {
        BuildNode::Leaf { .. } => {
            let pos = *leaf_offset;
            *leaf_offset -= 1;
            Ok(pos * size_of::<LeafNode>() as i32)
        }
        BuildNode::Branch { bbox, left, right } => {
            let left = write_branch_nodes(left, branch_offset, leaf_offset, write)?;
            let right = write_branch_nodes(right, branch_offset, leaf_offset, write)?;
            for &val in bbox {
                write.write_all(&val.to_le_bytes())?;
            }
            write.write_all(&left.to_le_bytes())?;
            write.write_all(&right.to_le_bytes())?;
            write.write_all(&[0u8; 8])?;
            let pos = *branch_offset;
            *branch_offset += 1;
            Ok(pos * size_of::<BranchNode>() as i32)
        }
    }
}

const VERSION: u16 = 1u16;

/// Builds and serializes a block kd-tree for spatial indexing of triangles.
///
/// Takes a collection of triangles and constructs a complete block kd-tree, writing both the
/// compressed leaf pages and tree structure to the output. The tree uses recursive median
/// partitioning on the dimension with maximum spread, storing up to 512 triangles per leaf.
///
/// The output format consists of:
/// - Version header (u16)
/// - Compressed leaf pages (delta-encoded doc_ids and triangle coordinates)
/// - 32-byte aligned tree structure (leaf nodes, then branch nodes)
/// - Footer with triangle count, root offset, and branch position
///
/// The `triangles` slice will be reordered during tree construction as partitioning sorts by the
/// selected dimension at each level.
pub fn write_block_kd_tree(
    triangles: &mut [Triangle],
    write: &mut CountingWriter<WritePtr>,
) -> io::Result<()> {
    write.write_all(&VERSION.to_le_bytes())?;

    let tree = write_leaf_pages(triangles, write)?;
    let current = write.written_bytes();
    let aligned = current.next_multiple_of(32);
    let padding = aligned - current;
    write.write_all(&vec![0u8; padding as usize])?;

    write_leaf_nodes(&tree, write)?;
    let branch_position = write.written_bytes();
    let mut branch_offset: i32 = 0;
    let mut leaf_offset: i32 = -1;
    let root = write_branch_nodes(&tree, &mut branch_offset, &mut leaf_offset, write)?;
    write.write_all(&[0u8; 12])?;
    write.write_all(&triangles.len().to_le_bytes())?;
    write.write_all(&root.to_le_bytes())?;
    write.write_all(&branch_position.to_le_bytes())?;
    Ok(())
}

fn decompress_leaf(mut data: &[u8]) -> io::Result<Vec<Triangle>> {
    use common::BinarySerializable;
    let triangle_count: usize = u16::deserialize(&mut data)? as usize;
    let mut offset: usize = 0;
    let mut triangles: Vec<Triangle> = Vec::with_capacity(triangle_count);
    offset += decompress::<u32, _>(&data[offset..], triangle_count, |_, doc_id| {
        triangles.push(Triangle::skeleton(doc_id))
    })?;
    for i in 0..7 {
        offset += decompress::<i32, _>(&data[offset..], triangle_count, |j, word| {
            triangles[j].words[i] = word
        })?;
    }
    Ok(triangles)
}

#[repr(C)]
struct BranchNode {
    bbox: [i32; 4],
    left: i32,
    right: i32,
    pad: [u8; 8],
}

#[repr(C)]
struct LeafNode {
    bbox: [i32; 4],
    pos: u64,
    len: u16,
    pad: [u8; 6],
}

/// A read-only view into a serialized block kd-tree segment.
///
/// Provides access to the tree structure and compressed leaf data through memory-mapped or
/// buffered byte slices. The segment contains compressed leaf pages followed by the tree structure
/// (leaf nodes and branch nodes), with a footer containing metadata for locating the root and
/// interpreting offsets.
pub struct Segment<'a> {
    data: &'a [u8],
    branch_position: u64,
    /// Offset to the root of the tree, used as the starting point for traversal.
    pub root_offset: i32,
}

impl<'a> Segment<'a> {
    /// Creates a new segment from serialized block kd-tree data.
    ///
    /// Reads the footer metadata from the last 12 bytes to locate the tree structure and root
    /// node.
    pub fn new(data: &'a [u8]) -> Self {
        Segment {
            data,
            branch_position: u64::from_le_bytes(data[data.len() - 8..].try_into().unwrap()),
            root_offset: i32::from_le_bytes(
                data[data.len() - 12..data.len() - 8].try_into().unwrap(),
            ),
        }
    }
    #[inline(always)]
    fn bounding_box(&self, offset: i32) -> [i32; 4] {
        let byte_offset = (self.branch_position as i64 + offset as i64) as usize;
        let bytes = &self.data[byte_offset..byte_offset + 16];
        [
            i32::from_le_bytes(bytes[0..4].try_into().unwrap()),
            i32::from_le_bytes(bytes[4..8].try_into().unwrap()),
            i32::from_le_bytes(bytes[8..12].try_into().unwrap()),
            i32::from_le_bytes(bytes[12..16].try_into().unwrap()),
        ]
    }
    #[inline(always)]
    fn branch_node(&self, offset: i32) -> BranchNode {
        let byte_offset = (self.branch_position as i64 + offset as i64) as usize;
        let bytes = &self.data[byte_offset..byte_offset + 32];
        BranchNode {
            bbox: [
                i32::from_le_bytes(bytes[0..4].try_into().unwrap()),
                i32::from_le_bytes(bytes[4..8].try_into().unwrap()),
                i32::from_le_bytes(bytes[8..12].try_into().unwrap()),
                i32::from_le_bytes(bytes[12..16].try_into().unwrap()),
            ],
            left: i32::from_le_bytes(bytes[16..20].try_into().unwrap()),
            right: i32::from_le_bytes(bytes[20..24].try_into().unwrap()),
            pad: [0u8; 8],
        }
    }
    #[inline(always)]
    fn leaf_node(&self, offset: i32) -> LeafNode {
        let byte_offset = (self.branch_position as i64 + offset as i64) as usize;
        let bytes = &self.data[byte_offset..byte_offset + 32];
        LeafNode {
            bbox: [
                i32::from_le_bytes(bytes[0..4].try_into().unwrap()),
                i32::from_le_bytes(bytes[4..8].try_into().unwrap()),
                i32::from_le_bytes(bytes[8..12].try_into().unwrap()),
                i32::from_le_bytes(bytes[12..16].try_into().unwrap()),
            ],
            pos: u64::from_le_bytes(bytes[16..24].try_into().unwrap()),
            len: u16::from_le_bytes(bytes[24..26].try_into().unwrap()),
            pad: [0u8; 6],
        }
    }
    fn leaf_page(&self, leaf_node: &LeafNode) -> &[u8] {
        &self.data[(leaf_node.pos as usize)..(leaf_node.pos as usize + leaf_node.len as usize)]
    }
}

fn collect_all_docs(segment: &Segment, offset: i32, result: &mut BitSet) -> io::Result<()> {
    if offset < 0 {
        let leaf_node = segment.leaf_node(offset);
        let data = segment.leaf_page(&leaf_node);
        let count = u16::from_le_bytes([data[0], data[1]]) as usize;
        decompress::<u32, _>(&data[2..], count, |_, doc_id| result.insert(doc_id))?;
    } else {
        let branch_node = segment.branch_node(offset);
        collect_all_docs(segment, branch_node.left, result)?;
        collect_all_docs(segment, branch_node.right, result)?;
    }
    Ok(())
}

fn bbox_within(bbox: &[i32; 4], query: &[i32; 4]) -> bool {
    bbox[0] >= query[0] &&  // min_y >= query_min_y
        bbox[1] >= query[1] &&  // min_x >= query_min_x
        bbox[2] <= query[2] &&  // max_y <= query_max_y
        bbox[3] <= query[3] // max_x <= query_max_x
}

fn bbox_intersects(bbox: &[i32; 4], query: &[i32; 4]) -> bool {
    !(bbox[2] < query[0] || bbox[0] > query[2] || bbox[3] < query[1] || bbox[1] > query[3])
}

/// Finds documents with triangles that intersect the query bounding box.
///
/// Traverses the tree starting at `offset` (typically `segment.root_offset`), pruning subtrees
/// whose bounding boxes don't intersect the query. When a node's bbox is entirely within the
/// query, all its documents are bulk-collected. Otherwise, individual triangles are tested using
/// exact geometric predicates.
///
/// The query is `[min_y, min_x, max_y, max_x]` in integer coordinates. Documents are inserted into
/// the `result` BitSet, which automatically deduplicates when the same document appears in
/// multiple leaves.
pub fn search_intersects(
    segment: &Segment,
    offset: i32,
    query: &[i32; 4],
    result: &mut BitSet,
) -> io::Result<()> {
    let bbox = segment.bounding_box(offset);
    // bbox doesn't intersect query → skip entire subtree
    if !bbox_intersects(&bbox, query) {
    }
    // bbox entirely within query → all triangles intersect
    else if bbox_within(&bbox, query) {
        collect_all_docs(segment, offset, result)?;
    } else if offset < 0 {
        // bbox crosses query → test each triangle
        let leaf_node = segment.leaf_node(offset);
        let triangles = decompress_leaf(segment.leaf_page(&leaf_node))?;
        for triangle in &triangles {
            if triangle_intersects(triangle, query) {
                result.insert(triangle.doc_id); // BitSet deduplicates
            }
        }
    } else {
        let branch_node = segment.branch_node(offset);
        // bbox crosses query → must check children
        search_intersects(segment, branch_node.left, query, result)?;
        search_intersects(segment, branch_node.right, query, result)?;
    }
    Ok(())
}

#[expect(clippy::too_many_arguments)]
fn line_intersects_line(
    x1: i32,
    y1: i32,
    x2: i32,
    y2: i32,
    x3: i32,
    y3: i32,
    x4: i32,
    y4: i32,
) -> bool {
    // Cast to i128 to prevent overflow in coordinate arithmetic
    let x1 = x1 as i128;
    let y1 = y1 as i128;
    let x2 = x2 as i128;
    let y2 = y2 as i128;
    let x3 = x3 as i128;
    let y3 = y3 as i128;
    let x4 = x4 as i128;
    let y4 = y4 as i128;

    // Proper segment-segment intersection test
    let d = (x1 - x2) * (y3 - y4) - (y1 - y2) * (x3 - x4);
    if d == 0 {
        // parallel
        return false;
    }

    let t = (x1 - x3) * (y3 - y4) - (y1 - y3) * (x3 - x4);
    let u = -((x1 - x2) * (y1 - y3) - (y1 - y2) * (x1 - x3));

    if d > 0 {
        t >= 0 && t <= d && u >= 0 && u <= d
    } else {
        t <= 0 && t >= d && u <= 0 && u >= d
    }
}

fn edge_intersects_bbox(x1: i32, y1: i32, x2: i32, y2: i32, bbox: &[i32; 4]) -> bool {
    // Test against all 4 rectangle edges, bottom, right, top, left.
    line_intersects_line(x1, y1, x2, y2, bbox[1], bbox[0], bbox[3], bbox[0])
        || line_intersects_line(x1, y1, x2, y2, bbox[3], bbox[0], bbox[3], bbox[2])
        || line_intersects_line(x1, y1, x2, y2, bbox[3], bbox[2], bbox[1], bbox[2])
        || line_intersects_line(x1, y1, x2, y2, bbox[1], bbox[2], bbox[1], bbox[0])
}

fn edge_crosses_bbox(x1: i32, y1: i32, x2: i32, y2: i32, bbox: &[i32; 4]) -> bool {
    // Edge has endpoint outside while other is inside (crosses boundary)
    let p1_inside = y1 >= bbox[0] && x1 >= bbox[1] && y1 <= bbox[2] && x1 <= bbox[3];
    let p2_inside = y2 >= bbox[0] && x2 >= bbox[1] && y2 <= bbox[2] && x2 <= bbox[3];
    p1_inside != p2_inside
}

fn triangle_within(triangle: &Triangle, query: &[i32; 4]) -> bool {
    let tri_bbox = &triangle.words[0..4];

    // Triangle bbox entirely within query → WITHIN
    if tri_bbox[0] >= query[0]
        && tri_bbox[1] >= query[1]
        && tri_bbox[2] <= query[2]
        && tri_bbox[3] <= query[3]
    {
        return true;
    }

    // Triangle bbox entirely outside → NOT WITHIN
    if tri_bbox[2] < query[0]
        || tri_bbox[3] < query[1]
        || tri_bbox[0] > query[2]
        || tri_bbox[1] > query[3]
    {
        return false;
    }

    // Decode vertices.
    let ([ay, ax, by, bx, cy, cx], [ab, bc, ca]) = triangle.decode();

    // Check each edge - if boundary edge crosses query bbox, NOT WITHIN
    if ab && edge_crosses_bbox(ax, ay, bx, by, query) {
        return false;
    }
    if bc && edge_crosses_bbox(bx, by, cx, cy, query) {
        return false;
    }
    if ca && edge_crosses_bbox(cx, cy, ax, ay, query) {
        return false;
    }

    // No boundary edges cross out
    true
}

#[expect(clippy::too_many_arguments)]
fn point_in_triangle(
    px: i32,
    py: i32,
    ax: i32,
    ay: i32,
    bx: i32,
    by: i32,
    cx: i32,
    cy: i32,
) -> bool {
    let v0x = (cx - ax) as i128;
    let v0y = (cy - ay) as i128;
    let v1x = (bx - ax) as i128;
    let v1y = (by - ay) as i128;
    let v2x = (px - ax) as i128;
    let v2y = (py - ay) as i128;

    let dot00 = v0x * v0x + v0y * v0y;
    let dot01 = v0x * v1x + v0y * v1y;
    let dot02 = v0x * v2x + v0y * v2y;
    let dot11 = v1x * v1x + v1y * v1y;
    let dot12 = v1x * v2x + v1y * v2y;

    let denom = dot00 * dot11 - dot01 * dot01;
    if denom == 0 {
        return false;
    }

    let u = dot11 * dot02 - dot01 * dot12;
    let v = dot00 * dot12 - dot01 * dot02;

    u >= 0 && v >= 0 && u + v <= denom
}

fn triangle_intersects(triangle: &Triangle, query: &[i32; 4]) -> bool {
    let tri_bbox = &triangle.words[0..4];

    // Quick reject: bboxes don't overlap
    if tri_bbox[2] < query[0]
        || tri_bbox[3] < query[1]
        || tri_bbox[0] > query[2]
        || tri_bbox[1] > query[3]
    {
        return false;
    }

    let ([ay, ax, by, bx, cy, cx], _) = triangle.decode();

    // Any triangle vertex inside rectangle?
    if (ax >= query[1] && ax <= query[3] && ay >= query[0] && ay <= query[2])
        || (bx >= query[1] && bx <= query[3] && by >= query[0] && by <= query[2])
        || (cx >= query[1] && cx <= query[3] && cy >= query[0] && cy <= query[2])
    {
        return true;
    }

    // Any rectangle corner inside triangle?
    let corners = [
        (query[1], query[0]), // min_x, min_y
        (query[3], query[0]), // max_x, min_y
        (query[3], query[2]), // max_x, max_y
        (query[1], query[2]), // min_x, max_y
    ];
    for (x, y) in corners {
        if point_in_triangle(x, y, ax, ay, bx, by, cx, cy) {
            return true;
        }
    }

    // Any triangle edge intersect rectangle edges?
    edge_intersects_bbox(ax, ay, bx, by, query)
        || edge_intersects_bbox(bx, by, cx, cy, query)
        || edge_intersects_bbox(cx, cy, ax, ay, query)
}

/// Finds documents where all triangles are within the query bounding box.
///
/// Traverses the tree starting at `offset` (typically `segment.root_offset`), testing each
/// triangle to determine if it lies entirely within the query bounds. Uses two `BitSet` instances
/// to track state: `include` accumulates candidate documents, while `exclude` marks documents that
/// have at least one triangle extending outside the query.
///
/// The query is `[min_y, min_x, max_y, max_x]` in integer coordinates. The final result is
/// documents in `include` that are NOT in `exclude` - the caller must compute this difference.
pub fn search_within(
    segment: &Segment,
    offset: i32,
    query: &[i32; 4], // [min_y, min_x, max_y, max_x]
    include: &mut BitSet,
    exclude: &mut BitSet,
) -> io::Result<()> {
    let bbox = segment.bounding_box(offset);
    if !bbox_intersects(&bbox, query) {
    } else if offset < 0 {
        let leaf_node = segment.leaf_node(offset);
        // bbox crosses query → test each triangle
        let triangles = decompress_leaf(segment.leaf_page(&leaf_node))?;
        for triangle in &triangles {
            if triangle_intersects(triangle, query) {
                if exclude.contains(triangle.doc_id) {
                    continue; // Already excluded
                }
                if triangle_within(triangle, query) {
                    include.insert(triangle.doc_id);
                } else {
                    exclude.insert(triangle.doc_id);
                }
            }
        }
    } else {
        let branch_node = segment.branch_node(offset);
        search_within(segment, branch_node.left, query, include, exclude)?;
        search_within(segment, branch_node.right, query, include, exclude)?;
    }
    Ok(())
}

enum ContainsRelation {
    Candidate, // Query might be contained
    NotWithin, // Query definitely not contained
    Disjoint,  // Triangle doesn't overlap query
}

fn triangle_contains_relation(triangle: &Triangle, query: &[i32; 4]) -> ContainsRelation {
    let tri_bbox = &triangle.words[0..4];

    if query[2] < tri_bbox[0]
        || query[3] < tri_bbox[1]
        || query[0] > tri_bbox[2]
        || query[1] > tri_bbox[3]
    {
        return ContainsRelation::Disjoint;
    }

    let ([ay, ax, by, bx, cy, cx], [ab, bc, ca]) = triangle.decode();

    let corners = [
        (query[1], query[0]),
        (query[3], query[0]),
        (query[3], query[2]),
        (query[1], query[2]),
    ];

    let mut any_corner_inside = false;
    for &(qx, qy) in &corners {
        if point_in_triangle(qx, qy, ax, ay, bx, by, cx, cy) {
            any_corner_inside = true;
            break;
        }
    }

    let ab_intersects = edge_intersects_bbox(ax, ay, bx, by, query);
    let bc_intersects = edge_intersects_bbox(bx, by, cx, cy, query);
    let ca_intersects = edge_intersects_bbox(cx, cy, ax, ay, query);

    if (ab && edge_crosses_bbox(ax, ay, bx, by, query))
        || (bc && edge_crosses_bbox(bx, by, cx, cy, query))
        || (ca && edge_crosses_bbox(cx, cy, ax, ay, query))
    {
        return ContainsRelation::NotWithin;
    }

    if any_corner_inside || ab_intersects || bc_intersects || ca_intersects {
        return ContainsRelation::Candidate;
    }

    ContainsRelation::Disjoint
}

/// Finds documents whose polygons contain the query bounding box.
///
/// Traverses the tree starting at `offset` (typically `segment.root_offset`), testing each
/// triangle using three-state logic: `Candidate` (query might be contained), `NotWithin` (boundary
/// edge crosses query), or `Disjoint` (no overlap). Only boundary edges are tested for crossing -
/// internal tessellation edges are ignored.
///
/// The query is `[min_y, min_x, max_y, max_x]` in integer coordinates. Uses two `BitSet`
/// instances: `include` accumulates candidates, `excluded` marks documents with disqualifying
/// boundary crossings. The final result is documents in `include` that are NOT in `exclude`.
pub fn search_contains(
    segment: &Segment,
    offset: i32,
    query: &[i32; 4],
    include: &mut BitSet,
    exclude: &mut BitSet,
) -> io::Result<()> {
    let bbox = segment.bounding_box(offset);
    if !bbox_intersects(&bbox, query) {
    } else if offset < 0 {
        let leaf_node = segment.leaf_node(offset);
        // bbox crosses query → test each triangle
        let triangles = decompress_leaf(segment.leaf_page(&leaf_node))?;
        for triangle in &triangles {
            if triangle_intersects(triangle, query) {
                let doc_id = triangle.doc_id;
                if exclude.contains(doc_id) {
                    continue;
                }
                match triangle_contains_relation(triangle, query) {
                    ContainsRelation::Candidate => include.insert(doc_id),
                    ContainsRelation::NotWithin => exclude.insert(doc_id),
                    ContainsRelation::Disjoint => {}
                }
            }
        }
    } else {
        let branch_node = segment.branch_node(offset);
        search_contains(segment, branch_node.left, query, include, exclude)?;
        search_contains(segment, branch_node.right, query, include, exclude)?;
    }
    Ok(())
}

/// HUSH
pub struct LeafPageIterator<'a> {
    segment: &'a Segment<'a>,
    descent_stack: Vec<i32>,
}

impl<'a> LeafPageIterator<'a> {
    /// HUSH
    pub fn new(segment: &'a Segment<'a>) -> Self {
        Self {
            segment,
            descent_stack: vec![segment.root_offset],
        }
    }
}

impl<'a> Iterator for LeafPageIterator<'a> {
    type Item = io::Result<Vec<Triangle>>;

    fn next(&mut self) -> Option<Self::Item> {
        let offset = self.descent_stack.pop()?;
        if offset < 0 {
            let leaf_node = self.segment.leaf_node(offset);
            let leaf_page = self.segment.leaf_page(&leaf_node);
            match decompress_leaf(&leaf_page) {
                Ok(triangles) => Some(Ok(triangles)),
                Err(e) => Some(Err(e)),
            }
        } else {
            let branch_node = self.segment.branch_node(offset);
            self.descent_stack.push(branch_node.right);
            self.descent_stack.push(branch_node.left);
            self.next()
        }
    }
}

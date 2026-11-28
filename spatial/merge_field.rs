//! Merge complete spatial fields from several source segments.

use std::io;
use std::io::Write;

use common::{CountingWriter, ReadOnlyBitSet};

use super::cell_index_reader::CellIndexReader;
use super::edge_reader::EdgeReader;
use super::edge_resolver::EdgeResolver;
use super::edge_writer::EdgeWriter;
use super::interleaver::Interleaver;
use super::shape_index::write_cell_footer;
use super::surface::Surface;

/// Raw spatial field data from one source segment.
pub struct MergeSource<'a> {
    pub cells_bytes: &'a [u8],
    pub edges_bytes: &'a [u8],
    pub segment_name: String,
}

/// Merge spatial field data and rewrite its document IDs.
pub fn merge_field<S, CellsWrite, EdgesWrite, DocIdsWrite>(
    sources: &[MergeSource<'_>],
    alive_bitsets: &[Option<ReadOnlyBitSet>],
    cells_write: &mut CountingWriter<CellsWrite>,
    edges_write: &mut CountingWriter<EdgesWrite>,
    doc_ids_write: &mut CountingWriter<DocIdsWrite>,
    doc_id_inverse: &[Vec<Option<u32>>],
) -> io::Result<()>
where
    S: Surface,
    CellsWrite: Write,
    EdgesWrite: Write,
    DocIdsWrite: Write,
{
    let mut cell_readers = Vec::new();
    let mut edge_readers = Vec::new();
    let mut geometry_counts = Vec::new();
    let mut segment_names = Vec::new();
    for source in sources {
        let cell_reader = CellIndexReader::open(source.cells_bytes)?;
        let edge_reader = EdgeReader::<S>::open(source.edges_bytes)?;
        geometry_counts.push(edge_reader.geometry_count());
        cell_readers.push(cell_reader);
        edge_readers.push(edge_reader);
        segment_names.push(source.segment_name.clone());
    }

    let edge_resolver = EdgeResolver::new(edge_readers);
    let iterators = cell_readers.iter().map(CellIndexReader::iter).collect();
    let mut interleave = Interleaver::new(iterators, &edge_resolver, alive_bitsets, segment_names);

    const NOT_ASSIGNED: u32 = u32::MAX;
    let mut old_to_new = geometry_counts
        .iter()
        .map(|&count| vec![NOT_ASSIGNED; count as usize])
        .collect::<Vec<_>>();
    let mut next_new_id = 0;
    let mut edge_writer = EdgeWriter::<S, _>::new(edges_write);
    let cells_base = cells_write.written_bytes();
    let doc_ids_base = doc_ids_write.written_bytes();
    let mut offsets = Vec::new();
    let mut cell_count = 0u32;

    while let Some(cell) = interleave.next() {
        cell_count += 1;
        for shape in &cell.shapes {
            let segment = shape.geometry_id.0 as usize;
            let position = shape.geometry_id.1 as usize;
            if old_to_new[segment][position] != NOT_ASSIGNED {
                continue;
            }
            let entry = edge_resolver.locate(shape.geometry_id);
            for member in 0..entry.member_count {
                let position = (entry.head + member) as usize;
                if old_to_new[segment][position] == NOT_ASSIGNED {
                    old_to_new[segment][position] = next_new_id;
                    next_new_id += 1;
                }
            }
            let new_doc_id = doc_id_inverse[segment][entry.doc_id as usize].unwrap();
            let (_, mut set) = edge_resolver.read_geometry_set(shape.geometry_id);
            set.doc_id = new_doc_id;
            edge_writer.insert(&set);
        }

        let offset = cells_write.written_bytes() - cells_base;
        let doc_id_offset = doc_ids_write.written_bytes() - doc_ids_base;
        cells_write.write_all(&cell.cell_id.0.to_le_bytes())?;
        cells_write.write_all(&(cell.shapes.len() as u32).to_le_bytes())?;
        for shape in &cell.shapes {
            let segment = shape.geometry_id.0 as usize;
            let position = shape.geometry_id.1 as usize;
            let new_id = old_to_new[segment][position];
            assert_ne!(new_id, NOT_ASSIGNED);
            cells_write.write_all(&new_id.to_le_bytes())?;
            cells_write.write_all(&[shape.contains_center as u8])?;
            cells_write.write_all(&(shape.edge_indices.len() as u32).to_le_bytes())?;
            for &edge_id in &shape.edge_indices {
                cells_write.write_all(&edge_id.to_le_bytes())?;
            }
            let old_doc_id = edge_resolver.doc_id_for(shape.geometry_id);
            let new_doc_id = doc_id_inverse[segment][old_doc_id as usize].unwrap();
            doc_ids_write.write_all(&new_doc_id.to_le_bytes())?;
        }
        offsets.push((cell.cell_id.0, offset, doc_id_offset));
    }

    assert_eq!(next_new_id, edge_writer.geometry_count());
    edge_writer.finish();
    let dir_offset = cells_write.written_bytes() - cells_base;
    for (cell_id, offset, doc_id_offset) in offsets {
        cells_write.write_all(&cell_id.to_le_bytes())?;
        cells_write.write_all(&offset.to_le_bytes())?;
        cells_write.write_all(&doc_id_offset.to_le_bytes())?;
    }
    write_cell_footer(cells_write, cell_count, dir_offset)?;
    cells_write.flush()?;
    edges_write.flush()?;
    doc_ids_write.flush()?;
    Ok(())
}

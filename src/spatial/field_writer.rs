//! Surface-specific accumulation and serialization for one spatial field.

use std::io;
use std::io::Write;

use common::CountingWriter;

use crate::spatial::clip_options::ClipOptions;
use crate::spatial::clipper::Clipper;
use crate::spatial::edge_writer::EdgeWriter;
use crate::spatial::geometry_set::GeometrySet;
use crate::spatial::surface::Surface;

/// Accumulates one field's geometry and writes its cell and edge indexes.
pub struct SurfaceFieldWriter<S: Surface> {
    sets: Vec<GeometrySet<S>>,
}

impl<S: Surface> SurfaceFieldWriter<S> {
    /// Create an empty field writer.
    pub fn new() -> Self {
        SurfaceFieldWriter { sets: Vec::new() }
    }

    /// Add one projected geometry value.
    pub fn add_set(&mut self, set: GeometrySet<S>) {
        self.sets.push(set);
    }

    /// Estimated memory used by accumulated vertices.
    pub fn mem_usage(&self) -> usize {
        self.sets
            .iter()
            .map(|set| {
                set.members
                    .iter()
                    .map(|member| member.vertices.len() * S::DIMENSIONS * 8)
                    .sum::<usize>()
            })
            .sum()
    }

    /// Write the accumulated cell and edge indexes.
    pub fn serialize<CellsWrite, EdgesWrite>(
        self,
        cells_write: &mut CountingWriter<CellsWrite>,
        edges_write: &mut CountingWriter<EdgesWrite>,
        old_to_new: Option<&[u32]>,
    ) -> io::Result<()>
    where
        CellsWrite: Write,
        EdgesWrite: Write,
    {
        let mut sets = self.sets;
        if let Some(old_to_new) = old_to_new {
            for set in &mut sets {
                set.doc_id = old_to_new[set.doc_id as usize];
            }
        }
        if sets.is_empty() {
            return Ok(());
        }

        let builder = Clipper::new(ClipOptions::default());
        let cell_index = builder.build(&sets);

        cell_index.write(cells_write);
        cells_write.flush()?;

        let mut edge_writer = EdgeWriter::<S, _>::new(edges_write);
        for set in &sets {
            edge_writer.insert(set);
        }
        edge_writer.finish();
        edges_write.flush()?;

        Ok(())
    }
}

impl<S: Surface> Default for SurfaceFieldWriter<S> {
    fn default() -> Self {
        Self::new()
    }
}

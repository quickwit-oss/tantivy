//! HNSW index persistence — serialize/deserialize to segment sidecar files.
//!
//! ## File Format (`.vec`)
//!
//! ```text
//! ┌────────────────────────────────────────────────┐
//! │  Magic: b"TVEC" (4 bytes)                      │
//! │  Version: u32 le (4 bytes)                     │
//! │  Header:                                        │
//! │    dimension: u32 le                            │
//! │    metric: u8 (0=L2, 1=Cosine, 2=Dot)          │
//! │    num_vectors: u32 le                          │
//! │    max_level: u32 le                            │
//! │    entry_point: u32 le (u32::MAX if none)       │
//! │    num_levels: u32 le                           │
//! │    _reserved: [u8; 16]                          │
//! ├────────────────────────────────────────────────┤
//! │  Vectors section:                               │
//! │    For each vector (num_vectors):               │
//! │      f32 × dimension (le)                       │
//! ├────────────────────────────────────────────────┤
//! │  Levels section:                                │
//! │    u8 × num_vectors (level of each node)        │
//! ├────────────────────────────────────────────────┤
//! │  Graph section:                                 │
//! │    For each level (num_levels):                 │
//! │      For each node (num_vectors):               │
//! │        num_neighbors: u16 le                    │
//! │        neighbor_ids: u32 le × num_neighbors     │
//! └────────────────────────────────────────────────┘
//! ```

use std::io::{self, Read, Write};

use crate::error::{GpuError, GpuResult};
use crate::vector::distance::DistanceMetric;
use crate::vector::hnsw::HnswIndex;

const MAGIC: &[u8; 4] = b"TVEC";
const VERSION: u32 = 1;

impl HnswIndex {
    /// Serialize the HNSW index to a writer.
    pub fn serialize<W: Write>(&self, writer: &mut W) -> GpuResult<()> {
        let mut w = IoWriter(writer);

        // Magic + version
        w.write_all(MAGIC)?;
        w.write_u32(VERSION)?;

        // Header
        w.write_u32(self.dim() as u32)?;
        w.write_u8(metric_to_u8(self.metric()))?;
        w.write_u32(self.len() as u32)?;
        w.write_u32(self.max_level() as u32)?;
        w.write_u32(self.entry_point_id().unwrap_or(u32::MAX))?;
        w.write_u32(self.num_levels() as u32)?;
        w.write_all(&[0u8; 16])?; // reserved

        // Vectors
        for vec in self.vectors() {
            for &val in vec {
                w.write_f32(val)?;
            }
        }

        // Levels
        for &level in self.node_levels() {
            w.write_u8(level as u8)?;
        }

        // Graph
        for level in 0..self.num_levels() {
            for node in 0..self.len() {
                let neighbors = self.neighbors(level, node);
                w.write_u16(neighbors.len() as u16)?;
                for &nbr in neighbors {
                    w.write_u32(nbr)?;
                }
            }
        }

        Ok(())
    }

    /// Deserialize an HNSW index from a reader.
    pub fn deserialize<R: Read>(reader: &mut R) -> GpuResult<Self> {
        let mut r = IoReader(reader);

        // Magic
        let mut magic = [0u8; 4];
        r.read_exact(&mut magic)?;
        if &magic != MAGIC {
            return Err(GpuError::Dispatch("Invalid TVEC magic bytes".to_string()));
        }

        // Version
        let version = r.read_u32()?;
        if version != VERSION {
            return Err(GpuError::Dispatch(format!(
                "Unsupported TVEC version: {version}"
            )));
        }

        // Header
        let dimension = r.read_u32()? as usize;
        let metric = u8_to_metric(r.read_u8()?)?;
        let num_vectors = r.read_u32()? as usize;
        let max_level = r.read_u32()? as usize;
        let entry_point_raw = r.read_u32()?;
        let entry_point = if entry_point_raw == u32::MAX {
            None
        } else {
            Some(entry_point_raw)
        };
        let num_levels = r.read_u32()? as usize;
        let mut _reserved = [0u8; 16];
        r.read_exact(&mut _reserved)?;

        // Vectors
        let mut vectors = Vec::with_capacity(num_vectors);
        for _ in 0..num_vectors {
            let mut vec = Vec::with_capacity(dimension);
            for _ in 0..dimension {
                vec.push(r.read_f32()?);
            }
            vectors.push(vec);
        }

        // Levels
        let mut levels = Vec::with_capacity(num_vectors);
        for _ in 0..num_vectors {
            levels.push(r.read_u8()? as usize);
        }

        // Graph
        let mut graph = Vec::with_capacity(num_levels);
        for _ in 0..num_levels {
            let mut level_graph = Vec::with_capacity(num_vectors);
            for _ in 0..num_vectors {
                let num_neighbors = r.read_u16()? as usize;
                let mut neighbors = Vec::with_capacity(num_neighbors);
                for _ in 0..num_neighbors {
                    neighbors.push(r.read_u32()?);
                }
                level_graph.push(neighbors);
            }
            graph.push(level_graph);
        }

        Ok(HnswIndex::from_parts(
            vectors,
            graph,
            levels,
            entry_point,
            max_level,
            dimension,
            metric,
        ))
    }
}

fn metric_to_u8(metric: DistanceMetric) -> u8 {
    match metric {
        DistanceMetric::L2 => 0,
        DistanceMetric::Cosine => 1,
        DistanceMetric::DotProduct => 2,
    }
}

fn u8_to_metric(val: u8) -> GpuResult<DistanceMetric> {
    match val {
        0 => Ok(DistanceMetric::L2),
        1 => Ok(DistanceMetric::Cosine),
        2 => Ok(DistanceMetric::DotProduct),
        _ => Err(GpuError::Dispatch(format!("Unknown metric: {val}"))),
    }
}

// ─── IO Helpers ───

struct IoWriter<'a, W: Write>(&'a mut W);

impl<W: Write> IoWriter<'_, W> {
    fn write_all(&mut self, buf: &[u8]) -> GpuResult<()> {
        self.0.write_all(buf).map_err(io_err)
    }
    fn write_u8(&mut self, val: u8) -> GpuResult<()> {
        self.write_all(&[val])
    }
    fn write_u16(&mut self, val: u16) -> GpuResult<()> {
        self.write_all(&val.to_le_bytes())
    }
    fn write_u32(&mut self, val: u32) -> GpuResult<()> {
        self.write_all(&val.to_le_bytes())
    }
    fn write_f32(&mut self, val: f32) -> GpuResult<()> {
        self.write_all(&val.to_le_bytes())
    }
}

struct IoReader<'a, R: Read>(&'a mut R);

impl<R: Read> IoReader<'_, R> {
    fn read_exact(&mut self, buf: &mut [u8]) -> GpuResult<()> {
        self.0.read_exact(buf).map_err(io_err)
    }
    fn read_u8(&mut self) -> GpuResult<u8> {
        let mut buf = [0u8; 1];
        self.read_exact(&mut buf)?;
        Ok(buf[0])
    }
    fn read_u16(&mut self) -> GpuResult<u16> {
        let mut buf = [0u8; 2];
        self.read_exact(&mut buf)?;
        Ok(u16::from_le_bytes(buf))
    }
    fn read_u32(&mut self) -> GpuResult<u32> {
        let mut buf = [0u8; 4];
        self.read_exact(&mut buf)?;
        Ok(u32::from_le_bytes(buf))
    }
    fn read_f32(&mut self) -> GpuResult<f32> {
        let mut buf = [0u8; 4];
        self.read_exact(&mut buf)?;
        Ok(f32::from_le_bytes(buf))
    }
}

fn io_err(e: io::Error) -> GpuError {
    GpuError::Dispatch(format!("IO error: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serialize_deserialize_roundtrip() {
        let mut index = HnswIndex::new(3, DistanceMetric::L2);
        for i in 0..20u32 {
            index
                .insert(vec![i as f32, (i * 2) as f32, (i * 3) as f32])
                .unwrap();
        }

        // Serialize
        let mut buf = Vec::new();
        index.serialize(&mut buf).unwrap();

        // Deserialize
        let restored = HnswIndex::deserialize(&mut &buf[..]).unwrap();

        assert_eq!(restored.len(), 20);
        assert_eq!(restored.dim(), 3);

        // Search should produce identical results
        let query = vec![10.0, 20.0, 30.0];
        let orig_results = index.search(&query, 5, 50);
        let rest_results = restored.search(&query, 5, 50);

        assert_eq!(orig_results.len(), rest_results.len());
        for (o, r) in orig_results.iter().zip(rest_results.iter()) {
            assert_eq!(o.1, r.1); // same doc_ids
            assert!((o.0 - r.0).abs() < 1e-6); // same distances
        }
    }

    #[test]
    fn test_serialize_empty_index() {
        let index = HnswIndex::new(4, DistanceMetric::Cosine);
        let mut buf = Vec::new();
        index.serialize(&mut buf).unwrap();

        let restored = HnswIndex::deserialize(&mut &buf[..]).unwrap();
        assert_eq!(restored.len(), 0);
        assert_eq!(restored.dim(), 4);
    }

    #[test]
    fn test_invalid_magic() {
        let data = b"XXXX\x01\x00\x00\x00";
        let result = HnswIndex::deserialize(&mut &data[..]);
        assert!(result.is_err());
    }
}

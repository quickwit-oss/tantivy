use crate::schema::VectorOptions;
use crate::vector::VectorElement;
use crate::{DocId, TantivyError};

pub trait IvfClusterer: Send + Sync + 'static {
    fn centroid_ratio(&self) -> f32;

    fn training_samples_per_centroid(&self) -> usize;

    fn train(
        &self,
        options: &VectorOptions,
        vectors: IvfTrainingVectors,
        num_centroids: usize,
    ) -> crate::Result<IvfCentroids>;

    fn assign(
        &self,
        options: &VectorOptions,
        vectors: IvfVectors<'_>,
        centroids: &IvfCentroids,
    ) -> crate::Result<Vec<u32>>;

    fn assign_batch_size(&self) -> usize {
        2048
    }

    fn merge_settings(&self, total_target_docs: usize) -> crate::Result<IvfMergeSettings> {
        let centroid_ratio = self.centroid_ratio();
        let training_samples_per_centroid = self.training_samples_per_centroid();
        let assign_batch_size = self.assign_batch_size();

        assert!(
            centroid_ratio > 0.0 && centroid_ratio <= 1.0,
            "IvfClusterer centroid_ratio must be greater than 0 and less than or equal to 1, got \
             {centroid_ratio}"
        );
        assert!(
            training_samples_per_centroid > 1,
            "IvfClusterer training_samples_per_centroid must be greater than 1, got \
             {training_samples_per_centroid}"
        );
        assert!(
            assign_batch_size > 0,
            "IvfClusterer assign_batch_size must be greater than 0, got {assign_batch_size}"
        );

        let num_centroids =
            ((total_target_docs as f64) * f64::from(centroid_ratio)).ceil() as usize;
        let num_centroids = num_centroids.clamp(1, total_target_docs);
        Ok(IvfMergeSettings {
            num_centroids,
            training_samples_per_centroid,
            assign_batch_size,
            // Replication off by default (primary-only layout).
            replicas: 1,
        })
    }
}

#[derive(Clone, Copy, Debug)]
pub struct IvfMergeSettings {
    pub num_centroids: usize,
    pub training_samples_per_centroid: usize,
    pub assign_batch_size: usize,
    /// Total number of cells a vector is written into (SPANN `ReplicaCount`):
    /// the primary plus up to `replicas - 1` additional cells taken from the
    /// nearest centroids — selected exactly for small centroid sets, via a
    /// transient build-time neighborhood graph for large ones. `1` (the
    /// default) disables replication entirely — no selector is built and the
    /// output is the primary-only layout.
    pub replicas: usize,
}

#[derive(Clone, Debug)]
pub enum IvfCentroids {
    F32(IvfMatrix<f32>),
}

#[derive(Clone, Copy, Debug)]
pub enum IvfVectors<'a> {
    F32(IvfVectorBatch<'a, f32>),
}

/// Owned training input: the merge hands its sampled buffers to the clusterer,
/// which may consume them in place instead of copying.
#[derive(Clone, Debug)]
pub enum IvfTrainingVectors {
    F32(IvfTrainingBatch<f32>),
}

#[derive(Clone, Debug)]
pub struct IvfTrainingBatch<T> {
    pub doc_ids: Vec<DocId>,
    pub matrix: IvfMatrix<T>,
}

#[derive(Clone, Debug)]
pub struct IvfMatrix<T> {
    pub values: Vec<T>,
    pub rows: usize,
    pub dims: usize,
}

#[derive(Clone, Copy, Debug)]
pub struct IvfMatrixView<'a, T> {
    pub values: &'a [T],
    pub rows: usize,
    pub dims: usize,
}

#[derive(Clone, Copy, Debug)]
pub struct IvfVectorBatch<'a, T> {
    pub doc_ids: &'a [DocId],
    pub matrix: IvfMatrixView<'a, T>,
}

pub(crate) fn decode_row<T: VectorElement>(bytes: &[u8], dim: usize) -> crate::Result<Vec<T>> {
    let expected = dim * T::SIZE_BYTES;
    if bytes.len() != expected {
        return Err(TantivyError::InvalidArgument(format!(
            "vector byte length mismatch: expected {expected} bytes, got {}",
            bytes.len()
        )));
    }
    Ok(bytes
        .chunks_exact(T::SIZE_BYTES)
        .map(T::decode_le)
        .collect())
}

pub(crate) fn encode_vector<T: VectorElement>(vector: &[T], dim: usize) -> crate::Result<Vec<u8>> {
    if vector.len() != dim {
        return Err(TantivyError::InvalidArgument(format!(
            "centroid length mismatch: expected {dim} elements, got {}",
            vector.len()
        )));
    }
    let mut bytes = Vec::with_capacity(dim * T::SIZE_BYTES);
    for element in vector {
        element.encode_le(&mut bytes)?;
    }
    Ok(bytes)
}

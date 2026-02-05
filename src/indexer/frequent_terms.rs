use std::collections::HashMap;
use std::io::{self, Read, Write};
use std::sync::Arc;

use ahash::AHashSet;
use parking_lot::RwLock;

/// Tracks frequent terms across the index for word ngram generation
#[derive(Clone)]
pub struct FrequentTermTracker {
    inner: Arc<RwLock<FrequentTermTrackerInner>>,
}

struct FrequentTermTrackerInner {
    /// Set of term hashes that are considered frequent
    frequent_hashes: AHashSet<u64>,
    /// Document frequency for each term (for building the frequent set)
    term_doc_freq: HashMap<u64, u32>,
    /// Total number of documents indexed
    total_docs: u32,
    /// Threshold for considering a term frequent (fraction of documents)
    threshold: f32,
    /// Maximum number of frequent terms to track
    max_frequent_terms: usize,
}

impl FrequentTermTracker {
    /// Create a new frequent term tracker
    pub fn new(threshold: f32, max_frequent_terms: usize) -> Self {
        FrequentTermTracker {
            inner: Arc::new(RwLock::new(FrequentTermTrackerInner {
                frequent_hashes: AHashSet::new(),
                term_doc_freq: HashMap::new(),
                total_docs: 0,
                threshold,
                max_frequent_terms,
            })),
        }
    }

    /// Create a frequent term tracker from pre-computed term document frequencies.
    /// This is useful when merging segments and we already know the term frequencies.
    pub fn from_term_frequencies(
        term_doc_freq: HashMap<u64, u32>,
        total_docs: u32,
        threshold: f32,
        max_frequent_terms: usize,
    ) -> Self {
        let mut inner = FrequentTermTrackerInner {
            frequent_hashes: AHashSet::new(),
            term_doc_freq,
            total_docs,
            threshold,
            max_frequent_terms,
        };
        
        // Compute the frequent set from the provided frequencies
        inner.update_frequent_set();
        
        FrequentTermTracker {
            inner: Arc::new(RwLock::new(inner)),
        }
    }

    /// Record terms from a document
    pub fn record_document_terms(&self, term_hashes: &[u64]) {
        let mut inner = self.inner.write();
        inner.total_docs += 1;
        
        // Use a temporary set to ensure each term is counted only once per document
        let unique_hashes: AHashSet<u64> = term_hashes.iter().copied().collect();
        
        for hash in unique_hashes {
            *inner.term_doc_freq.entry(hash).or_insert(0) += 1;
        }
        
        // Update the frequent set periodically
        // More frequent updates for small doc counts, less frequent for large
        let update_interval = if inner.total_docs < 100 {
            10 // Every 10 docs for first 100 documents
        } else if inner.total_docs < 10_000 {
            100 // Every 100 docs for next 10k
        } else {
            1000 // Every 1000 docs thereafter
        };
        
        if inner.total_docs % update_interval == 0 {
            inner.update_frequent_set();
        }
    }

    /// Check if a term is frequent
    pub fn is_frequent(&self, term_hash: u64) -> bool {
        self.inner.read().frequent_hashes.contains(&term_hash)
    }

    /// Force an update of the frequent term set
    pub fn update_frequent_set(&self) {
        self.inner.write().update_frequent_set();
    }

    /// Get statistics
    pub fn stats(&self) -> (usize, u32) {
        let inner = self.inner.read();
        (inner.frequent_hashes.len(), inner.total_docs)
    }
    
    /// Serialize frequent terms to a writer
    pub fn serialize<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        let inner = self.inner.read();
        
        // Write header: total_docs, threshold, max_frequent_terms
        writer.write_all(&inner.total_docs.to_le_bytes())?;
        writer.write_all(&inner.threshold.to_le_bytes())?;
        writer.write_all(&(inner.max_frequent_terms as u32).to_le_bytes())?;
        
        // Write number of frequent terms
        let num_frequent = inner.frequent_hashes.len() as u32;
        writer.write_all(&num_frequent.to_le_bytes())?;
        
        // Write each frequent term hash
        for &hash in &inner.frequent_hashes {
            writer.write_all(&hash.to_le_bytes())?;
        }
        
        Ok(())
    }
    
    /// Deserialize frequent terms from a reader
    pub fn deserialize<R: Read>(reader: &mut R) -> io::Result<Self> {
        // Read header
        let mut buf = [0u8; 4];
        reader.read_exact(&mut buf)?;
        let total_docs = u32::from_le_bytes(buf);
        
        let mut buf = [0u8; 4];
        reader.read_exact(&mut buf)?;
        let threshold = f32::from_le_bytes(buf);
        
        let mut buf = [0u8; 4];
        reader.read_exact(&mut buf)?;
        let max_frequent_terms = u32::from_le_bytes(buf) as usize;
        
        // Read number of frequent terms
        let mut buf = [0u8; 4];
        reader.read_exact(&mut buf)?;
        let num_frequent = u32::from_le_bytes(buf) as usize;
        
        // Read frequent term hashes
        // Pre-allocate to exact size to avoid rehashing
        let mut frequent_hashes = AHashSet::with_capacity(num_frequent);
        // Reuse single buffer for all reads to reduce allocations
        let mut hash_buf = [0u8; 8];
        for _ in 0..num_frequent {
            reader.read_exact(&mut hash_buf)?;
            frequent_hashes.insert(u64::from_le_bytes(hash_buf));
        }
        
        Ok(FrequentTermTracker {
            inner: Arc::new(RwLock::new(FrequentTermTrackerInner {
                frequent_hashes,
                term_doc_freq: HashMap::new(), // Don't persist full term_doc_freq
                total_docs,
                threshold,
                max_frequent_terms,
            })),
        })
    }
    
    /// Get the frequent term hashes for external use
    pub fn get_frequent_hashes(&self) -> Vec<u64> {
        self.inner.read().frequent_hashes.iter().copied().collect()
    }
}

impl FrequentTermTrackerInner {
    fn update_frequent_set(&mut self) {
        if self.total_docs == 0 {
            return;
        }
        
        let threshold_count = (self.total_docs as f32 * self.threshold).max(2.0) as u32;
        
        // Pre-allocate with estimated capacity
        let estimated_capacity = (self.term_doc_freq.len() / 10).min(self.max_frequent_terms);
        let mut candidates: Vec<(u64, u32)> = Vec::with_capacity(estimated_capacity);
        
        // Collect terms that meet the threshold
        for (&hash, &count) in self.term_doc_freq.iter() {
            if count >= threshold_count {
                candidates.push((hash, count));
            }
        }
        
        // Sort by frequency and limit to max_frequent_terms
        candidates.sort_unstable_by(|a, b| b.1.cmp(&a.1));
        candidates.truncate(self.max_frequent_terms);
        
        // Reuse existing set capacity if possible
        self.frequent_hashes.clear();
        self.frequent_hashes.reserve(candidates.len());
        for (hash, _) in candidates {
            self.frequent_hashes.insert(hash);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_frequent_term_tracking() {
        let tracker = FrequentTermTracker::new(0.5, 100);
        
        // Add some documents
        tracker.record_document_terms(&[1, 2, 3]);
        tracker.record_document_terms(&[1, 2, 4]);
        tracker.record_document_terms(&[1, 5, 6]);
        tracker.record_document_terms(&[1, 7, 8]);
        
        tracker.update_frequent_set();
        
        // Term 1 appears in all documents (100%) - should be frequent
        assert!(tracker.is_frequent(1));
        
        // Term 2 appears in 50% of documents - should be frequent
        assert!(tracker.is_frequent(2));
        
        // Terms 3-8 appear in < 50% - should not be frequent
        assert!(!tracker.is_frequent(3));
        assert!(!tracker.is_frequent(4));
    }

    #[test]
    fn test_max_frequent_terms() {
        let tracker = FrequentTermTracker::new(0.01, 2);
        
        // Add many terms that all meet the threshold
        for _ in 0..100 {
            tracker.record_document_terms(&[1, 2, 3, 4, 5]);
        }
        
        tracker.update_frequent_set();
        
        let (frequent_count, _) = tracker.stats();
        assert_eq!(frequent_count, 2); // Should be limited to max
    }
}

use serde::{Deserialize, Serialize};

// Size of the margin for the heap. A segment is closed when the remaining memory
// in the heap goes below MARGIN_IN_BYTES.
const MARGIN_IN_BYTES: u64 = 1_000_000;

// We impose the memory per thread to be at least 3 MB.
const HEAP_SIZE_MIN: u64 = MARGIN_IN_BYTES * 3u64;
const HEAP_SIZE_MAX: u64 = u32::max_value() as u64 - MARGIN_IN_BYTES;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexWriterConfig {
    pub max_indexing_threads: usize,
    pub max_merging_threads: usize,
    pub memory_budget: u64,
    pub store_flush_num_bytes: u64,
    pub persist_low: u64,
    pub persist_high: u64,
}

impl Default for IndexWriterConfig {
    fn default() -> Self {
        IndexWriterConfig {
            max_indexing_threads: 1,
            max_merging_threads: 3,
            memory_budget: 50_000_000u64,
            store_flush_num_bytes: 10_000_000u64,
            persist_low: 10_000_000u64,
            persist_high: 50_000_000u64
        }
    }
}

impl IndexWriterConfig {
    #[cfg(test)]
    pub fn for_test() -> IndexWriterConfig {
        IndexWriterConfig {
            max_indexing_threads: 1,
            max_merging_threads: 5,
            memory_budget: 4_000_000u64,
            store_flush_num_bytes: 500_000u64,
            persist_low: 2_000_000u64,
            persist_high: 3_000_000u64,
        }
    }

    // Ensures the `IndexWriterConfig` is correct.
    //
    // This method checks that the values in the `IndexWriterConfig`
    // are valid. If it is not, it may mutate some of the values (like `max_num_threads`) to
    // fit the contracts or return an error with an explicit error message.
    //
    // If called twice, the config is guaranteed to not be updated the second time.
    pub fn validate(&mut self) -> crate::Result<()> {
        if self.memory_budget < HEAP_SIZE_MIN {
            let err_msg = format!(
                "The heap size per thread needs to be at least {}.",
                HEAP_SIZE_MIN
            );
            return Err(crate::TantivyError::InvalidArgument(err_msg));
        }
        let heap_size_in_bytes_per_thread = self.heap_size_in_byte_per_thread();
        if heap_size_in_bytes_per_thread >= HEAP_SIZE_MAX {
            let err_msg = format!("The heap size per thread cannot exceed {}", HEAP_SIZE_MAX);
            return Err(crate::TantivyError::InvalidArgument(err_msg));
        }
        if heap_size_in_bytes_per_thread < HEAP_SIZE_MIN {
            self.max_indexing_threads = (self.memory_budget / HEAP_SIZE_MIN) as usize;
        }
        Ok(())
    }

    pub fn heap_size_in_byte_per_thread(&self) -> u64 {
        self.memory_budget / self.max_indexing_threads as u64
    }

    pub fn heap_size_before_flushing(&self) -> u64 {
        self.heap_size_in_byte_per_thread() - MARGIN_IN_BYTES
    }
}

#[cfg(test)]
mod tests {
    use crate::IndexWriterConfig;

    #[test]
    fn test_index_writer_config_simple() {
        let mut index = IndexWriterConfig {
            max_indexing_threads: 3,
            memory_budget: super::HEAP_SIZE_MIN * 3,
            ..Default::default()
        };
        assert!(index.validate().is_ok());
        assert_eq!(index.max_indexing_threads, 3);
        assert_eq!(index.heap_size_in_byte_per_thread(), super::HEAP_SIZE_MIN);
    }

    #[test]
    fn test_index_writer_config_reduce_num_threads() {
        let mut index = IndexWriterConfig {
            max_indexing_threads: 3,
            memory_budget: super::HEAP_SIZE_MIN,
            ..Default::default()
        };
        assert!(index.validate().is_ok());
        assert_eq!(index.max_indexing_threads, 1);
        assert_eq!(index.heap_size_in_byte_per_thread(), super::HEAP_SIZE_MIN);
    }

    #[test]
    fn test_index_writer_config_not_enough_memory() {
        let mut index = IndexWriterConfig {
            max_indexing_threads: 1,
            memory_budget: super::HEAP_SIZE_MIN - 1,
            ..Default::default()
        };
        assert!(
            matches!(index.validate(), Err(crate::TantivyError::InvalidArgument(msg) ) if msg.contains("The heap size per thread needs to be at least"))
        );
    }

    #[test]
    fn test_index_writer_config_too_much_memory() {
        let mut index = IndexWriterConfig {
            max_indexing_threads: 1,
            memory_budget: (u32::max_value() as u64) + 1,
            ..Default::default()
        };
        assert!(
            matches!(index.validate(), Err(crate::TantivyError::InvalidArgument(msg) ) if msg.contains("The heap size per thread cannot exceed"))
        );
    }
}

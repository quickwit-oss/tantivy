use crate::indexer::{SegmentManager, ResourceManager, MergeOperationInventory};
use std::thread::JoinHandle;
use crate::{IndexWriterConfig, SegmentId};
use std::collections::HashSet;

pub(crate) struct Persistor {
    memory_manager: ResourceManager,
    thread_handle: JoinHandle<()>,
}

impl Persistor {
    pub(crate) fn create_and_start(segment_manager: SegmentManager,
                            memory_manager: ResourceManager,
                            config: IndexWriterConfig) -> crate::Result<Persistor> {
        let memory_manager_clone = memory_manager.clone();
        let thread_handle = std::thread::Builder::new()
            .name("persistor-thread".to_string())
            .spawn(move || {
                while let Ok(_) = memory_manager_clone.wait_until_in_range(config.persist_low..) {
                    segment_manager.largest_segment_not_in_merge();
                }
            }).map_err(|_err| crate::TantivyError::ErrorInThread("Failed to start persistor thread.".to_string()))?;
        Ok(Persistor {
            memory_manager,
            thread_handle
        })
    }

    /// Stop the persisting thread.
    ///
    /// The memory manager will be terminated, which will unlock the thread from any waiting
    /// position.
    /// This method blocks for a short amount of tim until the persistor thread has terminated.
    pub fn stop(self) {
        self.memory_manager.terminate();
        let _ = self.thread_handle.join();
    }
}
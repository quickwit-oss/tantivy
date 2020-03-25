use std::ops::RangeBounds;
use std::sync::{Arc, Condvar, Mutex, MutexGuard, RwLock};

struct LockedData {
    count: u64,
    enabled: bool
}

impl Default for LockedData {
    fn default() -> Self {
        LockedData {
            count: 0u64,
            enabled: true
        }
    }
}

#[derive(Default)]
struct Inner {
    resource_level: Mutex<LockedData>,
    convdvar: Condvar,
}


/// The resource manager makes it possible to track the amount of level of a given resource.
/// There is no magic here : it is to the description of the user to declare how much
/// of the resource is being held.
///
/// Allocation of a resource is bound to the lifetime of a `Allocation` instance.
///
/// ```rust
/// let resource_manager = ResourceManager::default();
///
/// ```
///
/// In tantivy, this is used to check the number of merging thread and the number of memory
/// used by the volatile segments.
///
#[derive(Clone, Default)]
pub struct ResourceManager {
    inner: Arc<Inner>,
}

impl ResourceManager {
    /// Return the total amount of reousrce allocated
    pub fn total_amount(&self) -> u64 {
        self.lock().count
    }

    fn lock(&self) -> MutexGuard<LockedData> {
        self.inner
            .resource_level
            .lock()
            .expect("Failed to obtain lock for ReservedMemory. This should never happen.")
    }

    fn record_delta(&self, delta: i64) {
        if delta == 0i64 {
            return;
        }
        let mut lock = self.lock();
        let new_val = lock.count as i64 + delta;
        lock.count = new_val as u64;
        self.inner.convdvar.notify_all();
    }

    /// Records a new allocation.
    ///
    /// The returned allocate object is used to automatically release the allocated resource
    /// on drop.
    pub fn allocate(&self, amount: u64) -> Allocation {
        self.record_delta(amount as i64);
        Allocation {
            resource_manager: self.clone(),
            amount: RwLock::new(amount),
        }
    }

    /// Stops the resource manager.
    ///
    /// If any thread is waiting via `.wait_until_in_range(...)`, the method will stop
    /// being blocking and will return an error.
    pub fn terminate(&self) {
        self.lock().enabled = false;
        self.inner.convdvar.notify_all();
    }

    /// Blocks the current thread until the resource level reaches the given range,
    /// in a cpu-efficient way.
    ///
    /// This method does not necessarily wakes up the current thread at every transition
    /// into the targetted range, but any durable entry in the range will be detected.
    pub fn wait_until_in_range<R: RangeBounds<u64>>(&self, range: R) -> Result<u64, u64> {
        let mut levels = self.lock();
        if !levels.enabled {
            return Err(levels.count)
        }
        while !range.contains(&levels.count) {
           levels = self.inner.convdvar.wait(levels).unwrap();
            if !levels.enabled {
                return Err(levels.count)
            }
        }
        Ok(levels.count)
    }
}

pub struct Allocation {
    resource_manager: ResourceManager,
    amount: RwLock<u64>,
}

impl Allocation {
    pub fn amount(&self) -> u64 {
        *self.amount.read().unwrap()
    }

    pub fn modify(&self, new_amount: u64) {
        let mut wlock = self.amount.write().unwrap();
        let delta = new_amount as i64 - *wlock as i64;
        *wlock = new_amount;
        self.resource_manager.record_delta(delta);
    }
}

impl Drop for Allocation {
    fn drop(&mut self) {
        let amount = self.amount();
        self.resource_manager.record_delta(-(amount as i64))
    }
}

#[cfg(test)]
mod tests {
    use super::ResourceManager;
    use futures::channel::oneshot;
    use futures::executor::block_on;
    use std::{mem, thread};

    #[test]
    fn test_simple_allocation() {
        let memory = ResourceManager::default();
        assert_eq!(memory.total_amount(), 0u64);
        let _allocation = memory.allocate(10u64);
        assert_eq!(memory.total_amount(), 10u64);
    }

    #[test]
    fn test_multiple_allocation() {
        let memory = ResourceManager::default();
        assert_eq!(memory.total_amount(), 0u64);
        let _allocation = memory.allocate(10u64);
        let _allocation_2 = memory.allocate(11u64);
        assert_eq!(memory.total_amount(), 21u64);
    }

    #[test]
    fn test_release_on_drop() {
        let memory = ResourceManager::default();
        assert_eq!(memory.total_amount(), 0u64);
        let allocation = memory.allocate(10u64);
        let allocation_2 = memory.allocate(11u64);
        assert_eq!(memory.total_amount(), 21u64);
        mem::drop(allocation);
        assert_eq!(memory.total_amount(), 11u64);
        mem::drop(allocation_2);
        assert_eq!(memory.total_amount(), 0u64);
    }

    #[test]
    fn test_wait_until() {
        let memory = ResourceManager::default();
        let (send, recv) = oneshot::channel::<()>();
        let memory_clone = memory.clone();
        thread::spawn(move || {
            let _allocation1 = memory_clone.allocate(2u64);
            let _allocation2 = memory_clone.allocate(3u64);
            let _allocation3 = memory_clone.allocate(4u64);
            std::mem::drop(_allocation3);
            assert!(block_on(recv).is_ok());
        });
        assert_eq!(memory.wait_until_in_range(5u64..8u64), Ok(5u64));
        assert!(send.send(()).is_ok());
    }

    #[test]
    fn test_modify_amount() {
        let memory = ResourceManager::default();
        let alloc = memory.allocate(2u64);
        assert_eq!(memory.total_amount(), 2u64);
        assert_eq!(alloc.amount(), 2u64);
        let alloc2 = memory.allocate(3u64);
        assert_eq!(memory.total_amount(), 2u64 + 3u64);
        assert_eq!(alloc2.amount(), 3u64);
        alloc.modify(14u64);
        assert_eq!(alloc.amount(), 14u64);
        assert_eq!(memory.total_amount(), 14u64 + 3u64)
    }
    
    #[test]
    fn test_stop_resource_manager() {
        let resource_manager = ResourceManager::default();
        let resource_manager_clone = resource_manager.clone();
        let (sender, recv) = oneshot::channel();
        let join_handle = thread::spawn(move || {
            assert!(sender.send(()).is_ok());
            resource_manager_clone.wait_until_in_range(10..20)
        });
        let _ = block_on(recv);
        resource_manager.terminate();
        assert_eq!(join_handle.join().unwrap(), Err(0u64));
    }
}

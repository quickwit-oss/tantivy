#[derive(Debug, Clone, thiserror::Error)]
#[error("Failed to pin thread to CPU set")]
/// An error that occurred while attempting to pin a thread to a specific CPU set.
pub struct SetAffinityError(String);
type Result<T> = std::result::Result<T, SetAffinityError>;

#[derive(Default, Clone, Debug)]
/// Pin threads to a specific set of CPU cores.
///
/// This can allow for better performance tuning by containing indexing threads
/// and search threads to separate parts of the CPU.
pub struct ThreadAffinity {
    cpu_set: Option<Vec<usize>>,
}

impl ThreadAffinity {
    /// Returns the number of CPU cores available.
    pub fn num_cores(&self) -> usize {
        num_cpus::get()
    }

    /// Resets the current affinity configuration.
    ///
    /// This means any thread using this configuration can be put on any CPU cores.
    ///
    /// **NOTE:**
    /// This method does not change the affinity of already running threads.
    pub fn reset_affinity(&mut self) {
        self.cpu_set = None;
    }

    /// Set the thread affinity to a specific set of CPU cores.
    ///
    /// The core IDs iterator must not be empty and must be an ID which actually
    /// exists on the running machine.
    ///
    /// **NOTE:**
    /// This method does not change the affinity of already running threads.
    pub fn set_affinity(&mut self, core_ids: impl IntoIterator<Item = usize>) -> Result<()> {
        let max_id = num_cpus::get() - 1;

        let mut cpu_set = Vec::new();
        for core_id in core_ids {
            if core_id > max_id {
                return Err(SetAffinityError(format!(
                    "Invalid core ID provided: {core_id}"
                )));
            }

            cpu_set.push(core_id);
        }

        if cpu_set.is_empty() {
            return Err(SetAffinityError(
                "An empty CPU set cannot be used".to_string(),
            ));
        }

        self.cpu_set = Some(cpu_set);

        Ok(())
    }

    /// Configures the **curent** thread's affinity.
    pub(crate) fn configure_current_thread(&self) -> Result<()> {
        if let Some(cpu_set) = self.cpu_set.as_ref() {
            os_impl::set_thread_affinity(cpu_set)?;
        }
        Ok(())
    }
}

#[cfg(target_os = "linux")]
mod os_impl {
    use super::{Result, SetAffinityError};

    /// Sets the **current** thread affinity.
    pub fn set_thread_affinity(core_ids: &[usize]) -> Result<()> {
        let mut cpu_set = nix::sched::CpuSet::new();
        for cpu in cpus {
            cpu_set.set(cpu)?;
        }
        let pid = nix::unistd::Pid::from_raw(0);
        nix::sched::sched_setaffinity(pid, &cpu_set)
    }
}

// MacOS affinity is a bit weird
#[cfg(not(any(windows, target_os = "linux")))]
mod os_impl {
    /// Sets the **current** thread affinity.
    ///
    /// NOTE: This is a no-op on unsupported platforms.
    pub fn set_thread_affinity(_core_ids: &[usize]) -> Result<()> {
        Ok(())
    }
}

#[cfg(windows)]
mod os_impl {
    use std::ffi::c_void;

    use super::{Result, SetAffinityError};

    extern "system" {
        // Present in all windows processes (kernel32.dll)
        fn GetLastError() -> u32;
        fn GetCurrentThread() -> *mut c_void;

        fn SetThreadAffinityMask(thread_handle: *mut c_void, mask: usize) -> usize;
    }

    /// Sets the **current** thread affinity.
    pub fn set_thread_affinity(core_ids: &[usize]) -> Result<()> {
        if core_ids.is_empty() {
            return Err(SetAffinityError(
                "Cannot pin thread to empty CPU set".to_string(),
            ));
        }

        // Technically we can support more than 64 cores, but it's a fair bit of extra work
        // and should be a very very rare situation to ever run into on a windows system.
        if core_ids.iter().max().copied() > Some(64) {
            return Err(SetAffinityError(
                "Cannot pin thread to CPU set with more than 64 cores".to_string(),
            ));
        }

        let cur_thread = unsafe { GetCurrentThread() };
        let mut wanted_mask = 0usize;

        // Create the bitmask
        for core_id in core_ids {
            wanted_mask |= 1usize << core_id;
        }

        trace!("Binding thread to CPU set {core_ids:?}");

        if let Err(last_error) = set_thread_affinity_mask(cur_thread, wanted_mask) {
            return Err(SetAffinityError(format!(
                "SetThreadAffinityMask failed with error 0x{:x}",
                last_error
            )));
        }

        Ok(())
    }

    // Wrappers around unsafe OS calls
    fn set_thread_affinity_mask(
        thread: *mut c_void,
        mask: usize,
    ) -> std::result::Result<usize, u32> {
        let res = unsafe { SetThreadAffinityMask(thread, mask) };
        if res == 0 {
            return Err(unsafe { GetLastError() });
        }
        Ok(res)
    }
}

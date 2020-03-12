// Should be a struct

// Should take a file

// Should send a notification if the file has changed.

/*

std::fs::metadata can be used to check metadata.

*/

struct PollWatcher {
    watcher_router: Arc<WatchCallbackList>,
}

impl PollWatcher {

    pub fn run() {
        // while true

            // poll the meta file.

            // META_FILEPATH

            // if the metadata has changed.
                
                // watcher_router.broadcast
    }

    pub fn poll_meta() {
        // Should check the last updated date of the metadata file.
    }

    pub fn watch(&mut self, watch_callback: WatchCallback) -> WatchHandle {
        self.watcher_router.subscribe(watch_callback)
    }

}


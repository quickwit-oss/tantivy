use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};

use super::AddBatchReceiver;

// Quickwit
//  Copyright (C) 2021 Quickwit Inc.
//
//  Quickwit is offered under the AGPL v3.0 and as commercial software.
//  For commercial licensing, contact us at hello@quickwit.io.
//
//  AGPL:
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU Affero General Public License as
//  published by the Free Software Foundation, either version 3 of the
//  License, or (at your option) any later version.
//
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU Affero General Public License for more details.
//
//  You should have received a copy of the GNU Affero General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

#[derive(Clone)]
pub(crate) struct IndexWriterStatus {
    inner: Arc<Inner>,
}

impl IndexWriterStatus {
    /// Returns a copy of the operation receiver.
    /// If the index writer was killed, returns None.
    pub fn operation_receiver(&self) -> Option<AddBatchReceiver> {
        let rlock = self
            .inner
            .receive_channel
            .read()
            .expect("This lock should never be poisoned");
        rlock.as_ref().map(|receiver| receiver.clone())
    }

    /// Create an index writer bomb.
    /// If dropped, the index writer status will be killed.
    pub(crate) fn create_bomb(&self) -> IndexWriterBomb {
        IndexWriterBomb {
            inner: Some(self.inner.clone()),
        }
    }
}

struct Inner {
    is_alive: AtomicBool,
    receive_channel: RwLock<Option<AddBatchReceiver>>,
}

impl Inner {
    fn is_alive(&self) -> bool {
        self.is_alive.load(Ordering::Relaxed)
    }

    fn kill(&self) {
        self.is_alive.store(false, Ordering::Relaxed);
        self.receive_channel
            .write()
            .expect("This lock should never be poisoned")
            .take();
    }
}

impl From<AddBatchReceiver> for IndexWriterStatus {
    fn from(receiver: AddBatchReceiver) -> Self {
        IndexWriterStatus {
            inner: Arc::new(Inner {
                is_alive: AtomicBool::new(true),
                receive_channel: RwLock::new(Some(receiver)),
            }),
        }
    }
}

/// If dropped, the index writer will be killed.
/// To prevent this, clients can call `.defuse()`.
pub(crate) struct IndexWriterBomb {
    inner: Option<Arc<Inner>>,
}

impl IndexWriterBomb {
    /// Returns true iff the index writer is alive.
    pub fn is_alive(&self) -> bool {
        self.inner
            .as_ref()
            .map(|inner| inner.is_alive())
            .unwrap_or(false)
    }

    /// Defuses the bomb.
    ///
    /// This is the only way to drop the bomb without killing
    /// the index writer.
    pub fn defuse(mut self) {
        self.inner = None;
    }
}

impl Drop for IndexWriterBomb {
    fn drop(&mut self) {
        if let Some(inner) = self.inner.take() {
            inner.kill();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::IndexWriterStatus;
    use crossbeam::channel;
    use std::mem;

    #[test]
    fn test_bomb_goes_boom() {
        let (_tx, rx) = channel::bounded(10);
        let index_writer_status: IndexWriterStatus = IndexWriterStatus::from(rx);
        assert!(index_writer_status.operation_receiver().is_some());
        let bomb = index_writer_status.create_bomb();
        assert!(index_writer_status.operation_receiver().is_some());
        mem::drop(bomb);
        // boom!
        assert!(index_writer_status.operation_receiver().is_none());
    }

    #[test]
    fn test_bomb_defused() {
        let (_tx, rx) = channel::bounded(10);
        let index_writer_status: IndexWriterStatus = IndexWriterStatus::from(rx);
        assert!(index_writer_status.operation_receiver().is_some());
        let bomb = index_writer_status.create_bomb();
        bomb.defuse();
        assert!(index_writer_status.operation_receiver().is_some());
    }
}

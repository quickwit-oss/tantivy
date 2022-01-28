use futures::executor::block_on;

use super::IndexWriter;
use crate::Opstamp;

/// A prepared commit
pub struct PreparedCommit<'a> {
    index_writer: &'a mut IndexWriter,
    payload: Option<String>,
    opstamp: Opstamp,
}

impl<'a> PreparedCommit<'a> {
    pub(crate) fn new(index_writer: &'a mut IndexWriter, opstamp: Opstamp) -> PreparedCommit<'_> {
        PreparedCommit {
            index_writer,
            payload: None,
            opstamp,
        }
    }

    /// Returns the opstamp associated to the prepared commit.
    pub fn opstamp(&self) -> Opstamp {
        self.opstamp
    }

    /// Adds an arbitrary payload to the commit.
    pub fn set_payload(&mut self, payload: &str) {
        self.payload = Some(payload.to_string())
    }

    /// Rollbacks any change.
    pub fn abort(self) -> crate::Result<Opstamp> {
        self.index_writer.rollback()
    }

    /// Proceeds to commit.
    /// See `.commit_async()`.
    pub fn commit(self) -> crate::Result<Opstamp> {
        block_on(self.commit_async())
    }

    /// Proceeds to commit.
    ///
    /// Unfortunately, contrary to what `PrepareCommit` may suggests,
    /// this operation is not at all really light.
    /// At this point deletes have not been flushed yet.
    pub async fn commit_async(self) -> crate::Result<Opstamp> {
        info!("committing {}", self.opstamp);
        self.index_writer
            .segment_updater()
            .schedule_commit(self.opstamp, self.payload)
            .await?;
        Ok(self.opstamp)
    }
}

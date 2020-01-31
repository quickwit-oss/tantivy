use super::IndexWriter;
use crate::Opstamp;
use futures::executor::block_on;

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

    pub fn opstamp(&self) -> Opstamp {
        self.opstamp
    }

    pub fn set_payload(&mut self, payload: &str) {
        self.payload = Some(payload.to_string())
    }

    pub fn abort(self) -> crate::Result<Opstamp> {
        self.index_writer.rollback()
    }

    pub fn commit(self) -> crate::Result<Opstamp> {
        info!("committing {}", self.opstamp);
        let _ = block_on(
            self.index_writer
                .segment_updater()
                .schedule_commit(self.opstamp, self.payload),
        );
        Ok(self.opstamp)
    }
}

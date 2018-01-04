use Result;
use super::IndexWriter;

/// A prepared commit
pub struct PreparedCommit<'a> {
    index_writer: &'a mut IndexWriter,
    payload: Option<String>,
    opstamp: u64,
}

impl<'a> PreparedCommit<'a> {
    pub(crate) fn new(index_writer: &'a mut IndexWriter, opstamp: u64) -> PreparedCommit {
        PreparedCommit {
            index_writer: index_writer,
            payload: None,
            opstamp: opstamp,
        }
    }

    pub fn opstamp(&self) -> u64 {
        self.opstamp
    }

    pub fn set_payload(&mut self, payload: &str) {
        self.payload = Some(payload.to_string())
    }

    pub fn abort(self) -> Result<()> {
        self.index_writer.rollback()
    }

    pub fn commit(self) -> Result<u64> {
        info!("committing {}", self.opstamp);
        self.index_writer
            .segment_updater()
            .commit(self.opstamp, self.payload)?;
        Ok(self.opstamp)
    }
}

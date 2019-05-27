use super::IndexWriter;
use Opstamp;
use Result;

/// A prepared commit
pub struct PreparedCommit<'a> {
    index_writer: &'a mut IndexWriter,
    payload: Option<String>,
    opstamp: Opstamp,
}

impl<'a> PreparedCommit<'a> {
    pub(crate) fn new(index_writer: &'a mut IndexWriter, opstamp: Opstamp) -> PreparedCommit {
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

    pub fn abort(self) -> Result<Opstamp> {
        self.index_writer.rollback()
    }

    pub fn commit(self) -> Result<Opstamp> {
        info!("committing {}", self.opstamp);
        self.index_writer
            .segment_updater()
            .commit(self.opstamp, self.payload)?;
        Ok(self.opstamp)
    }
}

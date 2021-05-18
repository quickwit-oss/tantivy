use std::{ops::Deref, sync::Arc};

use tantivy_fst::{FakeArr, FakeArrPart, ShRange};

use super::FileHandle;

pub type OnDemandBox = Box<dyn FakeArr + Sync>;
#[derive(Debug)]
pub struct OnDemandBytes {
    file: Arc<dyn FileHandle>
}

impl OnDemandBytes {
    pub fn new(fh: Arc<dyn FileHandle>) -> OnDemandBytes {
        OnDemandBytes {
            file: fh
        }
    }
}
impl FakeArr for OnDemandBytes {
    fn len(&self) -> usize {
        self.file.len()
    }

    fn read_into(&self, offset: usize, buf: &mut [u8]) -> std::io::Result<()> {
        let bytes = self.file.read_bytes(offset, offset + buf.len())?;
        buf.copy_from_slice(&bytes[..]);
        Ok(())
    }

    fn as_dyn(&self) -> &dyn FakeArr {
        self
    }
}

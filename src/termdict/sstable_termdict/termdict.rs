use sstable::SSTable;

use crate::postings::TermInfo;
use crate::termdict::sstable_termdict::{TermInfoReader, TermInfoWriter};

pub struct TermInfoSSTable;
impl SSTable for TermInfoSSTable {
    type Value = TermInfo;
    type ValueReader = TermInfoReader;
    type ValueWriter = TermInfoWriter;
}

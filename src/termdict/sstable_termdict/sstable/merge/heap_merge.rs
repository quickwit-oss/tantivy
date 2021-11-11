use std::cmp::Ordering;
use std::collections::binary_heap::PeekMut;
use std::collections::BinaryHeap;
use std::io;

use super::{SingleValueMerger, ValueMerger};
use crate::termdict::sstable_termdict::sstable::{Reader, SSTable, Writer};

struct HeapItem<B: AsRef<[u8]>>(B);

impl<B: AsRef<[u8]>> Ord for HeapItem<B> {
    fn cmp(&self, other: &Self) -> Ordering {
        other.0.as_ref().cmp(self.0.as_ref())
    }
}
impl<B: AsRef<[u8]>> PartialOrd for HeapItem<B> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(other.0.as_ref().cmp(self.0.as_ref()))
    }
}

impl<B: AsRef<[u8]>> Eq for HeapItem<B> {}
impl<B: AsRef<[u8]>> PartialEq for HeapItem<B> {
    fn eq(&self, other: &Self) -> bool {
        self.0.as_ref() == other.0.as_ref()
    }
}

#[allow(dead_code)]
pub fn merge_sstable<SST: SSTable, W: io::Write, M: ValueMerger<SST::Value>>(
    readers: Vec<Reader<SST::Reader>>,
    mut writer: Writer<W, SST::Writer>,
    mut merger: M,
) -> io::Result<()> {
    let mut heap: BinaryHeap<HeapItem<Reader<SST::Reader>>> =
        BinaryHeap::with_capacity(readers.len());
    for mut reader in readers {
        if reader.advance()? {
            heap.push(HeapItem(reader));
        }
    }
    loop {
        let len = heap.len();
        let mut value_merger;
        if let Some(mut head) = heap.peek_mut() {
            writer.write_key(head.0.key());
            value_merger = merger.new_value(head.0.value());
            if !head.0.advance()? {
                PeekMut::pop(head);
            }
        } else {
            break;
        }
        for _ in 0..len - 1 {
            if let Some(mut head) = heap.peek_mut() {
                if head.0.key() == writer.current_key() {
                    value_merger.add(head.0.value());
                    if !head.0.advance()? {
                        PeekMut::pop(head);
                    }
                    continue;
                }
            }
            break;
        }
        let value = value_merger.finish();
        writer.write_value(&value)?;
        writer.flush_block_if_required()?;
    }
    writer.finalize()?;
    Ok(())
}

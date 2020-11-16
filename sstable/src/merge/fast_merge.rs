use {SSTable, Reader};
use std::io;
use merge::{ValueMerger, SingleValueMerger};
use Writer;
use std::collections::BinaryHeap;
use std::cmp::Ordering;
use std::cmp::Ord;
use std::option::Option::None;
use std::mem;
use std::fmt::Debug;
use common_prefix_len;

fn pick_lowest_with_ties<'a, 'b, T, FnKey: Fn(&'b T)->K, K>(elements: &'b [T], key: FnKey, ids: &'a mut [usize]) -> (&'a [usize], &'a [usize])
    where
        FnKey: Fn(&'b T)->K,
        K: Ord + Debug + 'b {
    debug_assert!(!ids.is_empty());
    if ids.len() <= 1 {
        return (ids, &[]);
    }
    let mut smallest_key = key(&elements[ids[0]]);
    let mut num_ties = 1;
    for i in 1..ids.len() {
        let cur = ids[i];
        let cur_key = key(&elements[cur]);
        match cur_key.cmp(&smallest_key) {
            Ordering::Less => {
                ids.swap(i, 0);
                smallest_key = cur_key;
                num_ties = 1;
            }
            Ordering::Equal => {
                ids.swap(i, num_ties);
                num_ties += 1;
            }
            Ordering::Greater => {}
        }
    }
    (&ids[..num_ties], &ids[num_ties..])
}


#[derive(Clone, Copy, Debug)]
struct HeapItem(pub u32);

impl HeapItem  {
    fn new(common_prefix_len: u32, next_byte: u8) -> Self {
        HeapItem(common_prefix_len << 8 | (next_byte as u32))
    }

    fn common_prefix_len(&self) -> usize {
        self.0 as usize >> 8
    }
}

struct Queue {
    queue: BinaryHeap<u32>,
    map: Vec<Vec<usize>>,
    spares: Vec<Vec<usize>>,
}


fn heap_item_to_id(heap_item: &HeapItem) -> usize {
    heap_item.0 as usize
}

impl Queue {

    // helper to trick the borrow checker.
    fn push_to_queue(heap_item: HeapItem,
                     idx: usize,
                     queue: &mut BinaryHeap<u32>,
                     map: &mut Vec<Vec<usize>>,
                     spares: &mut Vec<Vec<usize>>) {
        let heap_id = heap_item_to_id(&heap_item);
        let ids = &mut map[heap_id];
        if ids.is_empty() {
            queue.push(heap_item.0);
            *ids = spares.pop().unwrap_or_else(Vec::new);
        }
        ids.push(idx);
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Queue {
            queue: BinaryHeap::with_capacity(capacity),
            map: (0..256 * 100).map(|_| Vec::new()).collect::<Vec<Vec<usize>>>(),
            spares: (0..capacity).map(|_| Vec::with_capacity(capacity)).collect()
        }
    }

    pub fn register(&mut self, common_prefix_len: u32, next_byte: u8, idx: usize) {
        let heap_item = HeapItem::new(common_prefix_len, next_byte);
        Queue::push_to_queue(heap_item, idx, &mut self.queue, &mut self.map, &mut self.spares);
    }

    pub fn pop(&mut self, dest: &mut Vec<usize>) -> Option<HeapItem> {
        dest.clear();
        self.queue
            .pop()
            .map(|heap_item| {
                dest.clear();
                let idx = mem::replace(&mut self.map[heap_item as usize], Vec::new());
                self.spares.push(mem::replace(dest,idx));
                HeapItem(heap_item)
        })
    }
}

pub fn merge_sstable<SST: SSTable, W: io::Write, M: ValueMerger<SST::Value>>(
    unstarted_readers: Vec<Reader<SST::Reader>>,
    writer: Writer<W, SST::Writer>,
    mut merger: M
) -> io::Result<()> {
    let mut delta_writer = writer.into_delta_writer();
    let mut readers = vec![];
    let mut empty_key_values: Option<M::TSingleValueMerger> = None;
    for reader in unstarted_readers {
        let mut delta_reader = reader.into_delta_reader();
        if delta_reader.advance()? {
            if delta_reader.suffix().is_empty() {
                if let Some(value_merger) = empty_key_values.as_mut() {
                    value_merger.add(delta_reader.value());
                } // the borrow checker does not allow an else here... that's a bit lame.
                if empty_key_values.is_none() {
                    empty_key_values = Some(merger.new_value(delta_reader.value()));
                }
                if delta_reader.advance()? {
                    // duplicate keys are forbidden.
                    assert!(!delta_reader.suffix().is_empty());
                    readers.push(delta_reader);
                }
            } else {
                readers.push(delta_reader);
            }
        }
    }
    if let Some(value_merger) = empty_key_values {
        delta_writer.write_delta(0, &[], &value_merger.finish())?;
    }

    let mut queue = Queue::with_capacity(readers.len());

    for (idx, delta_reader) in readers.iter().enumerate() {
        queue.register(0u32, delta_reader.suffix()[0], idx);
    }

    let mut current_ids = Vec::with_capacity(readers.len());
    while let Some(heap_item) = queue.pop(&mut current_ids) {
        debug_assert!(!current_ids.is_empty());
        let (tie_ids, others) = pick_lowest_with_ties(
            &readers[..],
            |reader| reader.suffix_from(heap_item.common_prefix_len()),
            &mut current_ids[..]);
        {
            let first_reader = &readers[tie_ids[0]];
            let suffix = first_reader.suffix_from(heap_item.common_prefix_len());
            if tie_ids.len() > 1 {
                let mut single_value_merger = merger.new_value(first_reader.value());
                for &min_tie_id in &tie_ids[1..] {
                    single_value_merger.add(readers[min_tie_id].value());
                }
                delta_writer.write_delta(heap_item.common_prefix_len(),
                                         suffix,
                                         &single_value_merger.finish())?;
            } else {
                delta_writer.write_delta(heap_item.common_prefix_len(),
                                         suffix,
                                         first_reader.value())?;
            }
            for &reader_id in others {
                let reader = &readers[reader_id];
                let reader_suffix = reader.suffix_from(heap_item.common_prefix_len());
                let extra_common_prefix_len = common_prefix_len(reader_suffix, suffix);
                let next_byte = reader_suffix[extra_common_prefix_len];
                queue.register(heap_item.common_prefix_len() as u32 + extra_common_prefix_len as u32, next_byte, reader_id)
            }
        }
        for &tie_id in tie_ids {
            let reader = &mut readers[tie_id];
            if reader.advance()? {
                queue.register(reader.common_prefix_len() as u32, reader.suffix()[0], tie_id);
            }
        }
    }
    delta_writer.finalize()?;
    Ok(())
}



#[cfg(test)]
mod tests {
    use super::pick_lowest_with_ties;

    #[test]
    fn test_pick_lowest_with_ties() {
        {
            let mut ids = [0,1,3,2,5,4];
            assert_eq!(pick_lowest_with_ties(&[1,4,3,7,1,3,5], |el| *el, &mut ids),
                       (&[0,4][..], &[3,2,5,1][..]));
        }
        {
            let mut ids = [5,3,2,1,4];
            assert_eq!(pick_lowest_with_ties(&[1,4,3,7,1,3,5], |el| *el, &mut ids),
                       (&[4][..], &[2,3,1,5][..]));
        }
    }
}
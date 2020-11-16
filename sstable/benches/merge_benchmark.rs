#[macro_use]
extern crate criterion;
extern crate rand;
extern crate sstable;

use sstable::{SSTable, VoidSSTable};
use criterion::Criterion;
use rand::prelude::*;

use std::collections::BTreeSet;
use sstable::VoidMerge;

const NUM_SSTABLE: usize = 18;

fn generate_key(rng: &mut StdRng) -> String {
    let len = rng.gen_range(5, 10);
    (0..len)
        .map(|_| {
            let b = rng.gen_range(96u8, 96 + 16u8);
            char::from(b)
        })
        .collect::<String>()
}

fn create_sstables() -> Vec<Vec<u8>> {
    let mut keyset = BTreeSet::new();
    let seed = [1u8; 32];
    let mut rnd = StdRng::from_seed(seed);
    while keyset.len() < 10_000 {
        keyset.insert(generate_key(&mut rnd));
    }
    let mut buffers = (0..NUM_SSTABLE).map(|_| Vec::new()).collect::<Vec<Vec<u8>>>();
    {
        let mut writers: Vec<_> = buffers.iter_mut().map(VoidSSTable::writer).collect();
        for key in keyset {
            for writer in &mut writers {
                if rnd.gen_bool(0.2) {
                    writer.write(key.as_bytes(), &()).unwrap();
                }
            }
        }
        for writer in writers {
            writer.finalize().unwrap();
        }
    }
    buffers
}

fn merge_fast(buffers: &[Vec<u8>]) {
    let readers: Vec<&[u8]> = buffers.iter().map(|buf| &buf[..]).collect::<Vec<_>>();
    let mut buffer = Vec::with_capacity(10_000_000);
    assert!(VoidSSTable::merge(readers, &mut buffer, VoidMerge).is_ok());
}

fn criterion_benchmark(c: &mut Criterion) {
    let buffers = create_sstables();
    c.bench_function("Merge fast", move |b| b.iter(|| merge_fast(&buffers)));
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
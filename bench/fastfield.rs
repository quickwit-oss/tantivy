use criterion::criterion_group;
use criterion::criterion_main;
use criterion::Criterion;
use criterion::ParameterizedBenchmark;
use std::iter;
use tantivy::schema::{Schema, FAST};
use tantivy::{doc, DocId, Index};

const NUM_LOOKUPS: usize = 1_000;

fn generate_data(stride: usize, b: u8) -> Vec<u64> {
    let mut permutation: Vec<u64> = iter::repeat(0u64).take(NUM_LOOKUPS * stride).collect();
    permutation[0] = 1u64 << (b as u64);
    permutation
}

fn bench_linear_lookup(c: &mut Criterion) {
    c.bench(
        "lookup_stride",
        ParameterizedBenchmark::new(
            "baseline_vec",
            |bench, (stride, num_bits)| {
                let arr = generate_data(*stride, *num_bits);
                bench.iter(move || {
                    let mut a = 0u64;
                    for i in (0..NUM_LOOKUPS / stride).map(|v| v * 7) {
                        a ^= arr[i as usize];
                    }
                    a
                })
            },
            vec![(7, 1), (7, 5), (7, 20)],
        )
        .with_function("fastfield", |bench, (stride, num_bits)| {
            let mut schema_builder = Schema::builder();
            let val_field = schema_builder.add_u64_field("val", FAST);
            let schema = schema_builder.build();

            let index = Index::create_in_ram(schema);
            let mut index_writer = index.writer_with_num_threads(1, 80_000_000).unwrap();
            for el in generate_data(*stride, *num_bits) {
                index_writer.add_document(doc!(val_field=>el));
            }
            index_writer.commit().unwrap();
            let reader = index.reader().unwrap();
            let searcher = reader.searcher();
            let segment_reader = searcher.segment_reader(0u32);
            let fast_field_reader = segment_reader.fast_fields().u64(val_field).unwrap();
            bench.iter(move || {
                let mut a = 0u64;
                for i in (0..NUM_LOOKUPS / stride).map(|v| v * 7) {
                    a ^= fast_field_reader.get(i as DocId);
                }
                a
            })
        }),
    );
}

criterion_group!(benches, bench_linear_lookup);
criterion_main!(benches);

/*
fn bench_intfastfield_veclookup(b: &mut Bencher) {
    let permutation = generate_permutation();
    b.iter(|| {
        let n = test::black_box(1000u32);
        let mut a = 0u64;
        for _ in 0u32..n {
            a = permutation[a as usize];
        }
        a
    });
}

#[bench]
fn bench_intfastfield_linear_fflookup(b: &mut Bencher) {
    let path = Path::new("test");
    let permutation = generate_permutation();
    let mut directory: RAMDirectory = RAMDirectory::create();
    {
        let write: WritePtr = directory.open_write(Path::new("test")).unwrap();
        let mut serializer = FastFieldSerializer::from_write(write).unwrap();
        let mut fast_field_writers = FastFieldsWriter::from_schema(&SCHEMA);
        for &x in &permutation {
            fast_field_writers.add_document(&doc!(*FIELD=>x));
        }
        fast_field_writers
            .serialize(&mut serializer, &HashMap::new())
            .unwrap();
        serializer.close().unwrap();
    }
    let source = directory.open_read(&path).unwrap();
    {
        let fast_fields_composite = CompositeFile::open(&source).unwrap();
        let data = fast_fields_composite.open_read(*FIELD).unwrap();
        let fast_field_reader = FastFieldReader::<u64>::open(data);

        b.iter(|| {
            let n = test::black_box(7000u32);
            let mut a = 0u64;
            for i in (0u32..n / 7).map(|val| val * 7) {
                a ^= fast_field_reader.get(i);
            }
            a
        });
    }
}

#[bench]
fn bench_intfastfield_fflookup(b: &mut Bencher) {
    let path = Path::new("test");
    let permutation = generate_permutation();
    let mut directory: RAMDirectory = RAMDirectory::create();
    {
        let write: WritePtr = directory.open_write(Path::new("test")).unwrap();
        let mut serializer = FastFieldSerializer::from_write(write).unwrap();
        let mut fast_field_writers = FastFieldsWriter::from_schema(&SCHEMA);
        for &x in &permutation {
            fast_field_writers.add_document(&doc!(*FIELD=>x));
        }
        fast_field_writers
            .serialize(&mut serializer, &HashMap::new())
            .unwrap();
        serializer.close().unwrap();
    }
    let source = directory.open_read(&path).unwrap();
    {
        let fast_fields_composite = CompositeFile::open(&source).unwrap();
        let data = fast_fields_composite.open_read(*FIELD).unwrap();
        let fast_field_reader = FastFieldReader::<u64>::open(data);

        b.iter(|| {
            let n = test::black_box(1000u32);
            let mut a = 0u32;
            for _ in 0u32..n {
                a = fast_field_reader.get(a) as u32;
            }
            a
        });
    }
}
*/

use std::sync::Arc;

use binggan::{InputGroup, black_box};
use common::{DateTime, OwnedBytes};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use tantivy_columnar::column_values::{
    CodecType, load_u64_based_column_values, serialize_and_load_u64_based_column_values,
    serialize_u64_based_column_values,
};
use tantivy_columnar::{ColumnValues, MonotonicallyMappableToU64};

fn get_data() -> Vec<u64> {
    let mut rng = StdRng::seed_from_u64(2u64);
    let mut data: Vec<_> = (100..55_000_u64)
        .map(|num| num + rng.random::<u8>() as u64)
        .collect();
    data.push(99_000);
    data.insert(1000, 2000);
    data.insert(2000, 100);
    data.insert(3000, 4100);
    data.insert(4000, 100);
    data.insert(5000, 800);
    data
}

#[inline(never)]
fn value_iter() -> impl Iterator<Item = u64> {
    0..20_000
}

type Col = Arc<dyn ColumnValues<u64>>;

fn blockwise_columns<T: MonotonicallyMappableToU64>(
    values: &[T],
    v1_bytes: &[u8],
) -> Vec<(String, Arc<dyn ColumnValues<T>>)> {
    let mut bytes = Vec::new();
    serialize_u64_based_column_values(&values, &[CodecType::BlockwiseLinearV2], &mut bytes)
        .unwrap();
    [v1_bytes.to_vec(), bytes]
        .into_iter()
        .enumerate()
        .map(|(version, bytes)| {
            let column = load_u64_based_column_values::<T>(OwnedBytes::new(bytes)).unwrap();
            assert_eq!(column.iter().collect::<Vec<_>>(), values);
            let mut output = values.to_vec();
            column.get_range(0, &mut output);
            assert_eq!(output, values);
            (format!("blockwise_linear_v{}", version + 1), column)
        })
        .collect()
}

fn bench_ranges<T: MonotonicallyMappableToU64>(name: &str, values: &[T], v1_bytes: &[u8]) {
    let mut inputs = blockwise_columns(values, v1_bytes);
    for codec in [CodecType::Bitpacked, CodecType::Linear] {
        let mut bytes = Vec::new();
        serialize_u64_based_column_values(&values, &[codec], &mut bytes).unwrap();
        inputs.push((
            format!("{codec:?}"),
            load_u64_based_column_values::<T>(OwnedBytes::new(bytes)).unwrap(),
        ));
    }
    let mut group = InputGroup::new_with_inputs(inputs);
    group.register(
        format!("get_range/{name}/64"),
        |column: &Arc<dyn ColumnValues<T>>| {
            let mut output = [column.get_val(0); 64];
            for start in (0..20_000).step_by(64) {
                column.get_range(start, &mut output);
                black_box(&output);
            }
        },
    );
    group.run();
}

fn main() {
    let data = get_data();
    let mut inputs: Vec<(String, Col)> = vec![
        (
            "bitpacked".to_string(),
            serialize_and_load_u64_based_column_values(&data.as_slice(), &[CodecType::Bitpacked]),
        ),
        (
            "linear".to_string(),
            serialize_and_load_u64_based_column_values(&data.as_slice(), &[CodecType::Linear]),
        ),
    ];
    let v1_u64 = include_bytes!("fixtures/blockwise_linear_v1_u64.bin");
    inputs.extend(blockwise_columns(&data, v1_u64));

    let mut group: InputGroup<Col> = InputGroup::new_with_inputs(inputs);

    group.register("fastfield_get", |col: &Col| {
        let mut sum = 0u64;
        for pos in value_iter() {
            sum = sum.wrapping_add(col.get_val(pos as u32));
        }
        black_box(sum);
    });

    group.register("fastfield_get_random", |col: &Col| {
        let mut sum = 0u64;
        for pos in value_iter() {
            sum = sum.wrapping_add(col.get_val((pos * 7919 % col.num_vals() as u64) as u32));
        }
        black_box(sum);
    });
    group.run();

    bench_ranges("u64", &data, v1_u64);
    let signed: Vec<i64> = data.iter().map(|&value| value as i64 - 30_000).collect();
    bench_ranges(
        "i64",
        &signed,
        include_bytes!("fixtures/blockwise_linear_v1_i64.bin"),
    );
    let floats: Vec<f64> = signed.iter().map(|&value| value as f64 / 10.0).collect();
    bench_ranges(
        "f64",
        &floats,
        include_bytes!("fixtures/blockwise_linear_v1_f64.bin"),
    );
    let dates: Vec<DateTime> = signed
        .iter()
        .map(|&value| DateTime::from_timestamp_nanos(1_700_000_000_000_000_000 + value * 1000))
        .collect();
    bench_ranges(
        "datetime",
        &dates,
        include_bytes!("fixtures/blockwise_linear_v1_datetime.bin"),
    );

    let mut inputs = Vec::new();
    for len in [512, 65_536, 1_048_576] {
        let vals: Vec<u64> = (0..len).map(|i| i + (i % 17)).collect();
        let mut bytes = Vec::new();
        serialize_u64_based_column_values(
            &vals.as_slice(),
            &[CodecType::BlockwiseLinearV2],
            &mut bytes,
        )
        .unwrap();
        inputs.push((format!("BlockwiseLinearV2/{len}"), OwnedBytes::new(bytes)));
    }
    let mut group: InputGroup<OwnedBytes> = InputGroup::new_with_inputs(inputs);
    group.register("open", |bytes: &OwnedBytes| {
        black_box(load_u64_based_column_values::<u64>(bytes.clone()).unwrap());
    });
    group.run();
}

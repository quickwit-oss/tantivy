use binggan::{InputGroup, black_box};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use tantivy_columnar::MonotonicallyMappableToU64;
use tantivy_columnar::column_values::{CodecType, serialize_u64_based_column_values};

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

/// Decimal values, in the u64 bit mapping the codecs see: `Alp` refuses
/// integer data. Its serialization searches for an exponent per block, which
/// is the reason to watch its write side separately.
fn get_decimal_data() -> Vec<u64> {
    let mut rng = StdRng::seed_from_u64(3u64);
    (0..55_000)
        .map(|_| f64::to_u64(rng.random_range(0..10_000_000) as f64 / 100.0))
        .collect()
}

fn inputs(
    data: &[u64],
    codecs: &[(&str, CodecType)],
) -> Vec<(String, (CodecType, Vec<u64>))> {
    codecs
        .iter()
        .map(|(name, codec)| (name.to_string(), (*codec, data.to_vec())))
        .collect()
}

fn main() {
    for (data, codecs) in [
        (
            get_data(),
            &[
                ("bitpacked codec", CodecType::Bitpacked),
                ("linear codec", CodecType::Linear),
                ("blockwise linear codec", CodecType::BlockwiseLinear),
                ("block_for codec", CodecType::BlockFor),
            ][..],
        ),
        (
            get_decimal_data(),
            &[
                ("bitpacked codec dec", CodecType::Bitpacked),
                ("linear codec dec", CodecType::Linear),
                ("blockwise linear codec dec", CodecType::BlockwiseLinear),
                ("block_for codec dec", CodecType::BlockFor),
            ][..],
        ),
    ] {
        let mut group: InputGroup<(CodecType, Vec<u64>)> =
            InputGroup::new_with_inputs(inputs(&data, codecs));
        group.register("serialize column_values", move |data| {
            let mut buffer = Vec::new();
            serialize_u64_based_column_values(&data.1.as_slice(), &[data.0], &mut buffer).unwrap();
            black_box(buffer.len());
        });
        group.run();
    }
}

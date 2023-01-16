use proptest::prelude::*;
use proptest::strategy::Strategy;
use proptest::{prop_oneof, proptest};

use super::bitpacked::BitpackedCodec;
use super::blockwise_linear::BlockwiseLinearCodec;
use super::linear::LinearCodec;
use super::serialize::Header;

pub(crate) fn create_and_validate<Codec: FastFieldCodec>(
    data: &[u64],
    name: &str,
) -> Option<(f32, f32)> {
    let col = &VecColumn::from(data);
    let header = Header::compute_header(col, &[Codec::CODEC_TYPE])?;
    let normalized_col = header.normalize_column(col);
    let estimation = Codec::estimate(&normalized_col)?;

    let mut out = Vec::new();
    let col = VecColumn::from(data);
    serialize_column_values(&col, &[Codec::CODEC_TYPE], &mut out).unwrap();

    let actual_compression = out.len() as f32 / (data.len() as f32 * 8.0);

    let reader = super::open_u64_mapped::<u64>(OwnedBytes::new(out)).unwrap();
    assert_eq!(reader.num_vals(), data.len() as u32);
    for (doc, orig_val) in data.iter().copied().enumerate() {
        let val = reader.get_val(doc as u32);
        assert_eq!(
            val, orig_val,
            "val `{val}` does not match orig_val {orig_val:?}, in data set {name}, data `{data:?}`",
        );
    }

    if !data.is_empty() {
        let test_rand_idx = rand::thread_rng().gen_range(0..=data.len() - 1);
        let expected_positions: Vec<u32> = data
            .iter()
            .enumerate()
            .filter(|(_, el)| **el == data[test_rand_idx])
            .map(|(pos, _)| pos as u32)
            .collect();
        let mut positions = Vec::new();
        reader.get_docids_for_value_range(
            data[test_rand_idx]..=data[test_rand_idx],
            0..data.len() as u32,
            &mut positions,
        );
        assert_eq!(expected_positions, positions);
    }
    Some((estimation, actual_compression))
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    #[test]
    fn test_proptest_small_bitpacked(data in proptest::collection::vec(num_strategy(), 1..10)) {
        create_and_validate::<BitpackedCodec>(&data, "proptest bitpacked");
    }

    #[test]
    fn test_proptest_small_linear(data in proptest::collection::vec(num_strategy(), 1..10)) {
        create_and_validate::<LinearCodec>(&data, "proptest linearinterpol");
    }

    #[test]
    fn test_proptest_small_blockwise_linear(data in proptest::collection::vec(num_strategy(), 1..10)) {
        create_and_validate::<BlockwiseLinearCodec>(&data, "proptest multilinearinterpol");
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(10))]

    #[test]
    fn test_proptest_large_bitpacked(data in proptest::collection::vec(num_strategy(), 1..6000)) {
        create_and_validate::<BitpackedCodec>(&data, "proptest bitpacked");
    }

    #[test]
    fn test_proptest_large_linear(data in proptest::collection::vec(num_strategy(), 1..6000)) {
        create_and_validate::<LinearCodec>(&data, "proptest linearinterpol");
    }

    #[test]
    fn test_proptest_large_blockwise_linear(data in proptest::collection::vec(num_strategy(), 1..6000)) {
        create_and_validate::<BlockwiseLinearCodec>(&data, "proptest multilinearinterpol");
    }
}

fn num_strategy() -> impl Strategy<Value = u64> {
    prop_oneof![
        1 => prop::num::u64::ANY.prop_map(|num| u64::MAX - (num % 10) ),
        1 => prop::num::u64::ANY.prop_map(|num| num % 10 ),
        20 => prop::num::u64::ANY,
    ]
}

pub fn get_codec_test_datasets() -> Vec<(Vec<u64>, &'static str)> {
    let mut data_and_names = vec![];

    let data = (10..=10_000_u64).collect::<Vec<_>>();
    data_and_names.push((data, "simple monotonically increasing"));

    data_and_names.push((
        vec![5, 6, 7, 8, 9, 10, 99, 100],
        "offset in linear interpol",
    ));
    data_and_names.push((vec![5, 50, 3, 13, 1, 1000, 35], "rand small"));
    data_and_names.push((vec![10], "single value"));

    data_and_names.push((
        vec![1572656989877777, 1170935903116329, 720575940379279, 0],
        "overflow error",
    ));

    data_and_names
}

fn test_codec<C: FastFieldCodec>() {
    let codec_name = format!("{:?}", C::CODEC_TYPE);
    for (data, dataset_name) in get_codec_test_datasets() {
        let estimate_actual_opt: Option<(f32, f32)> =
            tests::create_and_validate::<C>(&data, dataset_name);
        let result = if let Some((estimate, actual)) = estimate_actual_opt {
            format!("Estimate `{estimate}` Actual `{actual}`")
        } else {
            "Disabled".to_string()
        };
        println!("Codec {codec_name}, DataSet {dataset_name}, {result}");
    }
}
#[test]
fn test_codec_bitpacking() {
    test_codec::<BitpackedCodec>();
}
#[test]
fn test_codec_interpolation() {
    test_codec::<LinearCodec>();
}
#[test]
fn test_codec_multi_interpolation() {
    test_codec::<BlockwiseLinearCodec>();
}

use super::*;

#[test]
fn estimation_good_interpolation_case() {
    let data = (10..=20000_u64).collect::<Vec<_>>();
    let data: VecColumn = data.as_slice().into();

    let linear_interpol_estimation = LinearCodec::estimate(&data).unwrap();
    assert_le!(linear_interpol_estimation, 0.01);

    let multi_linear_interpol_estimation = BlockwiseLinearCodec::estimate(&data).unwrap();
    assert_le!(multi_linear_interpol_estimation, 0.2);
    assert_lt!(linear_interpol_estimation, multi_linear_interpol_estimation);

    let bitpacked_estimation = BitpackedCodec::estimate(&data).unwrap();
    assert_lt!(linear_interpol_estimation, bitpacked_estimation);
}
#[test]
fn estimation_test_bad_interpolation_case() {
    let data: &[u64] = &[200, 10, 10, 10, 10, 1000, 20];

    let data: VecColumn = data.into();
    let linear_interpol_estimation = LinearCodec::estimate(&data).unwrap();
    assert_le!(linear_interpol_estimation, 0.34);

    let bitpacked_estimation = BitpackedCodec::estimate(&data).unwrap();
    assert_lt!(bitpacked_estimation, linear_interpol_estimation);
}

#[test]
fn estimation_prefer_bitpacked() {
    let data = VecColumn::from(&[10, 10, 10, 10]);
    let linear_interpol_estimation = LinearCodec::estimate(&data).unwrap();
    let bitpacked_estimation = BitpackedCodec::estimate(&data).unwrap();
    assert_lt!(bitpacked_estimation, linear_interpol_estimation);
}

#[test]
fn estimation_test_bad_interpolation_case_monotonically_increasing() {
    let mut data: Vec<u64> = (201..=20000_u64).collect();
    data.push(1_000_000);
    let data: VecColumn = data.as_slice().into();

    // in this case the linear interpolation can't in fact not be worse than bitpacking,
    // but the estimator adds some threshold, which leads to estimated worse behavior
    let linear_interpol_estimation = LinearCodec::estimate(&data).unwrap();
    assert_le!(linear_interpol_estimation, 0.35);

    let bitpacked_estimation = BitpackedCodec::estimate(&data).unwrap();
    assert_le!(bitpacked_estimation, 0.32);
    assert_le!(bitpacked_estimation, linear_interpol_estimation);
}

#[test]
fn test_fast_field_codec_type_to_code() {
    let mut count_codec = 0;
    for code in 0..=255 {
        if let Some(codec_type) = FastFieldCodecType::from_code(code) {
            assert_eq!(codec_type.to_code(), code);
            count_codec += 1;
        }
    }
    assert_eq!(count_codec, 3);
}

fn test_fastfield_gcd_i64_with_codec(
    codec_type: FastFieldCodecType,
    num_vals: usize,
) -> io::Result<()> {
    let mut vals: Vec<i64> = (-4..=(num_vals as i64) - 5).map(|val| val * 1000).collect();
    let mut buffer: Vec<u8> = Vec::new();
    crate::column_values::serialize_column_values(
        &VecColumn::from(&vals),
        &[codec_type],
        &mut buffer,
    )?;
    let buffer = OwnedBytes::new(buffer);
    let column = crate::column_values::open_u64_mapped::<i64>(buffer.clone())?;
    assert_eq!(column.get_val(0), -4000i64);
    assert_eq!(column.get_val(1), -3000i64);
    assert_eq!(column.get_val(2), -2000i64);
    assert_eq!(column.max_value(), (num_vals as i64 - 5) * 1000);
    assert_eq!(column.min_value(), -4000i64);

    // Can't apply gcd
    let mut buffer_without_gcd = Vec::new();
    vals.pop();
    vals.push(1001i64);
    crate::column_values::serialize_column_values(
        &VecColumn::from(&vals),
        &[codec_type],
        &mut buffer_without_gcd,
    )?;
    let buffer_without_gcd = OwnedBytes::new(buffer_without_gcd);
    assert!(buffer_without_gcd.len() > buffer.len());

    Ok(())
}

#[test]
fn test_fastfield_gcd_i64() -> io::Result<()> {
    for &codec_type in &[
        FastFieldCodecType::Bitpacked,
        FastFieldCodecType::BlockwiseLinear,
        FastFieldCodecType::Linear,
    ] {
        test_fastfield_gcd_i64_with_codec(codec_type, 5500)?;
    }
    Ok(())
}

fn test_fastfield_gcd_u64_with_codec(
    codec_type: FastFieldCodecType,
    num_vals: usize,
) -> io::Result<()> {
    let mut vals: Vec<u64> = (1..=num_vals).map(|i| i as u64 * 1000u64).collect();
    let mut buffer: Vec<u8> = Vec::new();
    crate::column_values::serialize_column_values(
        &VecColumn::from(&vals),
        &[codec_type],
        &mut buffer,
    )?;
    let buffer = OwnedBytes::new(buffer);
    let column = crate::column_values::open_u64_mapped::<u64>(buffer.clone())?;
    assert_eq!(column.get_val(0), 1000u64);
    assert_eq!(column.get_val(1), 2000u64);
    assert_eq!(column.get_val(2), 3000u64);
    assert_eq!(column.max_value(), num_vals as u64 * 1000);
    assert_eq!(column.min_value(), 1000u64);

    // Can't apply gcd
    let mut buffer_without_gcd = Vec::new();
    vals.pop();
    vals.push(1001u64);
    crate::column_values::serialize_column_values(
        &VecColumn::from(&vals),
        &[codec_type],
        &mut buffer_without_gcd,
    )?;
    let buffer_without_gcd = OwnedBytes::new(buffer_without_gcd);
    assert!(buffer_without_gcd.len() > buffer.len());
    Ok(())
}

#[test]
fn test_fastfield_gcd_u64() -> io::Result<()> {
    for &codec_type in &[
        FastFieldCodecType::Bitpacked,
        FastFieldCodecType::BlockwiseLinear,
        FastFieldCodecType::Linear,
    ] {
        test_fastfield_gcd_u64_with_codec(codec_type, 5500)?;
    }
    Ok(())
}

#[test]
pub fn test_fastfield2() {
    let test_fastfield = crate::column_values::serialize_and_load(&[100u64, 200u64, 300u64]);
    assert_eq!(test_fastfield.get_val(0), 100);
    assert_eq!(test_fastfield.get_val(1), 200);
    assert_eq!(test_fastfield.get_val(2), 300);
}

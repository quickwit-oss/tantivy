#[macro_use]
extern crate prettytable;
use fastfield_codecs::{Column, FastFieldCodecType, VecColumn};
use prettytable::{Cell, Row, Table};

fn main() {
    let mut table = Table::new();

    // Add a row per time
    table.add_row(row!["", "Compression Ratio", "Compression Estimation"]);

    for (data, data_set_name) in get_codec_test_data_sets() {
        let results: Vec<(f32, f32, FastFieldCodecType)> = [
            serialize_with_codec(&data, FastFieldCodecType::Bitpacked),
            serialize_with_codec(&data, FastFieldCodecType::Linear),
            serialize_with_codec(&data, FastFieldCodecType::BlockwiseLinear),
        ]
        .into_iter()
        .flatten()
        .collect();
        let best_compression_ratio_codec = results
            .iter()
            .min_by(|&res1, &res2| res1.partial_cmp(res2).unwrap())
            .cloned()
            .unwrap();

        table.add_row(Row::new(vec![Cell::new(data_set_name).style_spec("Bbb")]));
        for (est, comp, codec_type) in results {
            let est_cell = est.to_string();
            let ratio_cell = comp.to_string();
            let style = if comp == best_compression_ratio_codec.1 {
                "Fb"
            } else {
                ""
            };
            table.add_row(Row::new(vec![
                Cell::new(&format!("{codec_type:?}")).style_spec("bFg"),
                Cell::new(&ratio_cell).style_spec(style),
                Cell::new(&est_cell).style_spec(""),
            ]));
        }
    }

    table.printstd();
}

pub fn get_codec_test_data_sets() -> Vec<(Vec<u64>, &'static str)> {
    let mut data_and_names = vec![];

    let data = (1000..=200_000_u64).collect::<Vec<_>>();
    data_and_names.push((data, "Autoincrement"));

    let mut current_cumulative = 0;
    let data = (1..=200_000_u64)
        .map(|num| {
            let num = (num as f32 + num as f32).log10() as u64;
            current_cumulative += num;
            current_cumulative
        })
        .collect::<Vec<_>>();
    // let data = (1..=200000_u64).map(|num| num + num).collect::<Vec<_>>();
    data_and_names.push((data, "Monotonically increasing concave"));

    let mut current_cumulative = 0;
    let data = (1..=200_000_u64)
        .map(|num| {
            let num = (200_000.0 - num as f32).log10() as u64;
            current_cumulative += num;
            current_cumulative
        })
        .collect::<Vec<_>>();
    data_and_names.push((data, "Monotonically increasing convex"));

    let data = (1000..=200_000_u64)
        .map(|num| num + rand::random::<u8>() as u64)
        .collect::<Vec<_>>();
    data_and_names.push((data, "Almost monotonically increasing"));

    data_and_names
}

pub fn serialize_with_codec(
    data: &[u64],
    codec_type: FastFieldCodecType,
) -> Option<(f32, f32, FastFieldCodecType)> {
    let col = VecColumn::from(data);
    let estimation = fastfield_codecs::estimate(&col, codec_type)?;
    let mut out = Vec::new();
    fastfield_codecs::serialize(&col, &mut out, &[codec_type]).ok()?;
    let actual_compression = out.len() as f32 / (col.num_vals() * 8) as f32;
    Some((estimation, actual_compression, codec_type))
}

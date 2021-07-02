#[macro_use]
extern crate prettytable;
use fastfield_codecs::{
    linearinterpol::LinearInterpolFastFieldSerializer,
    multilinearinterpol::MultiLinearInterpolFastFieldSerializer, FastFieldCodecSerializer,
    FastFieldStats,
};
use prettytable::{Cell, Row, Table};

fn main() {
    let mut table = Table::new();

    // Add a row per time
    table.add_row(row!["", "Compression Ratio", "Compression Estimation"]);

    for (data, data_set_name) in get_codec_test_data_sets() {
        let mut results = vec![];
        let res = serialize_with_codec::<LinearInterpolFastFieldSerializer>(&data);
        results.push(res);
        let res = serialize_with_codec::<MultiLinearInterpolFastFieldSerializer>(&data);
        results.push(res);
        let res = serialize_with_codec::<fastfield_codecs::bitpacked::BitpackedFastFieldSerializer>(
            &data,
        );
        results.push(res);

        //let best_estimation_codec = results
        //.iter()
        //.min_by(|res1, res2| res1.partial_cmp(&res2).unwrap())
        //.unwrap();
        let best_compression_ratio_codec = results
            .iter()
            .min_by(|res1, res2| res1.partial_cmp(res2).unwrap())
            .cloned()
            .unwrap();

        table.add_row(Row::new(vec![Cell::new(data_set_name).style_spec("Bbb")]));
        for (is_applicable, est, comp, name) in results {
            let (est_cell, ratio_cell) = if !is_applicable {
                ("Codec Disabled".to_string(), "".to_string())
            } else {
                (est.to_string(), comp.to_string())
            };
            #[allow(clippy::all)]
            let style = if comp == best_compression_ratio_codec.1 {
                "Fb"
            } else {
                ""
            };

            table.add_row(Row::new(vec![
                Cell::new(&name.to_string()).style_spec("bFg"),
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
    //let data = (1..=200000_u64).map(|num| num + num).collect::<Vec<_>>();
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

pub fn serialize_with_codec<S: FastFieldCodecSerializer>(
    data: &[u64],
) -> (bool, f32, f32, &'static str) {
    let is_applicable = S::is_applicable(&data, stats_from_vec(data));
    if !is_applicable {
        return (false, 0.0, 0.0, S::NAME);
    }
    let estimation = S::estimate(&data, stats_from_vec(data));
    let mut out = vec![];
    S::serialize(
        &mut out,
        &data,
        stats_from_vec(data),
        data.iter().cloned(),
        data.iter().cloned(),
    )
    .unwrap();

    let actual_compression = out.len() as f32 / (data.len() * 8) as f32;
    (true, estimation, actual_compression, S::NAME)
}

pub fn stats_from_vec(data: &[u64]) -> FastFieldStats {
    let min_value = data.iter().cloned().min().unwrap_or(0);
    let max_value = data.iter().cloned().max().unwrap_or(0);
    FastFieldStats {
        min_value,
        max_value,
        num_vals: data.len() as u64,
    }
}

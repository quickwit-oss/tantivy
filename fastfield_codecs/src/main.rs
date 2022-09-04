#[macro_use]
extern crate prettytable;
use fastfield_codecs::bitpacked::BitpackedCodec;
use fastfield_codecs::blockwise_linear::BlockwiseLinearCodec;
use fastfield_codecs::linear::LinearCodec;
use fastfield_codecs::{Column, FastFieldCodec, FastFieldCodecType, FastFieldStats};
use prettytable::{Cell, Row, Table};

struct Data<'a>(&'a [u64]);

impl<'a> Column for Data<'a> {
    fn get_val(&self, position: u64) -> u64 {
        self.0[position as usize]
    }

    fn iter<'b>(&'b self) -> Box<dyn Iterator<Item = u64> + 'b> {
        Box::new(self.0.iter().cloned())
    }

    fn min_value(&self) -> u64 {
        *self.0.iter().min().unwrap_or(&0)
    }

    fn max_value(&self) -> u64 {
        *self.0.iter().max().unwrap_or(&0)
    }

    fn num_vals(&self) -> u64 {
        self.0.len() as u64
    }
}

fn main() {
    let mut table = Table::new();

    // Add a row per time
    table.add_row(row!["", "Compression Ratio", "Compression Estimation"]);

    for (data, data_set_name) in get_codec_test_data_sets() {
        let results: Vec<(f32, f32, FastFieldCodecType)> = [
            serialize_with_codec::<LinearCodec>(&data),
            serialize_with_codec::<BlockwiseLinearCodec>(&data),
            serialize_with_codec::<BlockwiseLinearCodec>(&data),
            serialize_with_codec::<BitpackedCodec>(&data),
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

pub fn serialize_with_codec<C: FastFieldCodec>(
    data: &[u64],
) -> Option<(f32, f32, FastFieldCodecType)> {
    let data = Data(data);
    let estimation = C::estimate(&data)?;
    let mut out = Vec::new();
    C::serialize(&mut out, &data).unwrap();
    let actual_compression = out.len() as f32 / (data.num_vals() * 8) as f32;
    Some((estimation, actual_compression, C::CODEC_TYPE))
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

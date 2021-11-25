#[macro_use]
extern crate prettytable;
use fastfield_codecs::bitpacked::BitpackedFastFieldReader;
use fastfield_codecs::linearinterpol::LinearInterpolFastFieldReader;
use fastfield_codecs::multilinearinterpol::MultiLinearInterpolFastFieldReader;
use fastfield_codecs::multilinearinterpol_v2::MultiLinearInterpolV2FastFieldReader;
use fastfield_codecs::FastFieldCodecReader;
use fastfield_codecs::{
    linearinterpol::LinearInterpolFastFieldSerializer,
    multilinearinterpol::MultiLinearInterpolFastFieldSerializer,
    multilinearinterpol_v2::MultiLinearInterpolV2FastFieldSerializer, FastFieldCodecSerializer,
    FastFieldStats,
};
use prettytable::{Cell, Row, Table};
use std::fs::File;
use std::io;
use std::io::BufRead;
use std::time::{Duration, Instant};

fn main() {
    let mut table = Table::new();

    // Add a row per time
    table.add_row(row![
        "",
        "Compression ratio",
        "Compression ratio estimation",
        "Compression time (micro)",
        "Reading time (micro)"
    ]);

    for (data, data_set_name) in get_codec_test_data_sets() {
        let mut results = vec![];
        let res = serialize_with_codec::<
            LinearInterpolFastFieldSerializer,
            LinearInterpolFastFieldReader,
        >(&data);
        results.push(res);
        let res = serialize_with_codec::<
            MultiLinearInterpolFastFieldSerializer,
            MultiLinearInterpolFastFieldReader,
        >(&data);
        results.push(res);
        let res = serialize_with_codec::<
            MultiLinearInterpolV2FastFieldSerializer,
            MultiLinearInterpolV2FastFieldReader,
        >(&data);
        results.push(res);
        let res = serialize_with_codec::<
            fastfield_codecs::bitpacked::BitpackedFastFieldSerializer,
            BitpackedFastFieldReader,
        >(&data);
        results.push(res);

        // let best_estimation_codec = results
        //.iter()
        //.min_by(|res1, res2| res1.partial_cmp(&res2).unwrap())
        //.unwrap();
        let best_compression_ratio_codec = results
            .iter()
            .min_by(|res1, res2| res1.partial_cmp(res2).unwrap())
            .cloned()
            .unwrap();

        table.add_row(Row::new(vec![Cell::new(data_set_name).style_spec("Bbb")]));
        for (is_applicable, est, comp, name, compression_duration, read_duration) in results {
            let (est_cell, ratio_cell) = if !is_applicable {
                ("Codec Disabled".to_string(), "".to_string())
            } else {
                (est.to_string(), comp.to_string())
            };
            let style = if comp == best_compression_ratio_codec.1 {
                "Fb"
            } else {
                ""
            };

            table.add_row(Row::new(vec![
                Cell::new(name).style_spec("bFg"),
                Cell::new(&ratio_cell).style_spec(style),
                Cell::new(&est_cell).style_spec(""),
                Cell::new(&compression_duration.as_micros().to_string()),
                Cell::new(&read_duration.as_micros().to_string()),
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

    let data = (1000..=200_000_u64)
        .map(|_| rand::random::<u8>() as u64)
        .collect::<Vec<_>>();
    data_and_names.push((data, "Random"));

    let mut data = load_dataset("datasets/hdfs_logs_timestamps.txt");
    data_and_names.push((data.clone(), "HDFS logs timestamps"));

    data.sort_unstable();
    data_and_names.push((data, "HDFS logs timestamps SORTED"));

    let data = load_dataset("datasets/http_logs_timestamps.txt");
    data_and_names.push((data, "HTTP logs timestamps SORTED"));

    let mut data = load_dataset("datasets/amazon_reviews_product_ids.txt");
    data_and_names.push((data.clone(), "Amazon review product ids"));

    data.sort_unstable();
    data_and_names.push((data, "Amazon review product ids SORTED"));

    data_and_names
}

pub fn load_dataset(file_path: &str) -> Vec<u64> {
    println!("Load dataset from `{}`", file_path);
    let file = File::open(file_path).expect("Error when opening file.");
    let lines = io::BufReader::new(file).lines();
    let mut data = Vec::new();
    for line in lines {
        let l = line.unwrap();
        data.push(l.parse::<u64>().unwrap());
    }
    data
}

pub fn serialize_with_codec<S: FastFieldCodecSerializer, R: FastFieldCodecReader>(
    data: &[u64],
) -> (bool, f32, f32, &'static str, Duration, Duration) {
    let is_applicable = S::is_applicable(&data, stats_from_vec(data));
    if !is_applicable {
        return (
            false,
            0.0,
            0.0,
            S::NAME,
            Duration::from_secs(0),
            Duration::from_secs(0),
        );
    }
    let start_time_compression = Instant::now();
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
    let elasped_time_compression = start_time_compression.elapsed();
    let actual_compression = out.len() as f32 / (data.len() * 8) as f32;

    let reader = R::open_from_bytes(&out).unwrap();
    let start_time_read = Instant::now();
    for doc in 0..data.len() {
        reader.get_u64(doc as u64, &out);
    }
    let elapsed_time_read = start_time_read.elapsed();
    (
        true,
        estimation,
        actual_compression,
        S::NAME,
        elasped_time_compression,
        elapsed_time_read,
    )
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

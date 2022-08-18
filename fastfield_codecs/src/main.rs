#[macro_use]
extern crate prettytable;
use std::collections::HashSet;
use std::env;
use std::io::BufRead;
use std::net::{IpAddr, Ipv6Addr};
use std::str::FromStr;

use fastfield_codecs::ip_codec::{IntervalEncoding, IntervallDecompressor};
use fastfield_codecs::linearinterpol::LinearInterpolFastFieldSerializer;
use fastfield_codecs::multilinearinterpol::MultiLinearInterpolFastFieldSerializer;
use fastfield_codecs::{FastFieldCodecSerializer, FastFieldStats};
use itertools::Itertools;
use measure_time::print_time;
use prettytable::{Cell, Row, Table};

fn print_set_stats(ip_addrs: &[u128]) {
    println!("NumIps\t{}", ip_addrs.len());
    let ip_addr_set: HashSet<u128> = ip_addrs.iter().cloned().collect();
    println!("NumUniqueIps\t{}", ip_addr_set.len());
    let ratio_unique = ip_addr_set.len() as f64 / ip_addrs.len() as f64;
    println!("RatioUniqueOverTotal\t{ratio_unique:.4}");

    // histogram
    let mut ip_addrs = ip_addrs.to_vec();
    ip_addrs.sort();
    let mut cnts: Vec<usize> = ip_addrs
        .into_iter()
        .dedup_with_count()
        .map(|(cnt, _)| cnt)
        .collect();
    cnts.sort();

    let top_256_cnt: usize = cnts.iter().rev().take(256).sum();
    let top_128_cnt: usize = cnts.iter().rev().take(128).sum();
    let top_64_cnt: usize = cnts.iter().rev().take(64).sum();
    let top_8_cnt: usize = cnts.iter().rev().take(8).sum();
    let total: usize = cnts.iter().sum();

    println!("{}", total);
    println!("{}", top_256_cnt);
    println!("{}", top_128_cnt);
    println!("Percentage Top8 {:02}", top_8_cnt as f32 / total as f32);
    println!("Percentage Top64 {:02}", top_64_cnt as f32 / total as f32);
    println!("Percentage Top128 {:02}", top_128_cnt as f32 / total as f32);
    println!("Percentage Top256 {:02}", top_256_cnt as f32 / total as f32);

    let mut cnts: Vec<(usize, usize)> = cnts.into_iter().dedup_with_count().collect();
    cnts.sort_by(|a, b| {
        if a.1 == b.1 {
            a.0.cmp(&b.0)
        } else {
            b.1.cmp(&a.1)
        }
    });

    println!("\n\n----\nIP Address histogram");
    println!("IPAddrCount\tFrequency");
    for (ip_addr_count, times) in cnts {
        println!("{}\t{}", ip_addr_count, times);
    }
}

fn ip_dataset() -> Vec<u128> {
    let mut ip_addr_v4 = 0;

    let stdin = std::io::stdin();
    let ip_addrs: Vec<u128> = stdin
        .lock()
        .lines()
        .flat_map(|line| {
            let line = line.unwrap();
            let line = line.trim();
            let ip_addr = IpAddr::from_str(line.trim()).ok()?;
            if ip_addr.is_ipv4() {
                ip_addr_v4 += 1;
            }
            let ip_addr_v6: Ipv6Addr = match ip_addr {
                IpAddr::V4(v4) => v4.to_ipv6_mapped(),
                IpAddr::V6(v6) => v6,
            };
            Some(ip_addr_v6)
        })
        .map(|ip_v6| u128::from_be_bytes(ip_v6.octets()))
        .collect();

    println!("IpAddrsAny\t{}", ip_addrs.len());
    println!("IpAddrsV4\t{}", ip_addr_v4);

    ip_addrs
}

fn bench_ip() {
    let encoding = IntervalEncoding();
    let dataset = ip_dataset();
    print_set_stats(&dataset);

    let compressor = encoding.train(dataset.to_vec());
    let data = compressor.compress(&dataset).unwrap();

    let decompressor = IntervallDecompressor::open(&data).unwrap();

    for i in 11100..11150 {
        print_time!("get range");
        let doc_values = decompressor.get_range(dataset[i]..=dataset[i], &data);
        println!("{:?}", doc_values.len());
    }
}

fn main() {
    if env::args().nth(1).unwrap() == "bench" {
        bench_ip();
        return;
    }
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
        for (is_applicable, est, comp, name) in results {
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

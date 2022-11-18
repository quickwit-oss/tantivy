#[macro_use]
extern crate prettytable;
use std::collections::HashSet;
use std::env;
use std::io::BufRead;
use std::net::{IpAddr, Ipv6Addr};
use std::str::FromStr;

use fastfield_codecs::{open_u128, serialize_u128, Column, FastFieldCodecType, VecColumn};
use itertools::Itertools;
use measure_time::print_time;
use ownedbytes::OwnedBytes;
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
    let dataset = ip_dataset();
    print_set_stats(&dataset);

    // Chunks
    {
        let mut data = vec![];
        for dataset in dataset.chunks(500_000) {
            serialize_u128(|| dataset.iter().cloned(), dataset.len() as u32, &mut data).unwrap();
        }
        let compression = data.len() as f64 / (dataset.len() * 16) as f64;
        println!("Compression 50_000 chunks {:.4}", compression);
        println!(
            "Num Bits per elem {:.2}",
            (data.len() * 8) as f32 / dataset.len() as f32
        );
    }

    let mut data = vec![];
    {
        print_time!("creation");
        serialize_u128(|| dataset.iter().cloned(), dataset.len() as u32, &mut data).unwrap();
    }

    let compression = data.len() as f64 / (dataset.len() * 16) as f64;
    println!("Compression {:.2}", compression);
    println!(
        "Num Bits per elem {:.2}",
        (data.len() * 8) as f32 / dataset.len() as f32
    );

    let decompressor = open_u128::<u128>(OwnedBytes::new(data))
        .unwrap()
        .to_full()
        .unwrap();
    // Sample some ranges
    let mut doc_values = Vec::new();
    for value in dataset.iter().take(1110).skip(1100).cloned() {
        doc_values.clear();
        print_time!("get range");
        decompressor.get_docids_for_value_range(
            value..=value,
            0..decompressor.num_vals(),
            &mut doc_values,
        );
        println!("{:?}", doc_values.len());
    }
}

fn main() {
    if env::args().nth(1).unwrap() == "bench_ip" {
        bench_ip();
        return;
    }

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

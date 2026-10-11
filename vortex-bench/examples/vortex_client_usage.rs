//! A tour of the Vortex client API (Vortex 0.88), used as the source of the code shown in the
//! report: building arrays, compressing them, slice / filter / take, decompression, single values,
//! files and scans. It also measures how Vortex encodes sparse columns.
//!
//! Usage: `cargo run --release --example vortex_client_usage`

use rand::rngs::StdRng;
use rand::seq::index::sample;
use rand::{Rng, SeedableRng};
use vortex::array::arrays::PrimitiveArray;
use vortex::array::expr::{gt, lit, root};
use vortex::array::iter::ArrayIteratorExt;
use vortex::array::validity::Validity;
use vortex::array::{ArrayRef, IntoArray, VortexSessionExecute};
use vortex::buffer::Buffer;
use vortex::compressor::BtrBlocksCompressor;
use vortex::error::VortexResult;
use vortex::file::{OpenOptionsSessionExt, WriteOptionsSessionExt};
use vortex::io::runtime::current::CurrentThreadRuntime;
use vortex::io::runtime::BlockingRuntime;
use vortex::io::session::RuntimeSessionExt;
use vortex::mask::Mask;
use vortex::session::VortexSession;
use vortex::VortexSessionDefault;
use vortex_bench::encoding_tree;

/// Prints the encoding tree with the dtype, length and size of every node.
fn print_tree(array: &ArrayRef, name: &str, depth: usize) {
    println!(
        "{}{name}: {} {} len={} nbytes={}",
        "  ".repeat(depth),
        array.encoding_id(),
        array.dtype(),
        array.len(),
        array.nbytes()
    );
    for (child_name, child) in array.named_children() {
        print_tree(&child, &child_name, depth + 1);
    }
}

fn main() -> VortexResult<()> {
    // --- [session]
    // A session registers encodings, layouts, compute functions and the async runtime.
    let runtime = CurrentThreadRuntime::new();
    let session = VortexSession::default().with_handle(runtime.handle());
    let mut ctx = session.create_execution_ctx();
    // --- [/session]

    // --- [build]
    // 1M uniform integers in [0, 1_000_000): a plain, uncompressed (canonical) array.
    let mut rng = StdRng::seed_from_u64(42);
    let values: Vec<u64> = (0..1_000_000).map(|_| rng.random_range(0..1_000_000)).collect();
    let array: ArrayRef =
        PrimitiveArray::new(Buffer::from(values.clone()), Validity::NonNullable).into_array();
    println!("canonical: {} rows, {} bytes, encoding {}", array.len(), array.nbytes(), array.encoding_id());
    // --- [/build]

    // --- [compress]
    // BtrBlocks samples the data and picks a cascade of encodings.
    let compressor = BtrBlocksCompressor::from_session(&session);
    let compressed: ArrayRef = compressor.compress(&array, &mut ctx)?;
    println!(
        "compressed: {} bytes, tree {}",
        compressed.nbytes(),
        encoding_tree(&compressed, 3)
    );
    // --- [/compress]

    // --- [slice]
    // slice() is lazy and cheap: on a bitpacked array it only adjusts an offset.
    let sliced: ArrayRef = compressed.slice(1_000..1_010)?;
    println!("slice: len {}, tree {}", sliced.len(), encoding_tree(&sliced, 3));
    // --- [/slice]

    // --- [filter]
    // filter() takes a Mask (a set of selected rows) and is lazy too.
    let mask = Mask::from_indices(compressed.len(), [3, 17, 500_000, 999_999]);
    let filtered: ArrayRef = compressed.filter(mask)?;
    println!("filter: len {}, tree {}", filtered.len(), encoding_tree(&filtered, 3));
    // --- [/filter]

    // --- [take]
    // take() gathers rows by index; the indices are themselves an array.
    let indices: ArrayRef =
        PrimitiveArray::new(Buffer::<u32>::from(vec![999_999u32, 3, 3, 42]), Validity::NonNullable)
            .into_array();
    let taken: ArrayRef = compressed.take(indices)?;
    println!("take: len {}, tree {}", taken.len(), encoding_tree(&taken, 3));
    // --- [/take]

    // --- [execute]
    // Nothing is decoded until the array is executed into a canonical form.
    let decoded: PrimitiveArray = taken.execute(&mut ctx)?;
    let decoded_values: &[u64] = decoded.as_slice::<u64>();
    assert_eq!(decoded_values, &[values[999_999], values[3], values[3], values[42]]);
    println!("take executed: {decoded_values:?}");

    let filtered_values: PrimitiveArray = filtered.execute(&mut ctx)?;
    assert_eq!(
        filtered_values.as_slice::<u64>(),
        &[values[3], values[17], values[500_000], values[999_999]]
    );
    // --- [/execute]

    // --- [scalar]
    // Single value access returns a Scalar (dtype + value). Convenient, not for hot loops.
    let scalar = compressed.execute_scalar(123_456, &mut ctx)?;
    println!("scalar at 123456: {scalar} (expected {})", values[123_456]);
    // --- [/scalar]

    // --- [file]
    // Write the array to an in-memory Vortex file, then scan it with a pushed-down filter.
    let mut file_bytes: Vec<u8> = Vec::new();
    session
        .write_options()
        .blocking(&runtime)
        .write(&mut file_bytes, array.to_array_iterator())?;
    let file = session.open_options().open_buffer(file_bytes.clone())?;
    let filter = gt(root(), lit(999_000u64)).bind(file.dtype())?.optimize_recursive()?;
    let matching: ArrayRef = file
        .scan()?
        .with_filter(filter)
        .into_array_iter(&runtime)?
        .read_all()?;
    let expected = values.iter().filter(|&&value| value > 999_000).count();
    println!(
        "file: {} bytes; scan with `value > 999000` returned {} rows (expected {expected})",
        file_bytes.len(),
        matching.len()
    );
    assert_eq!(matching.len(), expected);
    // --- [/file]

    // --- [sparse]
    // A nullable column where ~1 row in 100 has a value, at random positions.
    let num_rows = 65_536;
    let positions: Vec<usize> = sample(&mut rng, num_rows, num_rows / 100).into_vec();
    let mut column: Vec<Option<u64>> = vec![None; num_rows];
    for &position in &positions {
        column[position] = Some(rng.random_range(0..1_000_000));
    }
    let sparse = PrimitiveArray::from_option_iter(column.iter().copied()).into_array();
    let sparse_compressed = compressor.compress(&sparse, &mut ctx)?;
    println!(
        "sparse 1%: canonical {} bytes, compressed {} bytes, tree {}",
        sparse.nbytes(),
        sparse_compressed.nbytes(),
        encoding_tree(&sparse_compressed, 4)
    );
    print_tree(&sparse_compressed, "root", 1);
    // --- [/sparse]

    // --- [sparse_take]
    // take() on a sparse array: some requested rows have a value, others are null.
    let mut sorted_positions = positions.clone();
    sorted_positions.sort_unstable();
    let with_values = &sorted_positions[..3];
    let requested: Vec<u32> = vec![
        with_values[0] as u32,
        with_values[0] as u32 + 1, // almost surely null at 1 % density (checked by the output)
        with_values[1] as u32,
        with_values[2] as u32,
        with_values[2] as u32 + 1,
    ];
    let mixed = sparse_compressed.take(
        PrimitiveArray::new(Buffer::from(requested), Validity::NonNullable).into_array(),
    )?;
    println!("take mixed: len {}, tree {}", mixed.len(), encoding_tree(&mixed, 3));
    // One execution step runs the sparse take kernel, without decompressing the result.
    let mixed_step: ArrayRef = mixed.clone().execute(&mut ctx)?;
    println!("take mixed, one step: tree {}", encoding_tree(&mixed_step, 3));
    let mixed_values: PrimitiveArray = mixed.execute(&mut ctx)?;
    println!("take mixed executed: {:?}", (0..mixed_values.len())
        .map(|i| mixed_values.as_ref().is_valid(i, &mut ctx).unwrap()
            .then(|| mixed_values.as_slice::<u64>()[i]))
        .collect::<Vec<Option<u64>>>());

    // Only null rows: the result collapses to a constant null.
    let null_rows: Vec<u32> = (0..num_rows as u32)
        .filter(|row| column[*row as usize].is_none())
        .take(4)
        .collect();
    let all_null = sparse_compressed.take(
        PrimitiveArray::new(Buffer::from(null_rows), Validity::NonNullable).into_array(),
    )?;
    let all_null_step: ArrayRef = all_null.execute(&mut ctx)?;
    println!("take nulls only, one step: tree {}", encoding_tree(&all_null_step, 3));

    // Only rows with a value: the result is just the taken values.
    let value_rows: Vec<u32> = sorted_positions[..4].iter().map(|&row| row as u32).collect();
    let all_values = sparse_compressed.take(
        PrimitiveArray::new(Buffer::from(value_rows), Validity::NonNullable).into_array(),
    )?;
    let all_values_step: ArrayRef = all_values.execute(&mut ctx)?;
    println!("take values only, one step: tree {}", encoding_tree(&all_values_step, 3));
    // --- [/sparse_take]

    // Density sweep: which encoding wins, and how many bytes per non-null value.
    println!("density sweep ({num_rows} rows, random positions, values < 2^20):");
    for density_permille in [1usize, 10, 30, 50, 100, 300, 1000] {
        let num_values = num_rows * density_permille / 1000;
        let positions: Vec<usize> = sample(&mut rng, num_rows, num_values).into_vec();
        let mut column: Vec<Option<u64>> = vec![None; num_rows];
        for &position in &positions {
            column[position] = Some(rng.random_range(0..1 << 20));
        }
        let array = PrimitiveArray::from_option_iter(column.iter().copied()).into_array();
        let compressed = compressor.compress(&array, &mut ctx)?;
        println!(
            "  density {:>5.1} %: {:>7} bytes ({:>5.2} bytes per value), tree {}",
            density_permille as f64 / 10.0,
            compressed.nbytes(),
            compressed.nbytes() as f64 / num_values.max(1) as f64,
            encoding_tree(&compressed, 3)
        );
        if density_permille == 1 || density_permille == 300 {
            print_tree(&compressed, "root", 2);
        }
    }

    // Regular spacing: one value every 100 rows.
    let regular: Vec<Option<u64>> = (0..num_rows)
        .map(|row| (row % 100 == 0).then(|| rng.random_range(0..1 << 20)))
        .collect();
    let regular = PrimitiveArray::from_option_iter(regular).into_array();
    let regular_compressed = compressor.compress(&regular, &mut ctx)?;
    println!(
        "regular 1%: {} bytes, tree {}",
        regular_compressed.nbytes(),
        encoding_tree(&regular_compressed, 3)
    );
    print_tree(&regular_compressed, "root", 1);
    Ok(())
}

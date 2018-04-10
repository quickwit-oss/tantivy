use std::env;
use std::path::PathBuf;
use std::fs::File;
use std::io::Write;
extern crate tantivy;
use tantivy::directory::write_static_from_directory;

fn main() {
    // Prints each argument on a separate line
    let  mut args = env::args();
    args.next().unwrap();
    let directory_path= args.next().expect("Expect 2 args.<directory_path> <outputfile>");
    let output_path = args.next().expect("Expect 2 args.<directory_path> <outputfile>");
    println!("{} => {}", directory_path, output_path);
    let buffer = write_static_from_directory(&PathBuf::from(directory_path)).unwrap();
    println!("Read all");
    let mut output = File::create(output_path).unwrap();
    output.write_all(&buffer[..]).unwrap();
    output.flush().unwrap();
}
#[macro_use]
extern crate clap;
#[macro_use]
extern crate lazy_static;
extern crate rustc_serialize;
extern crate tantivy;
extern crate time;
extern crate persistent;
extern crate urlencoded;
extern crate iron;
extern crate staticfile;
extern crate ansi_term;
extern crate mount;

use clap::{AppSettings, Arg, App, SubCommand};
mod commands;
use self::commands::*;


fn main() {
    let index_arg = Arg::with_name("index")
                    .short("i")
                    .long("index")
                    .value_name("directory")
                    .help("Tantivy index directory filepath")
                    .required(true);
    
    let cli_options = App::new("Tantivy")
        .setting(AppSettings::SubcommandRequiredElseHelp)
        .version("0.1")
        .author("Paul Masurel <paul.masurel@gmail.com>")
        .about("Tantivy Search Engine's command line interface.")
        .subcommand(
            SubCommand::with_name("new")
                .about("Create a new index. The schema will be populated with a simple example schema")
                .arg(index_arg.clone())
        ) 
        .subcommand(
            SubCommand::with_name("serve")
                .about("Start a server")
                .arg(index_arg.clone())
                .arg(Arg::with_name("host")
                    .long("host")
                    .value_name("host")
                    .help("host to listen to")
                )
                .arg(Arg::with_name("port")
                    .short("p")
                    .long("port")
                    .value_name("port")
                    .help("Port")
                    .default_value("localhost")
                )
        )       
        .subcommand(
            SubCommand::with_name("index")
                .about("Index files")
                .arg(index_arg.clone())
                .arg(Arg::with_name("file")
                    .short("f")
                    .long("file")
                    .value_name("file")
                    .help("File containing the documents to index."))
        )
        .subcommand(
            SubCommand::with_name("bench")
                .about("Run a benchmark on your index")
                .arg(index_arg.clone())
                .arg(Arg::with_name("queries")
                    .short("q")
                    .long("queries")
                    .value_name("queries")
                    .help("File containing queries (one-per line) to run in the benchmark.")
                    .required(true))
                .arg(Arg::with_name("num_repeat")
                    .short("n")
                    .long("num_repeat")
                    .value_name("num_repeat")
                    .help("Number of time to repeat the benchmark.")
                    .default_value("1"))
        )
        .get_matches();
    
    let (subcommand, some_options) = cli_options.subcommand();
    
    let options = some_options.unwrap();
    
    match subcommand {
        "new" => run_new_cli(options).unwrap(),
        "index" => run_index_cli(options).unwrap(),
        "serve" => run_serve_cli(options).unwrap(),
        "bench" => {
            let res = run_bench_cli(options);
            match res {
                Err(e) => { println!("{}", e);}
                _ => {}
            }
        },
        _ => {}
    }
}
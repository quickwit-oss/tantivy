use clap::ArgMatches;
use std::convert::From;
use std::path::PathBuf;
use tantivy;
use tantivy::schema::*;
use tantivy::Index;
use std::io;
use ansi_term::Style;
use ansi_term::Colour::{Red, Blue, Green};
use std::io::Write;
use std::ascii::AsciiExt;
use rustc_serialize::json;


pub fn run_new_cli(matches: &ArgMatches) -> tantivy::Result<()> {
    let index_directory = PathBuf::from(matches.value_of("index").unwrap());
    run_new(index_directory)   
}


fn prompt_input<P: Fn(&str) -> Result<(), String>>(prompt_text: &str, predicate: P) -> String {
    loop {
        print!("{prompt_text:<width$} ? ", prompt_text=Style::new().bold().fg(Blue).paint(prompt_text), width=40);
        io::stdout().flush().unwrap();
        let mut buffer = String::new();
        io::stdin().read_line(&mut buffer).ok().expect("Failed to read line");
        let answer = buffer.trim_right_matches("\n").to_string();
        match predicate(&answer) {
            Ok(()) => {
                return answer;
            }
            Err(msg) => {
                println!("Error: {}", Style::new().bold().fg(Red).paint(msg));
            }
        }
    }
}


fn field_name_validate(field_name: &str) -> Result<(), String> {
    if is_valid_field_name(field_name) {
        Ok(())
    }
    else {
        Err(String::from("Field name must match the pattern [_a-zA-Z0-9]+"))
    }
}
    
    
fn prompt_options(msg: &str, codes: Vec<char>) -> char {
    let options_string: Vec<String> = codes.iter().map(|c| format!("{}", c)).collect();
    let options = options_string.join("/");
    let predicate = |entry: &str| {
        if entry.len() != 1 {
            return Err(format!("Invalid input. Options are ({})", options))
        }
        let c = entry.chars().next().unwrap().to_ascii_uppercase();
        if codes.contains(&c) {
            return Ok(())
        }
        else {
            return Err(format!("Invalid input. Options are ({})", options))
        }
    };
    let message = format!("{} ({})", msg, options);
    let entry = prompt_input(&message, predicate);
    entry.chars().next().unwrap().to_ascii_uppercase()
}

fn prompt_yn(msg: &str) -> bool {
    prompt_options(msg, vec!('Y', 'N')) == 'Y' 
}


fn ask_add_field_text(field_name: &str, schema: &mut Schema) {
    let mut text_options = TextOptions::new();
    if prompt_yn("Should the field be stored") {
        text_options = text_options.set_stored();
    }
    let is_indexed = prompt_yn("Should the field be indexed");
    let indexing_options = if is_indexed {
        if prompt_yn("Should the field be tokenized") {
            if prompt_yn("Should the term frequencies (per doc) be in the index") {
                if prompt_yn("Should the term positions (per doc) be in the index") {
                    TextIndexingOptions::TokenizedWithFreqAndPosition
                }
                else {
                    TextIndexingOptions::TokenizedWithFreq
                }
            }
            else {
                TextIndexingOptions::TokenizedNoFreq
            }
        }
        else {
            TextIndexingOptions::Unindexed
        }
    }
    else {
        TextIndexingOptions::Unindexed
    };
    text_options = text_options.set_indexing_options(indexing_options);
    schema.add_text_field(field_name, text_options);
}


fn ask_add_field_u32(field_name: &str, schema: &mut Schema) {
    let mut u32_options = U32Options::new();
    if prompt_yn("Should the field be stored") {
        u32_options = u32_options.set_stored();
    }
    if prompt_yn("Should the field be fast") {
        u32_options = u32_options.set_fast();
    }
    if prompt_yn("Should the field be indexed") {
        u32_options = u32_options.set_indexed();
    }
    schema.add_u32_field(field_name, u32_options);
}

fn ask_add_field(schema: &mut Schema) {
    println!("\n\n");
    let field_name = prompt_input("New field name ", field_name_validate);
    let text_or_integer = prompt_options("Text or unsigned 32-bit Integer", vec!('T', 'I'));
    if text_or_integer =='T' {
        ask_add_field_text(&field_name, schema);
    }
    else {
        ask_add_field_u32(&field_name, schema);        
    }
}

fn run_new(directory: PathBuf) -> tantivy::Result<()> {
    println!("\n{} ", Style::new().bold().fg(Green).paint("Creating new index"));
    println!("{} ", Style::new().bold().fg(Green).paint("Let's define it's schema!"));
    let mut schema = Schema::new();
    loop  {
        ask_add_field(&mut schema);
        if !prompt_yn("Add another field") {
            break;
        }
    }
    let schema_json = format!("{}", json::as_pretty_json(&schema));
    println!("\n{}\n", Style::new().fg(Green).paint(schema_json));
    let mut index = try!(Index::create(&directory, schema));
    index.save_metas()
}


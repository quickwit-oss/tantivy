extern crate gcc;


use std::process::Command;

fn main() {
    Command::new("make")
        .current_dir("cpp/simdcomp")
        .output()
        .unwrap_or_else(|e| { panic!("Failed to make simdcomp: {}", e) });
    
    gcc::Config::new()
                .cpp(true)
                .flag("-std=c++11")
                .flag("-O3")
                .flag("-mssse3")
                .include("./cpp/simdcomp/include")
                .object("cpp/simdcomp/avxbitpacking.o")
                .object("cpp/simdcomp/simdintegratedbitpacking.o")
                .object("cpp/simdcomp/simdbitpacking.o")
                .object("cpp/simdcomp/simdpackedsearch.o")
                .object("cpp/simdcomp/simdcomputil.o")
                .object("cpp/simdcomp/simdpackedselect.o")
                .object("cpp/simdcomp/simdfor.o")
                .file("cpp/simdcomp_wrapper.cpp")
                .compile("libsimdcomp.a");
    println!("cargo:rustc-flags=-l dylib=stdc++");
}

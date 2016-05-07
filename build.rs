extern crate gcc;


use std::process::Command;

fn main() {
    
    Command::new("make")
        .current_dir("cpp/SIMDCompressionAndIntersection")
        .output()
        .unwrap_or_else(|e| { panic!("Failed to make SIMDCompressionAndIntersection: {}", e) });

    Command::new("make")
        .current_dir("cpp/simdcomp")
        .output()
        .unwrap_or_else(|e| { panic!("Failed to make simdcomp: {}", e) });
    
    
    gcc::Config::new()
                .cpp(true)
                .flag("-std=c++11")
                .flag("-O3")
                .flag("-mssse3")
                .include("./cpp/SIMDCompressionAndIntersection/include")
                .include("./cpp/simdcomp/include")
                .object("cpp/SIMDCompressionAndIntersection/bitpacking.o")
                .object("cpp/SIMDCompressionAndIntersection/integratedbitpacking.o")
                .object("cpp/SIMDCompressionAndIntersection/simdbitpacking.o")
                .object("cpp/SIMDCompressionAndIntersection/usimdbitpacking.o")
                .object("cpp/SIMDCompressionAndIntersection/simdintegratedbitpacking.o")
                .object("cpp/SIMDCompressionAndIntersection/intersection.o")
                .object("cpp/SIMDCompressionAndIntersection/varintdecode.o")
                .object("cpp/SIMDCompressionAndIntersection/streamvbyte.o")
                .object("cpp/SIMDCompressionAndIntersection/simdpackedsearch.o")
                .object("cpp/SIMDCompressionAndIntersection/simdpackedselect.o")
                .object("cpp/SIMDCompressionAndIntersection/frameofreference.o")
                .object("cpp/SIMDCompressionAndIntersection/for.o")
                .object("cpp/simdcomp/avxbitpacking.o")
                .object("cpp/simdcomp/simdintegratedbitpacking.o")
                .object("cpp/simdcomp/simdbitpacking.o")
                .object("cpp/simdcomp/simdpackedsearch.o")
                .object("cpp/simdcomp/simdcomputil.o")
                .object("cpp/simdcomp/simdpackedselect.o")
                .object("cpp/simdcomp/simdfor.o")
                .file("cpp/encode.cpp")
                .file("cpp/simdcomp_wrapper.cpp")
                .compile("libsimdcompression.a");
    println!("cargo:rustc-flags=-l dylib=stdc++");
}

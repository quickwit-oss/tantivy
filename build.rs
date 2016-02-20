extern crate gcc;

fn main() {
    gcc::Config::new()
                .cpp(true)
                .flag("-std=c++11")
                .flag("-O3")
                .include("./cpp/SIMDCompressionAndIntersection/include")
                .object("cpp/SIMDCompressionAndIntersection/bitpacking.o")
                .object("cpp/SIMDCompressionAndIntersection/integratedbitpacking.o")
                .object("cpp/SIMDCompressionAndIntersection/simdbitpacking.o")
                .object("cpp/SIMDCompressionAndIntersection/usimdbitpacking.o")
                .object("cpp/SIMDCompressionAndIntersection/simdintegratedbitpacking.o")
                // .object("cpp/SIMDCompressionAndIntersection/intersection.o")
                .object("cpp/SIMDCompressionAndIntersection/varintdecode.o")
                .object("cpp/SIMDCompressionAndIntersection/streamvbyte.o")
                .object("cpp/SIMDCompressionAndIntersection/simdpackedsearch.o")
                .object("cpp/SIMDCompressionAndIntersection/simdpackedselect.o")
                .object("cpp/SIMDCompressionAndIntersection/frameofreference.o")
                .object("cpp/SIMDCompressionAndIntersection/for.o")
                .file("cpp/encode.cpp")
                .compile("libsimdcompression.a");
    println!("cargo:rustc-flags=-l dylib=stdc++");
}

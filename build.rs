#[cfg(feature= "simdcompression")]
mod build {
    extern crate gcc;

    use std::process::Command;

    pub fn build() {
        Command::new("make")
            .current_dir("cpp/simdcomp")
            .output()
            .unwrap_or_else(|e| { panic!("Failed to make simdcomp: {}", e) });
        gcc::Config::new()
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
                    .file("cpp/simdcomp_wrapper.c")
                    .compile("libsimdcomp.a");
    }
}

#[cfg(not(feature= "simdcompression"))]
mod build {
    pub fn build() {
    }
}


fn main() {
    build::build();
}

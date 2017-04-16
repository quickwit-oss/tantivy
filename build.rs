#[cfg(feature = "simdcompression")]
mod build {
    extern crate gcc;

    pub fn build() {
        let mut config = gcc::Config::new();
        config
            .include("./cpp/simdcomp/include")
            .file("cpp/simdcomp/src/avxbitpacking.c")
            .file("cpp/simdcomp/src/simdintegratedbitpacking.c")
            .file("cpp/simdcomp/src/simdbitpacking.c")
            .file("cpp/simdcomp/src/simdpackedsearch.c")
            .file("cpp/simdcomp/src/simdcomputil.c")
            .file("cpp/simdcomp/src/simdpackedselect.c")
            .file("cpp/simdcomp/src/simdfor.c")
            .file("cpp/simdcomp_wrapper.c")
            .include("./cpp/streamvbyte/include")
            .file("cpp/streamvbyte/src/streamvbyte.c")
            .file("cpp/streamvbyte/src/streamvbytedelta.c")
           ;

        if !cfg!(debug_assertions) {
            config.opt_level(3);

            if cfg!(target_env = "msvc") {
                config
                    .define("NDEBUG", None)
                    .flag("/Gm-")
                    .flag("/GS-")
                    .flag("/Gy")
                    .flag("/Oi")
                    .flag("/GL");
            }
        }

        if !cfg!(target_env = "msvc") {
            config
                .flag("-msse4.1")
                .flag("-march=native")
                .flag("-std=c99");
        }

        config.compile("libsimdcomp.a");

        // Workaround for linking static libraries built with /GL
        // https://github.com/rust-lang/rust/issues/26003
        if !cfg!(debug_assertions) && cfg!(target_env = "msvc") {
            println!("cargo:rustc-link-lib=dylib=simdcomp");
        }
    }
}

#[cfg(not(feature = "simdcompression"))]
mod build {
    pub fn build() {}
}

fn main() {
    build::build();
}

fn main() {
    println!("cargo::rustc-check-cfg=cfg(nightly)");
    let rustc = std::env::var("RUSTC").unwrap_or_else(|_| "rustc".into());
    let output = std::process::Command::new(rustc)
        .arg("--version")
        .output()
        .expect("failed to run rustc");
    if String::from_utf8_lossy(&output.stdout).contains("nightly") {
        println!("cargo:rustc-cfg=nightly");
    }
}

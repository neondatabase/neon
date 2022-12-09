extern crate cc;

fn main() {
    // Tell Cargo that if the given file changes, to rerun this build script.
    println!("cargo:rerun-if-changed=../../pgxn/neon_walredo/shmpipe.c");
    cc::Build::new()
        .file("../../pgxn/neon_walredo/shmpipe.c")
        .compile("libshmpipe.a");
}

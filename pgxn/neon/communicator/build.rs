use std::env;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let crate_dir = env::var("CARGO_MANIFEST_DIR").unwrap();
    println!("cargo:rustc-link-lib=framework=Security");
    println!("cargo:rustc-link-lib=framework=CoreFoundation");
    println!("cargo:rustc-link-lib=framework=CoreServices");
    println!("cargo:rustc-link-lib=framework=IOKit");

    cbindgen::generate(crate_dir).map_or_else(
        |error| match error {
            cbindgen::Error::ParseSyntaxError { .. } => {
                // This means there was a syntax error in the Rust sources. Don't panic, because
                // we want the build to continue and the Rust compiler to hit the error. The
                // Rust compiler produces a better error message than cbindgen.
                eprintln!("Generating C bindings failed because of a Rust syntax error");
            }
            e => panic!("Unable to generate C bindings: {:?}", e),
        },
        |bindings| {
            bindings.write_to_file("communicator_bindings.h");
        },
    );

    Ok(())
}

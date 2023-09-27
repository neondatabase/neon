use std::{env, path::PathBuf};

use anyhow::anyhow;
use bindgen::CargoCallbacks;

fn main() -> anyhow::Result<()> {
    // Finding the location of built libraries and Postgres C headers:
    // - if POSTGRES_INSTALL_DIR is set look into it, otherwise look into `<project_root>/pg_install`
    // - if there's a `bin/pg_config` file use it for getting include server, otherwise use `<project_root>/pg_install/{PG_MAJORVERSION}/include/postgresql/server`
    let pg_install_dir = if let Some(postgres_install_dir) = env::var_os("POSTGRES_INSTALL_DIR") {
        postgres_install_dir.into()
    } else {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../pg_install")
    };

    let pg_install_abs = std::fs::canonicalize(pg_install_dir)?;
    let walproposer_lib_dir = pg_install_abs.join("build/walproposer-lib");

    let search_dir_str = walproposer_lib_dir.to_str().ok_or(anyhow!("Bad path"))?;

    println!("cargo:rustc-link-lib=static=pgport");
    println!("cargo:rustc-link-lib=static=pgcommon");
    println!("cargo:rustc-link-lib=static=walproposer");
    println!("cargo:rustc-link-search={search_dir_str}");

    // The bindgen::Builder is the main entry point
    // to bindgen, and lets you build up options for
    // the resulting bindings.
    let bindings = bindgen::Builder::default()
        // The input header we would like to generate
        // bindings for.
        .header("bindgen_deps.h")
        // Tell cargo to invalidate the built crate whenever any of the
        // included header files changed.
        .parse_callbacks(Box::new(CargoCallbacks))
        .allowlist_function("test_call")
        // Finish the builder and generate the bindings.
        .generate()
        // Unwrap the Result and panic on failure.
        .expect("Unable to generate bindings");

    // Write the bindings to the $OUT_DIR/bindings.rs file.
    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap()).join("bindings.rs");
    bindings
        .write_to_file(out_path)
        .expect("Couldn't write bindings!");

    Ok(())
}

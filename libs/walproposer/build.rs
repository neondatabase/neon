use std::{path::PathBuf, env, process::Command};

use anyhow::{anyhow, Context};
use bindgen::{callbacks::ParseCallbacks, CargoCallbacks};

extern crate bindgen;

fn main() -> anyhow::Result<()> {
    let crate_dir = env::var("CARGO_MANIFEST_DIR").unwrap();

    cbindgen::Builder::new()
        .with_crate(crate_dir)
        .with_language(cbindgen::Language::C)
        .generate()
        .expect("Unable to generate bindings")
        .write_to_file("rust_bindings.h");

    // Tell cargo to invalidate the built crate whenever the wrapper changes
    println!("cargo:rerun-if-changed=bindgen_deps.h,walproposer.c,walproposer.h,test.c");
    println!("cargo:rustc-link-lib=walproposer");
    println!("cargo:rustc-link-search=/home/admin/simulator/libs/walproposer");

    if !std::process::Command::new("./build.sh")
        .output()
        .expect("could not spawn `clang`")
        .status
        .success()
    {
        // Panic if the command was not successful.
        panic!("could not compile object file");
    }

    // println!("cargo:rustc-link-lib=dylib=neon");
    // println!("cargo:rustc-link-search=/Users/arthur/zen/zenith/pg_install/build/neon-v15");
    // println!("cargo:rustc-link-arg=-Wl,-rpath,/Users/arthur/zen/zenith/pg_install/build/neon-v15");

    // // Finding the location of C headers for the Postgres server:
    // // - if POSTGRES_INSTALL_DIR is set look into it, otherwise look into `<project_root>/pg_install`
    // // - if there's a `bin/pg_config` file use it for getting include server, otherwise use `<project_root>/pg_install/{PG_MAJORVERSION}/include/postgresql/server`
    // let pg_install_dir = if let Some(postgres_install_dir) = env::var_os("POSTGRES_INSTALL_DIR") {
    //     postgres_install_dir.into()
    // } else {
    //     PathBuf::from("pg_install")
    // };

    // let pg_version = "v15";
    // let mut pg_install_dir_versioned = pg_install_dir.join(pg_version);
    // if pg_install_dir_versioned.is_relative() {
    //     let cwd = env::current_dir().context("Failed to get current_dir")?;
    //     pg_install_dir_versioned = cwd.join("..").join("..").join(pg_install_dir_versioned);
    // }

    // let pg_config_bin = pg_install_dir_versioned
    //     .join(pg_version)
    //     .join("bin")
    //     .join("pg_config");
    // let inc_server_path: String = if pg_config_bin.exists() {
    //     let output = Command::new(pg_config_bin)
    //         .arg("--includedir-server")
    //         .output()
    //         .context("failed to execute `pg_config --includedir-server`")?;

    //     if !output.status.success() {
    //         panic!("`pg_config --includedir-server` failed")
    //     }

    //     String::from_utf8(output.stdout)
    //         .context("pg_config output is not UTF-8")?
    //         .trim_end()
    //         .into()
    // } else {
    //     let server_path = pg_install_dir_versioned
    //         .join("include")
    //         .join("postgresql")
    //         .join("server")
    //         .into_os_string();
    //     server_path
    //         .into_string()
    //         .map_err(|s| anyhow!("Bad postgres server path {s:?}"))?
    // };

    // let inc_pgxn_path = "/Users/arthur/zen/zenith/pgxn/neon";

    // TODO: build a libwalproposer.a

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
        .allowlist_function("TestFunc")
        // .clang_arg(format!("-I{inc_server_path}"))
        // .clang_arg(format!("-I{inc_pgxn_path}"))
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

extern crate bindgen;

use std::env;
use std::path::PathBuf;
use std::process::Command;

use anyhow::{anyhow, Context};
use bindgen::callbacks::{DeriveInfo, ParseCallbacks};

#[derive(Debug)]
struct PostgresFfiCallbacks;

impl ParseCallbacks for PostgresFfiCallbacks {
    fn include_file(&self, filename: &str) {
        // This does the equivalent of passing bindgen::CargoCallbacks
        // to the builder .parse_callbacks() method.
        let cargo_callbacks = bindgen::CargoCallbacks::new();
        cargo_callbacks.include_file(filename)
    }

    // Add any custom #[derive] attributes to the data structures that bindgen
    // creates.
    fn add_derives(&self, derive_info: &DeriveInfo) -> Vec<String> {
        // This is the list of data structures that we want to serialize/deserialize.
        let serde_list = [
            "XLogRecord",
            "XLogPageHeaderData",
            "XLogLongPageHeaderData",
            "CheckPoint",
            "FullTransactionId",
            "ControlFileData",
        ];

        if serde_list.contains(&derive_info.name) {
            vec![
                "Default".into(), // Default allows us to easily fill the padding fields with 0.
                "Serialize".into(),
                "Deserialize".into(),
            ]
        } else {
            vec![]
        }
    }
}

fn main() -> anyhow::Result<()> {
    // Tell cargo to invalidate the built crate whenever the wrapper changes
    println!("cargo:rerun-if-changed=bindgen_deps.h");

    // Finding the location of C headers for the Postgres server:
    // - if POSTGRES_INSTALL_DIR is set look into it, otherwise look into `<project_root>/pg_install`
    // - if there's a `bin/pg_config` file use it for getting include server, otherwise use `<project_root>/pg_install/{PG_MAJORVERSION}/include/postgresql/server`
    let pg_install_dir = if let Some(postgres_install_dir) = env::var_os("POSTGRES_INSTALL_DIR") {
        postgres_install_dir.into()
    } else {
        PathBuf::from("pg_install")
    };

    for pg_version in &["v14", "v15", "v16", "v17"] {
        let mut pg_install_dir_versioned = pg_install_dir.join(pg_version);
        if pg_install_dir_versioned.is_relative() {
            let cwd = env::current_dir().context("Failed to get current_dir")?;
            pg_install_dir_versioned = cwd.join("..").join("..").join(pg_install_dir_versioned);
        }

        let pg_config_bin = pg_install_dir_versioned.join("bin").join("pg_config");
        let inc_server_path: String = if pg_config_bin.exists() {
            let output = Command::new(pg_config_bin)
                .arg("--includedir-server")
                .output()
                .context("failed to execute `pg_config --includedir-server`")?;

            if !output.status.success() {
                panic!("`pg_config --includedir-server` failed")
            }

            String::from_utf8(output.stdout)
                .context("pg_config output is not UTF-8")?
                .trim_end()
                .into()
        } else {
            let server_path = pg_install_dir_versioned
                .join("include")
                .join("postgresql")
                .join("server")
                .into_os_string();
            server_path
                .into_string()
                .map_err(|s| anyhow!("Bad postgres server path {s:?}"))?
        };

        // The bindgen::Builder is the main entry point
        // to bindgen, and lets you build up options for
        // the resulting bindings.
        let bindings = bindgen::Builder::default()
            //
            // All the needed PostgreSQL headers are included from 'bindgen_deps.h'
            //
            .header("bindgen_deps.h")
            //
            // Tell cargo to invalidate the built crate whenever any of the
            // included header files changed.
            //
            .parse_callbacks(Box::new(PostgresFfiCallbacks))
            //
            // These are the types and constants that we want to generate bindings for
            //
            .allowlist_type("BlockNumber")
            .allowlist_type("OffsetNumber")
            .allowlist_type("XLogRecPtr")
            .allowlist_type("XLogSegNo")
            .allowlist_type("TimeLineID")
            .allowlist_type("TimestampTz")
            .allowlist_type("MultiXactId")
            .allowlist_type("MultiXactOffset")
            .allowlist_type("MultiXactStatus")
            .allowlist_type("ControlFileData")
            .allowlist_type("CheckPoint")
            .allowlist_type("FullTransactionId")
            .allowlist_type("XLogRecord")
            .allowlist_type("XLogPageHeaderData")
            .allowlist_type("XLogLongPageHeaderData")
            .allowlist_var("XLOG_PAGE_MAGIC")
            .allowlist_var("PG_MAJORVERSION_NUM")
            .allowlist_var("PG_CONTROL_FILE_SIZE")
            .allowlist_var("PG_CONTROLFILEDATA_OFFSETOF_CRC")
            .allowlist_type("PageHeaderData")
            .allowlist_type("DBState")
            .allowlist_type("RelMapFile")
            .allowlist_type("RepOriginId")
            // Because structs are used for serialization, tell bindgen to emit
            // explicit padding fields.
            .explicit_padding(true)
            //
            .clang_arg(format!("-I{inc_server_path}"))
            //
            // Finish the builder and generate the bindings.
            //
            .generate()
            .context("Unable to generate bindings")?;

        // Write the bindings to the $OUT_DIR/bindings_$pg_version.rs file.
        let out_path: PathBuf = env::var("OUT_DIR")
            .context("Couldn't read OUT_DIR environment variable var")?
            .into();
        let filename = format!("bindings_{pg_version}.rs");

        bindings
            .write_to_file(out_path.join(filename))
            .context("Couldn't write bindings")?;
    }

    Ok(())
}

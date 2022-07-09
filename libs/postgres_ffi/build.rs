extern crate bindgen;

use std::env;
use std::path::PathBuf;
use std::process::Command;

use bindgen::callbacks::ParseCallbacks;

#[derive(Debug)]
struct PostgresFfiCallbacks;

impl ParseCallbacks for PostgresFfiCallbacks {
    fn include_file(&self, filename: &str) {
        // This does the equivalent of passing bindgen::CargoCallbacks
        // to the builder .parse_callbacks() method.
        let cargo_callbacks = bindgen::CargoCallbacks;
        cargo_callbacks.include_file(filename)
    }

    // Add any custom #[derive] attributes to the data structures that bindgen
    // creates.
    fn add_derives(&self, name: &str) -> Vec<String> {
        // This is the list of data structures that we want to serialize/deserialize.
        let serde_list = [
            "XLogRecord",
            "XLogPageHeaderData",
            "XLogLongPageHeaderData",
            "CheckPoint",
            "FullTransactionId",
            "ControlFileData",
        ];

        if serde_list.contains(&name) {
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

fn main() {
    // Tell cargo to invalidate the built crate whenever the wrapper changes
    println!("cargo:rerun-if-changed=pg_control_ffi.h");

    // Finding the location of C headers for the Postgres server:
    // - if POSTGRES_INSTALL_DIR is set look into it, otherwise look into `<project_root>/tmp_install`
    // - if there's a `bin/pg_config` file use it for getting include server, otherwise use `<project_root>/tmp_install/include/postgresql/server`
    let mut pg_install_dir: PathBuf;
    let inc_server_path: String;

    if let Some(postgres_install_dir) = env::var_os("POSTGRES_INSTALL_DIR") {
        pg_install_dir = postgres_install_dir.into();
    } else {
        pg_install_dir = PathBuf::from("tmp_install")
    }

    if pg_install_dir.is_relative() {
        let cwd = env::current_dir().unwrap();
        pg_install_dir = cwd.join("..").join("..").join(pg_install_dir);
    }

    let pg_config_bin = pg_install_dir.join("bin").join("pg_config");
    if pg_config_bin.exists() {
        let output = Command::new(pg_config_bin)
            .arg("--includedir-server")
            .output()
            .expect("failed to execute `pg_config --includedir-server`");

        if !output.status.success() {
            panic!("`pg_config --includedir-server` failed")
        }

        inc_server_path = String::from_utf8(output.stdout).unwrap().trim_end().into();
    } else {
        inc_server_path = pg_install_dir
            .join("include")
            .join("postgresql")
            .join("server")
            .into_os_string()
            .into_string()
            .unwrap();
    }

    // The bindgen::Builder is the main entry point
    // to bindgen, and lets you build up options for
    // the resulting bindings.
    let bindings = bindgen::Builder::default()
        //
        // All the needed PostgreSQL headers are included from 'pg_control_ffi.h'
        //
        .header("pg_control_ffi.h")
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
        .allowlist_var("PG_CONTROL_FILE_SIZE")
        .allowlist_var("PG_CONTROLFILEDATA_OFFSETOF_CRC")
        .allowlist_type("PageHeaderData")
        .allowlist_type("DBState")
        // Because structs are used for serialization, tell bindgen to emit
        // explicit padding fields.
        .explicit_padding(true)
        //
        .clang_arg(format!("-I{inc_server_path}"))
        //
        // Finish the builder and generate the bindings.
        //
        .generate()
        .expect("Unable to generate bindings");

    // Write the bindings to the $OUT_DIR/bindings.rs file.
    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
    bindings
        .write_to_file(out_path.join("bindings.rs"))
        .expect("Couldn't write bindings!");
}

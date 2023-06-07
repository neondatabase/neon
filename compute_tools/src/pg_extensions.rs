// This is some code for downloading postgres extensions from AWS s3
use std::path::Path;
use clap::{ArgMatches};
use toml_edit;
use remote_storage::*;
use anyhow::Context;

fn get_pg_config(argument: &str) -> String {
    // NOTE: this function panics if it runs into any issues;
    // If this is not desired, should FIXME this
    let config_output = std::process::Command::new("pg_config")
        .arg(argument)
        .output()
        .expect("pg_config should be installed");
    assert!(config_output.status.success());

    let stdout = std::str::from_utf8(&config_output.stdout).unwrap();
    stdout.trim().to_string()
}

fn download_helper(config: &RemoteStorageConfig, from_path: &str, to_path: &str) -> anyhow::Result<()> {
    let remote_storage = GenericRemoteStorage::from_config(config)?;
    let remote_from_path = RemotePath::new(Path::new(from_path))?;
    let _data = remote_storage.download(&remote_from_path);
    println!("received data, hopefully");
    // TODO: somehow write "data" to "to_path"
    // std::fs::write(to_path, XXX.to_string())
    println!("{:?}",to_path);
    Ok(())
}

pub enum ExtensionType {
    Shared,
    Tenant(String),
    Library(String)
}

// TODO: should I make this async?
pub fn download_extension(config: &RemoteStorageConfig, ext_type: ExtensionType) -> anyhow::Result<()>{
    let sharedir = get_pg_config("--sharedir");
    let sharedir = format!("{}/extension", sharedir);
    let libdir = get_pg_config("--libdir");

    match ext_type {
        ExtensionType::Shared => {
            // 1. Download control files from s3-bucket/public/*.control to SHAREDIR/extension
            // We can do this step even before we have spec,
            // because public extensions are common for all projects.
            let from_path = "s3-bucket/public/*.control";
            download_helper(config, from_path, &sharedir)?;
        }
        ExtensionType::Tenant(tenant_id) => {
            // 2. After we have spec, before project start
            // Download control files from s3-bucket/[tenant-id]/*.control to SHAREDIR/extension
            let from_path = format!("s3-bucket/{tenant_id}/*.control");
            download_helper(config, &from_path, &sharedir)?;
        }
        ExtensionType::Library(library_name) => {
            // 3. After we have spec, before postgres start
            // Download preload_shared_libraries from s3-bucket/public/[library-name].control into LIBDIR/
            let from_path = format!("s3-bucket/public/{library_name}.control");
            download_helper(config, &from_path, &libdir)?;
        }
    }
    Ok(())
}

pub fn get_s3_config(arg_matches: &ArgMatches) -> anyhow::Result<RemoteStorageConfig> {
    let workdir = arg_matches
        .get_one::<String>("workdir")
        .map(Path::new)
        .unwrap_or_else(|| Path::new(".neon"));
    let workdir = workdir.canonicalize()
        .with_context(|| format!("Error opening workdir '{}'", workdir.display()))?;

    // TODO: is this the correct file location? I mean I can't see the file...
    let cfg_file_path = workdir.join("pageserver.toml");

    if cfg_file_path.is_file() {
        // Supplement the CLI arguments with the config file
        let cfg_file_contents = std::fs::read_to_string(cfg_file_path)
            .expect("should be able to read pageserver config");
        let toml = cfg_file_contents
            .parse::<toml_edit::Document>()
            .expect("Error parsing toml");

        let remote_storage_data = toml.get("remote_storage")
            .expect("remote_storage field should be present");
        let remote_storage_config = RemoteStorageConfig::from_toml(remote_storage_data)
            .expect("error parsing remote storage config toml")
            .expect("error parsing remote storage config toml");
        return Ok(remote_storage_config);
    } else {
        anyhow::bail!("Couldn't find config file");
    }
}


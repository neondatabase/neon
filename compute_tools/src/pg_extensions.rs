// This is some code for downloading postgres extensions from AWS S3
use std::{ops::ControlFlow, path::Path};
use clap::{ArgMatches};
use tracing::*;
use remote_storage::*;

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
    let mut remote_storage = GenericRemoteStorage::from_config(config)?;
    let remote_from_path = RemotePath::new(Path::new(from_path))?;
    let data = remote_storage.download(&remote_from_path);
    println!("received data, hopefully");
    Ok(())
    // TODO: somehow write "data" to "to_path"
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
            download_helper(config, from_path, &sharedir);
        }
        ExtensionType::Tenant(tenant_id) => {
            // 2. After we have spec, before project start
            // Download control files from s3-bucket/[tenant-id]/*.control to SHAREDIR/extension
            let from_path = format!("s3-bucket/{tenant_id}/*.control");
            download_helper(config, &from_path, &sharedir);
        }
        ExtensionType::Library(library_name) => {
            // 3. After we have spec, before postgres start
            // Download preload_shared_libraries from s3-bucket/public/[library-name].control into LIBDIR/
            let from_path = format!("s3-bucket/public/{library_name}.control");
            download_helper(config, &from_path, &libdir);
        }
    }
    Ok(())
}

pub fn get_S3_config(arg_matches: ArgMatches) -> anyhow::Result<RemoteStorageConfig> {
    let workdir = arg_matches
        .get_one::<String>("workdir")
        .map(Path::new)
        .unwrap_or_else(|| Path::new(".neon"));
    workdir = workdir.canonicalize()
        .with_context(|| format!("Error opening workdir '{}'", workdir.display()))?;

    // TODO: is this the correct file location?
    let cfg_file_path = workdir.join("pageserver.toml");
    let conf = match initialize_config(&cfg_file_path, arg_matches, &workdir)? {
        ControlFlow::Continue(conf) => conf,
        ControlFlow::Break(()) => {
            info!("Pageserver config init successful");
            return Ok(());
        }
    };
    if let Some(config) = &conf.remote_storage_config {
        return config;
    } else { 
        // No remote storage configured.
        return Ok(None);
    };
}


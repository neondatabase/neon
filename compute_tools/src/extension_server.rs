use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::Path;
use std::str;
use std::{fs, thread};
use toml_edit;
use remote_storage::*;

use anyhow::Context;
use tracing::info;

pub fn download_file(mut stream: TcpStream) -> anyhow::Result<()> {
    let mut buf = [0; 512];

    stream.read(&mut buf).expect("Error reading from stream");

    let filename = str::from_utf8(&buf)
        .context("filename is not UTF-8")?
        .trim_end();

    println!("requested file {}", filename);

    download_extension(get_s3_config(), ExtensionType::Shared);
    let from_prefix = "/tmp/from_prefix";
    let to_prefix = "/tmp/to_prefix";

    let filepath = Path::new(from_prefix).join(filename);
    let copy_to_filepath = Path::new(to_prefix).join(filename);
    fs::copy(filepath, copy_to_filepath)?;

    // Write back the response to the TCP stream
    match stream.write("OK".as_bytes()) {
        Err(e) => anyhow::bail!("Read-Server: Error writing to stream {}", e),
        Ok(_) => (),
    }

    Ok(())
}

fn get_pg_config(argument: &str) -> String {
    // FIXME: this function panics if it runs into any issues
    let config_output = std::process::Command::new("pg_config")
        .arg(argument)
        .output()
        .expect("pg_config should be installed");
    assert!(config_output.status.success());
    let stdout = std::str::from_utf8(&config_output.stdout).unwrap();
    stdout.trim().to_string()
}

fn download_helper(remote_storage: &GenericRemoteStorage, remote_from_path: &RemotePath, to_path: &str) -> anyhow::Result<()> {
    let file_name = remote_from_path.object_name().expect("it must exist");
    info!("Downloading {:?}",file_name);
    let mut download = remote_storage.download(&remote_from_path).await?;
    let mut write_data_buffer = Vec::new(); 
    download.download_stream.read_to_end(&mut write_data_buffer).await?;
    let mut output_file = BufWriter::new(File::create(file_name)?);
    output_file.write_all(&mut write_data_buffer)?;
    Ok(())
}

pub enum ExtensionType {
    Shared, // we just use the public folder here
    Tenant(String), // String is tenant_id
    Library(String) // String is name of the extension
}

pub async fn download_extension(config: &RemoteStorageConfig, ext_type: ExtensionType) -> anyhow::Result<()>{
    let sharedir = get_pg_config("--sharedir");
    let sharedir = format!("{}/extension", sharedir);
    let libdir = get_pg_config("--libdir");
    let remote_storage = GenericRemoteStorage::from_config(config)?;

    match ext_type {
        ExtensionType::Shared => {
            // 1. Download control files from s3-bucket/public/*.control to SHAREDIR/extension
            // We can do this step even before we have spec,
            // because public extensions are common for all projects.
            let folder = RemotePath::new(Path::new("public_extensions"))?;
            let from_paths = remote_storage.list_files(Some(&folder)).await?;
            for remote_from_path in from_paths {
                if remote_from_path.extension() == Some("control") {
                    // FIXME: CAUTION: if you run this, it will actually write stuff to your postgress directory
                    // only run if you are ok with that
                    download_helper(&remote_storage, &remote_from_path, &sharedir)?;
                }
            }
        }
        ExtensionType::Tenant(tenant_id) => {
            // 2. After we have spec, before project start
            // Download control files from s3-bucket/[tenant-id]/*.control to SHAREDIR/extension
            let folder = RemotePath::new(Path::new(format!("{tenant_id}")))?;
            let from_paths = remote_storage.list_files(Some(&folder)).await?;
            for remote_from_path in from_paths {
                if remote_from_path.extension() == Some("control") {
                    download_helper(&remote_storage, &remote_from_path, &sharedir)?;
                }
            }
        }
        ExtensionType::Library(library_name) => {
            // 3. After we have spec, before postgres start
            // Download preload_shared_libraries from s3-bucket/public/[library-name].control into LIBDIR/
            let from_path = format!("neon-dev-extensions/public/{library_name}.control");
            let remote_from_path = RemotePath::new(Path::new(&from_path))?;
            download_helper(&remote_storage, &remote_from_path, &libdir)?;
        }
    }
    Ok(())
}

pub fn get_s3_config() -> anyhow::Result<RemoteStorageConfig> {
    // TODO: Right now we are using the same config parameters as pageserver; but should we have our own configs?
    // TODO: Should we read the s3_config from CLI arguments?
    let cfg_file_path = Path::new("./../.neon/pageserver.toml");
    let cfg_file_contents = std::fs::read_to_string(cfg_file_path)
    .with_context(|| format!( "Failed to read pageserver config at '{}'", cfg_file_path.display()))?;
    let toml = cfg_file_contents
        .parse::<toml_edit::Document>()
        .with_context(|| format!( "Failed to parse '{}' as pageserver config", cfg_file_path.display()))?;
    let remote_storage_data = toml.get("remote_storage")
        .context("field should be present")?;
    let remote_storage_config = RemoteStorageConfig::from_toml(remote_storage_data)?
        .context("error configuring remote storage")?;
    Ok(remote_storage_config)
}


//!
//! Functions for handling page server configuration options
//!
//! Configuration options can be set in the pageserver.toml configuration
//! file, or on the command line.

use crate::layered_repository::{TENANTS_SEGMENT_NAME, TIMELINES_SEGMENT_NAME};
use anyhow::{anyhow, bail, ensure, Result};
use toml_edit;
use toml_edit::{Document, Item};
use zenith_utils::postgres_backend::AuthType;
use zenith_utils::zid::{ZTenantId, ZTimelineId};

use std::env;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;

pub mod defaults {
    use const_format::formatcp;

    pub const DEFAULT_PG_LISTEN_PORT: u16 = 64000;
    pub const DEFAULT_PG_LISTEN_ADDR: &str = formatcp!("127.0.0.1:{DEFAULT_PG_LISTEN_PORT}");
    pub const DEFAULT_HTTP_LISTEN_PORT: u16 = 9898;
    pub const DEFAULT_HTTP_LISTEN_ADDR: &str = formatcp!("127.0.0.1:{DEFAULT_HTTP_LISTEN_PORT}");

    // FIXME: This current value is very low. I would imagine something like 1 GB or 10 GB
    // would be more appropriate. But a low value forces the code to be exercised more,
    // which is good for now to trigger bugs.
    pub const DEFAULT_CHECKPOINT_DISTANCE: u64 = 256 * 1024 * 1024;
    pub const DEFAULT_CHECKPOINT_PERIOD: &str = "1 s";

    pub const DEFAULT_GC_HORIZON: u64 = 64 * 1024 * 1024;
    pub const DEFAULT_GC_PERIOD: &str = "100 s";

    pub const DEFAULT_SUPERUSER: &str = "zenith_admin";
    pub const DEFAULT_REMOTE_STORAGE_MAX_CONCURRENT_SYNC_LIMITS: usize = 100;

    pub const DEFAULT_OPEN_MEM_LIMIT: usize = 128 * 1024 * 1024;
    pub const DEFAULT_MAX_FILE_DESCRIPTORS: usize = 100;

    ///
    /// Default built-in configuration file.
    ///
    pub const DEFAULT_CONFIG_FILE: &str = formatcp!(
        r###"
# Initial configuration file created by 'pageserver --init'

#listen_pg_addr = '{DEFAULT_PG_LISTEN_ADDR}'
#listen_http_addr = '{DEFAULT_HTTP_LISTEN_ADDR}'

#checkpoint_distance = '{DEFAULT_CHECKPOINT_DISTANCE}'  # in bytes
#checkpoint_period = '{DEFAULT_CHECKPOINT_PERIOD}'

#gc_period = '{DEFAULT_GC_PERIOD}'
#gc_horizon = '{DEFAULT_GC_HORIZON}'

#open_mem_limit = '{DEFAULT_OPEN_MEM_LIMIT} # in bytes
#max_file_descriptors = '{DEFAULT_MAX_FILE_DESCRIPTORS}

# initial superuser role name to use when creating a new tenant
#initial_superuser_name = '{DEFAULT_SUPERUSER}'

# [remote_storage]

"###
    );
}

use defaults::*;

#[derive(Debug, Clone)]
pub struct PageServerConf {
    pub listen_pg_addr: String,
    pub listen_http_addr: String,

    // Flush out an inmemory layer, if it's holding WAL older than this
    // This puts a backstop on how much WAL needs to be re-digested if the
    // page server crashes.
    pub checkpoint_distance: u64,
    pub checkpoint_period: Duration,

    pub gc_horizon: u64,
    pub gc_period: Duration,
    pub superuser: String,

    pub open_mem_limit: usize,
    pub max_file_descriptors: usize,

    // Repository directory, relative to current working directory.
    // Normally, the page server changes the current working directory
    // to the repository, and 'workdir' is always '.'. But we don't do
    // that during unit testing, because the current directory is global
    // to the process but different unit tests work on different
    // repositories.
    pub workdir: PathBuf,

    pub pg_distrib_dir: PathBuf,

    pub auth_type: AuthType,

    pub auth_validation_public_key_path: Option<PathBuf>,
    pub remote_storage_config: Option<RemoteStorageConfig>,
}

/// External relish storage configuration, enough for creating a client for that storage.
#[derive(Debug, Clone)]
pub struct RemoteStorageConfig {
    /// Limits the number of concurrent sync operations between pageserver and relish storage.
    pub max_concurrent_sync: usize,
    /// The storage connection configuration.
    pub storage: RemoteStorageKind,
}

/// A kind of a relish storage to connect to, with its connection configuration.
#[derive(Debug, Clone)]
pub enum RemoteStorageKind {
    /// Storage based on local file system.
    /// Specify a root folder to place all stored relish data into.
    LocalFs(PathBuf),
    /// AWS S3 based storage, storing all relishes into the root
    /// of the S3 bucket from the config.
    AwsS3(S3Config),
}

/// AWS S3 bucket coordinates and access credentials to manage the bucket contents (read and write).
#[derive(Clone)]
pub struct S3Config {
    /// Name of the bucket to connect to.
    pub bucket_name: String,
    /// The region where the bucket is located at.
    pub bucket_region: String,
    /// "Login" to use when connecting to bucket.
    /// Can be empty for cases like AWS k8s IAM
    /// where we can allow certain pods to connect
    /// to the bucket directly without any credentials.
    pub access_key_id: Option<String>,
    /// "Password" to use when connecting to bucket.
    pub secret_access_key: Option<String>,
}

impl std::fmt::Debug for S3Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Config")
            .field("bucket_name", &self.bucket_name)
            .field("bucket_region", &self.bucket_region)
            .finish()
    }
}

impl PageServerConf {
    //
    // Repository paths, relative to workdir.
    //

    pub fn tenants_path(&self) -> PathBuf {
        self.workdir.join(TENANTS_SEGMENT_NAME)
    }

    pub fn tenant_path(&self, tenantid: &ZTenantId) -> PathBuf {
        self.tenants_path().join(tenantid.to_string())
    }

    pub fn tags_path(&self, tenantid: &ZTenantId) -> PathBuf {
        self.tenant_path(tenantid).join("refs").join("tags")
    }

    pub fn tag_path(&self, tag_name: &str, tenantid: &ZTenantId) -> PathBuf {
        self.tags_path(tenantid).join(tag_name)
    }

    pub fn branches_path(&self, tenantid: &ZTenantId) -> PathBuf {
        self.tenant_path(tenantid).join("refs").join("branches")
    }

    pub fn branch_path(&self, branch_name: &str, tenantid: &ZTenantId) -> PathBuf {
        self.branches_path(tenantid).join(branch_name)
    }

    pub fn timelines_path(&self, tenantid: &ZTenantId) -> PathBuf {
        self.tenant_path(tenantid).join(TIMELINES_SEGMENT_NAME)
    }

    pub fn timeline_path(&self, timelineid: &ZTimelineId, tenantid: &ZTenantId) -> PathBuf {
        self.timelines_path(tenantid).join(timelineid.to_string())
    }

    pub fn ancestor_path(&self, timelineid: &ZTimelineId, tenantid: &ZTenantId) -> PathBuf {
        self.timeline_path(timelineid, tenantid).join("ancestor")
    }

    //
    // Postgres distribution paths
    //

    pub fn pg_bin_dir(&self) -> PathBuf {
        self.pg_distrib_dir.join("bin")
    }

    pub fn pg_lib_dir(&self) -> PathBuf {
        self.pg_distrib_dir.join("lib")
    }

    /// Parse a configuration file (pageserver.toml) into a PageServerConf struct.
    ///
    /// This leaves any options not present in the file in the built-in defaults.
    pub fn parse_config(toml: &Document) -> Result<Self> {
        use defaults::*;

        let mut conf = PageServerConf {
            workdir: PathBuf::from("."),

            listen_pg_addr: DEFAULT_PG_LISTEN_ADDR.to_string(),
            listen_http_addr: DEFAULT_HTTP_LISTEN_ADDR.to_string(),
            checkpoint_distance: DEFAULT_CHECKPOINT_DISTANCE,
            checkpoint_period: humantime::parse_duration(DEFAULT_CHECKPOINT_PERIOD)?,
            gc_horizon: DEFAULT_GC_HORIZON,
            gc_period: humantime::parse_duration(DEFAULT_GC_PERIOD)?,
            open_mem_limit: DEFAULT_OPEN_MEM_LIMIT,
            max_file_descriptors: DEFAULT_MAX_FILE_DESCRIPTORS,

            pg_distrib_dir: PathBuf::from(""),
            auth_validation_public_key_path: None,
            auth_type: AuthType::Trust,

            remote_storage_config: None,

            superuser: DEFAULT_SUPERUSER.to_string(),
        };

        for (key, item) in toml.iter() {
            match key {
                "listen_pg_addr" => conf.listen_pg_addr = parse_toml_str(key, item)?,
                "listen_http_addr" => conf.listen_http_addr = parse_toml_str(key, item)?,
                "checkpoint_distance" => conf.checkpoint_distance = parse_toml_u64(key, item)?,
                "checkpoint_period" => conf.checkpoint_period = parse_toml_duration(key, item)?,
                "gc_horizon" => conf.gc_horizon = parse_toml_u64(key, item)?,
                "gc_period" => conf.gc_period = parse_toml_duration(key, item)?,
                "open_mem_limit" => conf.open_mem_limit = parse_toml_u64(key, item)? as usize,
                "max_file_descriptors" => {
                    conf.max_file_descriptors = parse_toml_u64(key, item)? as usize
                }
                "pg_distrib_dir" => conf.pg_distrib_dir = PathBuf::from(parse_toml_str(key, item)?),
                "auth_validation_public_key_path" => {
                    conf.auth_validation_public_key_path =
                        Some(PathBuf::from(parse_toml_str(key, item)?))
                }
                "auth_type" => conf.auth_type = parse_toml_auth_type(key, item)?,
                "remote_storage" => {
                    conf.remote_storage_config = Some(Self::parse_remote_storage_config(item)?)
                }
                _ => {
                    bail!("unrecognized pageserver option '{}'", key);
                }
            }
        }

        if conf.auth_type == AuthType::ZenithJWT {
            if let Some(path_ref) = &conf.auth_validation_public_key_path {
                ensure!(
                    path_ref.exists(),
                    format!(
                        "Can't find auth_validation_public_key at {}",
                        path_ref.display()
                    )
                );
            } else {
                bail!("Missing auth_validation_public_key_path when auth_type is ZenithJWT");
            }
        }

        if conf.pg_distrib_dir == PathBuf::from("") {
            conf.pg_distrib_dir = env::current_dir()?.join("tmp_install")
        };
        if !conf.pg_distrib_dir.join("bin/postgres").exists() {
            bail!(
                "Can't find postgres binary at {}",
                conf.pg_distrib_dir.display()
            );
        }

        Ok(conf)
    }

    /// subroutine of parse_config(), to parse the `[remote_storage]` table.
    fn parse_remote_storage_config(toml: &toml_edit::Item) -> anyhow::Result<RemoteStorageConfig> {
        let local_path = toml.get("local_path");
        let bucket_name = toml.get("bucket_name");
        let bucket_region = toml.get("bucket_region");

        let max_concurrent_sync: usize =
            if let Some(s) = toml.get("remote_storage_max_concurrent_sync") {
                s.as_str().unwrap().parse()?
            } else {
                DEFAULT_REMOTE_STORAGE_MAX_CONCURRENT_SYNC_LIMITS
            };

        let storage = match (local_path, bucket_name, bucket_region) {
            (None, None, None) => bail!("no 'local_path' nor 'bucket_name' option"),
            (_, Some(_), None) => {
                bail!("'bucket_region' option is mandatory if 'bucket_name' is given ")
            }
            (_, None, Some(_)) => {
                bail!("'bucket_name' option is mandatory if 'bucket_region' is given ")
            }
            (None, Some(bucket_name), Some(bucket_region)) => RemoteStorageKind::AwsS3(S3Config {
                bucket_name: bucket_name.as_str().unwrap().to_string(),
                bucket_region: bucket_region.as_str().unwrap().to_string(),
                access_key_id: toml
                    .get("access_key_id")
                    .map(|x| x.as_str().unwrap().to_string()),
                secret_access_key: toml
                    .get("secret_access_key")
                    .map(|x| x.as_str().unwrap().to_string()),
            }),
            (Some(local_path), None, None) => {
                RemoteStorageKind::LocalFs(PathBuf::from(local_path.as_str().unwrap()))
            }
            (Some(_), Some(_), _) => bail!("local_path and bucket_name are mutually exclusive"),
        };

        Ok(RemoteStorageConfig {
            max_concurrent_sync,
            storage,
        })
    }

    #[cfg(test)]
    pub fn test_repo_dir(test_name: &str) -> PathBuf {
        PathBuf::from(format!("../tmp_check/test_{}", test_name))
    }

    #[cfg(test)]
    pub fn dummy_conf(repo_dir: PathBuf) -> Self {
        PageServerConf {
            checkpoint_distance: defaults::DEFAULT_CHECKPOINT_DISTANCE,
            checkpoint_period: Duration::from_secs(10),
            gc_horizon: defaults::DEFAULT_GC_HORIZON,
            gc_period: Duration::from_secs(10),
            open_mem_limit: defaults::DEFAULT_OPEN_MEM_LIMIT,
            max_file_descriptors: defaults::DEFAULT_MAX_FILE_DESCRIPTORS,
            listen_pg_addr: defaults::DEFAULT_PG_LISTEN_ADDR.to_string(),
            listen_http_addr: defaults::DEFAULT_HTTP_LISTEN_ADDR.to_string(),
            superuser: "zenith_admin".to_string(),
            workdir: repo_dir,
            pg_distrib_dir: "".into(),
            auth_type: AuthType::Trust,
            auth_validation_public_key_path: None,
            remote_storage_config: None,
        }
    }
}

// Helper functions to parse a toml Item

fn parse_toml_str(name: &str, item: &Item) -> Result<String> {
    let s = item
        .as_str()
        .ok_or_else(|| anyhow!("configure option {} is not a string", name))?;
    Ok(s.to_string())
}

fn parse_toml_u64(name: &str, item: &Item) -> Result<u64> {
    // A toml integer is signed, so it cannot represent the full range of an u64. That's OK
    // for our use, though.
    let i: i64 = item
        .as_integer()
        .ok_or_else(|| anyhow!("configure option {} is not an integer", name))?;
    if i < 0 {
        bail!("configure option {} cannot be negative", name);
    }
    Ok(i as u64)
}

fn parse_toml_duration(name: &str, item: &Item) -> Result<Duration> {
    let s = item
        .as_str()
        .ok_or_else(|| anyhow!("configure option {} is not a string", name))?;

    Ok(humantime::parse_duration(s)?)
}

fn parse_toml_auth_type(name: &str, item: &Item) -> Result<AuthType> {
    let v = item
        .as_str()
        .ok_or_else(|| anyhow!("configure option {} is not a string", name))?;
    AuthType::from_str(v)
}

//! Functions for handling per-tenant configuration options
//!
//! If tenant is created with --config option,
//! the tenant-specific config will be stored in tenant's directory.
//! Otherwise, global pageserver's config is used.
//!
//! If the tenant config file is corrupted, the tenant will be disabled.
//! We cannot use global or default config instead, because wrong settings
//! may lead to a data loss.
//!
use crate::config::PageServerConf;
use crate::STORAGE_FORMAT_VERSION;
use anyhow::ensure;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::time::Duration;
use zenith_utils::bin_ser::BeSer;
use zenith_utils::zid::ZTenantId;

pub const TENANT_CONFIG_NAME: &str = "config";

pub mod defaults {
    // FIXME: This current value is very low. I would imagine something like 1 GB or 10 GB
    // would be more appropriate. But a low value forces the code to be exercised more,
    // which is good for now to trigger bugs.
    // This parameter actually determines L0 layer file size.
    pub const DEFAULT_CHECKPOINT_DISTANCE: u64 = 256 * 1024 * 1024;

    // Target file size, when creating image and delta layers.
    // This parameter determines L1 layer file size.
    pub const DEFAULT_COMPACTION_TARGET_SIZE: u64 = 128 * 1024 * 1024;

    pub const DEFAULT_COMPACTION_PERIOD: &str = "1 s";

    pub const DEFAULT_GC_HORIZON: u64 = 64 * 1024 * 1024;
    pub const DEFAULT_GC_PERIOD: &str = "100 s";
    pub const DEFAULT_PITR_INTERVAL: &str = "30 days";
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TenantConf {
    pub checkpoint_distance: u64,
    pub compaction_target_size: u64,
    pub compaction_period: Duration,
    pub gc_horizon: u64,
    pub gc_period: Duration,
    pub pitr_interval: Duration,
}

/// We assume that a write of up to TENANTCONF_MAX_SIZE bytes is atomic.
///
/// This is the same assumption that PostgreSQL makes with the control file,
/// see PG_CONTROL_MAX_SAFE_SIZE
const TENANTCONF_MAX_SIZE: usize = 512;

/// TenantConfFile is stored on disk in tenant's directory
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TenantConfFile {
    hdr: TenantConfHeader,
    pub body: TenantConf,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct TenantConfHeader {
    checksum: u32,       // CRC of serialized tenantconf body
    size: u16,           // size of serialized tenantconf
    format_version: u16, // storage format version (used for compatibility checks)
}
const TENANTCONF_HDR_SIZE: usize = std::mem::size_of::<TenantConfHeader>();

impl TenantConfFile {
    pub fn new(
        checkpoint_distance: u64,
        compaction_target_size: u64,
        compaction_period: Duration,
        gc_horizon: u64,
        gc_period: Duration,
        pitr_interval: Duration,
    ) -> Self {
        Self {
            hdr: TenantConfHeader {
                checksum: 0,
                size: 0,
                format_version: STORAGE_FORMAT_VERSION,
            },
            body: TenantConf {
                gc_period,
                gc_horizon,
                pitr_interval,
                checkpoint_distance,
                compaction_period,
                compaction_target_size,
            },
        }
    }

    pub fn from(tconf: TenantConf) -> Self {
        Self {
            hdr: TenantConfHeader {
                checksum: 0,
                size: 0,
                format_version: STORAGE_FORMAT_VERSION,
            },
            body: TenantConf {
                gc_period: tconf.gc_period,
                gc_horizon: tconf.gc_horizon,
                pitr_interval: tconf.pitr_interval,
                checkpoint_distance: tconf.checkpoint_distance,
                compaction_period: tconf.compaction_period,
                compaction_target_size: tconf.compaction_target_size,
            },
        }
    }

    pub fn from_bytes(tenantconf_bytes: &[u8]) -> anyhow::Result<Self> {
        ensure!(
            tenantconf_bytes.len() == TENANTCONF_MAX_SIZE,
            "tenantconf bytes size is wrong"
        );
        let hdr = TenantConfHeader::des(&tenantconf_bytes[0..TENANTCONF_HDR_SIZE])?;
        ensure!(
            hdr.format_version == STORAGE_FORMAT_VERSION,
            "format version mismatch"
        );
        let tenantconf_size = hdr.size as usize;
        ensure!(
            tenantconf_size <= TENANTCONF_MAX_SIZE,
            "corrupted tenantconf file"
        );
        let calculated_checksum =
            crc32c::crc32c(&tenantconf_bytes[TENANTCONF_HDR_SIZE..tenantconf_size]);
        ensure!(
            hdr.checksum == calculated_checksum,
            "tenantconf checksum mismatch"
        );
        let body = TenantConf::des(&tenantconf_bytes[TENANTCONF_HDR_SIZE..tenantconf_size])?;

        Ok(TenantConfFile { hdr, body })
    }

    pub fn to_bytes(&self) -> anyhow::Result<Vec<u8>> {
        let body_bytes = self.body.ser()?;
        let tenantconf_size = TENANTCONF_HDR_SIZE + body_bytes.len();
        let hdr = TenantConfHeader {
            size: tenantconf_size as u16,
            format_version: STORAGE_FORMAT_VERSION,
            checksum: crc32c::crc32c(&body_bytes),
        };
        let hdr_bytes = hdr.ser()?;
        let mut tenantconf_bytes = vec![0u8; TENANTCONF_MAX_SIZE];
        tenantconf_bytes[0..TENANTCONF_HDR_SIZE].copy_from_slice(&hdr_bytes);
        tenantconf_bytes[TENANTCONF_HDR_SIZE..tenantconf_size].copy_from_slice(&body_bytes);
        Ok(tenantconf_bytes)
    }
}

impl TenantConf {
    pub fn default() -> TenantConf {
        use defaults::*;

        TenantConf {
            checkpoint_distance: DEFAULT_CHECKPOINT_DISTANCE,
            compaction_target_size: DEFAULT_COMPACTION_TARGET_SIZE,
            compaction_period: humantime::parse_duration(DEFAULT_COMPACTION_PERIOD)
                .expect("cannot parse default compaction period"),
            gc_horizon: DEFAULT_GC_HORIZON,
            gc_period: humantime::parse_duration(DEFAULT_GC_PERIOD)
                .expect("cannot parse default gc period"),
            pitr_interval: humantime::parse_duration(DEFAULT_PITR_INTERVAL)
                .expect("cannot parse default PITR interval"),
        }
    }

    /// Points to a place in pageserver's local directory,
    /// where certain tenant's tenantconf file should be located.
    pub fn tenantconf_path(conf: &'static PageServerConf, tenantid: ZTenantId) -> PathBuf {
        conf.tenant_path(&tenantid).join(TENANT_CONFIG_NAME)
    }

    #[cfg(test)]
    pub fn dummy_conf() -> Self {
        TenantConf {
            checkpoint_distance: defaults::DEFAULT_CHECKPOINT_DISTANCE,
            compaction_target_size: 4 * 1024 * 1024,
            compaction_period: Duration::from_secs(10),
            gc_horizon: defaults::DEFAULT_GC_HORIZON,
            gc_period: Duration::from_secs(10),
            pitr_interval: Duration::from_secs(60 * 60),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tenantconf_serializes_correctly() {
        let original_tenantconf = TenantConfFile::new(
            111,
            111,
            Duration::from_secs(111),
            222,
            Duration::from_secs(111),
            Duration::from_secs(60 * 60),
        );

        let tenantconf_bytes = original_tenantconf
            .to_bytes()
            .expect("Should serialize correct tenantconf to bytes");

        let deserialized_tenantconf = TenantConfFile::from_bytes(&tenantconf_bytes)
            .expect("Should deserialize its own bytes");

        assert_eq!(
            deserialized_tenantconf.body, original_tenantconf.body,
            "Tenantconf that was serialized to bytes and deserialized back should not change"
        );
    }
}

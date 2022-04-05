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
    pub fn from(conf: &PageServerConf) -> TenantConf {
        TenantConf {
            gc_period: conf.gc_period,
            gc_horizon: conf.gc_horizon,
            pitr_interval: conf.pitr_interval,
            checkpoint_distance: conf.checkpoint_distance,
            compaction_period: conf.compaction_period,
            compaction_target_size: conf.compaction_target_size,
        }
    }

    /// Points to a place in pageserver's local directory,
    /// where certain tenant's tenantconf file should be located.
    pub fn tenantconf_path(conf: &'static PageServerConf, tenantid: ZTenantId) -> PathBuf {
        conf.tenant_path(&tenantid).join(TENANT_CONFIG_NAME)
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

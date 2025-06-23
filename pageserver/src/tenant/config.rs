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

use pageserver_api::models;
use pageserver_api::shard::{ShardCount, ShardIdentity, ShardNumber, ShardStripeSize};
use serde::{Deserialize, Serialize};
use utils::generation::Generation;

#[derive(Debug, Copy, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum AttachmentMode {
    /// Our generation is current as far as we know, and as far as we know we are the only attached
    /// pageserver.  This is the "normal" attachment mode.
    Single,
    /// Our generation number is current as far as we know, but we are advised that another
    /// pageserver is still attached, and therefore to avoid executing deletions.   This is
    /// the attachment mode of a pagesever that is the destination of a migration.
    Multi,
    /// Our generation number is superseded, or about to be superseded.  We are advised
    /// to avoid remote storage writes if possible, and to avoid sending billing data.  This
    /// is the attachment mode of a pageserver that is the origin of a migration.
    Stale,
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct AttachedLocationConfig {
    pub(crate) generation: Generation,
    pub(crate) attach_mode: AttachmentMode,
    // TODO: add a flag to override AttachmentMode's policies under
    // disk pressure (i.e. unblock uploads under disk pressure in Stale
    // state, unblock deletions after timeout in Multi state)
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct SecondaryLocationConfig {
    /// If true, keep the local cache warm by polling remote storage
    pub(crate) warm: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum LocationMode {
    Attached(AttachedLocationConfig),
    Secondary(SecondaryLocationConfig),
}

/// Per-tenant, per-pageserver configuration.  All pageservers use the same TenantConf,
/// but have distinct LocationConf.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct LocationConf {
    /// The location-specific part of the configuration, describes the operating
    /// mode of this pageserver for this tenant.
    pub(crate) mode: LocationMode,

    /// The detailed shard identity.  This structure is already scoped within
    /// a TenantShardId, but we need the full ShardIdentity to enable calculating
    /// key->shard mappings.
    ///
    /// NB: we store this even for unsharded tenants, so that we agree with storcon on the intended
    /// stripe size. Otherwise, a split request that does not specify a stripe size may use a
    /// different default than storcon, which can lead to incorrect stripe sizes and corruption.
    pub(crate) shard: ShardIdentity,

    /// The pan-cluster tenant configuration, the same on all locations
    pub(crate) tenant_conf: pageserver_api::models::TenantConfig,
}

impl std::fmt::Debug for LocationConf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.mode {
            LocationMode::Attached(conf) => {
                write!(
                    f,
                    "Attached {:?}, gen={:?}",
                    conf.attach_mode, conf.generation
                )
            }
            LocationMode::Secondary(conf) => {
                write!(f, "Secondary, warm={}", conf.warm)
            }
        }
    }
}

impl AttachedLocationConfig {
    /// Consult attachment mode to determine whether we are currently permitted
    /// to delete layers.  This is only advisory, not required for data safety.
    /// See [`AttachmentMode`] for more context.
    pub(crate) fn may_delete_layers_hint(&self) -> bool {
        // TODO: add an override for disk pressure in AttachedLocationConfig,
        // and respect it here.
        match &self.attach_mode {
            AttachmentMode::Single => true,
            AttachmentMode::Multi | AttachmentMode::Stale => {
                // In Multi mode we avoid doing deletions because some other
                // attached pageserver might get 404 while trying to read
                // a layer we delete which is still referenced in their metadata.
                //
                // In Stale mode, we avoid doing deletions because we expect
                // that they would ultimately fail validation in the deletion
                // queue due to our stale generation.
                false
            }
        }
    }

    /// Whether we are currently hinted that it is worthwhile to upload layers.
    /// This is only advisory, not required for data safety.
    /// See [`AttachmentMode`] for more context.
    pub(crate) fn may_upload_layers_hint(&self) -> bool {
        // TODO: add an override for disk pressure in AttachedLocationConfig,
        // and respect it here.
        match &self.attach_mode {
            AttachmentMode::Single | AttachmentMode::Multi => true,
            AttachmentMode::Stale => {
                // In Stale mode, we avoid doing uploads because we expect that
                // our replacement pageserver will already have started its own
                // IndexPart that will never reference layers we upload: it is
                // wasteful.
                false
            }
        }
    }
}

impl LocationConf {
    /// For use when loading from a legacy configuration: presence of a tenant
    /// implies it is in AttachmentMode::Single, which used to be the only
    /// possible state.  This function should eventually be removed.
    pub(crate) fn attached_single(
        tenant_conf: pageserver_api::models::TenantConfig,
        generation: Generation,
        shard_params: &models::ShardParameters,
    ) -> Self {
        Self {
            mode: LocationMode::Attached(AttachedLocationConfig {
                generation,
                attach_mode: AttachmentMode::Single,
            }),
            shard: ShardIdentity::from_params(ShardNumber(0), shard_params),
            tenant_conf,
        }
    }

    /// For use when attaching/re-attaching: update the generation stored in this
    /// structure.  If we were in a secondary state, promote to attached (posession
    /// of a fresh generation implies this).
    pub(crate) fn attach_in_generation(
        &mut self,
        mode: AttachmentMode,
        generation: Generation,
        stripe_size: ShardStripeSize,
    ) {
        match &mut self.mode {
            LocationMode::Attached(attach_conf) => {
                attach_conf.generation = generation;
                attach_conf.attach_mode = mode;
            }
            LocationMode::Secondary(_) => {
                // We are promoted to attached by the control plane's re-attach response
                self.mode = LocationMode::Attached(AttachedLocationConfig {
                    generation,
                    attach_mode: mode,
                })
            }
        }

        self.shard.stripe_size = stripe_size;
    }

    pub(crate) fn try_from(conf: &'_ models::LocationConfig) -> anyhow::Result<Self> {
        let tenant_conf = conf.tenant_conf.clone();

        fn get_generation(conf: &'_ models::LocationConfig) -> Result<Generation, anyhow::Error> {
            conf.generation
                .map(Generation::new)
                .ok_or_else(|| anyhow::anyhow!("Generation must be set when attaching"))
        }

        let mode = match &conf.mode {
            models::LocationConfigMode::AttachedMulti => {
                LocationMode::Attached(AttachedLocationConfig {
                    generation: get_generation(conf)?,
                    attach_mode: AttachmentMode::Multi,
                })
            }
            models::LocationConfigMode::AttachedSingle => {
                LocationMode::Attached(AttachedLocationConfig {
                    generation: get_generation(conf)?,
                    attach_mode: AttachmentMode::Single,
                })
            }
            models::LocationConfigMode::AttachedStale => {
                LocationMode::Attached(AttachedLocationConfig {
                    generation: get_generation(conf)?,
                    attach_mode: AttachmentMode::Stale,
                })
            }
            models::LocationConfigMode::Secondary => {
                anyhow::ensure!(conf.generation.is_none());

                let warm = conf
                    .secondary_conf
                    .as_ref()
                    .map(|c| c.warm)
                    .unwrap_or(false);
                LocationMode::Secondary(SecondaryLocationConfig { warm })
            }
            models::LocationConfigMode::Detached => {
                // Should not have been called: API code should translate this mode
                // into a detach rather than trying to decode it as a LocationConf
                return Err(anyhow::anyhow!("Cannot decode a Detached configuration"));
            }
        };

        let shard = if conf.shard_count == 0 {
            // NB: carry over the persisted stripe size instead of using the default. This doesn't
            // matter for most practical purposes, since unsharded tenants don't use the stripe
            // size, but can cause inconsistencies between storcon and Pageserver and cause manual
            // splits without `new_stripe_size` to use an unintended stripe size.
            ShardIdentity::unsharded_with_stripe_size(ShardStripeSize(conf.shard_stripe_size))
        } else {
            ShardIdentity::new(
                ShardNumber(conf.shard_number),
                ShardCount::new(conf.shard_count),
                ShardStripeSize(conf.shard_stripe_size),
            )?
        };

        Ok(Self {
            shard,
            mode,
            tenant_conf,
        })
    }
}

impl Default for LocationConf {
    // TODO: this should be removed once tenant loading can guarantee that we are never
    // loading from a directory without a configuration.
    // => tech debt since https://github.com/neondatabase/neon/issues/1555
    fn default() -> Self {
        Self {
            mode: LocationMode::Attached(AttachedLocationConfig {
                generation: Generation::none(),
                attach_mode: AttachmentMode::Single,
            }),
            tenant_conf: pageserver_api::models::TenantConfig::default(),
            shard: ShardIdentity::unsharded(),
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn serde_roundtrip_tenant_conf_opt() {
        let small_conf = pageserver_api::models::TenantConfig {
            gc_horizon: Some(42),
            ..Default::default()
        };

        let toml_form = toml_edit::ser::to_string(&small_conf).unwrap();
        assert_eq!(toml_form, "gc_horizon = 42\n");
        assert_eq!(small_conf, toml_edit::de::from_str(&toml_form).unwrap());

        let json_form = serde_json::to_string(&small_conf).unwrap();
        assert_eq!(json_form, "{\"gc_horizon\":42}");
        assert_eq!(small_conf, serde_json::from_str(&json_form).unwrap());
    }
}

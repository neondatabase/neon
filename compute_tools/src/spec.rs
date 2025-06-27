use std::fs::File;
use std::path::Path;

use anyhow::{Result, anyhow, bail};
use compute_api::responses::{
    ComputeConfig, ControlPlaneComputeStatus, ControlPlaneConfigResponse,
};
use reqwest::StatusCode;
use tokio_postgres::Client;
use tracing::{error, info, instrument};

use crate::config;
use crate::metrics::{CPLANE_REQUESTS_TOTAL, CPlaneRequestRPC, UNKNOWN_HTTP_STATUS};
use crate::migration::MigrationRunner;
use crate::params::PG_HBA_ALL_MD5;

// Do control plane request and return response if any. In case of error it
// returns a bool flag indicating whether it makes sense to retry the request
// and a string with error message.
fn do_control_plane_request(
    uri: &str,
    jwt: &str,
) -> Result<ControlPlaneConfigResponse, (bool, String, String)> {
    let resp = reqwest::blocking::Client::new()
        .get(uri)
        .header("Authorization", format!("Bearer {jwt}"))
        .send()
        .map_err(|e| {
            (
                true,
                format!("could not perform request to control plane: {e:?}"),
                UNKNOWN_HTTP_STATUS.to_string(),
            )
        })?;

    let status = resp.status();
    match status {
        StatusCode::OK => match resp.json::<ControlPlaneConfigResponse>() {
            Ok(spec_resp) => Ok(spec_resp),
            Err(e) => Err((
                true,
                format!("could not deserialize control plane response: {e:?}"),
                status.to_string(),
            )),
        },
        StatusCode::SERVICE_UNAVAILABLE => Err((
            true,
            "control plane is temporarily unavailable".to_string(),
            status.to_string(),
        )),
        StatusCode::BAD_GATEWAY => {
            // We have a problem with intermittent 502 errors now
            // https://github.com/neondatabase/cloud/issues/2353
            // It's fine to retry GET request in this case.
            Err((
                true,
                "control plane request failed with 502".to_string(),
                status.to_string(),
            ))
        }
        // Another code, likely 500 or 404, means that compute is unknown to the control plane
        // or some internal failure happened. Doesn't make much sense to retry in this case.
        _ => Err((
            false,
            format!("unexpected control plane response status code: {status}"),
            status.to_string(),
        )),
    }
}

/// Request config from the control-plane by compute_id. If
/// `NEON_CONTROL_PLANE_TOKEN` env variable is set, it will be used for
/// authorization.
pub fn get_config_from_control_plane(base_uri: &str, compute_id: &str) -> Result<ComputeConfig> {
    let cp_uri = format!("{base_uri}/compute/api/v2/computes/{compute_id}/spec");
    let jwt: String = std::env::var("NEON_CONTROL_PLANE_TOKEN").unwrap_or_default();
    let mut attempt = 1;

    info!("getting config from control plane: {}", cp_uri);

    // Do 3 attempts to get spec from the control plane using the following logic:
    // - network error -> then retry
    // - compute id is unknown or any other error -> bail out
    // - no spec for compute yet (Empty state) -> return Ok(None)
    // - got config -> return Ok(Some(config))
    while attempt < 4 {
        let result = match do_control_plane_request(&cp_uri, &jwt) {
            Ok(config_resp) => {
                CPLANE_REQUESTS_TOTAL
                    .with_label_values(&[
                        CPlaneRequestRPC::GetConfig.as_str(),
                        &StatusCode::OK.to_string(),
                    ])
                    .inc();
                match config_resp.status {
                    ControlPlaneComputeStatus::Empty => Ok(config_resp.into()),
                    ControlPlaneComputeStatus::Attached => {
                        if config_resp.spec.is_some() {
                            Ok(config_resp.into())
                        } else {
                            bail!("compute is attached, but spec is empty")
                        }
                    }
                }
            }
            Err((retry, msg, status)) => {
                CPLANE_REQUESTS_TOTAL
                    .with_label_values(&[CPlaneRequestRPC::GetConfig.as_str(), &status])
                    .inc();
                if retry {
                    Err(anyhow!(msg))
                } else {
                    bail!(msg);
                }
            }
        };

        if let Err(e) = &result {
            error!("attempt {} to get config failed with: {}", attempt, e);
        } else {
            return result;
        }

        attempt += 1;
        std::thread::sleep(std::time::Duration::from_millis(100));
    }

    // All attempts failed, return error.
    Err(anyhow::anyhow!(
        "Exhausted all attempts to retrieve the config from the control plane"
    ))
}

/// Check `pg_hba.conf` and update if needed to allow external connections.
pub fn update_pg_hba(pgdata_path: &Path) -> Result<()> {
    // XXX: consider making it a part of config.json
    let pghba_path = pgdata_path.join("pg_hba.conf");

    if config::line_in_file(&pghba_path, PG_HBA_ALL_MD5)? {
        info!("updated pg_hba.conf to allow external connections");
    } else {
        info!("pg_hba.conf is up-to-date");
    }

    Ok(())
}

/// Create a standby.signal file
pub fn add_standby_signal(pgdata_path: &Path) -> Result<()> {
    // XXX: consider making it a part of config.json
    let signalfile = pgdata_path.join("standby.signal");

    if !signalfile.exists() {
        File::create(signalfile)?;
        info!("created standby.signal");
    } else {
        info!("reused pre-existing standby.signal");
    }
    Ok(())
}

#[instrument(skip_all)]
pub async fn handle_neon_extension_upgrade(client: &mut Client) -> Result<()> {
    let query = "ALTER EXTENSION neon UPDATE";
    info!("update neon extension version with query: {}", query);
    client.simple_query(query).await?;

    Ok(())
}

#[instrument(skip_all)]
pub async fn handle_migrations(client: &mut Client) -> Result<()> {
    info!("handle migrations");

    // !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    // !BE SURE TO ONLY ADD MIGRATIONS TO THE END OF THIS ARRAY. IF YOU DO NOT, VERY VERY BAD THINGS MAY HAPPEN!
    // !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

    // Add new migrations in numerical order.
    let migrations = [
        include_str!("./migrations/0001-neon_superuser_bypass_rls.sql"),
        include_str!("./migrations/0002-alter_roles.sql"),
        include_str!("./migrations/0003-grant_pg_create_subscription_to_neon_superuser.sql"),
        include_str!("./migrations/0004-grant_pg_monitor_to_neon_superuser.sql"),
        include_str!("./migrations/0005-grant_all_on_tables_to_neon_superuser.sql"),
        include_str!("./migrations/0006-grant_all_on_sequences_to_neon_superuser.sql"),
        include_str!(
            "./migrations/0007-grant_all_on_tables_to_neon_superuser_with_grant_option.sql"
        ),
        include_str!(
            "./migrations/0008-grant_all_on_sequences_to_neon_superuser_with_grant_option.sql"
        ),
        include_str!("./migrations/0009-revoke_replication_for_previously_allowed_roles.sql"),
        include_str!(
            "./migrations/0010-grant_snapshot_synchronization_funcs_to_neon_superuser.sql"
        ),
        include_str!(
            "./migrations/0011-grant_pg_show_replication_origin_status_to_neon_superuser.sql"
        ),
    ];

    MigrationRunner::new(client, &migrations)
        .run_migrations()
        .await?;

    Ok(())
}

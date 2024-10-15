use compute_api::responses::{InstalledExtension, InstalledExtensions};
use metrics::proto::MetricFamily;
use std::collections::HashMap;
use std::collections::HashSet;
use url::Url;
use utils::id::TenantId;
use utils::id::TimelineId;

use anyhow::Result;
use postgres::{Client, NoTls};
use tokio::task;

use metrics::core::Collector;
use metrics::{register_uint_gauge_vec, UIntGaugeVec};
use once_cell::sync::Lazy;

/// We don't reuse get_existing_dbs() just for code clarity
/// and to make database listing query here more explicit.
///
/// Limit the number of databases to 500 to avoid excessive load.
fn list_dbs(client: &mut Client) -> Result<Vec<String>> {
    // `pg_database.datconnlimit = -2` means that the database is in the
    // invalid state
    let databases = client
        .query(
            "SELECT datname FROM pg_catalog.pg_database
                WHERE datallowconn
                AND datconnlimit <> - 2
                LIMIT 500",
            &[],
        )?
        .iter()
        .map(|row| {
            let db: String = row.get("datname");
            db
        })
        .collect();

    Ok(databases)
}

/// Connect to every database (see list_dbs above) and get the list of installed extensions.
/// Same extension can be installed in multiple databases with different versions,
/// we only keep the highest and lowest version across all databases.
pub async fn get_installed_extensions(
    connstr: Url,
    tenant_id: TenantId,
    timeline_id: TimelineId,
) -> Result<InstalledExtensions> {
    let mut connstr = connstr.clone();

    task::spawn_blocking(move || {
        let mut client = Client::connect(connstr.as_str(), NoTls)?;
        let databases: Vec<String> = list_dbs(&mut client)?;

        let mut extensions_map: HashMap<String, InstalledExtension> = HashMap::new();
        for db in databases.iter() {
            connstr.set_path(db);
            let mut db_client = Client::connect(connstr.as_str(), NoTls)?;
            let extensions: Vec<(String, String)> = db_client
                .query(
                    "SELECT extname, extversion FROM pg_catalog.pg_extension;",
                    &[],
                )?
                .iter()
                .map(|row| (row.get("extname"), row.get("extversion")))
                .collect();

            for (extname, v) in extensions.iter() {
                let version = v.to_string();
                extensions_map
                    .entry(extname.to_string())
                    .and_modify(|e| {
                        e.versions.insert(version.clone());
                        // count the number of databases where the extension is installed
                        e.n_databases += 1;
                    })
                    .or_insert(InstalledExtension {
                        extname: extname.to_string(),
                        versions: HashSet::from([version.clone()]),
                        n_databases: 1,
                    });
            }
        }

        let res = InstalledExtensions {
            extensions: extensions_map.values().cloned().collect(),
        };

        // set the prometheus metrics
        for ext in res.extensions.iter() {
            let versions = {
                let mut vec: Vec<_> = ext.versions.iter().cloned().collect();
                vec.sort();
                vec.iter()
                    .map(|x| x.to_string())
                    .collect::<Vec<_>>()
                    .join(",")
            };

            INSTALLED_EXTENSIONS
                .with_label_values(&[
                    &tenant_id.to_string(),
                    &timeline_id.to_string(),
                    &ext.extname,
                    &versions,
                ])
                .set(ext.n_databases as u64);
        }

        Ok(res)
    })
    .await?
}

static INSTALLED_EXTENSIONS: Lazy<UIntGaugeVec> = Lazy::new(|| {
    register_uint_gauge_vec!(
        "installed_extensions",
        "Number of databases where extension is installed, versions passed as label",
        &["tenant_id", "timeline_id", "extension_name", "versions"]
    )
    .expect("failed to define a metric")
});

pub fn collect() -> Vec<MetricFamily> {
    INSTALLED_EXTENSIONS.collect()
}

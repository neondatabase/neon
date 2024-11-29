use compute_api::responses::{InstalledExtension, InstalledExtensions};
use metrics::proto::MetricFamily;
use std::collections::HashMap;
use std::collections::HashSet;

use anyhow::Result;
use postgres::{Client, NoTls};

use metrics::core::Collector;
use metrics::{register_uint_gauge_vec, UIntGaugeVec};
use once_cell::sync::Lazy;

use crate::pg_helpers::postgres_conf_for_db;

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
///
/// Same extension can be installed in multiple databases with different versions,
/// we only keep the highest and lowest version across all databases.
pub fn get_installed_extensions(connstr: &url::Url) -> Result<InstalledExtensions> {
    let mut client = Client::connect(connstr.as_str(), NoTls)?;
    let databases: Vec<String> = list_dbs(&mut client)?;

    let mut extensions_map: HashMap<String, InstalledExtension> = HashMap::new();
    for db in databases.iter() {
        let config = postgres_conf_for_db(connstr, db)?;
        let mut db_client = config.connect(NoTls)?;
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

            // increment the number of databases where the version of extension is installed
            INSTALLED_EXTENSIONS
                .with_label_values(&[extname, &version])
                .inc();

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

    Ok(res)
}

static INSTALLED_EXTENSIONS: Lazy<UIntGaugeVec> = Lazy::new(|| {
    register_uint_gauge_vec!(
        "compute_installed_extensions",
        "Number of databases where the version of extension is installed",
        &["extension_name", "version"]
    )
    .expect("failed to define a metric")
});

pub fn collect() -> Vec<MetricFamily> {
    INSTALLED_EXTENSIONS.collect()
}

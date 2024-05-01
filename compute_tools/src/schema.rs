use std::{path::Path, process::Command, sync::Arc};

use anyhow::Result;
use compute_api::{
    responses::SchemaObjects,
    spec::{Database, Role},
};
use postgres::{Client, NoTls};
use tokio::task;

use crate::{
    compute::ComputeNode,
    pg_helpers::{get_existing_dbs, get_existing_roles},
};

pub async fn get_schema_objects(compute: &Arc<ComputeNode>) -> Result<SchemaObjects> {
    let connstr = compute.connstr.clone();
    task::spawn_blocking(move || {
        let mut client = Client::connect(connstr.as_str(), NoTls)?;
        let roles: Vec<Role>;
        {
            let mut xact = client.transaction()?;
            roles = get_existing_roles(&mut xact)?;
        }
        let databases: Vec<Database> = get_existing_dbs(&mut client)?.values().cloned().collect();

        Ok(SchemaObjects { roles, databases })
    })
    .await?
}

pub async fn schema_dump(compute: &Arc<ComputeNode>) -> Result<Vec<u8>> {
    let pgbin = &compute.pgbin;
    let basepath = Path::new(pgbin).parent().unwrap();
    let pgdump = basepath.join("pg_dump");
    let connstr = compute.connstr.clone();
    let res = Command::new(pgdump)
        .arg("--schema-only")
        .arg(connstr.as_str())
        .output()?;

    Ok(res.stdout)
}

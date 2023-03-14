use std::collections::{BTreeSet, HashMap, VecDeque};
use std::path::Path;

use anyhow::Context;
use tokio::fs;
use tracing::info;

use crate::TenantId;

#[derive(Clone, Debug, Hash, PartialEq, Eq, serde::Serialize)]
#[serde(transparent)]
pub struct BucketName(pub String);

#[derive(Debug)]
pub struct TenantsToClean {
    buckets: Vec<BucketName>,
    tenants: VecDeque<(TenantId, usize)>,
}

const ALIVE_TENANTS_FILE_BASE_NAME: &str = "alive_tenants";
const BUCKET_TENANTS_FILE_SUFFIX: &str = "_tenants_list";

impl TenantsToClean {
    pub async fn new(dir_with_files: &Path) -> anyhow::Result<Self> {
        anyhow::ensure!(
            dir_with_files.is_dir(),
            "{dir_with_files:?} is not an existing directory"
        );

        let mut alive_tenants = BTreeSet::new();
        let mut bucket_tenants = HashMap::new();

        let mut files = fs::read_dir(dir_with_files)
            .await
            .context("base dir list")?;
        while let Some(dir_entry) = files.next_entry().await.context("file entry retrieval")? {
            let path = dir_entry.path();
            if !path.is_file() {
                info!("Skipping {path:?} as it's not the file");
                continue;
            }

            let extension = path.extension().and_then(|os_str| os_str.to_str());
            if extension != Some("txt") {
                info!("Skipping {path:?} as it's not a txt file");
                continue;
            }
            let Some(base_name) = path.file_stem().and_then(|os_str| os_str.to_str())
                else {
                    info!("Skipping {path:?} since it has no name");
                    continue;
                };

            if base_name == ALIVE_TENANTS_FILE_BASE_NAME {
                alive_tenants.extend(read_alive_tenants(&path).await?);
            } else if let Some(bucket_name) = base_name.strip_suffix(BUCKET_TENANTS_FILE_SUFFIX) {
                let bucket_files = read_bucket_tenants(&path).await?;
                let bucket_name = BucketName(bucket_name.to_owned());
                let old = bucket_tenants.insert(bucket_name, bucket_files);
                anyhow::ensure!(
                    old.is_none(),
                    "Bucket name got repeated twice in the file names, new file name: {base_name}"
                );
            } else {
                info!("Skipping unknown txt file {path:?}");
                continue;
            }
        }

        anyhow::ensure!(!alive_tenants.is_empty(), "Found no alive tenants");

        let mut buckets: Vec<BucketName> = Vec::with_capacity(bucket_tenants.len());
        let mut tenants = VecDeque::new();
        for (bucket, tenants_in_bucket) in bucket_tenants {
            let bucket_index = bucket_index(&mut buckets, bucket);
            tenants.extend(
                tenants_in_bucket
                    .into_iter()
                    .filter(|tenant_in_bucket| !alive_tenants.contains(tenant_in_bucket))
                    .map(|tenant_id| (tenant_id, bucket_index)),
            );
        }

        Ok(Self { buckets, tenants })
    }

    pub fn next_tenant(&mut self) -> Option<(BucketName, TenantId)> {
        self.tenants
            .pop_front()
            .map(|(tenant_id, bucket_index)| (self.buckets[bucket_index].clone(), tenant_id))
    }

    pub fn reschedule(&mut self, bucket: BucketName, tenant_id: TenantId) {
        let bucket_index = bucket_index(&mut self.buckets, bucket);
        self.tenants.push_back((tenant_id, bucket_index));
    }

    pub fn tenant_count(&self) -> usize {
        self.tenants.len()
    }

    pub fn tenant_ids(&self) -> Vec<TenantId> {
        self.tenants.iter().map(|(id, _)| id).copied().collect()
    }
}

fn bucket_index(buckets: &mut Vec<BucketName>, bucket: BucketName) -> usize {
    match buckets
        .iter()
        .enumerate()
        .find(|(_, bucket_name)| bucket_name == &&bucket)
    {
        Some((index, _)) => index,
        None => {
            buckets.push(bucket);
            buckets.len() - 1
        }
    }
}

async fn read_alive_tenants(path: &Path) -> anyhow::Result<BTreeSet<TenantId>> {
    let file_contents = fs::read_to_string(path)
        .await
        .context("alive tenants file read")?;
    let mut alive_tenant_strings = file_contents
        .trim()
        .lines()
        .map(|line| line.trim())
        .collect::<VecDeque<_>>();

    let last_string = alive_tenant_strings
        .pop_back()
        .context("no lines in the file")?;
    anyhow::ensure!(
        last_string.parse::<TenantId>().is_err(),
        "Last line {last_string} in the file should not be a tenant id"
    );
    let first_string = alive_tenant_strings
        .pop_front()
        .context("less than 2 lines in the file")?;
    anyhow::ensure!(
        first_string.parse::<TenantId>().is_err(),
        "First line {first_string} in the file should not be a tenant id"
    );
    let second_string = alive_tenant_strings
        .pop_front()
        .context("less than 3 lines in the file")?;
    anyhow::ensure!(
        second_string.parse::<TenantId>().is_err(),
        "Second line {second_string} in the file should not be a tenant id"
    );

    alive_tenant_strings
        .into_iter()
        .map(|tenant_str| {
            tenant_str
                .parse()
                .with_context(|| format!("parsing alive tenant str {tenant_str} as TenantId"))
        })
        .collect::<anyhow::Result<_>>()
}

async fn read_bucket_tenants(path: &Path) -> anyhow::Result<BTreeSet<TenantId>> {
    let expected_prefix = "PRE ";
    fs::read_to_string(path)
        .await
        .context("alive tenants file read")?
        .trim()
        .lines()
        .map(|line| line.trim())
        .map(|line| {
            anyhow::ensure!(
                line.starts_with(expected_prefix),
                "Unexpected bucket line {line}"
            );
            anyhow::ensure!(line.ends_with('/'), "Unexpected bucket line {line}");

            let tenant_str = &line[expected_prefix.len()..line.len() - 1];
            tenant_str
                .parse()
                .with_context(|| format!("parsing alive tenant str {tenant_str} as TenantId"))
        })
        .collect::<anyhow::Result<_>>()
}

use crate::{cplane_api::ClientCredentials, db::DatabaseConnInfo};


#[async_trait::async_trait]
pub trait ComputeProvider {
    async fn get_compute_node(&self, creds: &ClientCredentials) -> anyhow::Result<DatabaseConnInfo>;
}

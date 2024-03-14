use std::{collections::HashMap, str::FromStr};

use clap::{Parser, Subcommand};
use hyper::Method;
use pageserver_api::{
    models::{
        LocationConfig, ShardParameters, TenantConfig, TenantCreateRequest,
        TenantShardSplitRequest, TenantShardSplitResponse,
    },
    shard::{ShardStripeSize, TenantShardId},
};
use pageserver_client::mgmt_api::{self, ResponseErrorMessageExt};
use reqwest::Url;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use utils::id::{NodeId, TenantId};

use pageserver_api::controller_api::{
    NodeAvailability, NodeConfigureRequest, NodeRegisterRequest, NodeSchedulingPolicy,
    PlacementPolicy, TenantLocateResponse, TenantShardMigrateRequest, TenantShardMigrateResponse,
};

// TODO: de-duplicate struct definition.  Controller uses its internal `Node` for list endpoint.
#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct Node {
    id: NodeId,

    availability: NodeAvailability,
    scheduling: NodeSchedulingPolicy,

    listen_http_addr: String,
    listen_http_port: u16,

    listen_pg_addr: String,
    listen_pg_port: u16,
}

#[derive(Subcommand, Debug)]
enum Command {
    NodeRegister {
        #[arg(long)]
        node_id: NodeId,

        #[arg(long)]
        listen_pg_addr: String,
        #[arg(long)]
        listen_pg_port: u16,

        #[arg(long)]
        listen_http_addr: String,
        #[arg(long)]
        listen_http_port: u16,
    },
    NodeConfigure {
        #[arg(long)]
        node_id: NodeId,

        #[arg(long)]
        availability: Option<NodeAvailability>,
        #[arg(long)]
        scheduling: Option<NodeSchedulingPolicy>,
    },
    Nodes {},
    TenantCreate {
        #[arg(long)]
        tenant_id: TenantId,
    },
    TenantDelete {
        #[arg(long)]
        tenant_id: TenantId,
    },
    TenantShardSplit {
        #[arg(long)]
        tenant_id: TenantId,
        #[arg(long)]
        shard_count: u8,
        #[arg(long)]
        stripe_size: Option<u32>,
    },
    TenantShardMigrate {
        #[arg(long)]
        tenant_shard_id: TenantShardId,
        #[arg(long)]
        node: NodeId,
    },
    TenantConfig {
        #[arg(long)]
        tenant_id: TenantId,
        #[arg(long)]
        config: String,
    },
    ServiceRegister {
        #[arg(long)]
        http_host: String,
        #[arg(long)]
        http_port: u16,
        #[arg(long)]
        region_id: String,
        #[arg(long)]
        availability_zone_id: String,
        #[arg(long)]
        version: u64,
        // {
        //   "version": 1,
        //   "host": "${HOST}",
        //   "port": 6400,
        //   "region_id": "{{ console_region_id }}",
        //   "instance_id": "${INSTANCE_ID}",
        //   "http_host": "${HOST}",
        //   "http_port": 9898,
        //   "active": false,
        //   "availability_zone_id": "${AZ_ID}",
        //   "disk_size": ${DISK_SIZE},
        //   "instance_type": "${INSTANCE_TYPE}",
        //   "register_reason" : "New pageserver"
        // }
    },
    TenantOnboard {
        #[arg(long)]
        tenant_id: TenantId,
        #[arg(long)]
        target_pageserver_id: NodeId,
    },
    TenantScatter {
        #[arg(long)]
        tenant_id: TenantId,
    },
}

/// Request format for control plane POST /management/api/v2/pageservers
///
/// This is how physical pageservers register, but in this context it is how we
/// register the storage controller with the control plane, as a "virtual pageserver"
#[derive(Serialize, Deserialize, Debug)]
struct ServiceRegisterRequest {
    version: u64,
    host: String,
    port: u16,
    /// This is the **Neon** region ID, which looks something like `aws-eu-west-1`
    region_id: String,
    /// This expects an EC2 instance ID, for bare metal pageservers.  But it can be any unique identifier.
    instance_id: String,
    http_host: String,
    http_port: u16,
    /// This is an EC2 AZ name (the ZoneName, not actually the AZ ID).  e.g. eu-west-1b
    availability_zone_id: String,
    disk_size: u64,
    /// EC2 instance type.  If it doesn't make sense, leave it blank.
    instance_type: String,
    /// Freeform memo describing the request
    register_reason: String,
    // Set to true to indicate that this 'pageserver' is really a storage controller
    // FIXME: have to omit this because control plane won't allow setting it for existing pageserver
    is_storage_controller: bool,
}

#[derive(Serialize, Deserialize, Debug)]
struct ServiceRegisterResponse {
    // This is a partial representation of the management API's swagger `Pageserver` type.  Unused
    // fields are ignored.
    id: u64,
    node_id: u64,
    instance_id: String,
}

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
#[command(arg_required_else_help(true))]
struct Cli {
    #[arg(long)]
    api: Url,

    #[arg(long)]
    jwt: Option<String>,

    #[command(subcommand)]
    command: Command,
}

struct Client {
    base_url: Url,
    jwt_token: Option<String>,
    client: reqwest::Client,
}

impl Client {
    fn new(base_url: Url, jwt_token: Option<String>) -> Self {
        Self {
            base_url,
            jwt_token,
            client: reqwest::ClientBuilder::new()
                .build()
                .expect("Failed to construct http client"),
        }
    }

    /// Simple HTTP request wrapper for calling into attachment service
    async fn dispatch<RQ, RS>(
        &self,
        method: hyper::Method,
        path: String,
        body: Option<RQ>,
    ) -> mgmt_api::Result<RS>
    where
        RQ: Serialize + Sized,
        RS: DeserializeOwned + Sized,
    {
        // The configured URL has the /upcall path prefix for pageservers to use: we will strip that out
        // for general purpose API access.
        let url = Url::from_str(&format!(
            "http://{}:{}/{path}",
            self.base_url.host_str().unwrap(),
            self.base_url.port().unwrap()
        ))
        .unwrap();

        let mut builder = self.client.request(method, url);
        if let Some(body) = body {
            builder = builder.json(&body)
        }
        if let Some(jwt_token) = &self.jwt_token {
            builder = builder.header(
                reqwest::header::AUTHORIZATION,
                format!("Bearer {jwt_token}"),
            );
        }

        let response = builder.send().await.map_err(mgmt_api::Error::ReceiveBody)?;
        let response = response.error_from_body().await?;

        Ok(response
            .json()
            .await
            .map_err(pageserver_client::mgmt_api::Error::ReceiveBody)?)
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    let storcon_client = Client::new(cli.api.clone(), cli.jwt.clone());

    let mut trimmed = cli.api.to_string();
    trimmed.pop();
    let vps_client = mgmt_api::Client::new(trimmed, cli.jwt.as_ref().map(|s| s.as_str()));

    match cli.command {
        Command::NodeRegister {
            node_id,
            listen_pg_addr,
            listen_pg_port,
            listen_http_addr,
            listen_http_port,
        } => {
            storcon_client
                .dispatch::<_, ()>(
                    Method::POST,
                    "control/v1/node".to_string(),
                    Some(NodeRegisterRequest {
                        node_id,
                        listen_pg_addr,
                        listen_pg_port,
                        listen_http_addr,
                        listen_http_port,
                    }),
                )
                .await?;
        }
        Command::TenantCreate { tenant_id } => {
            vps_client
                .tenant_create(&TenantCreateRequest {
                    new_tenant_id: TenantShardId::unsharded(tenant_id),
                    generation: None,
                    shard_parameters: ShardParameters::default(),
                    placement_policy: Some(PlacementPolicy::Double(1)),
                    config: TenantConfig::default(),
                })
                .await?;
        }
        Command::TenantDelete { tenant_id } => {
            let status = vps_client
                .tenant_delete(TenantShardId::unsharded(tenant_id))
                .await?;
            tracing::info!("Delete status: {}", status);
        }
        Command::Nodes {} => {
            let resp = storcon_client
                .dispatch::<(), Vec<Node>>(Method::GET, "control/v1/node".to_string(), None)
                .await?;
            let mut table = comfy_table::Table::new();
            table.set_header(["Id", "Hostname", "Scheduling", "Availability"]);
            for node in resp {
                table.add_row([
                    format!("{}", node.id),
                    node.listen_http_addr,
                    format!("{:?}", node.scheduling),
                    format!("{:?}", node.availability),
                ]);
            }
            println!("{table}");
        }
        Command::NodeConfigure {
            node_id,
            availability,
            scheduling,
        } => {
            let req = NodeConfigureRequest {
                node_id,
                availability,
                scheduling,
            };
            storcon_client
                .dispatch::<_, ()>(
                    Method::PUT,
                    format!("control/v1/node/{node_id}/config"),
                    Some(req),
                )
                .await?;
        }
        Command::ServiceRegister {
            http_host,
            http_port,
            region_id,
            availability_zone_id,
            version,
        } => {
            let instance_id = http_host.clone();
            let req = ServiceRegisterRequest {
                instance_id: instance_id.clone(),
                // We do not expose postgres protocol, but provide a valid-looking host/port for it
                host: http_host.clone(),
                port: 6400,
                http_host: http_host.clone(),
                http_port,
                version,
                region_id,
                availability_zone_id: availability_zone_id,
                disk_size: 0,
                instance_type: "".to_string(),
                register_reason: "Storage Controller Virtual Pageserver".to_string(),
                is_storage_controller: true,
            };

            let response = storcon_client
                .dispatch::<ServiceRegisterRequest, ServiceRegisterResponse>(
                    Method::POST,
                    "management/api/v2/pageservers".to_string(),
                    Some(req),
                )
                .await?;
            eprintln!(
                "Registered {} as id={} node_id={}",
                http_host, response.id, response.node_id
            );
        }
        Command::TenantOnboard {
            tenant_id,
            target_pageserver_id,
        } => {
            #[derive(Serialize, Deserialize)]
            struct PageserverTenantMigrateRequest {
                tenant_id: String,
                target_pageserver_id: NodeId,
                skip_check_availability: bool,
            }

            #[derive(Serialize, Deserialize)]
            struct Operation {
                id: String,
                action: String,
                // Incomplete version of `Operation` in management-v2.yaml
            }

            #[derive(Serialize, Deserialize)]
            struct OperationResponse {
                operations: Vec<Operation>,
            }

            let req = PageserverTenantMigrateRequest {
                tenant_id: tenant_id.to_string(),
                target_pageserver_id,
                skip_check_availability: true,
            };

            let response = storcon_client
                .dispatch::<PageserverTenantMigrateRequest, OperationResponse>(
                    Method::POST,
                    "management/api/v2/pageservers/migrate_tenant".to_string(),
                    Some(req),
                )
                .await?;
            println!("{}", serde_json::to_string(&response).unwrap());
        }
        Command::TenantShardSplit {
            tenant_id,
            shard_count,
            stripe_size,
        } => {
            let req = TenantShardSplitRequest {
                new_shard_count: shard_count,
                new_stripe_size: stripe_size.map(ShardStripeSize),
            };

            let response = storcon_client
                .dispatch::<TenantShardSplitRequest, TenantShardSplitResponse>(
                    Method::PUT,
                    format!("control/v1/tenant/{tenant_id}/shard_split"),
                    Some(req),
                )
                .await?;
            println!(
                "Split tenant {} into {} shards: {}",
                tenant_id,
                shard_count,
                response
                    .new_shards
                    .iter()
                    .map(|s| format!("{:?}", s))
                    .collect::<Vec<_>>()
                    .join(",")
            );
        }
        Command::TenantShardMigrate {
            tenant_shard_id,
            node,
        } => {
            let req = TenantShardMigrateRequest {
                tenant_shard_id,
                node_id: node,
            };

            storcon_client
                .dispatch::<TenantShardMigrateRequest, TenantShardMigrateResponse>(
                    Method::PUT,
                    format!("control/v1/tenant/{tenant_shard_id}/migrate"),
                    Some(req),
                )
                .await?;
        }
        Command::TenantConfig { tenant_id, config } => {
            let tenant_conf = serde_json::from_str(&config)?;

            vps_client
                .location_config(
                    TenantShardId::unsharded(tenant_id),
                    LocationConfig {
                        mode: pageserver_api::models::LocationConfigMode::AttachedSingle,
                        generation: None,
                        secondary_conf: None,
                        shard_number: 0,
                        shard_count: 0,
                        shard_stripe_size: 0,
                        tenant_conf,
                    },
                    None,
                    false,
                )
                .await?;
        }
        Command::TenantScatter { tenant_id } => {
            // Find the shards
            let locate_response = storcon_client
                .dispatch::<(), TenantLocateResponse>(
                    Method::GET,
                    format!("control/v1/tenant/{tenant_id}/locate"),
                    None,
                )
                .await?;
            let shards = locate_response.shards;

            let mut node_to_shards: HashMap<NodeId, Vec<TenantShardId>> = HashMap::new();
            let shard_count = shards.len();
            for s in shards {
                let entry = node_to_shards.entry(s.node_id).or_default();
                entry.push(s.shard_id);
            }

            // Load list of available nodes
            let nodes_resp = storcon_client
                .dispatch::<(), Vec<Node>>(Method::GET, "control/v1/node".to_string(), None)
                .await?;

            for node in nodes_resp {
                if node.availability == NodeAvailability::Active {
                    node_to_shards.entry(node.id).or_default();
                }
            }

            let max_shard_per_node = shard_count / node_to_shards.len();

            loop {
                let mut migrate_shard = None;
                for (_node_id, shards) in &mut node_to_shards {
                    if shards.len() > max_shard_per_node {
                        // Pick the emptiest
                        migrate_shard = Some(shards.pop().unwrap());
                    }
                }
                let Some(migrate_shard) = migrate_shard else {
                    break;
                };

                // Pick the emptiest node to migrate to
                let mut destinations = node_to_shards
                    .iter()
                    .map(|(k, v)| (k, v.len()))
                    .collect::<Vec<_>>();
                destinations.sort_by_key(|i| i.1);
                let (destination_node, destination_count) = *destinations.iter().next().unwrap();
                if destination_count + 1 > max_shard_per_node {
                    // Even the emptiest destination doesn't have space: we're done
                    break;
                }
                let destination_node = *destination_node;

                node_to_shards
                    .get_mut(&destination_node)
                    .unwrap()
                    .push(migrate_shard);

                println!("Migrate {} -> {} ...", migrate_shard, destination_node);

                storcon_client
                    .dispatch::<TenantShardMigrateRequest, TenantShardMigrateResponse>(
                        Method::PUT,
                        format!("control/v1/tenant/{migrate_shard}/migrate"),
                        Some(TenantShardMigrateRequest {
                            tenant_shard_id: migrate_shard,
                            node_id: destination_node,
                        }),
                    )
                    .await?;
                println!("Migrate {} -> {} OK", migrate_shard, destination_node);
            }

            // Spread the shards across the nodes
        }
    }

    Ok(())
}

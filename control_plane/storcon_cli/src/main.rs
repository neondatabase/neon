use futures::StreamExt;
use std::{str::FromStr, time::Duration};

use clap::{Parser, Subcommand};
use pageserver_api::{
    controller_api::{
        NodeAvailabilityWrapper, NodeDescribeResponse, ShardSchedulingPolicy, TenantCreateRequest,
        TenantDescribeResponse, TenantPolicyRequest,
    },
    models::{
        EvictionPolicy, EvictionPolicyLayerAccessThreshold, LocationConfigSecondary,
        ShardParameters, TenantConfig, TenantConfigRequest, TenantShardSplitRequest,
        TenantShardSplitResponse,
    },
    shard::{ShardStripeSize, TenantShardId},
};
use pageserver_client::mgmt_api::{self, ResponseErrorMessageExt};
use reqwest::{Method, StatusCode, Url};
use serde::{de::DeserializeOwned, Serialize};
use utils::id::{NodeId, TenantId};

use pageserver_api::controller_api::{
    NodeConfigureRequest, NodeRegisterRequest, NodeSchedulingPolicy, PlacementPolicy,
    TenantShardMigrateRequest, TenantShardMigrateResponse,
};

#[derive(Subcommand, Debug)]
enum Command {
    /// Register a pageserver with the storage controller.  This shouldn't usually be necessary,
    /// since pageservers auto-register when they start up
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

    /// Modify a node's configuration in the storage controller
    NodeConfigure {
        #[arg(long)]
        node_id: NodeId,

        /// Availability is usually auto-detected based on heartbeats.  Set 'offline' here to
        /// manually mark a node offline
        #[arg(long)]
        availability: Option<NodeAvailabilityArg>,
        /// Scheduling policy controls whether tenant shards may be scheduled onto this node.
        #[arg(long)]
        scheduling: Option<NodeSchedulingPolicy>,
    },
    NodeDelete {
        #[arg(long)]
        node_id: NodeId,
    },
    /// Modify a tenant's policies in the storage controller
    TenantPolicy {
        #[arg(long)]
        tenant_id: TenantId,
        /// Placement policy controls whether a tenant is `detached`, has only a secondary location (`secondary`),
        /// or is in the normal attached state with N secondary locations (`attached:N`)
        #[arg(long)]
        placement: Option<PlacementPolicyArg>,
        /// Scheduling policy enables pausing the controller's scheduling activity involving this tenant.  `active` is normal,
        /// `essential` disables optimization scheduling changes, `pause` disables all scheduling changes, and `stop` prevents
        /// all reconciliation activity including for scheduling changes already made.  `pause` and `stop` can make a tenant
        /// unavailable, and are only for use in emergencies.
        #[arg(long)]
        scheduling: Option<ShardSchedulingPolicyArg>,
    },
    /// List nodes known to the storage controller
    Nodes {},
    /// List tenants known to the storage controller
    Tenants {},
    /// Create a new tenant in the storage controller, and by extension on pageservers.
    TenantCreate {
        #[arg(long)]
        tenant_id: TenantId,
    },
    /// Delete a tenant in the storage controller, and by extension on pageservers.
    TenantDelete {
        #[arg(long)]
        tenant_id: TenantId,
    },
    /// Split an existing tenant into a higher number of shards than its current shard count.
    TenantShardSplit {
        #[arg(long)]
        tenant_id: TenantId,
        #[arg(long)]
        shard_count: u8,
        /// Optional, in 8kiB pages.  e.g. set 2048 for 16MB stripes.
        #[arg(long)]
        stripe_size: Option<u32>,
    },
    /// Migrate the attached location for a tenant shard to a specific pageserver.
    TenantShardMigrate {
        #[arg(long)]
        tenant_shard_id: TenantShardId,
        #[arg(long)]
        node: NodeId,
    },
    /// Modify the pageserver tenant configuration of a tenant: this is the configuration structure
    /// that is passed through to pageservers, and does not affect storage controller behavior.
    TenantConfig {
        #[arg(long)]
        tenant_id: TenantId,
        #[arg(long)]
        config: String,
    },
    /// Print details about a particular tenant, including all its shards' states.
    TenantDescribe {
        #[arg(long)]
        tenant_id: TenantId,
    },
    /// For a tenant which hasn't been onboarded to the storage controller yet, add it in secondary
    /// mode so that it can warm up content on a pageserver.
    TenantWarmup {
        #[arg(long)]
        tenant_id: TenantId,
    },
    /// Uncleanly drop a tenant from the storage controller: this doesn't delete anything from pageservers. Appropriate
    /// if you e.g. used `tenant-warmup` by mistake on a tenant ID that doesn't really exist, or is in some other region.
    TenantDrop {
        #[arg(long)]
        tenant_id: TenantId,
        #[arg(long)]
        unclean: bool,
    },
    NodeDrop {
        #[arg(long)]
        node_id: NodeId,
        #[arg(long)]
        unclean: bool,
    },
    TenantSetTimeBasedEviction {
        #[arg(long)]
        tenant_id: TenantId,
        #[arg(long)]
        period: humantime::Duration,
        #[arg(long)]
        threshold: humantime::Duration,
    },
    // Drain a set of specified pageservers by moving the primary attachments to pageservers
    // outside of the specified set.
    Drain {
        // Set of pageserver node ids to drain.
        #[arg(long)]
        nodes: Vec<NodeId>,
        // Optional: migration concurrency (default is 8)
        #[arg(long)]
        concurrency: Option<usize>,
        // Optional: maximum number of shards to migrate
        #[arg(long)]
        max_shards: Option<usize>,
        // Optional: when set to true, nothing is migrated, but the plan is printed to stdout
        #[arg(long)]
        dry_run: Option<bool>,
    },
}

#[derive(Parser)]
#[command(
    author,
    version,
    about,
    long_about = "CLI for Storage Controller Support/Debug"
)]
#[command(arg_required_else_help(true))]
struct Cli {
    #[arg(long)]
    /// URL to storage controller.  e.g. http://127.0.0.1:1234 when using `neon_local`
    api: Url,

    #[arg(long)]
    /// JWT token for authenticating with storage controller.  Depending on the API used, this
    /// should have either `pageserverapi` or `admin` scopes: for convenience, you should mint
    /// a token with both scopes to use with this tool.
    jwt: Option<String>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Clone)]
struct PlacementPolicyArg(PlacementPolicy);

impl FromStr for PlacementPolicyArg {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "detached" => Ok(Self(PlacementPolicy::Detached)),
            "secondary" => Ok(Self(PlacementPolicy::Secondary)),
            _ if s.starts_with("attached:") => {
                let mut splitter = s.split(':');
                let _prefix = splitter.next().unwrap();
                match splitter.next().and_then(|s| s.parse::<usize>().ok()) {
                    Some(n) => Ok(Self(PlacementPolicy::Attached(n))),
                    None => Err(anyhow::anyhow!(
                        "Invalid format '{s}', a valid example is 'attached:1'"
                    )),
                }
            }
            _ => Err(anyhow::anyhow!(
                "Unknown placement policy '{s}', try detached,secondary,attached:<n>"
            )),
        }
    }
}

#[derive(Debug, Clone)]
struct ShardSchedulingPolicyArg(ShardSchedulingPolicy);

impl FromStr for ShardSchedulingPolicyArg {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "active" => Ok(Self(ShardSchedulingPolicy::Active)),
            "essential" => Ok(Self(ShardSchedulingPolicy::Essential)),
            "pause" => Ok(Self(ShardSchedulingPolicy::Pause)),
            "stop" => Ok(Self(ShardSchedulingPolicy::Stop)),
            _ => Err(anyhow::anyhow!(
                "Unknown scheduling policy '{s}', try active,essential,pause,stop"
            )),
        }
    }
}

#[derive(Debug, Clone)]
struct NodeAvailabilityArg(NodeAvailabilityWrapper);

impl FromStr for NodeAvailabilityArg {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "active" => Ok(Self(NodeAvailabilityWrapper::Active)),
            "offline" => Ok(Self(NodeAvailabilityWrapper::Offline)),
            _ => Err(anyhow::anyhow!("Unknown availability state '{s}'")),
        }
    }
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

    /// Simple HTTP request wrapper for calling into storage controller
    async fn dispatch<RQ, RS>(
        &self,
        method: Method,
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

        response
            .json()
            .await
            .map_err(pageserver_client::mgmt_api::Error::ReceiveBody)
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    let storcon_client = Client::new(cli.api.clone(), cli.jwt.clone());

    let mut trimmed = cli.api.to_string();
    trimmed.pop();
    let vps_client = mgmt_api::Client::new(trimmed, cli.jwt.as_deref());

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
            storcon_client
                .dispatch(
                    Method::POST,
                    "v1/tenant".to_string(),
                    Some(TenantCreateRequest {
                        new_tenant_id: TenantShardId::unsharded(tenant_id),
                        generation: None,
                        shard_parameters: ShardParameters::default(),
                        placement_policy: Some(PlacementPolicy::Attached(1)),
                        config: TenantConfig::default(),
                    }),
                )
                .await?;
        }
        Command::TenantDelete { tenant_id } => {
            let status = vps_client
                .tenant_delete(TenantShardId::unsharded(tenant_id))
                .await?;
            tracing::info!("Delete status: {}", status);
        }
        Command::Nodes {} => {
            let mut resp = storcon_client
                .dispatch::<(), Vec<NodeDescribeResponse>>(
                    Method::GET,
                    "control/v1/node".to_string(),
                    None,
                )
                .await?;

            resp.sort_by(|a, b| a.listen_http_addr.cmp(&b.listen_http_addr));

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
                availability: availability.map(|a| a.0),
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
        Command::Tenants {} => {
            let mut resp = storcon_client
                .dispatch::<(), Vec<TenantDescribeResponse>>(
                    Method::GET,
                    "control/v1/tenant".to_string(),
                    None,
                )
                .await?;

            resp.sort_by(|a, b| a.tenant_id.cmp(&b.tenant_id));

            let mut table = comfy_table::Table::new();
            table.set_header([
                "TenantId",
                "ShardCount",
                "StripeSize",
                "Placement",
                "Scheduling",
            ]);
            for tenant in resp {
                let shard_zero = tenant.shards.into_iter().next().unwrap();
                table.add_row([
                    format!("{}", tenant.tenant_id),
                    format!("{}", shard_zero.tenant_shard_id.shard_count.literal()),
                    format!("{:?}", tenant.stripe_size),
                    format!("{:?}", tenant.policy),
                    format!("{:?}", shard_zero.scheduling_policy),
                ]);
            }

            println!("{table}");
        }
        Command::TenantPolicy {
            tenant_id,
            placement,
            scheduling,
        } => {
            let req = TenantPolicyRequest {
                scheduling: scheduling.map(|s| s.0),
                placement: placement.map(|p| p.0),
            };
            storcon_client
                .dispatch::<_, ()>(
                    Method::PUT,
                    format!("control/v1/tenant/{tenant_id}/policy"),
                    Some(req),
                )
                .await?;
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
                .tenant_config(&TenantConfigRequest {
                    tenant_id,
                    config: tenant_conf,
                })
                .await?;
        }
        Command::TenantDescribe { tenant_id } => {
            let describe_response = storcon_client
                .dispatch::<(), TenantDescribeResponse>(
                    Method::GET,
                    format!("control/v1/tenant/{tenant_id}"),
                    None,
                )
                .await?;
            let shards = describe_response.shards;
            let mut table = comfy_table::Table::new();
            table.set_header(["Shard", "Attached", "Secondary", "Last error", "status"]);
            for shard in shards {
                let secondary = shard
                    .node_secondary
                    .iter()
                    .map(|n| format!("{}", n))
                    .collect::<Vec<_>>()
                    .join(",");

                let mut status_parts = Vec::new();
                if shard.is_reconciling {
                    status_parts.push("reconciling");
                }

                if shard.is_pending_compute_notification {
                    status_parts.push("pending_compute");
                }

                if shard.is_splitting {
                    status_parts.push("splitting");
                }
                let status = status_parts.join(",");

                table.add_row([
                    format!("{}", shard.tenant_shard_id),
                    shard
                        .node_attached
                        .map(|n| format!("{}", n))
                        .unwrap_or(String::new()),
                    secondary,
                    shard.last_error,
                    status,
                ]);
            }
            println!("{table}");
        }
        Command::TenantWarmup { tenant_id } => {
            let describe_response = storcon_client
                .dispatch::<(), TenantDescribeResponse>(
                    Method::GET,
                    format!("control/v1/tenant/{tenant_id}"),
                    None,
                )
                .await;
            match describe_response {
                Ok(describe) => {
                    if matches!(describe.policy, PlacementPolicy::Secondary) {
                        // Fine: it's already known to controller in secondary mode: calling
                        // again to put it into secondary mode won't cause problems.
                    } else {
                        anyhow::bail!("Tenant already present with policy {:?}", describe.policy);
                    }
                }
                Err(mgmt_api::Error::ApiError(StatusCode::NOT_FOUND, _)) => {
                    // Fine: this tenant isn't know to the storage controller yet.
                }
                Err(e) => {
                    // Unexpected API error
                    return Err(e.into());
                }
            }

            vps_client
                .location_config(
                    TenantShardId::unsharded(tenant_id),
                    pageserver_api::models::LocationConfig {
                        mode: pageserver_api::models::LocationConfigMode::Secondary,
                        generation: None,
                        secondary_conf: Some(LocationConfigSecondary { warm: true }),
                        shard_number: 0,
                        shard_count: 0,
                        shard_stripe_size: ShardParameters::DEFAULT_STRIPE_SIZE.0,
                        tenant_conf: TenantConfig::default(),
                    },
                    None,
                    true,
                )
                .await?;

            let describe_response = storcon_client
                .dispatch::<(), TenantDescribeResponse>(
                    Method::GET,
                    format!("control/v1/tenant/{tenant_id}"),
                    None,
                )
                .await?;

            let secondary_ps_id = describe_response
                .shards
                .first()
                .unwrap()
                .node_secondary
                .first()
                .unwrap();

            println!("Tenant {tenant_id} warming up on pageserver {secondary_ps_id}");
            loop {
                let (status, progress) = vps_client
                    .tenant_secondary_download(
                        TenantShardId::unsharded(tenant_id),
                        Some(Duration::from_secs(10)),
                    )
                    .await?;
                println!(
                    "Progress: {}/{} layers, {}/{} bytes",
                    progress.layers_downloaded,
                    progress.layers_total,
                    progress.bytes_downloaded,
                    progress.bytes_total
                );
                match status {
                    StatusCode::OK => {
                        println!("Download complete");
                        break;
                    }
                    StatusCode::ACCEPTED => {
                        // Loop
                    }
                    _ => {
                        anyhow::bail!("Unexpected download status: {status}");
                    }
                }
            }
        }
        Command::TenantDrop { tenant_id, unclean } => {
            if !unclean {
                anyhow::bail!("This command is not a tenant deletion, and uncleanly drops all controller state for the tenant.  If you know what you're doing, add `--unclean` to proceed.")
            }
            storcon_client
                .dispatch::<(), ()>(
                    Method::POST,
                    format!("debug/v1/tenant/{tenant_id}/drop"),
                    None,
                )
                .await?;
        }
        Command::NodeDrop { node_id, unclean } => {
            if !unclean {
                anyhow::bail!("This command is not a clean node decommission, and uncleanly drops all controller state for the node, without checking if any tenants still refer to it.  If you know what you're doing, add `--unclean` to proceed.")
            }
            storcon_client
                .dispatch::<(), ()>(Method::POST, format!("debug/v1/node/{node_id}/drop"), None)
                .await?;
        }
        Command::NodeDelete { node_id } => {
            storcon_client
                .dispatch::<(), ()>(Method::DELETE, format!("control/v1/node/{node_id}"), None)
                .await?;
        }
        Command::TenantSetTimeBasedEviction {
            tenant_id,
            period,
            threshold,
        } => {
            vps_client
                .tenant_config(&TenantConfigRequest {
                    tenant_id,
                    config: TenantConfig {
                        eviction_policy: Some(EvictionPolicy::LayerAccessThreshold(
                            EvictionPolicyLayerAccessThreshold {
                                period: period.into(),
                                threshold: threshold.into(),
                            },
                        )),
                        ..Default::default()
                    },
                })
                .await?;
        }
        Command::Drain {
            nodes,
            concurrency,
            max_shards,
            dry_run,
        } => {
            // Load the list of nodes, split them up into the drained and filled sets,
            // and validate that draining is possible.
            let node_descs = storcon_client
                .dispatch::<(), Vec<NodeDescribeResponse>>(
                    Method::GET,
                    "control/v1/node".to_string(),
                    None,
                )
                .await?;

            let mut node_to_drain_descs = Vec::new();
            let mut node_to_fill_descs = Vec::new();

            for desc in node_descs {
                let to_drain = nodes.iter().any(|id| *id == desc.id);
                if to_drain {
                    node_to_drain_descs.push(desc);
                } else {
                    node_to_fill_descs.push(desc);
                }
            }

            if nodes.len() != node_to_drain_descs.len() {
                anyhow::bail!("Drain requested for node which doesn't exist.")
            }

            node_to_fill_descs.retain(|desc| {
                matches!(desc.availability, NodeAvailabilityWrapper::Active)
                    && matches!(
                        desc.scheduling,
                        NodeSchedulingPolicy::Active | NodeSchedulingPolicy::Filling
                    )
            });

            if node_to_fill_descs.is_empty() {
                anyhow::bail!("There are no nodes to drain to")
            }

            // Set the node scheduling policy to draining for the nodes which
            // we plan to drain.
            for node_desc in node_to_drain_descs.iter() {
                let req = NodeConfigureRequest {
                    node_id: node_desc.id,
                    availability: None,
                    scheduling: Some(NodeSchedulingPolicy::Draining),
                };

                storcon_client
                    .dispatch::<_, ()>(
                        Method::PUT,
                        format!("control/v1/node/{}/config", node_desc.id),
                        Some(req),
                    )
                    .await?;
            }

            // Perform the drain: move each tenant shard scheduled on a node to
            // be drained to a node which is being filled. A simple round robin
            // strategy is used to pick the new node.
            let tenants = storcon_client
                .dispatch::<(), Vec<TenantDescribeResponse>>(
                    Method::GET,
                    "control/v1/tenant".to_string(),
                    None,
                )
                .await?;

            let mut selected_node_idx = 0;

            struct DrainMove {
                tenant_shard_id: TenantShardId,
                from: NodeId,
                to: NodeId,
            }

            let mut moves: Vec<DrainMove> = Vec::new();

            let shards = tenants
                .into_iter()
                .flat_map(|tenant| tenant.shards.into_iter());
            for shard in shards {
                if let Some(max_shards) = max_shards {
                    if moves.len() >= max_shards {
                        println!(
                            "Stop planning shard moves since the requested maximum was reached"
                        );
                        break;
                    }
                }

                let should_migrate = {
                    if let Some(attached_to) = shard.node_attached {
                        node_to_drain_descs
                            .iter()
                            .map(|desc| desc.id)
                            .any(|id| id == attached_to)
                    } else {
                        false
                    }
                };

                if !should_migrate {
                    continue;
                }

                moves.push(DrainMove {
                    tenant_shard_id: shard.tenant_shard_id,
                    from: shard
                        .node_attached
                        .expect("We only migrate attached tenant shards"),
                    to: node_to_fill_descs[selected_node_idx].id,
                });
                selected_node_idx = (selected_node_idx + 1) % node_to_fill_descs.len();
            }

            let total_moves = moves.len();

            if dry_run == Some(true) {
                println!("Dryrun requested. Planned {total_moves} moves:");
                for mv in &moves {
                    println!("{}: {} -> {}", mv.tenant_shard_id, mv.from, mv.to)
                }

                return Ok(());
            }

            const DEFAULT_MIGRATE_CONCURRENCY: usize = 8;
            let mut stream = futures::stream::iter(moves)
                .map(|mv| {
                    let client = Client::new(cli.api.clone(), cli.jwt.clone());
                    async move {
                        client
                            .dispatch::<TenantShardMigrateRequest, TenantShardMigrateResponse>(
                                Method::PUT,
                                format!("control/v1/tenant/{}/migrate", mv.tenant_shard_id),
                                Some(TenantShardMigrateRequest {
                                    tenant_shard_id: mv.tenant_shard_id,
                                    node_id: mv.to,
                                }),
                            )
                            .await
                            .map_err(|e| (mv.tenant_shard_id, mv.from, mv.to, e))
                    }
                })
                .buffered(concurrency.unwrap_or(DEFAULT_MIGRATE_CONCURRENCY));

            let mut success = 0;
            let mut failure = 0;

            while let Some(res) = stream.next().await {
                match res {
                    Ok(_) => {
                        success += 1;
                    }
                    Err((tenant_shard_id, from, to, error)) => {
                        failure += 1;
                        println!(
                            "Failed to migrate {} from node {} to node {}: {}",
                            tenant_shard_id, from, to, error
                        );
                    }
                }

                if (success + failure) % 20 == 0 {
                    println!(
                        "Processed {}/{} shards: {} succeeded, {} failed",
                        success + failure,
                        total_moves,
                        success,
                        failure
                    );
                }
            }

            println!(
                "Processed {}/{} shards: {} succeeded, {} failed",
                success + failure,
                total_moves,
                success,
                failure
            );
        }
    }

    Ok(())
}

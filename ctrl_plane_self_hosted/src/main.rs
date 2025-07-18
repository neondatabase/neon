use anyhow::Context;
use axum::{Json, Router, extract::State, routing::put};
use pageserver_api::shard::ShardStripeSize;
use serde::{Deserialize, Serialize};
use sqlx::Row;
use sqlx::postgres::PgPool;
use std::collections::HashSet;
use tokio::net::TcpListener;
use tracing::info;
use tracing_subscriber;
use utils::id::{NodeId, TenantId, TimelineId};
use utils::shard::ShardNumber;

//tracing_subscriber::fmt::fmt().with_env_filter(EnvFilter::from_default_env()).init();

#[derive(Clone, Debug)]
struct DbPoolsState {
    compute: PgPool,
    storcon: PgPool,
}

impl DbPoolsState {
    async fn new() -> anyhow::Result<Self> {
        Ok(DbPoolsState {
            compute: sqlx::postgres::PgPoolOptions::new()
                .max_connections(5)
                .connect(&dotenvy::var("compute_dsn").context("compute_dsn must be set")?)
                .await
                .context("failed to connect to compute_dsn")?,
            storcon: sqlx::postgres::PgPoolOptions::new()
                .max_connections(5)
                .connect(&dotenvy::var("storcon_dsn").context("storcon_dsn must be set")?)
                .await
                .context("failed to connect to storcon_dsn")?,
        })
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let subscriber = tracing_subscriber::FmtSubscriber::new();
    tracing::subscriber::set_global_default(subscriber)?;

    let state = DbPoolsState::new().await?;
    let app = Router::new()
        .route("/notify-attach", put(notify_attach_handler))
        .route("/notify-safekeepers", put(notify_safekeepers_handler))
        .with_state(state);

    // run our app with hyper
    let listener = TcpListener::bind("0.0.0.0:3000").await.unwrap();
    info!("listening on {}", listener.local_addr().unwrap());
    axum::serve(listener, app).await.unwrap();

    Ok(())
}

#[derive(Serialize, Deserialize, Debug)]
struct ComputeHookNotifyRequestShard {
    // NodeId(pub u64)
    node_id: NodeId,
    // ShardNumber(pub u8)
    shard_number: ShardNumber,
}

#[derive(Serialize, Deserialize, Debug)]
struct ComputeHookNotifyRequest {
    // TenantId(Id([u8; 16]))
    tenant_id: TenantId,
    preferred_az: Option<String>,
    // Some(ShardStripeSize(pub u32))
    stripe_size: Option<ShardStripeSize>,
    shards: Vec<ComputeHookNotifyRequestShard>,
}

async fn update_pg_parameter(
    pool_conn: &PgPool,
    query: &str,
) -> anyhow::Result<(), http::StatusCode> {
    sqlx::query(query)
        .execute(pool_conn)
        .await
        .map_err(|_: sqlx::Error| {
            eprintln!("compute config update");
            http::StatusCode::INTERNAL_SERVER_ERROR
        })?;

    // SIGHUP
    sqlx::query("select pg_reload_conf()")
        .execute(pool_conn)
        .await
        .map_err(|_: sqlx::Error| {
            eprintln!("compute config update");
            http::StatusCode::INTERNAL_SERVER_ERROR
        })?;
    Ok(())
}

async fn notify_attach_handler(
    State(pooler): State<DbPoolsState>,
    Json(payload): Json<ComputeHookNotifyRequest>,
) -> anyhow::Result<String, http::StatusCode> {
    info!(
        "Received notify-attach request with params: {:?}, {:?}, {:?}",
        payload.tenant_id, payload.stripe_size, payload.shards,
    );

    tracing::debug!("{:?}", payload);

    let node_ids: Vec<i32> = payload
        .shards
        .into_iter()
        .map(|s| s.node_id.0 as i32)
        .collect::<HashSet<i32>>()
        .into_iter()
        .collect::<Vec<i32>>();

    let sql = "select 'host=' || listen_pg_addr || ' ' || 'port=' || listen_pg_port as addr from nodes where node_id =  ANY($1)";
    let connstring: Vec<String> = sqlx::query(sql)
        .bind(&node_ids)
        .fetch_all(&pooler.storcon)
        .await
        .map_err(|_| http::StatusCode::INTERNAL_SERVER_ERROR)?
        .iter()
        .map(|r| r.get("addr"))
        .collect();

    tracing::debug!("our connstring: {:?}", connstring);
    tracing::debug!("our node ids: {:?}", node_ids);

    // TODO probably moot
    if node_ids.len() != connstring.len() {
        eprintln!(
            "not all node_ids {:?} in storage_controller database",
            node_ids
        );
        return Err(http::StatusCode::INTERNAL_SERVER_ERROR);
    }

    // set neon.pageserver_connstring to a comma-separated list of PG conn strings to pageservers according to the shards list
    // -- the shards identified by NodeId must be converted to the address+port of the node.
    // -- if stripe_size is not None, set neon.shard_stripe_size to this value
    // https://github.com/neondatabase/neon/blob/3b7cc4234c8675b777a3f85798734c0b41748d11/pgxn/neon/libpagestore.c#L96C3-L111C51
    let connstring = connstring.join(",");
    let query = format!(
        "alter system set neon.pageserver_connstring = '{}'",
        connstring
    );
    update_pg_parameter(&pooler.compute, &query).await?;

    info!("updating pageserver_connstring: {}", connstring);

    if let Some(stripe_size) = payload.stripe_size {
        let query = format!("alter system set neon.stripe_size = '{}'", stripe_size);
        update_pg_parameter(&pooler.compute, &query).await?;
        info!("updating stripe_size: {}", stripe_size);
    }
    Ok("pageserver attach success".to_string())
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SafekeeperInfo {
    pub id: NodeId,
    pub hostname: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SafekeepersNotifyRequest {
    pub tenant_id: TenantId,
    pub timeline_id: TimelineId,
    pub generation: u32,
    pub safekeepers: Vec<SafekeeperInfo>,
}

async fn notify_safekeepers_handler(
    State(pooler): State<DbPoolsState>,
    Json(payload): Json<SafekeepersNotifyRequest>,
) -> anyhow::Result<String, http::StatusCode> {
    info!(
        "Received notify-safekeepers request: {:?}, {:?}, {:?}, {:?}",
        payload.tenant_id, payload.timeline_id, payload.generation, payload.safekeepers,
    );

    tracing::debug!("{:?}", payload);

    // It's my understanding that a SafeKeeper deployment is 1:1 with a POST to
    // /control/v1/safekeepers/{id} with a payload that will write a `host` and `port` to
    // `storage_controller` database. So, this is fine to expect. My money is on `id: NodeID` is
    // `--id` passed to `safekeeper` binary.
    // https://github.com/neondatabase/neon/blob/045ae13e060c3717c921097444d5c6b09925e87c/storage_controller/src/http.rs#L1509C1-L1513C96
    let node_ids: Vec<i32> = payload.safekeepers.iter().map(|x| x.id.0 as i32).collect();

    let sql = "select host || ':' || port as addr from safekeepers where node_id =  ANY($1)";
    let safekeepers: Vec<String> = sqlx::query(sql)
        .bind(&node_ids)
        .fetch_all(&pooler.storcon)
        .await
        .map_err(|_| http::StatusCode::INTERNAL_SERVER_ERROR)?
        .iter()
        .map(|r| r.get("addr"))
        .collect::<Vec<String>>();

    // TODO probably moot
    if node_ids.len() != safekeepers.len() {
        eprintln!(
            "not all node_ids {:?} in storage_controller database",
            node_ids
        );
        return Err(http::StatusCode::INTERNAL_SERVER_ERROR);
    }

    let safekeepers = safekeepers.join(",");
    info!("updating safekeepers: {}", safekeepers);

    // set neon.safekeeper_connstrings to an array of postgres connection strings to safekeepers according to the safekeepers list.
    // -- the safekeepers identified by NodeId must be converted to the address+port of the respective safekeeper.
    // -- the hostname is provided for debugging purposes, so we reserve changes to how we pass it.
    let query = format!("alter system set neon.safekeepers = '{}'", safekeepers);
    update_pg_parameter(&pooler.compute, &query).await?;

    // TODO? https://github.com/neondatabase/neon/blob/045ae13e060c3717c921097444d5c6b09925e87c/docs/storage_controller.md#notify-safekeepers-body
    // -- "set neon.safekeepers_generation to the provided generation value."

    Ok("safekeeper notify success".to_string())
}

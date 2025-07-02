// @generated automatically by Diesel CLI.

pub mod sql_types {
    #[derive(diesel::query_builder::QueryId, diesel::sql_types::SqlType)]
    #[diesel(postgres_type(name = "pg_lsn", schema = "pg_catalog"))]
    pub struct PgLsn;
}

diesel::table! {
    controllers (address, started_at) {
        address -> Varchar,
        started_at -> Timestamptz,
    }
}

diesel::table! {
    metadata_health (tenant_id, shard_number, shard_count) {
        tenant_id -> Varchar,
        shard_number -> Int4,
        shard_count -> Int4,
        healthy -> Bool,
        last_scrubbed_at -> Timestamptz,
    }
}

diesel::table! {
    nodes (node_id) {
        node_id -> Int8,
        scheduling_policy -> Varchar,
        listen_http_addr -> Varchar,
        listen_http_port -> Int4,
        listen_pg_addr -> Varchar,
        listen_pg_port -> Int4,
        availability_zone_id -> Varchar,
        listen_https_port -> Nullable<Int4>,
        lifecycle -> Varchar,
        listen_grpc_addr -> Nullable<Varchar>,
        listen_grpc_port -> Nullable<Int4>,
    }
}

diesel::table! {
    safekeeper_timeline_pending_ops (tenant_id, timeline_id, sk_id) {
        sk_id -> Int8,
        tenant_id -> Varchar,
        timeline_id -> Varchar,
        generation -> Int4,
        op_kind -> Varchar,
    }
}

diesel::table! {
    safekeepers (id) {
        id -> Int8,
        region_id -> Text,
        version -> Int8,
        host -> Text,
        port -> Int4,
        http_port -> Int4,
        availability_zone_id -> Text,
        scheduling_policy -> Varchar,
        https_port -> Nullable<Int4>,
    }
}

diesel::table! {
    tenant_shards (tenant_id, shard_number, shard_count) {
        tenant_id -> Varchar,
        shard_number -> Int4,
        shard_count -> Int4,
        shard_stripe_size -> Int4,
        generation -> Nullable<Int4>,
        generation_pageserver -> Nullable<Int8>,
        placement_policy -> Varchar,
        splitting -> Int2,
        config -> Text,
        scheduling_policy -> Varchar,
        preferred_az_id -> Nullable<Varchar>,
    }
}

diesel::table! {
    timeline_imports (tenant_id, timeline_id) {
        tenant_id -> Varchar,
        timeline_id -> Varchar,
        shard_statuses -> Jsonb,
    }
}

diesel::table! {
    use diesel::sql_types::*;
    use super::sql_types::PgLsn;

    timelines (tenant_id, timeline_id) {
        tenant_id -> Varchar,
        timeline_id -> Varchar,
        start_lsn -> PgLsn,
        generation -> Int4,
        sk_set -> Array<Nullable<Int8>>,
        new_sk_set -> Nullable<Array<Nullable<Int8>>>,
        cplane_notified_generation -> Int4,
        deleted_at -> Nullable<Timestamptz>,
    }
}

diesel::allow_tables_to_appear_in_same_query!(
    controllers,
    metadata_health,
    nodes,
    safekeeper_timeline_pending_ops,
    safekeepers,
    tenant_shards,
    timeline_imports,
    timelines,
);

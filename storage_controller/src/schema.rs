// @generated automatically by Diesel CLI.

diesel::table! {
    leader (hostname, port, started_at) {
        hostname -> Varchar,
        port -> Int4,
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
    }
}

diesel::allow_tables_to_appear_in_same_query!(leader, metadata_health, nodes, tenant_shards,);

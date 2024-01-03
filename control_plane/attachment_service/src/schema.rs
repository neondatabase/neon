// @generated automatically by Diesel CLI.

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
        generation -> Int4,
        generation_pageserver -> Int8,
        placement_policy -> Varchar,
        splitting -> Int4,
        config -> Text,
    }
}

diesel::allow_tables_to_appear_in_same_query!(nodes, tenant_shards,);
